"""Durable daily OHLCV history store for price-context calculations."""

# pylint: disable=too-many-locals

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from numbers import Integral
from pathlib import Path
import sqlite3
import threading
import weakref

import pandas as pd

from opx_chain.error_summary import compact_exception_summary
from opx_chain.paths import get_data_dir
from opx_chain.price_context import normalize_price_history_frame
from opx_chain.storage.history_snapshot import HistorySnapshotWindow, MAX_SNAPSHOT_ROWS
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import parse_iso_datetime, utc_now
from opx_chain.utils import finite_float_or_none


PRICE_HISTORY_SCHEMA_VERSION = 1
PRICE_HISTORY_SCHEMA_MIGRATIONS: dict[int, str] = {}
PRICE_HISTORY_TAIL_REFRESH_DAYS = 7
PRICE_HISTORY_INTEGRITY_ERROR = "ERROR"
PRICE_HISTORY_INTEGRITY_MISSING = "MISSING"
PRICE_HISTORY_INTEGRITY_OK = "OK"
_SYNC_STATUSES = frozenset({"ok", "error"})
_EMPTY_PROVIDER_RESPONSE = "provider returned no usable price history rows"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_price_bars (
    provider     TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    open         REAL,
    high         REAL NOT NULL,
    low          REAL NOT NULL,
    close        REAL NOT NULL,
    volume       REAL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (provider, ticker, trading_date)
);

CREATE TABLE IF NOT EXISTS price_history_syncs (
    provider                TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    lookback_days           INTEGER NOT NULL,
    checked_at              TEXT NOT NULL,
    status                  TEXT NOT NULL,
    requested_lookback_days INTEGER,
    latest_trading_date     TEXT,
    fetched_rows            INTEGER NOT NULL DEFAULT 0,
    stored_rows             INTEGER NOT NULL DEFAULT 0,
    error_summary           TEXT,
    PRIMARY KEY (provider, ticker, lookback_days)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_bars_ticker_date
    ON daily_price_bars(provider, ticker, trading_date DESC);
"""


def _date_to_str(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _history_db_path(config=None) -> Path:
    base = Path(config.storage_dir) if config is not None and config.storage_dir else get_data_dir()
    return base / "price-history.db"


def _non_empty_text(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a non-empty string")
    text = value.strip()
    if not text:
        raise ValueError(f"{name} must be a non-empty string")
    return text


def _optional_text(value: object, *, name: str) -> str | None:
    if value is None:
        return None
    return _non_empty_text(value, name=name)


def _normalize_provider(value: object) -> str:
    return _non_empty_text(value, name="provider").strip().lower()


def _normalize_ticker(value: object) -> str:
    text = _non_empty_text(value, name="ticker").upper().strip()
    if not is_valid_ticker(text):
        raise ValueError("ticker must be a valid stock ticker symbol")
    return text


def _positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a positive integer")
    resolved = int(value)
    if resolved <= 0:
        raise ValueError(f"{name} must be positive")
    return resolved


def _non_negative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a non-negative integer")
    resolved = int(value)
    if resolved < 0:
        raise ValueError(f"{name} must be non-negative")
    return resolved


def _date_arg(value: object, *, name: str) -> date:
    if isinstance(value, datetime):
        raise ValueError(f"{name} must be a date")
    if isinstance(value, date):
        return value
    raise ValueError(f"{name} must be a date")


def _optional_date_arg(value: object, *, name: str) -> date | None:
    if value is None:
        return None
    return _date_arg(value, name=name)


def _datetime_arg(value: object, *, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _history_identity_column(history: pd.DataFrame) -> str | None:
    """Return the first supported ticker identity column in a provider history frame."""
    normalized_columns = {
        str(column).strip().lower(): column
        for column in history.columns
    }
    for alias in ("symbol", "ticker", "underlying_symbol"):
        column = normalized_columns.get(alias)
        if column is not None:
            return column
    return None


def _filter_history_identity(history: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    """Keep only provider daily bars that match the requested ticker identity."""
    if not isinstance(history, pd.DataFrame) or history.empty:
        return history
    identity_column = _history_identity_column(history)
    if identity_column is None:
        return history
    identities = history[identity_column].astype("string").str.strip().str.upper()
    return history.loc[identities == ticker].copy()


@dataclass(frozen=True)
class PriceHistoryStats:
    """Stored daily-bar coverage metadata for one provider/ticker."""

    row_count: int
    earliest_date: date | None
    latest_date: date | None


@dataclass(frozen=True)
class PriceHistorySync:
    """Last reconciliation attempt metadata for one provider/ticker/lookback."""

    checked_at: datetime
    status: str
    requested_lookback_days: int | None
    latest_trading_date: date | None
    fetched_rows: int
    stored_rows: int
    error_summary: str | None


@dataclass(frozen=True)
class PriceHistoryReconcileResult:
    """Result of reconciling local daily bars before price-context calculation."""

    history: pd.DataFrame
    fetched: bool
    requested_lookback_days: int | None = None
    fetched_rows: int = 0
    stored_rows: int = 0
    error_summary: str | None = None


@dataclass(frozen=True)
class PriceHistoryIntegrity:
    """Read-only integrity result for one durable price-history database."""

    path: Path
    status: str
    error_summary: str | None = None

    @property
    def healthy(self) -> bool:
        """Return whether the database passed integrity and schema checks."""
        return self.status == PRICE_HISTORY_INTEGRITY_OK


class PriceHistoryStore:
    """SQLite-backed local store of immutable-ish daily OHLCV bars."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._connection: sqlite3.Connection | None = None
        self._connection_finalizer: weakref.finalize | None = None
        self._lock = threading.RLock()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def close(self) -> None:
        """Close the pooled SQLite connection."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                if self._connection_finalizer is not None:
                    self._connection_finalizer.detach()
                    self._connection_finalizer = None
                self._connection = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # pragma: no cover  # pylint: disable=broad-exception-caught
            pass

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _connection_for_use(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = self._connect()
            self._connection_finalizer = weakref.finalize(self, self._connection.close)
        return self._connection

    @contextmanager
    def _open_connection(self):
        """Yield the pooled SQLite connection and rollback failed writes."""
        with self._lock:
            conn = self._connection_for_use()
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise

    def _init_schema(self) -> None:
        with self._open_connection() as conn:
            conn.executescript(_SCHEMA_SQL)
            current_version = self._read_schema_version(conn)
            if current_version is None:
                conn.execute(
                    "INSERT INTO _schema_meta VALUES ('schema_version', ?)",
                    (str(PRICE_HISTORY_SCHEMA_VERSION),),
                )
            elif current_version > PRICE_HISTORY_SCHEMA_VERSION:
                raise RuntimeError(
                    "Price history schema version "
                    f"{current_version} is newer than supported version "
                    f"{PRICE_HISTORY_SCHEMA_VERSION}"
                )
            elif current_version < PRICE_HISTORY_SCHEMA_VERSION:
                self._migrate_schema(conn, current_version, PRICE_HISTORY_SCHEMA_VERSION)
            conn.commit()

    def _read_schema_version(self, conn: sqlite3.Connection) -> int | None:
        row = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return None
        try:
            return int(row["value"])
        except ValueError as exc:
            raise RuntimeError(
                "Price history schema version is not an integer: "
                f"{row['value']!r}"
            ) from exc

    def _migration_statements(self, migration: str) -> list[str]:
        return [statement.strip() for statement in migration.split(";") if statement.strip()]

    def _migrate_schema(
        self,
        conn: sqlite3.Connection,
        current_version: int,
        target_version: int,
    ) -> None:
        for next_version in range(current_version + 1, target_version + 1):
            migration = PRICE_HISTORY_SCHEMA_MIGRATIONS.get(next_version)
            if migration is None:
                raise RuntimeError(
                    "Price history schema migration missing: "
                    f"{current_version}->{target_version}"
                )
            for statement in self._migration_statements(migration):
                conn.execute(statement)
            conn.execute(
                "UPDATE _schema_meta SET value = ? WHERE key = 'schema_version'",
                (str(next_version),),
            )

    def snapshot_bundle_rows(self, window: HistorySnapshotWindow) -> list[dict]:
        """Read exact retained OHLCV rows known by a portable bundle cutoff."""
        placeholders = ",".join("?" for _ in window.tickers)
        with self._lock:
            rows = self._connection_for_use().execute(
                "SELECT provider,ticker,trading_date AS date,open,high,low,close,volume,fetched_at "
                "FROM daily_price_bars WHERE provider=? AND ticker IN (" + placeholders + ") "
                "AND trading_date BETWEEN ? AND ? AND julianday(fetched_at)<=julianday(?) "
                "ORDER BY ticker,trading_date LIMIT ?", (*window.arguments(), MAX_SNAPSHOT_ROWS + 1)
            ).fetchall()
        return window.retain(rows)

    def load_bars(
        self,
        *,
        provider: str,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Load stored daily bars for an inclusive date window."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        start = _date_arg(start_date, name="start_date")
        end = _date_arg(end_date, name="end_date")
        if start > end:
            raise ValueError("start_date must be on or before end_date")
        with self._lock:
            conn = self._connection_for_use()
            rows = conn.execute(
                """
                SELECT trading_date, open, high, low, close, volume
                FROM daily_price_bars
                WHERE provider = ?
                  AND ticker = ?
                  AND trading_date >= ?
                  AND trading_date <= ?
                ORDER BY trading_date
                """,
                (
                    provider_key,
                    ticker_key,
                    start.isoformat(),
                    end.isoformat(),
                ),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        return pd.DataFrame(
            {
                "date": pd.to_datetime([row["trading_date"] for row in rows], utc=True),
                "open": [row["open"] for row in rows],
                "high": [row["high"] for row in rows],
                "low": [row["low"] for row in rows],
                "close": [row["close"] for row in rows],
                "volume": [row["volume"] for row in rows],
            }
        )

    def load_recent_bars(
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
        end_date: date,
    ) -> pd.DataFrame:
        """Load the latest stored daily bars up to an inclusive end date."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        resolved_lookback = _positive_int(lookback_days, name="lookback_days")
        end = _date_arg(end_date, name="end_date")
        with self._lock:
            conn = self._connection_for_use()
            rows = conn.execute(
                """
                SELECT trading_date, open, high, low, close, volume
                FROM daily_price_bars
                WHERE provider = ?
                  AND ticker = ?
                  AND trading_date <= ?
                ORDER BY trading_date DESC
                LIMIT ?
                """,
                (provider_key, ticker_key, end.isoformat(), resolved_lookback),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        ordered = list(reversed(rows))
        return pd.DataFrame(
            {
                "date": pd.to_datetime([row["trading_date"] for row in ordered], utc=True),
                "open": [row["open"] for row in ordered],
                "high": [row["high"] for row in ordered],
                "low": [row["low"] for row in ordered],
                "close": [row["close"] for row in ordered],
                "volume": [row["volume"] for row in ordered],
            }
        )

    def stats(self, *, provider: str, ticker: str) -> PriceHistoryStats:
        """Return total coverage stats for one provider/ticker."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT COUNT(*) AS row_count,
                       MIN(trading_date) AS earliest_date,
                       MAX(trading_date) AS latest_date
                FROM daily_price_bars
                WHERE provider = ? AND ticker = ?
                """,
                (provider_key, ticker_key),
            ).fetchone()
        return PriceHistoryStats(
            row_count=int(row["row_count"] or 0),
            earliest_date=_parse_date(row["earliest_date"]),
            latest_date=_parse_date(row["latest_date"]),
        )

    def upsert_bars(
        self,
        *,
        provider: str,
        ticker: str,
        history: pd.DataFrame,
        fetched_at: datetime | None = None,
    ) -> int:
        """Normalize and upsert daily bars. Returns normalized row count."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        normalized = normalize_price_history_frame(
            _filter_history_identity(history, ticker=ticker_key)
        )
        if normalized.empty:
            return 0
        fetched = (
            _datetime_arg(fetched_at, name="fetched_at")
            if fetched_at is not None
            else utc_now()
        )
        rows = []
        for _, row in normalized.iterrows():
            high = finite_float_or_none(row["high"])
            low = finite_float_or_none(row["low"])
            close = finite_float_or_none(row["close"])
            if high is None or low is None or close is None:
                continue
            rows.append(
                (
                    provider_key,
                    ticker_key,
                    pd.Timestamp(row["date"]).date().isoformat(),
                    finite_float_or_none(row.get("open")),
                    high,
                    low,
                    close,
                    finite_float_or_none(row.get("volume")),
                    fetched.isoformat(),
                )
            )
        if not rows:
            return 0
        with self._open_connection() as conn:
            conn.executemany(
                """
                INSERT INTO daily_price_bars
                    (provider, ticker, trading_date, open, high, low, close, volume, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, ticker, trading_date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def get_sync(
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
    ) -> PriceHistorySync | None:
        """Return last sync metadata for one provider/ticker/lookback."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        resolved_lookback = _positive_int(lookback_days, name="lookback_days")
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT checked_at, status, requested_lookback_days,
                       latest_trading_date, fetched_rows, stored_rows, error_summary
                FROM price_history_syncs
                WHERE provider = ? AND ticker = ? AND lookback_days = ?
                """,
                (provider_key, ticker_key, resolved_lookback),
            ).fetchone()
        if row is None:
            return None
        return PriceHistorySync(
            checked_at=parse_iso_datetime(row["checked_at"]),
            status=str(row["status"]),
            requested_lookback_days=row["requested_lookback_days"],
            latest_trading_date=_parse_date(row["latest_trading_date"]),
            fetched_rows=int(row["fetched_rows"] or 0),
            stored_rows=int(row["stored_rows"] or 0),
            error_summary=row["error_summary"],
        )

    def record_sync(  # pylint: disable=too-many-arguments
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
        status: str,
        requested_lookback_days: int | None,
        latest_trading_date: date | None,
        fetched_rows: int,
        stored_rows: int,
        error_summary: str | None = None,
        checked_at: datetime | None = None,
    ) -> None:
        """Record the latest reconciliation attempt."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker)
        lookback = _positive_int(lookback_days, name="lookback_days")
        status_key = _non_empty_text(status, name="status").lower()
        if status_key not in _SYNC_STATUSES:
            expected = ", ".join(sorted(_SYNC_STATUSES))
            raise ValueError(f"status must be one of: {expected}")
        requested_lookback = (
            _positive_int(requested_lookback_days, name="requested_lookback_days")
            if requested_lookback_days is not None
            else None
        )
        latest = _optional_date_arg(latest_trading_date, name="latest_trading_date")
        fetched_count = _non_negative_int(fetched_rows, name="fetched_rows")
        stored_count = _non_negative_int(stored_rows, name="stored_rows")
        error_text = _optional_text(error_summary, name="error_summary")
        checked = (
            _datetime_arg(checked_at, name="checked_at")
            if checked_at is not None
            else utc_now()
        )
        with self._open_connection() as conn:
            conn.execute(
                """
                INSERT INTO price_history_syncs
                    (provider, ticker, lookback_days, checked_at, status,
                     requested_lookback_days, latest_trading_date, fetched_rows,
                     stored_rows, error_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, ticker, lookback_days) DO UPDATE SET
                    checked_at = excluded.checked_at,
                    status = excluded.status,
                    requested_lookback_days = excluded.requested_lookback_days,
                    latest_trading_date = excluded.latest_trading_date,
                    fetched_rows = excluded.fetched_rows,
                    stored_rows = excluded.stored_rows,
                    error_summary = excluded.error_summary
                """,
                (
                    provider_key,
                    ticker_key,
                    lookback,
                    checked.isoformat(),
                    status_key,
                    requested_lookback,
                    _date_to_str(latest),
                    fetched_count,
                    stored_count,
                    error_text,
                ),
            )
            conn.commit()


def get_price_history_store(config=None) -> PriceHistoryStore:
    """Return the durable local daily-bar store for price context."""
    return PriceHistoryStore(_history_db_path(config))


def get_price_history_db_path(config=None) -> Path:
    """Return the configured durable price-history database path."""
    return _history_db_path(config)


def check_price_history_integrity(db_path: Path) -> PriceHistoryIntegrity:
    """Validate a price-history database without opening it for writes."""
    path = Path(db_path)
    if not path.is_file():
        return PriceHistoryIntegrity(
            path=path,
            status=PRICE_HISTORY_INTEGRITY_MISSING,
            error_summary="price history database does not exist",
        )
    try:
        uri = path.resolve(strict=True).as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            rows = conn.execute("PRAGMA quick_check").fetchall()
            messages = tuple(str(row[0]) for row in rows)
            if messages != ("ok",):
                return PriceHistoryIntegrity(
                    path=path,
                    status=PRICE_HISTORY_INTEGRITY_ERROR,
                    error_summary="; ".join(messages) or "SQLite quick_check failed",
                )
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            required_tables = {
                "_schema_meta",
                "daily_price_bars",
                "price_history_syncs",
            }
            missing = sorted(required_tables - tables)
            if missing:
                return PriceHistoryIntegrity(
                    path=path,
                    status=PRICE_HISTORY_INTEGRITY_ERROR,
                    error_summary=(
                        "missing price history table(s): " + ", ".join(missing)
                    ),
                )
            row = conn.execute(
                "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None or str(row[0]) != str(PRICE_HISTORY_SCHEMA_VERSION):
                value = None if row is None else row[0]
                return PriceHistoryIntegrity(
                    path=path,
                    status=PRICE_HISTORY_INTEGRITY_ERROR,
                    error_summary=f"unsupported price history schema version: {value!r}",
                )
    except (OSError, sqlite3.Error) as exc:
        return PriceHistoryIntegrity(
            path=path,
            status=PRICE_HISTORY_INTEGRITY_ERROR,
            error_summary=str(exc).strip() or type(exc).__name__,
        )
    return PriceHistoryIntegrity(path=path, status=PRICE_HISTORY_INTEGRITY_OK)


def _sync_recent(sync: PriceHistorySync | None, *, ttl_seconds: int, now: datetime) -> bool:
    if sync is None:
        return False
    if sync.status != "ok":
        return False
    if sync.checked_at > now:
        return False
    return (now - sync.checked_at).total_seconds() < ttl_seconds


def _latest_history_date(history: pd.DataFrame) -> date | None:
    if history.empty or "date" not in history.columns:
        return None
    dates = pd.to_datetime(history["date"], utc=True, errors="coerce").dropna()
    if dates.empty:
        return None
    return dates.max().date()


def _fetch_days_for_reason(
    *,
    reason: str,
    lookback_days: int,
    latest_date: date | None,
    today: date,
) -> int:
    if reason in {"missing", "backfill", "refresh"} or latest_date is None:
        return lookback_days
    age_days = max((today - latest_date).days, 0)
    return min(lookback_days, max(PRICE_HISTORY_TAIL_REFRESH_DAYS, age_days + 1))


def _reconciliation_reason(
    *,
    history: pd.DataFrame,
    lookback_days: int,
    today: date,
) -> str | None:
    if history.empty:
        return "missing"
    if len(history) < lookback_days:
        return "backfill"
    latest_date = _latest_history_date(history)
    if latest_date is None or latest_date < today:
        return "tail"
    return None


def _load_sync_for_reconcile(
    *,
    store: PriceHistoryStore,
    provider: str,
    ticker: str,
    lookback_days: int,
    logger=None,
) -> PriceHistorySync | None:
    try:
        return store.get_sync(
            provider=provider,
            ticker=ticker,
            lookback_days=lookback_days,
        )
    except (TypeError, ValueError) as exc:
        if logger:
            logger.warning(
                "%s: ignoring malformed price_history sync metadata: %s",
                ticker,
                exc,
            )
        return None


def reconcile_price_history(  # pylint: disable=too-many-locals
    *,
    ticker: str,
    provider,
    config,
    logger=None,
    store: PriceHistoryStore | None = None,
) -> PriceHistoryReconcileResult:
    """Ensure local daily OHLCV coverage and return bars for price-context calculation."""
    store = store or get_price_history_store(config)
    provider_name = provider.name
    today = config.today
    lookback_days = config.price_context_lookback_days
    ttl_seconds = _non_negative_int(
        config.provider_price_context_ttl,
        name="provider_price_context_ttl",
    )
    now = utc_now()
    history = store.load_recent_bars(
        provider=provider_name,
        ticker=ticker,
        lookback_days=lookback_days,
        end_date=today,
    )
    reason = _reconciliation_reason(
        history=history,
        lookback_days=lookback_days,
        today=today,
    )
    sync = _load_sync_for_reconcile(
        store=store,
        provider=provider_name,
        ticker=ticker,
        lookback_days=lookback_days,
        logger=logger,
    )
    if reason is None:
        if ttl_seconds > 0:
            return PriceHistoryReconcileResult(history=history, fetched=False)
        reason = "refresh"
    elif (
        reason == "tail"
        and not history.empty
        and _sync_recent(
            sync,
            ttl_seconds=ttl_seconds,
            now=now,
        )
    ):
        return PriceHistoryReconcileResult(history=history, fetched=False)

    requested_lookback_days = _fetch_days_for_reason(
        reason=reason,
        lookback_days=lookback_days,
        latest_date=_latest_history_date(history),
        today=today,
    )
    try:
        raw_history = provider.load_price_history(
            ticker,
            lookback_days=requested_lookback_days,
        )
        fetched_rows = len(raw_history) if isinstance(raw_history, pd.DataFrame) else 0
        stored_rows = store.upsert_bars(
            provider=provider_name,
            ticker=ticker,
            history=raw_history,
            fetched_at=now,
        )
        history = store.load_recent_bars(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            end_date=today,
        )
        latest_history_date = _latest_history_date(history)
        if fetched_rows == 0 or stored_rows == 0:
            store.record_sync(
                provider=provider_name,
                ticker=ticker,
                lookback_days=lookback_days,
                status="error",
                requested_lookback_days=requested_lookback_days,
                latest_trading_date=latest_history_date,
                fetched_rows=fetched_rows,
                stored_rows=stored_rows,
                error_summary=_EMPTY_PROVIDER_RESPONSE,
                checked_at=now,
            )
            return PriceHistoryReconcileResult(
                history=history,
                fetched=False,
                requested_lookback_days=requested_lookback_days,
                fetched_rows=fetched_rows,
                stored_rows=stored_rows,
                error_summary=_EMPTY_PROVIDER_RESPONSE,
            )
        store.record_sync(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            status="ok",
            requested_lookback_days=requested_lookback_days,
            latest_trading_date=latest_history_date,
            fetched_rows=fetched_rows,
            stored_rows=stored_rows,
            checked_at=now,
        )
        return PriceHistoryReconcileResult(
            history=history,
            fetched=True,
            requested_lookback_days=requested_lookback_days,
            fetched_rows=fetched_rows,
            stored_rows=stored_rows,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        message = compact_exception_summary(exc)
        history = store.load_recent_bars(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            end_date=today,
        )
        store.record_sync(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            status="error",
            requested_lookback_days=requested_lookback_days,
            latest_trading_date=_latest_history_date(history),
            fetched_rows=0,
            stored_rows=0,
            error_summary=message,
            checked_at=now,
        )
        if logger:
            logger.warning("%s: price_history reconcile failed: %s", ticker, message)
        return PriceHistoryReconcileResult(
            history=history,
            fetched=False,
            requested_lookback_days=requested_lookback_days,
            error_summary=message,
        )

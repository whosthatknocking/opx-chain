"""Durable implied-volatility history store for volatility advisory features."""

# pylint: disable=duplicate-code,too-many-arguments,too-many-instance-attributes,too-many-locals

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading
import weakref

import numpy as np
import pandas as pd

from opx_chain.paths import get_data_dir
from opx_chain.storage.history_snapshot import HistorySnapshotWindow, MAX_SNAPSHOT_ROWS
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import parse_iso_datetime, utc_now
from opx_chain.utils import finite_float_or_none
from opx_chain.volatility_features import dte_bucket


IV_HISTORY_SCHEMA_VERSION = 1
IV_HISTORY_SCHEMA_MIGRATIONS: dict[int, str] = {}
IV_HISTORY_INTEGRITY_ERROR = "ERROR"
IV_HISTORY_INTEGRITY_MISSING = "MISSING"
IV_HISTORY_INTEGRITY_OK = "OK"

DELTA_BUCKET_ALL = "ALL"
OPTION_TYPE_ALL = "ALL"
DTE_BUCKET_ALL = "ALL"
_SYNC_STATUSES = frozenset({"INGESTED", "EMPTY", "ERROR", "SKIPPED"})

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS iv_observations (
    provider             TEXT NOT NULL,
    ticker               TEXT NOT NULL,
    observation_date     TEXT NOT NULL,
    option_type          TEXT NOT NULL,
    dte_bucket           TEXT NOT NULL,
    delta_bucket         TEXT NOT NULL,
    representative_iv    REAL NOT NULL,
    observation_count    INTEGER NOT NULL,
    dataset_id           TEXT,
    run_id               TEXT,
    source_created_at    TEXT,
    fetched_at           TEXT NOT NULL,
    PRIMARY KEY (
        provider,
        ticker,
        observation_date,
        option_type,
        dte_bucket,
        delta_bucket
    )
);

CREATE TABLE IF NOT EXISTS iv_history_syncs (
    dataset_id        TEXT PRIMARY KEY,
    provider          TEXT NOT NULL,
    run_id            TEXT,
    checked_at        TEXT NOT NULL,
    status            TEXT NOT NULL,
    observation_date  TEXT,
    source_rows       INTEGER NOT NULL DEFAULT 0,
    stored_rows       INTEGER NOT NULL DEFAULT 0,
    error_summary     TEXT
);

CREATE INDEX IF NOT EXISTS idx_iv_observations_ticker_date
    ON iv_observations(provider, ticker, observation_date DESC);
"""


def _history_db_path(config=None) -> Path:
    base = Path(config.storage_dir) if config is not None and config.storage_dir else get_data_dir()
    return base / "iv-history.db"


def _date_to_str(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


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


def _normalize_ticker(value: object, *, required: bool = False) -> str | None:
    if not isinstance(value, str):
        if required:
            raise ValueError("ticker must be a non-empty string")
        return None
    text = value.strip().upper()
    if not text:
        if required:
            raise ValueError("ticker must be a non-empty string")
        return None
    if not is_valid_ticker(text):
        if required:
            raise ValueError("ticker must be a valid stock ticker symbol")
        return None
    return text


def _normalize_provider(value: object) -> str:
    return _non_empty_text(value, name="provider").lower()


def _positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise ValueError(f"{name} must be a positive integer")
    resolved = int(value)
    if resolved <= 0:
        raise ValueError(f"{name} must be positive")
    return resolved


def _non_negative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
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


def _normalize_option_type(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"CALL", "C"}:
        return "CALL"
    if text in {"PUT", "P"}:
        return "PUT"
    return OPTION_TYPE_ALL


def _delta_bucket(value: object) -> str:
    delta = finite_float_or_none(value)
    if delta is None:
        return DELTA_BUCKET_ALL
    magnitude = abs(delta)
    if magnitude < 0.10:
        return "ABS_DELTA_0_10"
    if magnitude < 0.20:
        return "ABS_DELTA_10_20"
    if magnitude < 0.30:
        return "ABS_DELTA_20_30"
    if magnitude < 0.50:
        return "ABS_DELTA_30_50"
    return "ABS_DELTA_50_PLUS"


def _observation_date(
    frame: pd.DataFrame,
    *,
    observed_at: date | datetime | None,
) -> date:
    if observed_at is not None:
        if isinstance(observed_at, datetime):
            return observed_at.date()
        return observed_at
    if "option_quote_time" in frame.columns:
        quote_times = pd.to_datetime(frame["option_quote_time"], utc=True, errors="coerce")
        quote_times = quote_times.dropna()
        if not quote_times.empty:
            return quote_times.dt.date.mode().iloc[0]
    return utc_now().date()


@dataclass(frozen=True)
class IVHistoryStats:
    """Stored implied-volatility coverage metadata for one provider/ticker."""

    row_count: int
    observation_dates: int
    earliest_date: date | None
    latest_date: date | None


@dataclass(frozen=True)
class IVHistorySync:
    """Last ingestion metadata for one option-chain dataset."""

    checked_at: datetime
    status: str
    provider: str
    run_id: str | None
    observation_date: date | None
    source_rows: int
    stored_rows: int
    error_summary: str | None


@dataclass(frozen=True)
class IVHistoryIntegrity:
    """Read-only integrity result for one durable IV-history database."""

    path: Path
    status: str
    error_summary: str | None = None

    @property
    def healthy(self) -> bool:
        """Return whether the database passed integrity and schema checks."""
        return self.status == IV_HISTORY_INTEGRITY_OK


class IVHistoryStore:
    """SQLite-backed local store of daily IV aggregate observations."""

    def __init__(self, db_path: Path, *, read_only: bool = False) -> None:
        self._db_path = Path(db_path)
        self._read_only = read_only
        self._connection: sqlite3.Connection | None = None
        self._connection_finalizer: weakref.finalize | None = None
        self._lock = threading.RLock()
        if read_only:
            try:
                with self._open_connection() as conn:
                    version = self._read_schema_version(conn)
                    if version != IV_HISTORY_SCHEMA_VERSION:
                        raise RuntimeError(f"Unsupported IV history schema version: {version!r}")
            except (OSError, sqlite3.Error, RuntimeError):
                self.close()
                raise
        else:
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
        path = str(self._db_path)
        if self._read_only:
            path = self._db_path.resolve(strict=True).as_uri() + "?mode=ro"
        conn = sqlite3.connect(path, uri=self._read_only, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        if not self._read_only:
            conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _connection_for_use(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = self._connect()
            self._connection_finalizer = weakref.finalize(self, self._connection.close)
        return self._connection

    @contextmanager
    def _open_connection(self):
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
                    (str(IV_HISTORY_SCHEMA_VERSION),),
                )
            elif current_version > IV_HISTORY_SCHEMA_VERSION:
                raise RuntimeError(
                    "IV history schema version "
                    f"{current_version} is newer than supported version "
                    f"{IV_HISTORY_SCHEMA_VERSION}"
                )
            elif current_version < IV_HISTORY_SCHEMA_VERSION:
                self._migrate_schema(conn, current_version, IV_HISTORY_SCHEMA_VERSION)
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
                f"IV history schema version is not an integer: {row['value']!r}"
            ) from exc

    @staticmethod
    def _migration_statements(migration: str) -> list[str]:
        return [statement.strip() for statement in migration.split(";") if statement.strip()]

    def _migrate_schema(
        self,
        conn: sqlite3.Connection,
        current_version: int,
        target_version: int,
    ) -> None:
        for next_version in range(current_version + 1, target_version + 1):
            migration = IV_HISTORY_SCHEMA_MIGRATIONS.get(next_version)
            if migration is None:
                raise RuntimeError(
                    "IV history schema migration missing: "
                    f"{current_version}->{target_version}"
                )
            for statement in self._migration_statements(migration):
                conn.execute(statement)
            conn.execute(
                "UPDATE _schema_meta SET value = ? WHERE key = 'schema_version'",
                (str(next_version),),
            )

    def upsert_observations(self, observations: pd.DataFrame) -> int:
        """Upsert normalized IV aggregate observations."""
        if not isinstance(observations, pd.DataFrame) or observations.empty:
            return 0
        required = {
            "provider",
            "ticker",
            "observation_date",
            "option_type",
            "dte_bucket",
            "delta_bucket",
            "representative_iv",
            "observation_count",
        }
        missing = sorted(required - set(observations.columns))
        if missing:
            raise ValueError(f"missing IV observation columns: {', '.join(missing)}")
        fetched_at = utc_now().isoformat()
        rows = []
        for _, row in observations.iterrows():
            representative_iv = finite_float_or_none(row.get("representative_iv"))
            observation_count = _positive_int(
                row.get("observation_count"),
                name="observation_count",
            )
            provider = _normalize_provider(row.get("provider"))
            ticker = _normalize_ticker(row.get("ticker"), required=True)
            if representative_iv is None or representative_iv <= 0 or observation_count <= 0:
                continue
            observed = _parse_date(str(row.get("observation_date") or ""))
            if observed is None:
                raise ValueError("observation_date must be YYYY-MM-DD")
            option_type = _non_empty_text(row.get("option_type"), name="option_type")
            dte_bucket_value = _non_empty_text(row.get("dte_bucket"), name="dte_bucket")
            delta_bucket_value = _non_empty_text(row.get("delta_bucket"), name="delta_bucket")
            rows.append(
                (
                    provider,
                    ticker,
                    observed.isoformat(),
                    option_type.upper().strip(),
                    dte_bucket_value.upper().strip(),
                    delta_bucket_value.upper().strip(),
                    representative_iv,
                    observation_count,
                    _optional_text(row.get("dataset_id"), name="dataset_id"),
                    _optional_text(row.get("run_id"), name="run_id"),
                    _optional_text(row.get("source_created_at"), name="source_created_at"),
                    _optional_text(row.get("fetched_at"), name="fetched_at") or fetched_at,
                )
            )
        if not rows:
            return 0
        with self._open_connection() as conn:
            conn.executemany(
                """
                INSERT INTO iv_observations
                    (provider, ticker, observation_date, option_type, dte_bucket,
                     delta_bucket, representative_iv, observation_count, dataset_id,
                     run_id, source_created_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    provider, ticker, observation_date, option_type, dte_bucket,
                    delta_bucket
                ) DO UPDATE SET
                    representative_iv = excluded.representative_iv,
                    observation_count = excluded.observation_count,
                    dataset_id = excluded.dataset_id,
                    run_id = excluded.run_id,
                    source_created_at = excluded.source_created_at,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def snapshot_bundle_rows(self, window: HistorySnapshotWindow) -> list[dict]:
        """Read exact retained IV aggregates known by a portable bundle cutoff."""
        placeholders = ",".join("?" for _ in window.tickers)
        with self._lock:
            rows = self._connection_for_use().execute(
                "SELECT provider,ticker,observation_date,option_type,dte_bucket,delta_bucket,"
                "representative_iv,observation_count,dataset_id,run_id,"
                "source_created_at,fetched_at "
                "FROM iv_observations WHERE provider=? AND ticker IN (" + placeholders + ") "
                "AND observation_date BETWEEN ? AND ? AND julianday(fetched_at)<=julianday(?) "
                "ORDER BY ticker,observation_date,option_type,dte_bucket,delta_bucket LIMIT ?",
                (*window.arguments(), MAX_SNAPSHOT_ROWS + 1),
            ).fetchall()
        return window.retain(rows)

    def load_history(
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
        end_date: date,
        option_type: str = OPTION_TYPE_ALL,
        delta_bucket: str = DELTA_BUCKET_ALL,
    ) -> pd.DataFrame:
        """Load ticker-wide and DTE-bucket IV observations for percentile features."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker, required=True)
        resolved_lookback = _positive_int(lookback_days, name="lookback_days")
        resolved_end_date = _date_arg(end_date, name="end_date")
        start_date = resolved_end_date - timedelta(days=resolved_lookback)
        option_type_key = _normalize_option_type(option_type)
        delta_bucket_key = _non_empty_text(delta_bucket, name="delta_bucket").upper().strip()
        with self._lock:
            conn = self._connection_for_use()
            rows = conn.execute(
                """
                SELECT ticker, observation_date, option_type, dte_bucket, delta_bucket,
                       representative_iv, observation_count, provider, dataset_id, run_id
                FROM iv_observations
                WHERE provider = ?
                  AND ticker = ?
                  AND observation_date >= ?
                  AND observation_date <= ?
                  AND option_type = ?
                  AND delta_bucket = ?
                ORDER BY observation_date
                """,
                (
                    provider_key,
                    ticker_key,
                    start_date.isoformat(),
                    resolved_end_date.isoformat(),
                    option_type_key,
                    delta_bucket_key,
                ),
            ).fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "ticker",
                    "date",
                    "observation_date",
                    "option_type",
                    "dte_bucket",
                    "delta_bucket",
                    "representative_iv",
                    "observation_count",
                    "provider",
                    "dataset_id",
                    "run_id",
                ]
            )
        return pd.DataFrame(
            {
                "ticker": [row["ticker"] for row in rows],
                "date": pd.to_datetime([row["observation_date"] for row in rows], utc=True),
                "observation_date": [row["observation_date"] for row in rows],
                "option_type": [row["option_type"] for row in rows],
                "dte_bucket": [row["dte_bucket"] for row in rows],
                "delta_bucket": [row["delta_bucket"] for row in rows],
                "representative_iv": [row["representative_iv"] for row in rows],
                "observation_count": [row["observation_count"] for row in rows],
                "provider": [row["provider"] for row in rows],
                "dataset_id": [row["dataset_id"] for row in rows],
                "run_id": [row["run_id"] for row in rows],
            }
        )

    def stats(self, *, provider: str, ticker: str) -> IVHistoryStats:
        """Return total coverage stats for one provider/ticker."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker, required=True)
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT observation_date) AS observation_dates,
                       MIN(observation_date) AS earliest_date,
                       MAX(observation_date) AS latest_date
                FROM iv_observations
                WHERE provider = ? AND ticker = ?
                """,
                (provider_key, ticker_key),
            ).fetchone()
        return IVHistoryStats(
            row_count=int(row["row_count"] or 0),
            observation_dates=int(row["observation_dates"] or 0),
            earliest_date=_parse_date(row["earliest_date"]),
            latest_date=_parse_date(row["latest_date"]),
        )

    def has_observation_date(
        self,
        *,
        provider: str,
        ticker: str,
        observation_date: date,
    ) -> bool:
        """Return True when any IV aggregate exists for one provider/ticker/date."""
        provider_key = _normalize_provider(provider)
        ticker_key = _normalize_ticker(ticker, required=True)
        observed = _date_arg(observation_date, name="observation_date")
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT 1
                FROM iv_observations
                WHERE provider = ?
                  AND ticker = ?
                  AND observation_date = ?
                LIMIT 1
                """,
                (provider_key, ticker_key, observed.isoformat()),
            ).fetchone()
        return row is not None

    def get_sync(self, *, dataset_id: str) -> IVHistorySync | None:
        """Return last ingestion metadata for one option-chain dataset."""
        dataset_key = _non_empty_text(dataset_id, name="dataset_id")
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT provider, run_id, checked_at, status, observation_date,
                       source_rows, stored_rows, error_summary
                FROM iv_history_syncs
                WHERE dataset_id = ?
                """,
                (dataset_key,),
            ).fetchone()
        if row is None:
            return None
        try:
            checked_at = parse_iso_datetime(row["checked_at"])
        except (TypeError, ValueError):
            return None
        return IVHistorySync(
            checked_at=checked_at,
            status=str(row["status"]),
            provider=str(row["provider"]),
            run_id=row["run_id"],
            observation_date=_parse_date(row["observation_date"]),
            source_rows=int(row["source_rows"] or 0),
            stored_rows=int(row["stored_rows"] or 0),
            error_summary=row["error_summary"],
        )

    def record_sync(
        self,
        *,
        dataset_id: str,
        provider: str,
        run_id: str | None,
        status: str,
        observation_date: date | None,
        source_rows: int,
        stored_rows: int,
        error_summary: str | None = None,
        checked_at: datetime | None = None,
    ) -> None:
        """Record the latest ingestion attempt for a dataset."""
        dataset_key = _non_empty_text(dataset_id, name="dataset_id")
        provider_key = _normalize_provider(provider)
        run_id_value = _optional_text(run_id, name="run_id")
        status_key = _non_empty_text(status, name="status").upper()
        if status_key not in _SYNC_STATUSES:
            expected = ", ".join(sorted(_SYNC_STATUSES))
            raise ValueError(f"status must be one of: {expected}")
        observed = _optional_date_arg(observation_date, name="observation_date")
        source_row_count = _non_negative_int(source_rows, name="source_rows")
        stored_row_count = _non_negative_int(stored_rows, name="stored_rows")
        error_text = _optional_text(error_summary, name="error_summary")
        checked = (
            _datetime_arg(checked_at, name="checked_at")
            if checked_at is not None
            else utc_now()
        )
        with self._open_connection() as conn:
            conn.execute(
                """
                INSERT INTO iv_history_syncs
                    (dataset_id, provider, run_id, checked_at, status,
                     observation_date, source_rows, stored_rows, error_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET
                    provider = excluded.provider,
                    run_id = excluded.run_id,
                    checked_at = excluded.checked_at,
                    status = excluded.status,
                    observation_date = excluded.observation_date,
                    source_rows = excluded.source_rows,
                    stored_rows = excluded.stored_rows,
                    error_summary = excluded.error_summary
                """,
                (
                    dataset_key,
                    provider_key,
                    run_id_value,
                    checked.isoformat(),
                    status_key,
                    _date_to_str(observed),
                    source_row_count,
                    stored_row_count,
                    error_text,
                ),
            )
            conn.commit()


def _chain_base_frame(
    chain: pd.DataFrame,
    *,
    provider: str,
    dataset_id: str | None,
    run_id: str | None,
    observed_at: date | datetime | None,
    source_created_at: datetime | None,
) -> pd.DataFrame:
    if not isinstance(chain, pd.DataFrame) or chain.empty:
        return pd.DataFrame()
    frame = chain.copy()
    ticker_column = next(
        (column for column in ("underlying_symbol", "ticker", "symbol") if column in frame.columns),
        None,
    )
    if ticker_column is None or "implied_volatility" not in frame.columns:
        return pd.DataFrame()
    observation_date = _observation_date(frame, observed_at=observed_at)
    iv = pd.to_numeric(frame["implied_volatility"], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )
    base = pd.DataFrame(
        {
            "provider": _normalize_provider(provider),
            "ticker": frame[ticker_column].map(_normalize_ticker),
            "observation_date": observation_date.isoformat(),
            "option_type": (
                frame["option_type"].map(_normalize_option_type)
                if "option_type" in frame.columns
                else OPTION_TYPE_ALL
            ),
            "representative_iv": iv,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "source_created_at": (
                source_created_at.isoformat() if source_created_at is not None else None
            ),
            "fetched_at": utc_now().isoformat(),
        }
    )
    if "days_to_expiration" in frame.columns:
        base["dte_bucket"] = frame["days_to_expiration"].map(dte_bucket)
    elif "expiration_date" in frame.columns:
        expirations = pd.to_datetime(frame["expiration_date"], errors="coerce")
        base["dte_bucket"] = [
            dte_bucket((expiration.date() - observation_date).days)
            if not pd.isna(expiration)
            else None
            for expiration in expirations
        ]
    else:
        base["dte_bucket"] = None

    if "delta_abs" in frame.columns:
        delta_values = frame["delta_abs"]
    elif "delta" in frame.columns:
        delta_values = frame["delta"]
    else:
        delta_values = pd.Series([None] * len(frame), index=frame.index)
    base["delta_bucket"] = delta_values.map(_delta_bucket)
    base = base.dropna(subset=["ticker", "dte_bucket", "representative_iv"])
    base = base.loc[base["representative_iv"] > 0].copy()
    return base


def _aggregate_variant(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    grouped = (
        frame.groupby(group_columns, dropna=False)["representative_iv"]
        .agg(representative_iv="median", observation_count="count")
        .reset_index()
    )
    for column in (
        "provider",
        "ticker",
        "observation_date",
        "option_type",
        "dte_bucket",
        "delta_bucket",
        "dataset_id",
        "run_id",
        "source_created_at",
        "fetched_at",
    ):
        if column not in grouped.columns:
            grouped[column] = frame[column].iloc[0]
    return grouped


def build_iv_observation_frame(
    chain: pd.DataFrame,
    *,
    provider: str,
    dataset_id: str | None = None,
    run_id: str | None = None,
    observed_at: date | datetime | None = None,
    source_created_at: datetime | None = None,
) -> pd.DataFrame:
    """Build daily IV aggregate rows suitable for ``IVHistoryStore``."""
    base = _chain_base_frame(
        chain,
        provider=provider,
        dataset_id=dataset_id,
        run_id=run_id,
        observed_at=observed_at,
        source_created_at=source_created_at,
    )
    if base.empty:
        return pd.DataFrame()
    variants: list[pd.DataFrame] = []
    variants.append(
        _aggregate_variant(
            base.assign(
                option_type=OPTION_TYPE_ALL,
                dte_bucket=DTE_BUCKET_ALL,
                delta_bucket=DELTA_BUCKET_ALL,
            ),
            ["provider", "ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"],
        )
    )
    variants.append(
        _aggregate_variant(
            base.assign(option_type=OPTION_TYPE_ALL, delta_bucket=DELTA_BUCKET_ALL),
            ["provider", "ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"],
        )
    )
    variants.append(
        _aggregate_variant(
            base.assign(dte_bucket=DTE_BUCKET_ALL, delta_bucket=DELTA_BUCKET_ALL),
            ["provider", "ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"],
        )
    )
    variants.append(
        _aggregate_variant(
            base.assign(delta_bucket=DELTA_BUCKET_ALL),
            ["provider", "ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"],
        )
    )
    variants.append(
        _aggregate_variant(
            base,
            ["provider", "ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"],
        )
    )
    result = pd.concat(variants, ignore_index=True)
    result = result.drop_duplicates(
        subset=[
            "provider",
            "ticker",
            "observation_date",
            "option_type",
            "dte_bucket",
            "delta_bucket",
        ],
        keep="last",
    )
    return result[
        [
            "provider",
            "ticker",
            "observation_date",
            "option_type",
            "dte_bucket",
            "delta_bucket",
            "representative_iv",
            "observation_count",
            "dataset_id",
            "run_id",
            "source_created_at",
            "fetched_at",
        ]
    ].copy()


def get_iv_history_store(config=None) -> IVHistoryStore:
    """Return the durable local implied-volatility history store."""
    return IVHistoryStore(_history_db_path(config))


def get_iv_history_read_store(config=None) -> IVHistoryStore:
    """Read an existing local IV history without creating or migrating it."""
    return IVHistoryStore(_history_db_path(config), read_only=True)


def get_iv_history_db_path(config=None) -> Path:
    """Return the configured durable IV-history database path."""
    return _history_db_path(config)


def check_iv_history_integrity(db_path: Path) -> IVHistoryIntegrity:
    """Validate an IV-history database without opening it for writes."""
    path = Path(db_path)
    if not path.is_file():
        return IVHistoryIntegrity(
            path=path,
            status=IV_HISTORY_INTEGRITY_MISSING,
            error_summary="IV history database does not exist",
        )
    try:
        uri = path.resolve(strict=True).as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            rows = conn.execute("PRAGMA quick_check").fetchall()
            messages = tuple(str(row[0]) for row in rows)
            if messages != ("ok",):
                return IVHistoryIntegrity(
                    path=path,
                    status=IV_HISTORY_INTEGRITY_ERROR,
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
                "iv_observations",
                "iv_history_syncs",
            }
            missing = sorted(required_tables - tables)
            if missing:
                return IVHistoryIntegrity(
                    path=path,
                    status=IV_HISTORY_INTEGRITY_ERROR,
                    error_summary=(
                        "missing IV history table(s): " + ", ".join(missing)
                    ),
                )
            row = conn.execute(
                "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None or str(row[0]) != str(IV_HISTORY_SCHEMA_VERSION):
                value = None if row is None else row[0]
                return IVHistoryIntegrity(
                    path=path,
                    status=IV_HISTORY_INTEGRITY_ERROR,
                    error_summary=f"unsupported IV history schema version: {value!r}",
                )
    except (OSError, sqlite3.Error) as exc:
        return IVHistoryIntegrity(
            path=path,
            status=IV_HISTORY_INTEGRITY_ERROR,
            error_summary=str(exc).strip() or type(exc).__name__,
        )
    return IVHistoryIntegrity(path=path, status=IV_HISTORY_INTEGRITY_OK)


__all__ = [
    "DELTA_BUCKET_ALL",
    "DTE_BUCKET_ALL",
    "IV_HISTORY_SCHEMA_VERSION",
    "IV_HISTORY_INTEGRITY_ERROR",
    "IV_HISTORY_INTEGRITY_MISSING",
    "IV_HISTORY_INTEGRITY_OK",
    "IVHistoryIntegrity",
    "IVHistoryStats",
    "IVHistoryStore",
    "IVHistorySync",
    "OPTION_TYPE_ALL",
    "build_iv_observation_frame",
    "check_iv_history_integrity",
    "get_iv_history_db_path",
    "get_iv_history_read_store",
    "get_iv_history_store",
]

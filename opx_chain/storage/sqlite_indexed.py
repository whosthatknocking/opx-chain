"""SQLite-indexed StorageBackend implementation."""
# pylint: disable=duplicate-code

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from opx_chain.storage.models import (
    ArtifactRecord,
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunRecord,
    RunSummary,
    TickerFetchResult,
    TickerRunRecord,
    record_to_handle,
)
from opx_chain.storage._disk import write_artifact_bytes, write_dataset_artifact
from opx_chain.storage.serializers import get_serializer


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id                TEXT PRIMARY KEY,
    started_at            TEXT NOT NULL,
    finished_at           TEXT,
    status                TEXT NOT NULL,
    provider              TEXT NOT NULL,
    config_fingerprint    TEXT NOT NULL,
    positions_fingerprint TEXT NOT NULL,
    dataset_id            TEXT,
    error_summary         TEXT
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id      TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    created_at      TEXT NOT NULL,
    provider        TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    format          TEXT NOT NULL,
    location        TEXT NOT NULL,
    content_hash    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticker_results (
    run_id               TEXT NOT NULL REFERENCES runs(run_id),
    ticker               TEXT NOT NULL,
    raw_row_count        INTEGER NOT NULL,
    normalized_row_count INTEGER NOT NULL,
    kept_row_count       INTEGER NOT NULL,
    filtered_row_count   INTEGER NOT NULL,
    expiration_count     INTEGER NOT NULL,
    status               TEXT NOT NULL,
    error_summary        TEXT,
    PRIMARY KEY (run_id, ticker)
);

CREATE TABLE IF NOT EXISTS validations (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL REFERENCES runs(run_id),
    severity TEXT NOT NULL,
    code     TEXT NOT NULL,
    count    INTEGER NOT NULL,
    sample   TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs(run_id),
    artifact_type TEXT NOT NULL,
    location      TEXT NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_run_id     ON datasets(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_status         ON runs(status);
"""

_SCHEMA_VERSION = 1


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _dt_to_str(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _str_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class SqliteIndexedBackend:
    """StorageBackend that stores run/dataset metadata in SQLite and artifacts on disk."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        db_path: Path,
        output_dir: Path,
        logs_dir: Path,
        debug_dir: Path,
        max_runs_retained: int = 0,
        dataset_format: str = "csv",
    ) -> None:
        """Initialise with the SQLite db path, artifact directories, and retention limit."""
        self._db_path = Path(db_path)
        self._output_dir = Path(output_dir)
        self._logs_dir = Path(logs_dir)
        self._debug_dir = Path(debug_dir)
        self._max_runs_retained = max_runs_retained
        self._dataset_format = dataset_format
        self._serializer = get_serializer(dataset_format)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA_SQL)
            existing = conn.execute(
                "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO _schema_meta VALUES ('schema_version', ?)",
                    (str(_SCHEMA_VERSION),),
                )
            conn.commit()

    def _prune_datasets(self, conn: sqlite3.Connection) -> None:
        if self._max_runs_retained <= 0:
            return
        rows = conn.execute(
            "SELECT dataset_id, location FROM datasets ORDER BY created_at DESC"
        ).fetchall()
        excess = rows[self._max_runs_retained:]
        for row in excess:
            artifact = Path(row["location"])
            if artifact.exists():
                artifact.unlink(missing_ok=True)
            conn.execute("DELETE FROM datasets WHERE dataset_id = ?", (row["dataset_id"],))

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    def create_run(self, context: RunContext) -> str:
        """Insert a new run row and return its run_id."""
        run_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO runs
                   (run_id, started_at, finished_at, status, provider,
                    config_fingerprint, positions_fingerprint, dataset_id, error_summary)
                   VALUES (?, ?, NULL, 'running', ?, ?, ?, NULL, NULL)""",
                (
                    run_id,
                    _dt_to_str(_now()),
                    context.provider,
                    context.config_fingerprint,
                    context.positions_fingerprint,
                ),
            )
            conn.commit()
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Insert or replace a per-ticker result row."""
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO ticker_results
                   (run_id, ticker, raw_row_count, normalized_row_count,
                    kept_row_count, filtered_row_count, expiration_count,
                    status, error_summary)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    result.ticker,
                    result.raw_row_count,
                    result.normalized_row_count,
                    result.kept_row_count,
                    result.filtered_row_count,
                    result.expiration_count,
                    result.status,
                    result.error_summary,
                ),
            )
            conn.commit()

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame, store metadata in SQLite, and return a DatasetRecord."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        dataset_id, artifact_path, content_hash = write_dataset_artifact(
            dataset.data, self._output_dir, self._dataset_format, self._serializer
        )
        now = _now()
        record = DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=now,
            provider=dataset.provider,
            schema_version=dataset.schema_version,
            row_count=len(dataset.data),
            format=self._dataset_format,
            location=str(artifact_path),
            content_hash=content_hash,
        )
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO datasets
                   (dataset_id, run_id, created_at, provider, schema_version,
                    row_count, format, location, content_hash)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    dataset_id,
                    run_id,
                    _dt_to_str(now),
                    dataset.provider,
                    dataset.schema_version,
                    len(dataset.data),
                    self._dataset_format,
                    str(artifact_path),
                    content_hash,
                ),
            )
            conn.execute(
                "UPDATE runs SET dataset_id = ? WHERE run_id = ?",
                (dataset_id, run_id),
            )
            self._prune_datasets(conn)
            conn.commit()
        return record

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Write artifact bytes to disk and record metadata in SQLite."""
        artifact_id, dest, content_hash = write_artifact_bytes(
            artifact.content, self._debug_dir, artifact.filename
        )
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO artifacts
                   (artifact_id, run_id, artifact_type, location, content_hash)
                   VALUES (?, ?, ?, ?, ?)""",
                (artifact_id, run_id, artifact.artifact_type, str(dest.resolve()), content_hash),
            )
            conn.commit()
        return ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            location=str(dest.resolve()),
            content_hash=content_hash,
        )

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,  # pylint: disable=unused-argument
    ) -> list[DatasetRecord]:
        """Return dataset records from SQLite, newest first."""
        sql = "SELECT * FROM datasets"
        params: list = []
        conditions: list[str] = []
        if provider is not None:
            conditions.append("provider = ?")
            params.append(provider)
        if since is not None:
            conditions.append("created_at >= ?")
            params.append(_dt_to_str(since))
        if until is not None:
            conditions.append("created_at <= ?")
            params.append(_dt_to_str(until))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle for the given dataset_id."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM datasets WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"dataset not found: {dataset_id}")
        return record_to_handle(self._row_to_record(row))

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Update the run row with a completion status."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, error_summary = ? WHERE run_id = ?",
                (summary.status, _dt_to_str(_now()), summary.error_summary, run_id),
            )
            conn.commit()

    def fail_run(self, run_id: str, error: str) -> None:
        """Update the run row with a failed status and error message."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET status = 'failed', finished_at = ?, error_summary = ? "
                "WHERE run_id = ?",
                (_dt_to_str(_now()), error, run_id),
            )
            conn.commit()

    def get_run(self, run_id: str) -> RunRecord:
        """Return a RunRecord for the given run_id."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"run not found: {run_id}")
        return RunRecord(
            run_id=row["run_id"],
            started_at=_str_to_dt(row["started_at"]),
            finished_at=_str_to_dt(row["finished_at"]),
            status=row["status"],
            provider=row["provider"],
            config_fingerprint=row["config_fingerprint"],
            positions_fingerprint=row["positions_fingerprint"],
            dataset_id=row["dataset_id"],
            error_summary=row["error_summary"],
        )

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results for a run."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM ticker_results WHERE run_id = ?", (run_id,)
            ).fetchall()
        return [
            TickerRunRecord(
                run_id=row["run_id"],
                ticker=row["ticker"],
                raw_row_count=row["raw_row_count"],
                normalized_row_count=row["normalized_row_count"],
                kept_row_count=row["kept_row_count"],
                filtered_row_count=row["filtered_row_count"],
                expiration_count=row["expiration_count"],
                status=row["status"],
                error_summary=row["error_summary"],
            )
            for row in rows
        ]

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> DatasetRecord:
        return DatasetRecord(
            dataset_id=row["dataset_id"],
            run_id=row["run_id"],
            created_at=_str_to_dt(row["created_at"]),
            provider=row["provider"],
            schema_version=row["schema_version"],
            row_count=row["row_count"],
            format=row["format"],
            location=row["location"],
            content_hash=row["content_hash"],
        )

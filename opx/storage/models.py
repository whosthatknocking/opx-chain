"""Domain records and write payload types for the storage layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
# pylint: disable=too-many-instance-attributes
class RunRecord:
    """One record per fetch run."""

    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str  # pending | running | complete | failed | interrupted
    provider: str
    config_fingerprint: str
    positions_fingerprint: str
    dataset_id: str | None
    error_summary: str | None


@dataclass
# pylint: disable=too-many-instance-attributes
class DatasetRecord:
    """One record per successfully written canonical dataset."""

    dataset_id: str
    run_id: str
    created_at: datetime
    provider: str
    schema_version: int
    row_count: int
    format: str  # csv | parquet
    location: str
    content_hash: str


@dataclass
class DatasetHandle:
    """Stable external reference returned by get_dataset."""

    dataset_id: str
    location: str
    schema_version: int
    row_count: int
    format: str
    content_hash: str
    created_at: datetime


def record_to_handle(record: DatasetRecord) -> DatasetHandle:
    """Convert a DatasetRecord to a DatasetHandle."""
    return DatasetHandle(
        dataset_id=record.dataset_id,
        location=record.location,
        schema_version=record.schema_version,
        row_count=record.row_count,
        format=record.format,
        content_hash=record.content_hash,
        created_at=record.created_at,
    )


@dataclass
# pylint: disable=too-many-instance-attributes
class TickerRunRecord:
    """Per-ticker summary for a single fetch run."""

    run_id: str
    ticker: str
    raw_row_count: int
    normalized_row_count: int
    kept_row_count: int
    filtered_row_count: int
    expiration_count: int
    status: str  # ok | skipped | error
    error_summary: str | None


@dataclass
class ValidationRecord:
    """One record per validation finding per run."""

    run_id: str
    severity: str  # error | warning | info
    code: str
    count: int
    sample: str | None


@dataclass
class ArtifactRecord:
    """One record per auxiliary artifact (debug payload, run log, sidecar)."""

    artifact_id: str
    run_id: str
    artifact_type: str  # debug_payload | run_log | sidecar
    location: str
    content_hash: str


# Write payloads — callers pass these into the storage port, not raw records.


@dataclass
# pylint: disable=too-many-instance-attributes
class RunContext:
    """Payload supplied by the caller when opening a new run."""

    provider: str
    tickers: tuple[str, ...]
    config_fingerprint: str
    positions_fingerprint: str


@dataclass
class TickerFetchResult:
    """Per-ticker fetch outcome written during the fetch loop."""

    ticker: str
    raw_row_count: int
    normalized_row_count: int
    kept_row_count: int
    filtered_row_count: int
    expiration_count: int
    status: str
    error_summary: str | None = None


@dataclass
class DatasetWrite:
    """Payload supplied to write_dataset."""

    data: pd.DataFrame
    provider: str
    schema_version: int
    format: str = "csv"


@dataclass
class ArtifactWrite:
    """Payload supplied to write_artifact."""

    artifact_type: str
    content: bytes
    filename: str


@dataclass
class RunSummary:
    """Payload supplied to finalize_run or fail_run."""

    status: str  # complete | failed | interrupted
    error_summary: str | None = None

"""Storage port protocols for the opx storage layer."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

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
)


@runtime_checkable
class StorageBackend(Protocol):  # pylint: disable=too-few-public-methods
    """Write and read protocol for run history and canonical datasets."""

    def create_run(self, context: RunContext) -> str: ...  # pylint: disable=missing-function-docstring
    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None: ...  # pylint: disable=missing-function-docstring
    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord: ...  # pylint: disable=missing-function-docstring
    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord: ...  # pylint: disable=missing-function-docstring
    def list_datasets(  # pylint: disable=missing-function-docstring,too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]: ...
    def get_dataset(self, dataset_id: str) -> DatasetHandle: ...  # pylint: disable=missing-function-docstring
    def get_run(self, run_id: str) -> RunRecord: ...  # pylint: disable=missing-function-docstring
    def finalize_run(self, run_id: str, summary: RunSummary) -> None: ...  # pylint: disable=missing-function-docstring
    def fail_run(self, run_id: str, error: str) -> None: ...  # pylint: disable=missing-function-docstring
    def count_runs_today(self, provider: str) -> int: ...  # pylint: disable=missing-function-docstring


class ProviderCache(Protocol):  # pylint: disable=too-few-public-methods
    """Cache protocol for upstream provider responses."""

    def get(self, key: str) -> bytes | None: ...  # pylint: disable=missing-function-docstring
    def put(self, key: str, value: bytes, ttl_seconds: int) -> None: ...  # pylint: disable=missing-function-docstring
    def invalidate(self, key: str) -> None: ...  # pylint: disable=missing-function-docstring

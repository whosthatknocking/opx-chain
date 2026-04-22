"""Filesystem-based StorageBackend implementation."""

from __future__ import annotations

import json
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


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _dt_to_str(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _str_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class FilesystemBackend:
    """StorageBackend that writes metadata as JSON sidecars and artifacts as files.

    Dataset artifacts land in output_dir as {dataset_id}.csv (or .parquet).
    Dataset metadata lands alongside as {dataset_id}.meta.json.
    Run records land in logs_dir as run_{run_id}.json.
    Artifact files land in debug_dir as {artifact_id}/{filename}.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        output_dir: Path,
        logs_dir: Path,
        debug_dir: Path,
        max_runs_retained: int = 0,
        dataset_format: str = "csv",
    ) -> None:
        """Initialise with the three storage directories and optional retention limit."""
        self._output_dir = output_dir
        self._logs_dir = logs_dir
        self._debug_dir = debug_dir
        self._max_runs_retained = max_runs_retained
        self._dataset_format = dataset_format
        self._serializer = get_serializer(dataset_format)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_path(self, run_id: str) -> Path:
        return self._logs_dir / f"run_{run_id}.json"

    def _meta_path(self, dataset_id: str) -> Path:
        return self._output_dir / f"{dataset_id}.meta.json"

    def _artifact_path(self, artifact_id: str, filename: str) -> Path:
        return self._debug_dir / artifact_id / filename

    def _read_run(self, run_id: str) -> dict:
        path = self._run_path(run_id)
        with path.open() as fh:
            return json.load(fh)

    def _write_run(self, run_id: str, data: dict) -> None:
        path = self._run_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _read_meta(self, dataset_id: str) -> dict:
        with self._meta_path(dataset_id).open() as fh:
            return json.load(fh)

    def _write_meta(self, record: DatasetRecord) -> None:
        data = {
            "dataset_id": record.dataset_id,
            "run_id": record.run_id,
            "created_at": _dt_to_str(record.created_at),
            "provider": record.provider,
            "schema_version": record.schema_version,
            "row_count": record.row_count,
            "format": record.format,
            "location": record.location,
            "content_hash": record.content_hash,
        }
        path = self._meta_path(record.dataset_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @staticmethod
    def _meta_to_record(data: dict) -> DatasetRecord:
        return DatasetRecord(
            dataset_id=data["dataset_id"],
            run_id=data["run_id"],
            created_at=_str_to_dt(data["created_at"]),
            provider=data["provider"],
            schema_version=data["schema_version"],
            row_count=data["row_count"],
            format=data["format"],
            location=data["location"],
            content_hash=data["content_hash"],
        )

    def _prune_datasets(self) -> None:
        if self._max_runs_retained <= 0:
            return
        meta_files = sorted(
            self._output_dir.glob("*.meta.json"),
            key=lambda p: p.stat().st_mtime,
        )
        excess = len(meta_files) - self._max_runs_retained
        for meta_path in meta_files[:excess]:
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
                artifact = self._output_dir / Path(data["location"]).name
                if artifact.exists():
                    artifact.unlink()
            except (OSError, KeyError, json.JSONDecodeError):
                pass
            meta_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    def create_run(self, context: RunContext) -> str:
        """Create a run sidecar JSON and return its run_id."""
        run_id = str(uuid.uuid4())
        data = {
            "run_id": run_id,
            "started_at": _dt_to_str(_now()),
            "finished_at": None,
            "status": "running",
            "provider": context.provider,
            "config_fingerprint": context.config_fingerprint,
            "positions_fingerprint": context.positions_fingerprint,
            "dataset_id": None,
            "error_summary": None,
            "ticker_results": [],
        }
        self._write_run(run_id, data)
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Append a per-ticker result to the run sidecar."""
        data = self._read_run(run_id)
        data["ticker_results"].append({
            "ticker": result.ticker,
            "raw_row_count": result.raw_row_count,
            "normalized_row_count": result.normalized_row_count,
            "kept_row_count": result.kept_row_count,
            "filtered_row_count": result.filtered_row_count,
            "expiration_count": result.expiration_count,
            "status": result.status,
            "error_summary": result.error_summary,
        })
        self._write_run(run_id, data)

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame, compute its hash, and write metadata."""
        dataset_id, artifact_path, content_hash = write_dataset_artifact(
            dataset.data, self._output_dir, self._dataset_format, self._serializer
        )
        record = DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=_now(),
            provider=dataset.provider,
            schema_version=dataset.schema_version,
            row_count=len(dataset.data),
            format=self._dataset_format,
            location=str(artifact_path),
            content_hash=content_hash,
        )
        self._write_meta(record)
        data = self._read_run(run_id)
        data["dataset_id"] = dataset_id
        self._write_run(run_id, data)
        self._prune_datasets()
        return record

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Write artifact bytes to disk and return an ArtifactRecord."""
        artifact_id, dest, content_hash = write_artifact_bytes(
            artifact.content, self._debug_dir, artifact.filename
        )
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
        """Return dataset records from meta files, newest first."""
        if not self._output_dir.exists():
            return []
        meta_files = sorted(
            self._output_dir.glob("*.meta.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        results = []
        for meta_path in meta_files:
            try:
                record = self._meta_to_record(
                    json.loads(meta_path.read_text(encoding="utf-8"))
                )
            except (OSError, KeyError, json.JSONDecodeError, ValueError):
                continue
            if provider is not None and record.provider != provider:
                continue
            if since is not None and record.created_at < since:
                continue
            if until is not None and record.created_at > until:
                continue
            results.append(record)
            if len(results) >= limit:
                break
        return results

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle by loading the dataset's meta file."""
        meta_path = self._meta_path(dataset_id)
        if not meta_path.exists():
            raise KeyError(f"dataset not found: {dataset_id}")
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        return record_to_handle(self._meta_to_record(data))

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Update the run sidecar with a completion status."""
        data = self._read_run(run_id)
        data["status"] = summary.status
        data["finished_at"] = _dt_to_str(_now())
        data["error_summary"] = summary.error_summary
        self._write_run(run_id, data)

    def fail_run(self, run_id: str, error: str) -> None:
        """Update the run sidecar with a failed status and error message."""
        data = self._read_run(run_id)
        data["status"] = "failed"
        data["finished_at"] = _dt_to_str(_now())
        data["error_summary"] = error
        self._write_run(run_id, data)

    def get_run(self, run_id: str) -> RunRecord:
        """Return a RunRecord by loading the run sidecar."""
        data = self._read_run(run_id)
        return RunRecord(
            run_id=data["run_id"],
            started_at=_str_to_dt(data["started_at"]),
            finished_at=_str_to_dt(data.get("finished_at")),
            status=data["status"],
            provider=data["provider"],
            config_fingerprint=data["config_fingerprint"],
            positions_fingerprint=data["positions_fingerprint"],
            dataset_id=data.get("dataset_id"),
            error_summary=data.get("error_summary"),
        )

    def count_runs_today(self, provider: str) -> int:
        """Return the number of run sidecars started today (UTC) for the given provider."""
        today_start = _now().date().isoformat()
        count = 0
        if not self._logs_dir.exists():
            return count
        for run_path in self._logs_dir.glob("run_*.json"):
            try:
                data = json.loads(run_path.read_text(encoding="utf-8"))
                if data.get("provider") != provider:
                    continue
                started_at = data.get("started_at", "")
                if started_at[:10] >= today_start:
                    count += 1
            except (OSError, json.JSONDecodeError):
                continue
        return count

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results stored in the run sidecar."""
        data = self._read_run(run_id)
        return [
            TickerRunRecord(
                run_id=run_id,
                ticker=r["ticker"],
                raw_row_count=r["raw_row_count"],
                normalized_row_count=r["normalized_row_count"],
                kept_row_count=r["kept_row_count"],
                filtered_row_count=r["filtered_row_count"],
                expiration_count=r["expiration_count"],
                status=r["status"],
                error_summary=r.get("error_summary"),
            )
            for r in data.get("ticker_results", [])
        ]

"""Tests for FilesystemBackend and get_storage_backend factory."""
# pylint: disable=duplicate-code

import hashlib
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.factory import get_storage_backend
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
)


def _make_backend(
    tmp_path: Path,
    max_runs_retained: int = 0,
    dataset_format: str = "csv",
) -> FilesystemBackend:
    return FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
        max_runs_retained=max_runs_retained,
        dataset_format=dataset_format,
    )


def _make_context(**kwargs) -> RunContext:
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _make_dataframe(rows: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {"underlying_symbol": ["TSLA"] * rows, "strike": [100.0, 110.0, 120.0][:rows]}
    )


def _write(backend: FilesystemBackend, run_id: str, rows: int = 3, provider: str = "yfinance"):
    return backend.write_dataset(
        run_id,
        DatasetWrite(data=_make_dataframe(rows), provider=provider, schema_version=1),
    )


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------

def test_filesystem_backend_satisfies_protocol(tmp_path: Path):
    """FilesystemBackend must satisfy the StorageBackend runtime-checkable protocol."""
    assert isinstance(_make_backend(tmp_path), StorageBackend)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------

def test_create_run_writes_sidecar(tmp_path: Path):
    """create_run must write a JSON sidecar to runs_dir/{run_id}/run.json."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    assert (tmp_path / "runs" / run_id / "run.json").exists()


def test_create_run_initial_status_is_running(tmp_path: Path):
    """Newly created run sidecar must have status=running."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    run = backend.get_run(run_id)
    assert run.status == "running"
    assert run.finished_at is None


def test_finalize_run_sets_status_complete(tmp_path: Path):
    """finalize_run must update status to complete and set finished_at."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    backend.finalize_run(run_id, RunSummary(status="complete"))

    run = backend.get_run(run_id)
    assert run.status == "complete"
    assert run.finished_at is not None
    assert run.error_summary is None


def test_fail_run_sets_status_and_error(tmp_path: Path):
    """fail_run must update status to failed and persist the error message."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    backend.fail_run(run_id, "network error")

    run = backend.get_run(run_id)
    assert run.status == "failed"
    assert run.error_summary == "network error"


def test_record_ticker_result_persisted(tmp_path: Path):
    """record_ticker_result must persist the result in the run sidecar."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    result = TickerFetchResult(
        ticker="TSLA",
        raw_row_count=50,
        normalized_row_count=48,
        kept_row_count=40,
        filtered_row_count=8,
        expiration_count=4,
        status="ok",
    )
    backend.record_ticker_result(run_id, result)

    ticker_results = backend.get_ticker_results(run_id)
    assert len(ticker_results) == 1
    assert ticker_results[0].ticker == "TSLA"
    assert ticker_results[0].kept_row_count == 40


# ---------------------------------------------------------------------------
# Dataset write and read
# ---------------------------------------------------------------------------

def test_write_dataset_creates_csv_and_meta(tmp_path: Path):
    """write_dataset must create both the artifact CSV and its .meta.json."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    assert Path(record.location).exists()
    assert (tmp_path / "runs" / run_id / "output" / f"{record.dataset_id}.meta.json").exists()


def test_write_dataset_returns_correct_record(tmp_path: Path):
    """DatasetRecord returned by write_dataset must have correct field values."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id, DatasetWrite(data=df, provider="yfinance", schema_version=1)
    )

    assert isinstance(record, DatasetRecord)
    assert record.run_id == run_id
    assert record.row_count == len(df)
    assert record.format == "csv"
    assert len(record.content_hash) == 64
    assert Path(record.location).is_absolute()


def test_content_hash_matches_artifact_bytes(tmp_path: Path):
    """content_hash must equal SHA-256 of the written artifact file."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    actual_hash = hashlib.sha256(Path(record.location).read_bytes()).hexdigest()
    assert record.content_hash == actual_hash


def test_get_dataset_returns_handle(tmp_path: Path):
    """get_dataset must return a DatasetHandle matching the written record."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    handle = backend.get_dataset(record.dataset_id)

    assert isinstance(handle, DatasetHandle)
    assert handle.dataset_id == record.dataset_id
    assert handle.content_hash == record.content_hash
    assert handle.created_at == record.created_at


def test_get_dataset_raises_for_unknown_id(tmp_path: Path):
    """get_dataset must raise KeyError for an unrecognised dataset_id."""
    backend = _make_backend(tmp_path)
    with pytest.raises(KeyError):
        backend.get_dataset("no-such-id")


def test_list_datasets_most_recent_first(tmp_path: Path):
    """list_datasets must return records newest first."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id, rows=1)
    r2 = _write(backend, run_id, rows=2)

    records = backend.list_datasets()

    assert records[0].dataset_id == r2.dataset_id
    assert records[1].dataset_id == r1.dataset_id


def test_list_datasets_limit(tmp_path: Path):
    """list_datasets must honour the limit parameter."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets(limit=2)) == 2


def test_list_datasets_filter_provider(tmp_path: Path):
    """list_datasets must filter by provider when the argument is given."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    _write(backend, run_id, provider="yfinance")
    _write(backend, run_id, provider="marketdata")

    results = backend.list_datasets(provider="yfinance")

    assert len(results) == 1
    assert results[0].provider == "yfinance"


def test_list_datasets_empty_when_no_runs_dir(tmp_path: Path):
    """list_datasets must return empty list when runs_dir does not exist."""
    backend = _make_backend(tmp_path)
    assert not backend.list_datasets()


def test_write_dataset_links_run(tmp_path: Path):
    """write_dataset must update the run sidecar's dataset_id field."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    run = backend.get_run(run_id)
    assert run.dataset_id == record.dataset_id


# ---------------------------------------------------------------------------
# Artifact write
# ---------------------------------------------------------------------------

def test_write_artifact_creates_file(tmp_path: Path):
    """write_artifact must write the content bytes to disk."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    payload = ArtifactWrite(
        artifact_type="debug_payload", content=b"payload", filename="data.json"
    )

    record = backend.write_artifact(run_id, payload)

    assert Path(record.location).read_bytes() == b"payload"
    assert len(record.content_hash) == 64


# ---------------------------------------------------------------------------
# Retention pruning
# ---------------------------------------------------------------------------

def test_pruning_removes_oldest_when_limit_exceeded(tmp_path: Path):
    """Datasets beyond max_runs_retained must be pruned after each write."""
    backend = _make_backend(tmp_path, max_runs_retained=2)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    r2 = _write(backend, run_id)
    r3 = _write(backend, run_id)

    records = backend.list_datasets()
    ids = {r.dataset_id for r in records}

    assert len(records) == 2
    assert r1.dataset_id not in ids
    assert r2.dataset_id in ids
    assert r3.dataset_id in ids


def test_pruning_removes_artifact_file(tmp_path: Path):
    """Pruning must delete the artifact CSV in addition to the meta file."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    _write(backend, run_id)

    assert not Path(r1.location).exists()


def test_no_pruning_when_max_runs_retained_zero(tmp_path: Path):
    """When max_runs_retained = 0 (default), no datasets are ever pruned."""
    backend = _make_backend(tmp_path, max_runs_retained=0)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets()) == 5


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_none_when_storage_disabled():
    """get_storage_backend must return None when storage_enabled = False."""
    config = make_runtime_config(storage_enabled=False)
    assert get_storage_backend(config) is None


def test_factory_returns_filesystem_backend_when_enabled(tmp_path: Path):
    """get_storage_backend must return a FilesystemBackend when enabled."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    backend = get_storage_backend(config)
    assert isinstance(backend, FilesystemBackend)


# ---------------------------------------------------------------------------
# Parquet format
# ---------------------------------------------------------------------------

def test_write_dataset_parquet_creates_parquet_file(tmp_path: Path):
    """write_dataset with dataset_format='parquet' must create a .parquet artifact."""
    pytest.importorskip("pyarrow")
    backend = _make_backend(tmp_path, dataset_format="parquet")
    run_id = backend.create_run(_make_context())
    record = backend.write_dataset(
        run_id, DatasetWrite(data=_make_dataframe(), provider="yfinance", schema_version=1)
    )

    assert record.format == "parquet"
    assert Path(record.location).suffix == ".parquet"
    assert Path(record.location).exists()


def test_write_dataset_parquet_is_readable(tmp_path: Path):
    """A parquet artifact written by FilesystemBackend must be readable by pandas."""
    pytest.importorskip("pyarrow")
    backend = _make_backend(tmp_path, dataset_format="parquet")
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id, DatasetWrite(data=df, provider="yfinance", schema_version=1)
    )

    result = pd.read_parquet(record.location)
    assert list(result.columns) == list(df.columns)
    assert len(result) == len(df)


def test_factory_passes_dataset_format_to_backend(tmp_path: Path):
    """get_storage_backend must honour storage_dataset_format from config."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dataset_format="parquet",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    backend = get_storage_backend(config)
    assert isinstance(backend, FilesystemBackend)
    assert backend._dataset_format == "parquet"  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# get_run error path
# ---------------------------------------------------------------------------

def test_get_run_raises_for_unknown_id(tmp_path: Path):
    """get_run must raise an exception when the run sidecar does not exist."""
    backend = _make_backend(tmp_path)
    with pytest.raises((KeyError, FileNotFoundError, OSError)):
        backend.get_run("no-such-run")


# ---------------------------------------------------------------------------
# list_datasets date range filters
# ---------------------------------------------------------------------------

def test_list_datasets_since_excludes_older_records(tmp_path: Path):
    """list_datasets(since=T) must exclude records whose created_at is before T."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    future = record.created_at + timedelta(seconds=1)
    results = backend.list_datasets(since=future)

    assert not results


def test_list_datasets_until_excludes_newer_records(tmp_path: Path):
    """list_datasets(until=T) must exclude records whose created_at is after T."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    past = record.created_at - timedelta(seconds=1)
    results = backend.list_datasets(until=past)

    assert not results


# ---------------------------------------------------------------------------
# Pruning resilience
# ---------------------------------------------------------------------------

def test_prune_tolerates_corrupt_meta_file(tmp_path: Path):
    """A corrupt meta JSON must be silently skipped and removed during pruning."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    _write(backend, run_id)

    corrupt_dir = tmp_path / "runs" / "fake-run-id" / "output"
    corrupt_dir.mkdir(parents=True, exist_ok=True)
    corrupt_meta = corrupt_dir / "corrupt.meta.json"
    corrupt_meta.write_text("not-valid-json", encoding="utf-8")

    _write(backend, run_id)

    assert not corrupt_meta.exists()


# ---------------------------------------------------------------------------
# get_serializer error path
# ---------------------------------------------------------------------------

def test_get_serializer_raises_for_unknown_format():
    """get_serializer must raise ValueError for an unrecognised format name."""
    from opx_chain.storage.serializers import get_serializer  # pylint: disable=import-outside-toplevel
    with pytest.raises(ValueError, match="Unsupported dataset format"):
        get_serializer("avro")


# ---------------------------------------------------------------------------
# count_runs_today
# ---------------------------------------------------------------------------

def test_count_runs_today_counts_same_provider_only(tmp_path: Path):
    """count_runs_today must count runs for the given provider, not others."""
    backend = _make_backend(tmp_path)
    backend.create_run(_make_context(provider="marketdata"))
    backend.create_run(_make_context(provider="marketdata"))
    backend.create_run(_make_context(provider="yfinance"))

    assert backend.count_runs_today("marketdata") == 2
    assert backend.count_runs_today("yfinance") == 1


def test_count_runs_today_returns_zero_when_no_runs(tmp_path: Path):
    """count_runs_today must return 0 when no runs exist for that provider."""
    backend = _make_backend(tmp_path)
    assert backend.count_runs_today("marketdata") == 0

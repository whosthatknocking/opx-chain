"""Tests for MemoryBackend: protocol satisfaction and write roundtrips."""

from datetime import timedelta

import pandas as pd
import pytest

from opx_chain import SCHEMA_VERSION
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
)


def _make_context(**kwargs):
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _make_dataframe(rows=3):
    return pd.DataFrame(
        {"underlying_symbol": ["TSLA"] * rows, "strike": [100.0, 110.0, 120.0][:rows]}
    )


def _write(backend, run_id, rows=3, provider="yfinance"):
    return backend.write_dataset(
        run_id,
        DatasetWrite(data=_make_dataframe(rows), provider=provider, schema_version=1),
    )


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------

def test_memory_backend_satisfies_protocol():
    """MemoryBackend must satisfy the StorageBackend runtime-checkable protocol."""
    assert isinstance(MemoryBackend(), StorageBackend)


# ---------------------------------------------------------------------------
# Full run lifecycle roundtrip
# ---------------------------------------------------------------------------

def test_create_run_returns_string_id():
    """create_run must return a non-empty string identifier."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    assert isinstance(run_id, str) and run_id


def test_write_dataset_roundtrip():
    """write_dataset must return a DatasetRecord with correct fields."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    payload = DatasetWrite(data=df, provider="yfinance", schema_version=SCHEMA_VERSION)

    record = backend.write_dataset(run_id, payload)

    assert isinstance(record, DatasetRecord)
    assert record.run_id == run_id
    assert record.row_count == len(df)
    assert record.schema_version == SCHEMA_VERSION
    assert record.provider == "yfinance"
    assert record.format == "csv"
    assert len(record.content_hash) == 64
    assert record.location.startswith("memory://")


def test_get_dataset_returns_handle():
    """get_dataset must return a DatasetHandle matching the DatasetRecord."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    handle = backend.get_dataset(record.dataset_id)

    assert isinstance(handle, DatasetHandle)
    assert handle.dataset_id == record.dataset_id
    assert handle.schema_version == record.schema_version
    assert handle.content_hash == record.content_hash
    assert handle.created_at == record.created_at
    assert handle.row_count == record.row_count
    assert handle.format == record.format


def test_get_dataset_raises_for_unknown_id():
    """get_dataset must raise KeyError for an unrecognised dataset_id."""
    backend = MemoryBackend()
    with pytest.raises(KeyError):
        backend.get_dataset("no-such-id")


def test_list_datasets_most_recent_first():
    """list_datasets must return datasets newest-first."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id, rows=1)
    r2 = _write(backend, run_id, rows=2)

    records = backend.list_datasets()

    assert records[0].dataset_id == r2.dataset_id
    assert records[1].dataset_id == r1.dataset_id


def test_list_datasets_limit():
    """list_datasets must honour the limit parameter."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets(limit=2)) == 2


def test_list_datasets_filter_provider():
    """list_datasets must filter by provider when the argument is given."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    _write(backend, run_id, provider="yfinance")
    _write(backend, run_id, provider="marketdata")

    results = backend.list_datasets(provider="yfinance")

    assert len(results) == 1
    assert results[0].provider == "yfinance"


def test_list_datasets_empty():
    """list_datasets on a fresh backend must return an empty list."""
    assert MemoryBackend().list_datasets() == []


# ---------------------------------------------------------------------------
# Ticker results
# ---------------------------------------------------------------------------

def test_record_ticker_result_stored():
    """record_ticker_result must persist the result under the run_id."""
    backend = MemoryBackend()
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

    stored = backend._ticker_results[run_id]  # pylint: disable=protected-access
    assert len(stored) == 1
    assert stored[0].ticker == "TSLA"
    assert stored[0].kept_row_count == 40


# ---------------------------------------------------------------------------
# Artifact write
# ---------------------------------------------------------------------------

def test_write_artifact_roundtrip():
    """write_artifact must return an ArtifactRecord with a valid content_hash."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    payload = ArtifactWrite(
        artifact_type="debug_payload", content=b"hello", filename="debug.json"
    )

    record = backend.write_artifact(run_id, payload)

    assert record.run_id == run_id
    assert record.artifact_type == "debug_payload"
    assert len(record.content_hash) == 64
    assert "debug.json" in record.location


# ---------------------------------------------------------------------------
# Run lifecycle transitions
# ---------------------------------------------------------------------------

def test_finalize_run_sets_status_complete():
    """finalize_run must update status, finished_at, and clear error_summary."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    backend.finalize_run(run_id, RunSummary(status="complete"))

    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "complete"
    assert run.finished_at is not None
    assert run.error_summary is None


def test_fail_run_sets_status_and_error():
    """fail_run must mark the run as failed and store the error message."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    backend.fail_run(run_id, "provider timeout")

    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "failed"
    assert run.error_summary == "provider timeout"
    assert run.finished_at is not None


def test_write_dataset_links_run_to_dataset_id():
    """write_dataset must update the run record's dataset_id field."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    assert backend._runs[run_id].dataset_id == record.dataset_id  # pylint: disable=protected-access


def test_content_hash_is_deterministic():
    """Identical DataFrames written twice must produce the same content_hash."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    def make_write():
        return DatasetWrite(data=df.copy(), provider="yfinance", schema_version=1)

    r1 = backend.write_dataset(run_id, make_write())
    r2 = backend.write_dataset(run_id, make_write())

    assert r1.content_hash == r2.content_hash


def test_schema_version_constant_is_positive_int():
    """SCHEMA_VERSION must be importable from opx and be a positive integer."""
    assert isinstance(SCHEMA_VERSION, int) and SCHEMA_VERSION >= 1


# ---------------------------------------------------------------------------
# get_run
# ---------------------------------------------------------------------------

def test_get_run_returns_record():
    """get_run must return the RunRecord via the public API."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context(provider="marketdata"))

    run = backend.get_run(run_id)

    assert run.run_id == run_id
    assert run.provider == "marketdata"
    assert run.status == "running"
    assert run.finished_at is None


def test_get_run_raises_for_unknown_id():
    """get_run must raise KeyError for a run_id that was never created."""
    backend = MemoryBackend()
    with pytest.raises(KeyError):
        backend.get_run("no-such-run")


# ---------------------------------------------------------------------------
# list_datasets date range filters
# ---------------------------------------------------------------------------

def test_list_datasets_since_excludes_older_records():
    """list_datasets(since=T) must exclude records created before T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    future = record.created_at + timedelta(seconds=1)
    results = backend.list_datasets(since=future)

    assert results == []


def test_list_datasets_since_includes_records_at_boundary():
    """list_datasets(since=T) must include a record created exactly at T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    results = backend.list_datasets(since=record.created_at)

    assert len(results) == 1
    assert results[0].dataset_id == record.dataset_id


def test_list_datasets_until_excludes_newer_records():
    """list_datasets(until=T) must exclude records created after T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    past = record.created_at - timedelta(seconds=1)
    results = backend.list_datasets(until=past)

    assert results == []


def test_list_datasets_until_includes_records_at_boundary():
    """list_datasets(until=T) must include a record created exactly at T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    results = backend.list_datasets(until=record.created_at)

    assert len(results) == 1
    assert results[0].dataset_id == record.dataset_id


# ---------------------------------------------------------------------------
# count_runs_today
# ---------------------------------------------------------------------------

def test_count_runs_today_counts_same_provider_only():
    """count_runs_today must count runs for the given provider, not others."""
    backend = MemoryBackend()
    backend.create_run(_make_context(provider="marketdata"))
    backend.create_run(_make_context(provider="marketdata"))
    backend.create_run(_make_context(provider="yfinance"))

    assert backend.count_runs_today("marketdata") == 2
    assert backend.count_runs_today("yfinance") == 1


def test_count_runs_today_returns_zero_when_no_runs():
    """count_runs_today must return 0 when no runs exist for that provider."""
    backend = MemoryBackend()
    assert backend.count_runs_today("marketdata") == 0

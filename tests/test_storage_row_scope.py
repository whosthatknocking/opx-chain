"""Provider-neutral row-scope publication and durable-load contract tests."""
# pylint: disable=duplicate-code,missing-function-docstring

from dataclasses import replace
from pathlib import Path

import pytest

from conftest import make_option_chain_frame
from opx_chain import SCHEMA_VERSION
from opx_chain.integrity import (
    OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
    OptionChainRowScope,
    OptionChainRowScopeIntegrityError,
    OptionChainRowScopeStatus,
)
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.models import (
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
)
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _memory_backend(_tmp_path: Path):
    return MemoryBackend()


def _filesystem_backend(tmp_path: Path):
    return FilesystemBackend(tmp_path / "runs", tmp_path / "debug")


def _sqlite_backend(tmp_path: Path):
    return SqliteIndexedBackend(
        tmp_path / "chain.db",
        tmp_path / "runs",
        tmp_path / "debug",
    )


BACKENDS = (_memory_backend, _filesystem_backend, _sqlite_backend)


def _scope(*, kept: int = 2) -> OptionChainRowScope:
    return OptionChainRowScope(
        schema_version=OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
        post_download_filters_enabled=False,
        max_expiration_weeks=34,
        normalized_row_count=kept,
        kept_row_count=kept,
        filtered_row_count=0,
        ticker_count=1,
    )


def _publish(backend, *, warning=None):
    run_id = backend.create_run(
        RunContext("synthetic-provider", ("SYNTH",), "config", "positions")
    )
    backend.record_ticker_result(
        run_id,
        TickerFetchResult(
            ticker="SYNTH",
            raw_row_count=2,
            normalized_row_count=2,
            kept_row_count=2,
            filtered_row_count=0,
            expiration_count=1,
            status="ok_with_warnings" if warning else "ok",
            error_summary=warning,
        ),
    )
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            make_option_chain_frame(
                rows=2,
                ticker="SYNTH",
                provider="synthetic-provider",
            ),
            "synthetic-provider",
            SCHEMA_VERSION,
            row_scope=_scope(),
        ),
    )
    backend.finalize_run(run_id, RunSummary("complete"))
    return record


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_quote_warning_survives_publication_and_validated_reuse(backend_factory, tmp_path):
    backend = backend_factory(tmp_path)
    evidence = '{"quarantined_count":1,"affected_quotes":[{"bid":0.01,"ask":0}]}'
    record = _publish(backend, warning=evidence)
    loaded = backend.load_validated_option_chain_dataset(record.dataset_id)
    assert loaded.integrity.status == "valid"
    result = backend.get_ticker_results(record.run_id)[0]
    assert result.status == "ok_with_warnings"
    assert result.error_summary == evidence


def test_row_scope_is_strict_and_count_conserving():
    serialized = _scope().to_dict()
    assert OptionChainRowScope.from_dict(serialized) == _scope()
    with pytest.raises(ValueError, match="fields are malformed"):
        OptionChainRowScope.from_dict({**serialized, "extra": True})
    with pytest.raises(ValueError, match="must equal"):
        replace(_scope(), normalized_row_count=3)
    with pytest.raises(ValueError, match="must be zero"):
        replace(_scope(), filtered_row_count=1, normalized_row_count=3)


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_current_row_scope_round_trips_on_handle_and_validated_load(
    backend_factory,
    tmp_path,
):
    backend = backend_factory(tmp_path)
    record = _publish(backend)

    handle = backend.get_dataset(record.dataset_id)
    loaded = backend.load_validated_option_chain_dataset(record.dataset_id)

    assert handle.row_scope_status is OptionChainRowScopeStatus.AVAILABLE
    assert handle.row_scope == _scope()
    assert loaded.handle.row_scope_status is OptionChainRowScopeStatus.AVAILABLE
    assert loaded.handle.row_scope == _scope()


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_legacy_row_scope_absence_remains_explicitly_unknown(
    backend_factory,
    tmp_path,
):
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(
        RunContext("synthetic-provider", ("SYNTH",), "config", "positions")
    )
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            make_option_chain_frame(
                rows=2,
                ticker="SYNTH",
                provider="synthetic-provider",
            ),
            "synthetic-provider",
            SCHEMA_VERSION,
        ),
    )
    backend.finalize_run(run_id, RunSummary("complete"))

    loaded = backend.load_validated_option_chain_dataset(record.dataset_id)
    assert loaded.handle.row_scope_status is OptionChainRowScopeStatus.UNKNOWN
    assert loaded.handle.row_scope is None


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_validated_load_rejects_row_scope_ticker_total_mismatch(
    backend_factory,
    tmp_path,
):
    backend = backend_factory(tmp_path)
    record = _publish(backend)
    run_id = record.run_id
    backend.record_ticker_result(
        run_id,
        TickerFetchResult(
            ticker="EXTRA",
            raw_row_count=0,
            normalized_row_count=0,
            kept_row_count=0,
            filtered_row_count=0,
            expiration_count=0,
            status="skipped",
        ),
    )

    with pytest.raises(OptionChainRowScopeIntegrityError, match="ticker_count"):
        backend.load_validated_option_chain_dataset(record.dataset_id)

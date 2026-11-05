"""Tests for the storage-enabled branches of fetcher.py and check_positions.py."""
# pylint: disable=duplicate-code,too-many-lines

import builtins
from contextlib import ExitStack, nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import hashlib
import json

import pandas as pd
import pytest
from conftest import make_option_chain_frame, make_runtime_config
from opx_chain._integrity_validation import validate_option_chain_frame
from opx_chain.config_coercion import ConfigError
from opx_chain.fetcher import acquire_fetcher_lock, release_fetcher_lock
from opx_chain.integrity import (
    OptionChainDataIntegrityError,
    OptionChainIntegrityBoundary,
)
from opx_chain.providers.base import ProviderQuotaError
from opx_chain.runlog import logger_name
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend
from opx_chain.valuation_evidence import parse_option_valuation_evidence
from opx_chain.validate import ValidationFinding


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticker_df(ticker: str = "TSLA") -> pd.DataFrame:
    frame = make_option_chain_frame(
        rows=2,
        ticker=ticker,
        expiration="2026-06-20",
    )
    frame["passes_primary_screen"] = True
    return frame


def _fetcher_patches(tmp_path: Path, config, backend, ticker_df=None, validation_findings=None):
    """Return a list of patch context managers for a minimal fetcher run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    ticker_fetch_patch = None
    if ticker_df is None:
        ticker_fetch_patch = patch.object(
            fetcher,
            "fetch_ticker_option_chain",
            side_effect=lambda ticker, *args, **kwargs: _make_ticker_df(ticker),
        )
    else:
        ticker_fetch_patch = patch.object(
            fetcher,
            "fetch_ticker_option_chain",
            return_value=ticker_df,
        )
    if validation_findings is None:
        validation_findings = []

    (tmp_path / "output").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    return [
        patch.object(fetcher, "RUNS_DIR", tmp_path / "output"),
        nullcontext(tmp_path / "logs"),
        patch.object(fetcher, "FETCHER_LOCK_PATH", tmp_path / "logs" / "fetcher.lock"),
        patch.object(fetcher, "acquire_fetcher_lock", return_value=MagicMock()),
        patch.object(fetcher, "release_fetcher_lock"),
        patch.object(fetcher, "get_runtime_config", return_value=config),
        patch.object(fetcher, "set_runtime_config_override"),
        patch.object(fetcher, "create_run_logger",
                     return_value=(MagicMock(), tmp_path / "logs" / "run.log")),
        patch.object(fetcher, "load_positions", return_value=MagicMock(
            stock_tickers=set(), option_keys=set(), empty=True
        )),
        ticker_fetch_patch,
        patch.object(fetcher, "validate_export_frame", return_value=validation_findings),
        patch.object(fetcher, "get_storage_backend", return_value=backend),
    ]


# ---------------------------------------------------------------------------
# fetcher storage wiring
# ---------------------------------------------------------------------------

def test_fetcher_calls_write_dataset_when_storage_enabled(tmp_path: Path):
    """When storage is enabled, fetcher must call write_dataset after write_options_csv."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    datasets = backend.list_datasets()
    assert len(datasets) == 1


def test_run_fetch_returns_the_exact_published_dataset_handle(tmp_path: Path):
    """Programmatic fetch success returns the handle written by that attempt."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for fetcher_patch in patches:
            stack.enter_context(fetcher_patch)
        handle = fetcher.run_fetch()

    assert handle is not None
    assert handle.dataset_id == backend.list_datasets()[0].dataset_id


@pytest.mark.parametrize("backend_name", ["filesystem", "sqlite"])
@pytest.mark.parametrize("format_name", ["csv", "parquet"])
def test_fetch_always_saves_bound_valuation_receipt(tmp_path, backend_name, format_name):
    """Normal publication retains receipts even when debug dumps are disabled."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    if backend_name == "sqlite":
        backend = SqliteIndexedBackend(tmp_path / "chain.db", tmp_path / "runs", tmp_path / "debug")
    else:
        backend = FilesystemBackend(tmp_path / "runs", tmp_path / "debug")
    config = make_runtime_config(storage_enabled=True, storage_dataset_format=format_name,
                                 debug_dump_provider_payload=False, storage_also_write_csv=False)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_bytes(b"Symbol\nTSLA\n")
    with ExitStack() as stack:
        for item in _fetcher_patches(tmp_path, config, backend):
            stack.enter_context(item)
        handle = fetcher.run_fetch(positions_path=positions_file)
    dataset = backend.load_validated_option_chain_dataset(handle.dataset_id)
    receipt_path = Path(handle.location).parent.parent / f"{handle.dataset_id}.valuation.json"
    receipt = parse_option_valuation_evidence(receipt_path.read_bytes(), dataset)
    positions_hash = hashlib.sha256(positions_file.read_bytes()).hexdigest()
    assert receipt["positions_content_hash"] == positions_hash
    assert (receipt_path.parent / "positions.csv").read_bytes() == positions_file.read_bytes()
    assert len(receipt["rows"]) == len(dataset.frame)


def test_receipt_write_failure_prevents_completed_publication(tmp_path):
    """A dataset with missing replay evidence must never become discoverable."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    original = backend.write_artifact

    def write(run_id, artifact):
        if artifact.filename.endswith(".valuation.json"):
            raise OSError("receipt disk unavailable")
        return original(run_id, artifact)

    with ExitStack() as stack:
        for item in _fetcher_patches(tmp_path, config, backend):
            stack.enter_context(item)
        stack.enter_context(patch.object(backend, "write_artifact", side_effect=write))
        with pytest.raises(OSError, match="receipt disk"):
            fetcher.run_fetch()
    assert backend.list_datasets() == []


def test_positions_receipt_uses_input_frozen_before_acquisition(tmp_path):
    """A later portfolio upload must not replace the fetch's saved holdings."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    backend = FilesystemBackend(tmp_path / "runs", tmp_path / "debug")
    config = make_runtime_config(storage_enabled=True, storage_also_write_csv=False)
    positions_file = tmp_path / "positions.csv"
    original = b"Symbol\nTSLA\n"
    positions_file.write_bytes(original)

    def fetch(ticker, *_args, **_kwargs):
        positions_file.write_bytes(b"Symbol\nORCL\n")
        return _make_ticker_df(ticker)

    with ExitStack() as stack:
        for item in _fetcher_patches(tmp_path, config, backend):
            stack.enter_context(item)
        stack.enter_context(patch.object(fetcher, "fetch_ticker_option_chain", side_effect=fetch))
        handle = fetcher.run_fetch(positions_path=positions_file)
    receipt_path = Path(handle.location).parent.parent / f"{handle.dataset_id}.valuation.json"
    receipt = parse_option_valuation_evidence(receipt_path.read_bytes(),
        backend.load_validated_option_chain_dataset(handle.dataset_id))
    assert receipt["positions_content_hash"] == hashlib.sha256(original).hexdigest()
    assert (receipt_path.parent / "positions.csv").read_bytes() == original


def test_positions_change_during_parsing_prevents_fetch(tmp_path):
    """A load-time race must fail before any provider or publication work."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    backend = MemoryBackend()
    positions_file = tmp_path / "positions.csv"
    positions_file.write_bytes(b"Symbol\nTSLA\n")

    def load(_path):
        positions_file.write_bytes(b"Symbol\nORCL\n")
        return MagicMock()

    with ExitStack() as stack:
        mocks = [stack.enter_context(item) for item in
                 _fetcher_patches(tmp_path, make_runtime_config(storage_enabled=True), backend)]
        stack.enter_context(patch.object(fetcher, "load_positions", side_effect=load))
        with pytest.raises(ValueError, match="positions changed"):
            fetcher.run_fetch(positions_path=positions_file)
    mocks[9].assert_not_called()
    assert backend.list_datasets() == []


def test_csv_only_publication_retains_receipt_and_positions(tmp_path):
    """CSV-only users receive the same durable source/quantity evidence."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    backend = None
    config = make_runtime_config(storage_enabled=False, debug_dump_provider_payload=False)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_bytes(b"Symbol\nTSLA\n")
    with ExitStack() as stack:
        for item in _fetcher_patches(tmp_path, config, backend):
            stack.enter_context(item)
        fetcher.run_fetch(positions_path=positions_file)
    latest = tmp_path / "output" / "options_engine_output_latest.csv"
    receipt = json.loads(latest.with_suffix(".valuation.json").read_bytes())
    assert receipt["dataset_content_hash"] == hashlib.sha256(latest.read_bytes()).hexdigest()
    positions_hash = hashlib.sha256(positions_file.read_bytes()).hexdigest()
    assert receipt["positions_content_hash"] == positions_hash
    assert latest.with_suffix(".positions.csv").read_bytes() == positions_file.read_bytes()


def test_compatibility_csv_failure_does_not_retract_published_handle(tmp_path: Path):
    """A post-publication compatibility-copy error remains operational only."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, storage_also_write_csv=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for fetcher_patch in patches:
            stack.enter_context(fetcher_patch)
        stack.enter_context(
            patch.object(
                fetcher,
                "_write_validated_csv_artifacts",
                side_effect=OSError("compatibility disk unavailable"),
            )
        )
        handle = fetcher.run_fetch()

    assert handle is not None
    assert backend.get_dataset(handle.dataset_id).dataset_id == handle.dataset_id


def test_integrity_failure_is_recorded_and_never_published(tmp_path: Path):
    """Fatal integrity summaries survive isolation as failed-run diagnostics."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)
    invalid = make_option_chain_frame(rows=1, ticker="TEST", expiration="2026-06-20")
    invalid.loc[0, "option_type"] = "put"
    with pytest.raises(OptionChainDataIntegrityError) as captured:
        validate_option_chain_frame(
            invalid,
            boundary=OptionChainIntegrityBoundary.PRE_FILTER,
        )

    with ExitStack() as stack:
        for fetcher_patch in patches:
            stack.enter_context(fetcher_patch)
        stack.enter_context(
            patch.object(
                fetcher,
                "fetch_ticker_option_chain",
                side_effect=captured.value,
            )
        )
        with pytest.raises(OptionChainDataIntegrityError):
            fetcher.run_fetch()

    assert not backend.list_datasets()
    run = next(iter(backend._runs.values()))  # pylint: disable=protected-access
    assert run.status == "failed"
    records = backend._validations[run.run_id]  # pylint: disable=protected-access
    assert records[0].severity == "error"
    assert records[0].code == "CONTRACT_IDENTITY_MISMATCH"


def test_fetcher_records_fetch_row_counts_from_dataframe_attrs(tmp_path: Path):
    """Storage ticker metadata must preserve raw and normalized fetch counts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    ticker_df = _make_ticker_df()
    ticker_df.attrs["raw_row_count"] = 5
    ticker_df.attrs["normalized_row_count"] = 3
    ticker_df.attrs["filtered_row_count"] = 1
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=ticker_df)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    ticker_result = backend._ticker_results[run_id][0]  # pylint: disable=protected-access
    assert ticker_result.raw_row_count == 5
    assert ticker_result.normalized_row_count == 3
    assert ticker_result.kept_row_count == 2
    assert ticker_result.filtered_row_count == 1


def test_fetcher_records_ticker_error_status_from_dataframe_attrs(tmp_path: Path):
    """Per-ticker fetch failures must persist as errors, not skipped tickers."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("BAD", "GOOD"))
    error_df = pd.DataFrame()
    error_df.attrs["fetch_status"] = "error"
    error_df.attrs["fetch_error_summary"] = "RuntimeError: provider exploded for BAD"
    ok_df = _make_ticker_df("GOOD")
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=ok_df)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], \
         patch.object(fetcher, "fetch_ticker_option_chain", side_effect=[error_df, ok_df]), \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    ticker_results = backend._ticker_results[run_id]  # pylint: disable=protected-access
    by_ticker = {result.ticker: result for result in ticker_results}
    assert by_ticker["BAD"].status == "error"
    assert by_ticker["BAD"].error_summary == "RuntimeError: provider exploded for BAD"
    assert by_ticker["GOOD"].status == "ok"


def test_fetcher_records_validation_findings_when_storage_enabled(tmp_path: Path):
    """Storage-backed fetch runs must persist grouped validation summaries."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    findings = [
        ValidationFinding(
            severity="warning",
            code="MISSING_FIELD",
            message="bid is missing",
            row_index=0,
            contract_symbol="TSLA260620C00100000",
            field="bid",
        ),
        ValidationFinding(
            severity="warning",
            code="MISSING_FIELD",
            message="ask is missing",
            row_index=1,
            field="ask",
        ),
        ValidationFinding(
            severity="error",
            code="DUPLICATE_CONTRACT",
            message="duplicate contract row",
            contract_symbol="TSLA260620C00100000",
        ),
    ]
    patches = _fetcher_patches(tmp_path, config, backend, validation_findings=findings)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    records = {
        (record.severity, record.code): record
        for record in backend._validations[run_id]  # pylint: disable=protected-access
    }
    assert records[("warning", "MISSING_FIELD")].count == 2
    assert records[("error", "DUPLICATE_CONTRACT")].count == 1
    assert '"field": "bid"' in records[("warning", "MISSING_FIELD")].sample


def test_fetcher_finalizes_run_on_success(tmp_path: Path):
    """Successful fetch must finalize the run with status=complete."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        fetcher.main([])

    run_id = backend.list_datasets()[0].run_id
    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "complete"


def test_fetcher_ingests_iv_history_after_dataset_publication(tmp_path: Path):
    """Successful storage-backed fetches should ingest the exact published dataset."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAA",))
    ticker_df = pd.concat(
        (
            make_option_chain_frame(rows=1, ticker="AAA", expiration="2026-06-20"),
            make_option_chain_frame(rows=1, ticker="BBB", expiration="2026-06-20"),
        ),
        ignore_index=True,
    )
    ticker_df["passes_primary_screen"] = True
    ticker_df["implied_volatility"] = [0.25, 0.30]
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=ticker_df)
    backfill_result = SimpleNamespace(rows=(
        SimpleNamespace(status="INGESTED", source_rows=2, stored_rows=7),
    ))

    with ExitStack() as stack:
        for patcher in patches:
            stack.enter_context(patcher)
        mock_backfill = stack.enter_context(
            patch.object(fetcher, "run_iv_history_backfill", return_value=backfill_result)
        )
        result = fetcher.main([])

    assert result == 0
    dataset = backend.list_datasets()[0]
    mock_backfill.assert_called_once()
    call_kwargs = mock_backfill.call_args.kwargs
    assert call_kwargs["providers"] == ("yfinance",)
    assert call_kwargs["tickers"] == ("AAA", "BBB")
    assert call_kwargs["dataset_ids"] == (dataset.dataset_id,)
    assert call_kwargs["config"] is config
    assert call_kwargs["storage"] is backend


def test_fetcher_keeps_run_complete_when_iv_history_ingest_fails(
    tmp_path: Path,
    capsys,
):
    """Automatic IV-history ingestion is advisory and must not fail a fetch run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for patcher in patches:
            stack.enter_context(patcher)
        stack.enter_context(
            patch.object(
                fetcher,
                "run_iv_history_backfill",
                side_effect=RuntimeError("iv store unavailable"),
            )
        )
        result = fetcher.main([])

    stdout = capsys.readouterr().out
    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "complete"
    assert "IV history: skipped" in stdout
    assert "iv store unavailable" in stdout


def test_fetcher_iv_history_ingest_blank_error_has_summary(
    tmp_path: Path,
    capsys,
):
    """Blank advisory IV-ingest errors should not crash fetch completion."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class BlankIngestError(Exception):
        """Ingest error whose string representation is blank."""

        def __str__(self) -> str:
            """Return a deliberately blank message."""
            return ""

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for patcher in patches:
            stack.enter_context(patcher)
        stack.enter_context(
            patch.object(
                fetcher,
                "run_iv_history_backfill",
                side_effect=BlankIngestError(),
            )
        )
        result = fetcher.main([])

    stdout = capsys.readouterr().out
    assert result == 0
    assert backend.list_datasets()[0].run_id in backend._runs  # pylint: disable=protected-access
    assert "IV history: skipped" in stdout
    assert "BlankIngestError" in stdout


def test_fetcher_snapshots_positions_only_after_success(tmp_path: Path):
    """Successful storage-backed runs must persist positions.csv as a sidecar."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    artifacts = backend._artifacts[run_id]  # pylint: disable=protected-access
    sidecars = [artifact for artifact in artifacts if artifact.artifact_type == "sidecar"
                and artifact.location.endswith("/positions.csv")]
    assert len(sidecars) == 1
    assert sidecars[0].location.endswith("/positions.csv")


def test_fetcher_records_run_log_reference_artifact(tmp_path: Path):
    """Successful storage-backed runs must register the shared run log reference."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    artifacts = backend._artifacts[run_id]  # pylint: disable=protected-access
    run_logs = [artifact for artifact in artifacts if artifact.artifact_type == "run_log"]
    assert len(run_logs) == 1
    assert run_logs[0].location.endswith("/run_log_reference.json")


def test_fetcher_logs_storage_run_id_on_start(tmp_path: Path):
    """The shared run log must use the storage UUID, not a parallel timestamp id."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(patcher) for patcher in patches]
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    logger = mocks[7].return_value[0]  # create_run_logger return value
    run_started = [
        call for call in logger.info.call_args_list
        if call.args and call.args[0].startswith("run_started ")
    ]
    assert len(run_started) == 1
    assert run_started[0].args[1] == run_id


def test_fetcher_fails_run_on_no_data(tmp_path: Path):
    """When no data is fetched, the run must be marked as failed."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=pd.DataFrame())

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 1
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_fails_run_when_pre_chain_price_context_fails(tmp_path: Path):
    """Full-run price-context failures must still leave a failed run record."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, price_context_enable=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(patcher) for patcher in patches]
        stack.enter_context(
            patch.object(
                fetcher,
                "_run_price_context_fetch",
                side_effect=RuntimeError("price context exploded"),
            )
        )
        result = fetcher.main([])

    assert result == 1
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert runs[0].error_summary == "price context exploded"
    mocks[9].assert_not_called()


def test_fetcher_does_not_snapshot_positions_when_run_fails(tmp_path: Path):
    """Failed runs must not leave behind a positions sidecar artifact."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=pd.DataFrame())

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert not backend._artifacts  # pylint: disable=protected-access


def test_fetcher_quota_error_fails_run_without_writing_dataset(tmp_path: Path):
    """A mid-loop ProviderQuotaError must mark the run failed and write no dataset."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[11]:
        with patch.object(
            fetcher, "fetch_ticker_option_chain",
            side_effect=ProviderQuotaError("daily request limit reached"),
        ):
            result = fetcher.main([])

    assert result == 1
    assert not backend.list_datasets()
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert "request limit" in (runs[0].error_summary or "")


def test_fetcher_artifact_failure_fails_without_writing_dataset(tmp_path: Path):
    """Storage artifact failures must not publish a dataset for a failed run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class ArtifactFailingBackend(MemoryBackend):
        """Memory backend that fails before the dataset commit point."""

        def __init__(self):
            super().__init__()
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            raise OSError(f"cannot write {artifact.filename}")

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = ArtifactFailingBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_rolls_back_partial_artifacts_before_dataset_publication(tmp_path: Path):
    """A second artifact failure must remove any earlier run artifacts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class SecondArtifactFailingBackend(MemoryBackend):
        """Memory backend that fails after the first artifact is written."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                raise OSError(f"cannot write {artifact.filename}")
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = SecondArtifactFailingBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_rolls_back_partial_artifacts_on_keyboard_interrupt(tmp_path: Path):
    """KeyboardInterrupt before dataset publication must remove earlier artifacts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class InterruptingArtifactBackend(MemoryBackend):
        """Memory backend that interrupts after writing the first artifact."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                raise KeyboardInterrupt
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = InterruptingArtifactBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 130
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "interrupted"


def test_fetcher_rolls_back_partial_artifacts_on_sigterm(tmp_path: Path, monkeypatch):
    """SIGTERM should route through the interrupted cleanup path."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    previous_handler = object()
    signal_calls = []
    captured = {}

    def fake_signal(signum, handler):
        signal_calls.append((signum, handler))
        captured["handler"] = handler

    monkeypatch.setattr(fetcher.signal, "getsignal", lambda signum: previous_handler)
    monkeypatch.setattr(fetcher.signal, "signal", fake_signal)

    class SigtermArtifactBackend(MemoryBackend):
        """Memory backend that triggers the installed SIGTERM handler."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                captured["handler"](fetcher.signal.SIGTERM, None)
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = SigtermArtifactBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 130
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "interrupted"
    assert signal_calls[0] == (
        fetcher.signal.SIGTERM,
        fetcher._raise_keyboard_interrupt_on_sigterm,  # pylint: disable=protected-access
    )
    assert signal_calls[-1] == (fetcher.signal.SIGTERM, previous_handler)


def test_fetcher_skips_storage_when_disabled(tmp_path: Path):
    """When storage is disabled, write_dataset must never be called."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=False)
    patches = _fetcher_patches(tmp_path, config, backend=None)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    assert not backend.list_datasets()


# ---------------------------------------------------------------------------
# check_positions storage wiring
# ---------------------------------------------------------------------------

def test_check_positions_uses_storage_when_enabled(tmp_path: Path):
    """opx-check must use list_datasets when storage is enabled."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    runs_dir = tmp_path / "runs"
    artifact = runs_dir / "run-1" / "output" / "ds.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )
    record = DatasetRecord(
        dataset_id="ds-id", run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance", schema_version=1, row_count=1,
        format="csv", location=str(artifact), content_hash="a" * 64,
    )
    mock_backend = MagicMock()
    setattr(mock_backend, "_runs_dir", runs_dir)
    mock_backend.list_datasets.return_value = [record]
    mock_backend.load_validated_option_chain_dataset.return_value = SimpleNamespace(
        handle=SimpleNamespace(location=record.location),
        frame=pd.read_csv(artifact),
    )

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text(
        "Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8"
    )

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0


def test_check_positions_uses_validated_loader_for_newest_format(tmp_path: Path):
    """opx-check is format-neutral because storage returns a validated frame."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    runs_dir = tmp_path / "runs"
    parquet_path = runs_dir / "run-1" / "output" / "parquet-id.parquet"
    parquet_path.parent.mkdir(parents=True)
    parquet_path.write_bytes(b"fake-parquet")
    parquet_record = DatasetRecord(
        dataset_id="parquet-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=5,
        format="parquet",
        location=str(parquet_path),
        content_hash="a" * 64,
    )
    csv_path = runs_dir / "run-1" / "output" / "csv-id.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_record = DatasetRecord(
        dataset_id="csv-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(csv_path),
        content_hash="b" * 64,
    )
    csv_path.write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    setattr(mock_backend, "_runs_dir", runs_dir)
    mock_backend.list_datasets.return_value = [parquet_record, csv_record]
    mock_backend.load_validated_option_chain_dataset.return_value = SimpleNamespace(
        handle=SimpleNamespace(location=parquet_record.location),
        frame=pd.DataFrame(),
    )

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0
    mock_backend.list_datasets.assert_called_once_with(limit=100)
    mock_backend.load_validated_option_chain_dataset.assert_called_once_with("parquet-id")


def test_check_positions_fails_closed_on_missing_latest_artifact(tmp_path: Path):
    """opx-check must not silently substitute an older artifact after load failure."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    stale_record = DatasetRecord(
        dataset_id="stale-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=5,
        format="csv",
        location="/old/workspace/output/stale-id.csv",
        content_hash="a" * 64,
    )
    runs_dir = tmp_path / "runs"
    current_path = runs_dir / "run-2" / "output" / "current-id.csv"
    current_path.parent.mkdir(parents=True)
    current_record = DatasetRecord(
        dataset_id="current-id",
        run_id="run-2",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(current_path),
        content_hash="b" * 64,
    )
    current_path.write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    setattr(mock_backend, "_runs_dir", runs_dir)
    mock_backend.list_datasets.return_value = [stale_record, current_record]
    mock_backend.load_validated_option_chain_dataset.side_effect = FileNotFoundError(
        stale_record.location
    )

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 1
    mock_backend.load_validated_option_chain_dataset.assert_called_once_with("stale-id")


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------

def test_dry_run_makes_no_api_calls_and_no_writes(tmp_path: Path):
    """--dry-run must not call fetch_ticker_option_chain or write any output."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        result = fetcher.main(["--dry-run"])

    assert result == 0
    mocks[3].assert_not_called()  # acquire_fetcher_lock
    mocks[4].assert_not_called()  # release_fetcher_lock
    mock_fetch = mocks[9]  # fetch_ticker_option_chain
    mock_fetch.assert_not_called()
    assert not backend.list_datasets()


def test_dry_run_logger_uses_stdlib_null_logger():
    """Dry-run logging should keep the complete stdlib logger method surface."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    logger = fetcher._dry_run_logger()  # pylint: disable=protected-access

    assert logger.name == logger_name("fetcher.dry_run")
    assert logger.propagate is False
    assert any(isinstance(handler, fetcher.logging.NullHandler) for handler in logger.handlers)
    logger.debug("debug calls should be supported")
    logger.critical("critical calls should be supported")
    logger.log(fetcher.logging.INFO, "generic log calls should be supported")


def test_fetcher_lock_blocks_second_holder(tmp_path: Path):
    """Fetcher locks must remain non-blocking without requiring fcntl imports."""
    lock_path = tmp_path / "fetcher.lock"
    first = acquire_fetcher_lock(lock_path)
    assert first is not None
    try:
        assert acquire_fetcher_lock(lock_path) is None
    finally:
        release_fetcher_lock(first, lock_path)
    assert lock_path.exists()
    second = acquire_fetcher_lock(lock_path)
    assert second is not None
    release_fetcher_lock(second, lock_path)


def test_dry_run_prints_would_fetch_summary(tmp_path: Path, capsys):
    """--dry-run must print the tickers it would fetch and storage backend class."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAPL", "TSLA"))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        fetcher.main(["--dry-run"])

    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out
    assert "AAPL" in captured.out
    assert "TSLA" in captured.out
    assert "Dry-run complete" in captured.out


# ---------------------------------------------------------------------------
# run_fetch API
# ---------------------------------------------------------------------------

def test_run_fetch_passes_positions_path(tmp_path: Path):
    """run_fetch must forward positions_path to load_positions."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    positions_file = tmp_path / "custom_positions.csv"
    positions_file.write_text("", encoding="utf-8")

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(positions_path=positions_file)

    mock_load = mocks[8]
    mock_load.assert_called_once()
    called_path = mock_load.call_args[0][0]
    assert called_path == positions_file.expanduser()


def test_run_fetch_accepts_string_positions_path(tmp_path: Path):
    """run_fetch(positions_path=...) should accept common string path callers."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    positions_file = tmp_path / "custom_positions.csv"
    positions_file.write_text("", encoding="utf-8")

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(positions_path=str(positions_file))

    mock_load = mocks[8]
    mock_load.assert_called_once()
    called_path = mock_load.call_args[0][0]
    assert called_path == positions_file.expanduser()


def test_run_fetch_emits_non_blocking_ticker_progress(tmp_path: Path):
    """Programmatic callers receive truthful completed/total ticker counts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAPL", "MSFT"))
    patches = _fetcher_patches(tmp_path, config, backend)
    events = []

    with ExitStack() as stack:
        for fetcher_patch in patches:
            stack.enter_context(fetcher_patch)
        fetcher.run_fetch(progress_callback=events.append)

    assert events == [
        fetcher.TickerFetchProgress("AAPL", 0, 2, "starting"),
        fetcher.TickerFetchProgress("AAPL", 1, 2, "completed"),
        fetcher.TickerFetchProgress("MSFT", 1, 2, "starting"),
        fetcher.TickerFetchProgress("MSFT", 2, 2, "completed"),
    ]


def test_run_fetch_ignores_progress_callback_failures(tmp_path: Path):
    """Progress instrumentation cannot abort a successful provider fetch."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAPL",))
    patches = _fetcher_patches(tmp_path, config, backend)

    def fail_progress(_progress):
        raise RuntimeError("progress sink unavailable")

    with ExitStack() as stack:
        mocks = [stack.enter_context(fetcher_patch) for fetcher_patch in patches]
        fetcher.run_fetch(progress_callback=fail_progress)

    logger = mocks[7].return_value[0]
    assert logger.warning.call_count == 2
    assert "ticker_progress_callback_failed" in logger.warning.call_args_list[0].args[0]


def test_run_fetch_rejects_non_callable_progress_callback(tmp_path: Path):
    """Malformed callback values fail before provider or storage work."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for fetcher_patch in patches:
            stack.enter_context(fetcher_patch)
        with pytest.raises(ConfigError, match="run_fetch.progress_callback"):
            fetcher.run_fetch(progress_callback=1)


def test_run_fetch_tickers_override_replaces_config_tickers(tmp_path: Path):
    """run_fetch(tickers=...) must use the supplied tickers, not config.tickers."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("NVDA", "MSFT"))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(tickers=("AAPL",))

    # set_runtime_config_override is called twice: once to set, once to clear (None)
    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    assert set_call[0][0].tickers == ("AAPL",)


@pytest.mark.parametrize(
    "bad_tickers",
    [
        "MSFT",
        b"MSFT",
        (),
        ("",),
        ("MSFT", 1),
        ("BAD/TICKER",),
        ("A0",),
        ("...",),
        ("ABCDEFGHIJK",),
        ("BRK..B",),
        ("A.",),
    ],
)
def test_run_fetch_tickers_override_rejects_malformed_shapes(tmp_path: Path, bad_tickers):
    """run_fetch(tickers=...) should reject scalar and malformed overrides."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("NVDA", "MSFT"))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match="run_fetch.tickers"):
            fetcher.run_fetch(tickers=bad_tickers)

    mocks[3].assert_not_called()  # acquire_fetcher_lock


def test_run_fetch_data_provider_override_replaces_config_provider(tmp_path: Path):
    """run_fetch(data_provider=...) must use the supplied provider for this run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        data_provider="yfinance",
        marketdata_api_token="secret",
    )
    frame = _make_ticker_df("TEST")
    frame["data_source"] = "marketdata"
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=frame)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(data_provider="marketdata")

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    assert set_call[0][0].data_provider == "marketdata"


def test_run_fetch_data_provider_override_accepts_massive_credentials(tmp_path: Path):
    """Paid-provider overrides should work when their credentials are present."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        data_provider="yfinance",
        massive_api_key="secret",
    )
    frame = _make_ticker_df("TEST")
    frame["data_source"] = "massive"
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=frame)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(data_provider="massive")

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    assert set_call[0][0].data_provider == "massive"


def test_run_fetch_data_provider_override_rejects_unknown_provider(tmp_path: Path):
    """Provider overrides should fail before opening a fetcher run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, data_provider="yfinance")
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        try:
            fetcher.run_fetch(data_provider="bad-provider")
        except ConfigError as exc:
            assert "unsupported data provider" in str(exc)
        else:  # pragma: no cover - defensive assertion branch
            raise AssertionError("expected invalid data provider to raise ConfigError")

    mocks[3].assert_not_called()  # acquire_fetcher_lock


@pytest.mark.parametrize(
    ("provider_name", "expected_field"),
    [
        ("marketdata", "providers.marketdata.api_token"),
        ("massive", "providers.massive.api_key"),
    ],
)
def test_run_fetch_data_provider_override_requires_credentials(
    tmp_path: Path,
    provider_name,
    expected_field,
):
    """Paid-provider overrides should fail with config errors before provider lookup."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, data_provider="yfinance")
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match=expected_field):
            fetcher.run_fetch(data_provider=provider_name)

    mocks[3].assert_not_called()  # acquire_fetcher_lock


def test_run_fetch_max_expiration_override_updates_derived_date(tmp_path: Path):
    """run_fetch(max_expiration_weeks=...) must keep the derived filter date in sync."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        max_expiration_weeks=14,
        max_expiration="2026-06-30",
    )
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(max_expiration_weeks=4)

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    active_config = set_call[0][0]
    assert active_config.max_expiration_weeks == 4
    assert active_config.max_expiration == "2026-04-17"


@pytest.mark.parametrize("enabled", [True, False])
def test_run_fetch_filter_override_is_scoped_and_persisted(tmp_path: Path, enabled: bool):
    """The programmatic filter override must drive config and durable row scope."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, enable_filters=not enabled)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        handle = fetcher.run_fetch(enable_filters=enabled)

    active_config = mocks[6].call_args_list[0][0][0]
    assert active_config.enable_filters is enabled
    assert handle.row_scope.post_download_filters_enabled is enabled


def test_run_fetch_filter_override_rejects_false_like_strings(tmp_path: Path):
    """Programmatic filter overrides accept only actual booleans."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match="run_fetch.enable_filters"):
            fetcher.run_fetch(enable_filters="false")

    mocks[3].assert_not_called()


def test_run_fetch_max_expiration_override_can_disable_filter(tmp_path: Path):
    """run_fetch(max_expiration_weeks=0) should disable the max-expiration filter."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        max_expiration_weeks=14,
        max_expiration="2026-06-30",
    )
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(max_expiration_weeks=0)

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    active_config = set_call[0][0]
    assert active_config.max_expiration_weeks == 0
    assert active_config.max_expiration is None


@pytest.mark.parametrize("bad_weeks", [True, 1.5, "4", -1])
def test_run_fetch_max_expiration_override_rejects_malformed_values(tmp_path: Path, bad_weeks):
    """Direct expiration overrides should use config-loader integer/range rules."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match="run_fetch.max_expiration_weeks"):
            fetcher.run_fetch(max_expiration_weeks=bad_weeks)

    mocks[3].assert_not_called()  # acquire_fetcher_lock


@pytest.mark.parametrize("bad_seconds", [False, 1.5, "3600", -1])
def test_run_fetch_stale_quote_seconds_rejects_malformed_values(tmp_path: Path, bad_seconds):
    """Direct staleness overrides should reject bool, fractional, string, and negative values."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match="run_fetch.stale_quote_seconds"):
            fetcher.run_fetch(stale_quote_seconds=bad_seconds)

    mocks[3].assert_not_called()  # acquire_fetcher_lock


@pytest.mark.parametrize(
    ("override_name", "kwargs"),
    [
        ("run_fetch.dry_run", {"dry_run": "false"}),
        ("run_fetch.price_context_only", {"price_context_only": "false"}),
        ("run_fetch.skip_events", {"skip_events": "false"}),
    ],
)
def test_run_fetch_boolean_overrides_reject_false_like_strings(
    tmp_path: Path,
    override_name,
    kwargs,
):
    """String booleans should not select run_fetch modes by raw truthiness."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        with pytest.raises(ConfigError, match=override_name):
            fetcher.run_fetch(**kwargs)

    mocks[3].assert_not_called()  # acquire_fetcher_lock


def test_run_fetch_skip_events_forwards_to_ticker_fetch(tmp_path: Path):
    """run_fetch(skip_events=True) should suppress event fetches in ticker work."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAPL",))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(skip_events=True)

    mock_fetch = mocks[9]
    mock_fetch.assert_called_once()
    assert mock_fetch.call_args.kwargs["skip_events"] is True


def test_run_fetch_dry_run_makes_no_api_calls_and_no_writes(tmp_path: Path):
    """run_fetch(dry_run=True) should match the CLI dry-run zero-call behavior."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(dry_run=True)

    mocks[3].assert_not_called()  # acquire_fetcher_lock
    mocks[4].assert_not_called()  # release_fetcher_lock
    mock_fetch = mocks[9]  # fetch_ticker_option_chain
    mock_fetch.assert_not_called()
    assert not backend.list_datasets()
    assert not list(backend._runs.values())  # pylint: disable=protected-access


def test_run_fetch_dry_run_checks_parquet_dependency_before_api_calls(tmp_path: Path):
    """Dry-run should fail fast when parquet output is configured without pyarrow."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    config = make_runtime_config(
        storage_enabled=True,
        storage_dataset_format="parquet",
        storage_dir=tmp_path,
    )
    fetcher_patches = _fetcher_patches(tmp_path, config, MemoryBackend())
    patches = [
        patcher
        for patcher in fetcher_patches
        if getattr(patcher, "attribute", None) != "get_storage_backend"
    ]
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pyarrow":
            raise ImportError("missing pyarrow")
        return original_import(name, *args, **kwargs)

    with ExitStack() as stack:
        mocks = {
            getattr(patcher, "attribute", ""): stack.enter_context(patcher)
            for patcher in patches
        }
        with patch("builtins.__import__", side_effect=fake_import):
            result = fetcher.main(["--dry-run"])

    mock_fetch = mocks["fetch_ticker_option_chain"]
    assert result == 1
    mock_fetch.assert_not_called()


def test_run_fetch_restores_existing_runtime_override(tmp_path: Path):
    """Programmatic one-off fetches should not clear an embedding caller override."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel
    from opx_chain.config import (  # pylint: disable=import-outside-toplevel
        get_runtime_config,
        set_runtime_config_override,
    )

    backend = MemoryBackend()
    outer_config = make_runtime_config(storage_enabled=True, tickers=("OUTER",))
    config = make_runtime_config(storage_enabled=True, tickers=("AAA",))
    patches = [
        patcher
        for patcher in _fetcher_patches(tmp_path, config, backend)
        if getattr(patcher, "attribute", None) != "set_runtime_config_override"
    ]
    set_runtime_config_override(outer_config)

    with ExitStack() as stack:
        for patcher in patches:
            stack.enter_context(patcher)
        fetcher.run_fetch(tickers=("AAPL",))

    assert get_runtime_config() is outer_config


def test_check_positions_falls_back_to_scan_when_disabled(tmp_path: Path):
    """opx-check must fall back to directory scanning when storage is disabled."""
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text(
        "Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8"
    )

    with (
        patch.object(cp, "get_storage_backend", return_value=None),
        patch.object(cp, "find_latest_output", return_value=None),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 1

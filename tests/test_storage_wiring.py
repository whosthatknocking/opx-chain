"""Tests for the storage-enabled branches of fetcher.py and check_positions.py."""
# pylint: disable=duplicate-code

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from conftest import make_runtime_config
from opx_chain.providers.base import ProviderQuotaError
from opx_chain.storage.memory import MemoryBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticker_df(ticker: str = "TSLA") -> pd.DataFrame:
    return pd.DataFrame({
        "underlying_symbol": [ticker] * 2,
        "strike": [100.0, 110.0],
        "expiration_date": ["2026-06-20", "2026-06-20"],
        "passes_primary_screen": [True, True],
    })


def _fetcher_patches(tmp_path: Path, config, backend, ticker_df=None):
    """Return a list of patch context managers for a minimal fetcher run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    if ticker_df is None:
        ticker_df = _make_ticker_df()

    (tmp_path / "output").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    return [
        patch.object(fetcher, "RUNS_DIR", tmp_path / "output"),
        patch.object(fetcher, "LOCKS_DIR", tmp_path / "logs"),
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
        patch.object(fetcher, "fetch_ticker_option_chain", return_value=ticker_df),
        patch.object(fetcher, "validate_export_frame", return_value=[]),
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
    assert len(artifacts) == 1
    assert artifacts[0].artifact_type == "sidecar"
    assert artifacts[0].location.endswith("/positions.csv")


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

    artifact = tmp_path / "ds.csv"
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
    mock_backend.list_datasets.return_value = [record]

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


def test_check_positions_prefers_csv_over_parquet_dataset(tmp_path: Path):
    """opx-check must skip parquet records and use the newest CSV dataset."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    parquet_record = DatasetRecord(
        dataset_id="parquet-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=5,
        format="parquet",
        location="/fake/output/parquet-id.parquet",
        content_hash="a" * 64,
    )
    csv_record = DatasetRecord(
        dataset_id="csv-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(tmp_path / "csv-id.csv"),
        content_hash="b" * 64,
    )
    (tmp_path / "csv-id.csv").write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    mock_backend.list_datasets.return_value = [parquet_record, csv_record]

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0
    mock_backend.list_datasets.assert_called_once_with(limit=100)


def test_check_positions_skips_records_with_missing_artifact(tmp_path: Path):
    """opx-check must skip storage records whose artifact file no longer exists."""
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
    current_record = DatasetRecord(
        dataset_id="current-id",
        run_id="run-2",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(tmp_path / "current-id.csv"),
        content_hash="b" * 64,
    )
    (tmp_path / "current-id.csv").write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    mock_backend.list_datasets.return_value = [stale_record, current_record]

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0


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
    mock_fetch = mocks[9]  # fetch_ticker_option_chain
    mock_fetch.assert_not_called()
    assert not backend.list_datasets()
    assert not list(backend._runs.values())  # pylint: disable=protected-access


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

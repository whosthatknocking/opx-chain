"""Entry-point tests for the console output emitted by the main fetch run."""

# pylint: disable=duplicate-code

from dataclasses import fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_runtime_config
import main
from opx_chain.config import RuntimeConfig, get_runtime_config as get_process_runtime_config
from opx_chain.fetcher import (
    _CONFIG_FINGERPRINT_EXCLUDED_FIELDS,
    _canonical_json_fingerprint,
    _config_fingerprint,
    _config_fingerprint_payload,
    _positions_fingerprint,
)
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.models import RunContext, RunSummary
from opx_chain.validate import validate_option_rows


class StubLogger:
    """Minimal logger stub that satisfies the main entrypoint contract."""

    def info(self, *_args, **_kwargs):
        """Accept info messages without side effects during tests."""
        return None

    def warning(self, *_args, **_kwargs):
        """Accept warning messages without side effects during tests."""
        return None

    def error(self, *_args, **_kwargs):
        """Accept error messages without side effects during tests."""
        return None


class CapturingLogger(StubLogger):
    """Logger stub that stores formatted info messages for assertions."""

    def __init__(self):
        self.info_messages = []

    def info(self, *args, **_kwargs):
        """Store formatted info messages emitted by the fetcher."""
        if not args:
            return None
        message = args[0]
        fmt_args = args[1:]
        if fmt_args:
            message = message % fmt_args
        self.info_messages.append(message)
        return None


def make_export_row(**overrides):
    """Build one minimal exported row for main-entrypoint tests."""
    row = {
        "data_source": "yfinance",
        "underlying_symbol": "AAA",
        "contract_symbol": "AAA260417C00100000",
        "option_type": "call",
        "expiration_date": "2026-04-17",
        "strike": 100.0,
        "underlying_price": 101.0,
        "bid": 1.0,
        "ask": 1.2,
    }
    row.update(overrides)
    return row


def make_run_context(**overrides):
    """Build a storage run context for main-entrypoint tests."""
    defaults = {
        "provider": "yfinance",
        "tickers": ("AAA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **overrides})


def test_positions_fingerprint_uses_canonical_parsed_positions(tmp_path: Path):
    """Cosmetic CSV differences must not change the positions fingerprint."""
    lf_positions = tmp_path / "positions_lf.csv"
    crlf_positions = tmp_path / "positions_crlf.csv"
    reordered_positions = tmp_path / "positions_reordered.csv"
    lf_positions.write_text(
        "Symbol,Quantity\n"
        "TSLA,100\n"
        "-NVDA260605P200,1\n",
        encoding="utf-8",
    )
    crlf_positions.write_bytes(
        b"Symbol,Quantity\r\n"
        b"TSLA,100\r\n"
        b"-NVDA260605P200,1\r\n"
    )
    reordered_positions.write_text(
        "Quantity,Symbol\n"
        "100,TSLA\n"
        "1,-NVDA260605P200\n",
        encoding="utf-8",
    )

    assert _positions_fingerprint(lf_positions) == _positions_fingerprint(crlf_positions)
    assert _positions_fingerprint(lf_positions) == _positions_fingerprint(reordered_positions)


def test_positions_fingerprint_changes_when_parsed_positions_change(tmp_path: Path):
    """Semantic portfolio changes must still change the positions fingerprint."""
    base_positions = tmp_path / "positions_base.csv"
    changed_positions = tmp_path / "positions_changed.csv"
    base_positions.write_text(
        "Symbol,Quantity\n"
        "TSLA,100\n"
        "-NVDA260605P200,1\n",
        encoding="utf-8",
    )
    changed_positions.write_text(
        "Symbol,Quantity\n"
        "TSLA,100\n"
        "-NVDA260605P210,1\n",
        encoding="utf-8",
    )

    assert _positions_fingerprint(base_positions) != _positions_fingerprint(changed_positions)


def test_canonical_json_fingerprint_rejects_non_finite_values():
    """Fetcher fingerprints must not admit NaN/Infinity JSON literals."""
    with pytest.raises(ValueError, match="Out of range float values"):
        _canonical_json_fingerprint({"value": float("nan")})


def test_main_prints_rows_written_after_saved(monkeypatch, capsys, tmp_path: Path):
    """Show the saved path first, then row count and file size details."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA", "BBB")),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )

    frames = {
        "AAA": pd.DataFrame([
            make_export_row(),
            make_export_row(
                contract_symbol="AAA260417C00110000",
                strike=110.0,
            ),
        ]),
        "BBB": pd.DataFrame([
            make_export_row(
                underlying_symbol="BBB",
                contract_symbol="BBB260417C00100000",
            )
        ]),
    }
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: frames[ticker]
        ),
    )

    exit_code = main.main()

    stdout = capsys.readouterr().out
    written_path = next(
        path
        for path in (tmp_path / "output").glob("options_engine_output_*.csv")
        if path.name != "options_engine_output_latest.csv"
    )
    assert exit_code == 0
    assert "Config:" in stdout
    assert "provider: yfinance" in stdout
    assert f"Saved: {written_path}" in stdout
    assert "rows=3  size=" in stdout
    assert stdout.index(f"Saved: {written_path}") < stdout.index("rows=3  size=")


def test_main_uses_storage_dir_for_side_csv_and_lock(monkeypatch, tmp_path: Path):
    """storage.dir should override side-write and lock paths, not just storage artifacts."""
    custom_data_dir = tmp_path / "custom-data"
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(
            tickers=("AAA",),
            storage_dir=custom_data_dir,
            storage_enabled=False,
        ),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame(
                [make_export_row()]
            )
        ),
    )

    assert main.main() == 0

    written_path = next(
        path
        for path in (custom_data_dir / "runs").glob("options_engine_output_*.csv")
        if path.name != "options_engine_output_latest.csv"
    )
    assert written_path.parent == custom_data_dir / "runs"
    assert (custom_data_dir / "runs" / "options_engine_output_latest.csv").exists()
    assert (custom_data_dir / "fetcher.lock").exists()


def test_storage_disabled_integrity_failure_preserves_previous_latest(
    monkeypatch,
    tmp_path: Path,
):
    """CSV-only publication fails closed before replacing the latest artifact."""
    runs_dir = tmp_path / "output"
    runs_dir.mkdir()
    latest = runs_dir / "options_engine_output_latest.csv"
    latest.write_text("previous", encoding="utf-8")
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", runs_dir)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), storage_enabled=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    invalid = make_export_row(option_type="put")
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        lambda *args, **kwargs: pd.DataFrame([invalid]),
    )

    assert main.main() == 1
    assert latest.read_text(encoding="utf-8") == "previous"
    assert not list(runs_dir.glob("options_engine_output_20*.csv"))


def test_main_recovers_stale_running_runs_before_count(monkeypatch, capsys, tmp_path: Path):
    """Real fetch startup should mark stale running rows interrupted before count output."""
    backend = MemoryBackend()
    stale_run = backend.create_run(make_run_context(provider="yfinance"))
    completed_run = backend.create_run(make_run_context(provider="yfinance"))
    backend._runs[stale_run].started_at = (  # pylint: disable=protected-access
        datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    )
    backend.finalize_run(completed_run, RunSummary(status="complete"))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), storage_enabled=True),
    )
    monkeypatch.setattr(main, "get_storage_backend", lambda _config: backend)
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame(
                [make_export_row()]
            )
        ),
    )
    def stub_write_options_csv(ticker_frames, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("ok", encoding="utf-8")
        return ticker_frames[0]

    monkeypatch.setattr(main, "write_options_csv", stub_write_options_csv)

    assert main.main() == 0

    stdout = capsys.readouterr().out
    assert "Recovered stale running runs: 1 marked interrupted" in stdout
    assert "Completed runs today (yfinance): 1 (this will be run 2)" in stdout
    stale_record = backend.get_run(stale_run)
    assert stale_record.status == "interrupted"
    assert stale_record.error_summary == "process_terminated_uncleanly"


def test_config_fingerprint_includes_output_affecting_settings():
    """Fingerprint must change when resolved settings alter persisted output."""
    base_overrides = {"data_provider": "marketdata", "marketdata_mode": "delayed"}
    baseline = make_runtime_config(**base_overrides)
    baseline_fingerprint = _config_fingerprint(baseline)

    for overrides in (
        {"risk_free_rate": 0.055},
        {"hv_lookback_days": 45},
        {"trading_days_per_year": 260},
        {"stale_quote_seconds": 3600},
        {"marketdata_mode": "live"},
        {"enable_validation": False},
        {"marketdata_max_retries": 7},
        {"marketdata_request_interval_seconds": 0.5},
        {"marketdata_backoff_seconds": 2.0},
        {"yfinance_max_retries": 3},
        {"yfinance_request_interval_seconds": 0.5},
        {"yfinance_backoff_seconds": 2.0},
        {"massive_snapshot_page_limit": 500},
        {"massive_max_retries": 7},
        {"massive_request_interval_seconds": 0.25},
        {"massive_backoff_seconds": 2.0},
        {"provider_cache_backend": "filesystem"},
        {"provider_cache_dir": Path("/tmp/opx-provider-cache-alt")},
        {"provider_snapshot_ttl": 900},
        {"provider_chain_ttl": 1200},
        {"provider_events_ttl": 3600},
        {"provider_price_context_ttl": 43200},
        {"price_context_enable": True},
        {"price_context_lookback_days": 300},
        {"price_context_max_age_days": 3},
    ):
        changed = make_runtime_config(**{**base_overrides, **overrides})

        assert _config_fingerprint(changed) != baseline_fingerprint


def test_config_fingerprint_covers_runtime_config_fields_by_default():
    """New RuntimeConfig fields should fingerprint unless explicitly excluded."""
    payload = _config_fingerprint_payload(make_runtime_config())
    runtime_fields = {field.name for field in dataclass_fields(RuntimeConfig)}

    assert set(payload) == runtime_fields - _CONFIG_FINGERPRINT_EXCLUDED_FIELDS
    assert _CONFIG_FINGERPRINT_EXCLUDED_FIELDS <= runtime_fields


def test_config_fingerprint_excludes_runtime_metadata_and_secrets():
    """Local paths, warning text, and credentials should not affect the config hash."""
    baseline = make_runtime_config()
    baseline_fingerprint = _config_fingerprint(baseline)

    for overrides in (
        {"config_path": Path("/tmp/other.toml")},
        {"config_warnings": ("settings.min_bid: using default 0.0.",)},
        {"debug_dump_dir": Path("/tmp/other-debug")},
        {"marketdata_api_token": "secret-token"},
        {"massive_api_key": "secret-key"},
        {"storage_dir": Path("/tmp/other-storage")},
        {"today": baseline.today + timedelta(days=1)},
        {"viewer_host": "0.0.0.0"},
        {"viewer_port": 8123},
    ):
        changed = make_runtime_config(**overrides)

        assert _config_fingerprint(changed) == baseline_fingerprint


def test_main_uses_utc_timestamp_for_side_csv_filename(monkeypatch, tmp_path: Path):
    """Timestamped side-write CSV names should align with UTC run-log timestamps."""

    class FixedDateTime:  # pylint: disable=too-few-public-methods
        """Datetime shim that fails if the code asks for local time."""

        @classmethod
        def now(cls, tz=None):
            """Return a fixed UTC timestamp and assert timezone-aware usage."""
            assert tz is timezone.utc
            return datetime(2026, 4, 27, 17, 0, 0, tzinfo=timezone.utc)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "datetime", FixedDateTime)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), storage_enabled=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame(
                [make_export_row()]
            )
        ),
    )

    assert main.main() == 0

    assert (
        tmp_path / "output" / "options_engine_output_20260427_170000.csv"
    ).exists()


def test_main_prints_config_fallbacks(monkeypatch, capsys, tmp_path: Path):
    """Config fallback warnings should be shown when defaults were applied."""
    config = make_runtime_config(
        config_warnings=(
            "settings.filters_min_bid: using default 0.5.",
        ),
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(main, "get_runtime_config", lambda: config)
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame()
        ),
    )

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert "Config fallbacks:" in stdout
    assert "settings.filters_min_bid: using default 0.5." in stdout


def test_main_can_disable_filters_via_cli(monkeypatch, capsys, tmp_path: Path):
    """CLI flags should override the configured filter toggle for one run."""
    captured = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_filters=True),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )

    def fetch_and_capture_config(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        skip_events=False,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
        del skip_events
        captured["config"] = get_process_runtime_config()
        return pd.DataFrame([make_export_row()])

    monkeypatch.setattr(main, "fetch_ticker_option_chain", fetch_and_capture_config)
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main(["--disable-filters"])

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert captured["config"].enable_filters is False
    assert "CLI override:" in stdout
    assert "filters_enable=false" in stdout
    assert "filters_enable: False" in stdout


def test_main_can_enable_filters_via_cli(monkeypatch, capsys, tmp_path: Path):
    """CLI flags should also allow forcing filters on for one run."""
    captured = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_filters=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )

    def fetch_and_capture_config(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        skip_events=False,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
        del skip_events
        captured["config"] = get_process_runtime_config()
        return pd.DataFrame([make_export_row()])

    monkeypatch.setattr(main, "fetch_ticker_option_chain", fetch_and_capture_config)
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main(["--enable-filters"])

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert captured["config"].enable_filters is True
    assert "CLI override:" in stdout
    assert "filters_enable=true" in stdout
    assert "filters_enable: True" in stdout


def test_main_prints_validation_summary_before_export(monkeypatch, capsys, tmp_path: Path):
    """Runs should emit a validation summary even when the export still succeeds."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )

    def fetch_with_invalid_quote(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        skip_events=False,
    ):
        del logger
        del filtered_row_counts
        del position_set
        del skip_events
        if validation_findings is not None:
            validation_findings.extend(
                validate_option_rows(
                    pd.DataFrame(
                        [
                            make_export_row(bid=None)
                        ]
                    )
                )
            )
        return pd.DataFrame(
            [
                make_export_row()
            ]
        )

    monkeypatch.setattr(main, "fetch_ticker_option_chain", fetch_with_invalid_quote)
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert "Validation summary:" in stdout
    assert "errors: 1" in stdout


def test_main_can_disable_validation_summary(monkeypatch, capsys, tmp_path: Path):
    """Disabling validation should suppress the validation report output."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_validation=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame(
                [make_export_row()]
            )
        ),
    )
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert "Validation summary:" not in stdout


def test_main_returns_failure_when_no_data_is_fetched(monkeypatch, tmp_path: Path):
    """An empty run should return a non-zero exit status."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame()
        ),
    )

    assert main.main() == 1
    assert (tmp_path / "fetcher.lock").exists()


def test_main_returns_failure_when_fetcher_lock_is_held(monkeypatch, capsys, tmp_path: Path):
    """A second fetcher run should fail fast while the lock is held."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")

    held_lock = main.acquire_fetcher_lock()
    assert held_lock is not None

    try:
        exit_code = main.main()
    finally:
        held_lock.close()

    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert "Another fetcher run is already active:" in stdout


def test_main_keeps_lock_file_after_success(monkeypatch, tmp_path: Path):
    """Successful runs should keep the fetcher lock path stable on exit."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None, skip_events=False: pd.DataFrame(
                [make_export_row()]
            )
        ),
    )
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    assert main.main() == 0
    assert (tmp_path / "fetcher.lock").exists()


def test_main_handles_ctrl_c_gracefully(monkeypatch, capsys, tmp_path: Path):
    """Keyboard interrupts should return 130 and keep the lock path stable."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("/tmp/opx-run.log")),
    )

    def interrupting_fetch(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        skip_events=False,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
        del skip_events
        raise KeyboardInterrupt

    monkeypatch.setattr(main, "fetch_ticker_option_chain", interrupting_fetch)

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 130
    assert "Interrupted." in stdout
    assert (tmp_path / "fetcher.lock").exists()


def test_main_can_override_positions_path_via_cli(monkeypatch, capsys, tmp_path: Path):
    """The --positions flag should load a non-default positions file for one run."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    logger = CapturingLogger()
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (logger, Path("/tmp/opx-run.log")),
    )
    positions_path = tmp_path / "data" / "runs" / "run-123" / "positions.csv"
    positions_path.parent.mkdir(parents=True, exist_ok=True)
    positions_path.write_text(
        "\n".join([
            "Account Number,Account Name,Symbol,Description,Type",
            "1,Sample,AAA,AAA INC,Margin",
            "1,Sample,MSFT,MICROSOFT CORP,Margin",
        ]),
        encoding="utf-8",
    )

    captured = {}

    def fetch_and_capture_positions(
        ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        **_kwargs,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        captured.setdefault("tickers", []).append(ticker)
        captured["position_set"] = position_set
        return pd.DataFrame([
            make_export_row(
                underlying_symbol=ticker,
                contract_symbol=f"{ticker}260417C00100000",
            )
        ])

    monkeypatch.setattr(main, "fetch_ticker_option_chain", fetch_and_capture_positions)
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main(["--positions", str(positions_path)])

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert f"Positions ({positions_path}): 2 stocks, 0 options" in stdout
    assert captured["tickers"] == ["AAA", "MSFT"]
    assert captured["position_set"].stock_tickers == frozenset({"AAA", "MSFT"})
    assert any(
        message == f"positions path: {positions_path}"
        for message in logger.info_messages
    )


def test_main_adds_option_only_position_tickers(monkeypatch, capsys, tmp_path: Path):
    """Held option tickers should expand the effective fetch list even without stock rows."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "RUNS_DIR", tmp_path / "output")
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (CapturingLogger(), Path("/tmp/opx-run.log")),
    )
    positions_path = tmp_path / "positions.csv"
    positions_path.write_text(
        "\n".join([
            "Account Number,Account Name,Symbol,Description,Type",
            "1,Sample,-TSLA260320C100,TSLA CALL,Margin",
        ]),
        encoding="utf-8",
    )

    captured = []

    def fetch_and_capture_ticker(
        ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
        **_kwargs,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
        captured.append(ticker)
        return pd.DataFrame([
            make_export_row(
                underlying_symbol=ticker,
                contract_symbol=f"{ticker}260417C00100000",
            )
        ])

    monkeypatch.setattr(main, "fetch_ticker_option_chain", fetch_and_capture_ticker)
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    exit_code = main.main(["--positions", str(positions_path)])

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert f"Positions ({positions_path}): 0 stocks, 1 options" in stdout
    assert "Added from positions: TSLA" in stdout
    assert captured == ["AAA", "TSLA"]

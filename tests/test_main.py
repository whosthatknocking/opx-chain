"""Entry-point tests for the console output emitted by the main fetch run."""

# pylint: disable=duplicate-code

from pathlib import Path

import pandas as pd

from conftest import make_runtime_config
import main
from opx.validate import validate_option_rows


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


def make_export_row(**overrides):
    """Build one minimal exported row for main-entrypoint tests."""
    row = {
        "data_source": "stub",
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


def test_main_prints_rows_written_after_saved(monkeypatch, capsys, tmp_path: Path):
    """Show the saved path first, then row count and file size details."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA", "BBB")),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )

    frames = {
        "AAA": pd.DataFrame([{"x": 1}, {"x": 2}]),
        "BBB": pd.DataFrame([{"x": 3}]),
    }
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None, filtered_row_counts=None:
            frames[ticker]
        ),
    )

    written = {}

    def stub_write_options_csv(ticker_frames, output_path):
        written["rows"] = sum(len(frame) for frame in ticker_frames)
        written["path"] = output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("x" * 2048, encoding="utf-8")

    monkeypatch.setattr(main, "write_options_csv", stub_write_options_csv)

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert "Resolved config:" in stdout
    assert "Applied provider: yfinance" in stdout
    assert f"Saved: {written['path']}" in stdout
    assert "Rows written: 3 | File size: 2.0 KB" in stdout
    assert stdout.index(f"Saved: {written['path']}") < stdout.index(
        "Rows written: 3 | File size: 2.0 KB"
    )


def test_main_prints_config_fallbacks(monkeypatch, capsys, tmp_path: Path):
    """Config fallback warnings should be shown when defaults were applied."""
    config = make_runtime_config(
        config_warnings=(
            "settings.filters_min_bid: using default 0.5.",
        ),
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(main, "get_runtime_config", lambda: config)
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None, filtered_row_counts=None:
            pd.DataFrame()
        ),
    )

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert "Config fallbacks:" in stdout
    assert "settings.filters_min_bid: using default 0.5." in stdout


def test_main_prints_validation_summary_before_export(monkeypatch, capsys, tmp_path: Path):
    """Runs should emit a validation summary even when the export still succeeds."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )

    def fetch_with_invalid_quote(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
    ):
        del logger
        del filtered_row_counts
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
    assert "Filter summary:" in stdout
    assert "filtered_out_rows: 0" in stdout
    assert "Validation summary:" in stdout
    assert "errors: 1" in stdout


def test_main_can_disable_validation_summary(monkeypatch, capsys, tmp_path: Path):
    """Disabling validation should suppress the validation report output."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_validation=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None, filtered_row_counts=None:
            pd.DataFrame([make_export_row()])
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
    assert "Filter summary:" in stdout
    assert "filtered_out_rows: 0" in stdout
    assert "Validation summary:" not in stdout


def test_main_returns_failure_when_no_data_is_fetched(monkeypatch, tmp_path: Path):
    """An empty run should return a non-zero exit status."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None, filtered_row_counts=None:
            pd.DataFrame()
        ),
    )

    assert main.main() == 1
    assert not (tmp_path / "fetcher.lock").exists()


def test_main_returns_failure_when_fetcher_lock_is_held(monkeypatch, capsys, tmp_path: Path):
    """A second fetcher run should fail fast while the lock is held."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)

    held_lock = main.acquire_fetcher_lock()
    assert held_lock is not None

    try:
        exit_code = main.main()
    finally:
        held_lock.close()

    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert "Another fetcher run is already active:" in stdout


def test_main_removes_lock_file_after_success(monkeypatch, tmp_path: Path):
    """Successful runs should remove the fetcher lock file on exit."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )
    monkeypatch.setattr(
        main,
        "fetch_ticker_option_chain",
        (
            lambda ticker, logger=None, validation_findings=None, filtered_row_counts=None:
            pd.DataFrame([make_export_row()])
        ),
    )
    monkeypatch.setattr(
        main,
        "write_options_csv",
        lambda ticker_frames, output_path: output_path.parent.mkdir(parents=True, exist_ok=True)
        or output_path.write_text("ok", encoding="utf-8"),
    )

    assert main.main() == 0
    assert not (tmp_path / "fetcher.lock").exists()


def test_main_handles_ctrl_c_gracefully(monkeypatch, capsys, tmp_path: Path):
    """Keyboard interrupts should return 130 and still remove the lock file."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )

    def interrupting_fetch(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        raise KeyboardInterrupt

    monkeypatch.setattr(main, "fetch_ticker_option_chain", interrupting_fetch)

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 130
    assert "Interrupted." in stdout
    assert not (tmp_path / "fetcher.lock").exists()

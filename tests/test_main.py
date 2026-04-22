"""Entry-point tests for the console output emitted by the main fetch run."""

# pylint: disable=duplicate-code

from pathlib import Path

import pandas as pd

from conftest import make_runtime_config
import main
from opx_chain.config import get_runtime_config as get_process_runtime_config
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
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None: frames[ticker]
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
    assert "Config:" in stdout
    assert "provider: yfinance" in stdout
    assert f"Saved: {written['path']}" in stdout
    assert "rows=3  size=2.0 KB" in stdout
    assert stdout.index(f"Saved: {written['path']}") < stdout.index("rows=3  size=2.0 KB")


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
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None: pd.DataFrame()
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
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_filters=True),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )

    def fetch_and_capture_config(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
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
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",), enable_filters=False),
    )
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (StubLogger(), Path("logs/run.log")),
    )

    def fetch_and_capture_config(
        _ticker,
        logger=None,
        validation_findings=None,
        filtered_row_counts=None,
        position_set=None,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
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
        position_set=None,
    ):
        del logger
        del filtered_row_counts
        del position_set
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
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None: pd.DataFrame([make_export_row()])
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
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None: pd.DataFrame()
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
            lambda ticker, logger=None, validation_findings=None,
            filtered_row_counts=None, position_set=None: pd.DataFrame([make_export_row()])
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
        position_set=None,
    ):
        del logger
        del validation_findings
        del filtered_row_counts
        del position_set
        raise KeyboardInterrupt

    monkeypatch.setattr(main, "fetch_ticker_option_chain", interrupting_fetch)

    exit_code = main.main()

    stdout = capsys.readouterr().out
    assert exit_code == 130
    assert "Interrupted." in stdout
    assert not (tmp_path / "fetcher.lock").exists()


def test_main_can_override_positions_path_via_cli(monkeypatch, capsys, tmp_path: Path):
    """The --positions flag should load a non-default positions file for one run."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock")
    monkeypatch.setattr(main, "LOCKS_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "get_runtime_config",
        lambda: make_runtime_config(tickers=("AAA",)),
    )
    logger = CapturingLogger()
    monkeypatch.setattr(
        main,
        "create_run_logger",
        lambda: (logger, Path("logs/run.log")),
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

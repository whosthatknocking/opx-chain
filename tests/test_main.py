"""Entry-point tests for the console output emitted by the main fetch run."""

from pathlib import Path

import pandas as pd

from conftest import make_runtime_config
import main


class StubLogger:
    """Minimal logger stub that satisfies the main entrypoint contract."""

    def info(self, *_args, **_kwargs):
        """Accept info messages without side effects during tests."""
        return None

    def warning(self, *_args, **_kwargs):
        """Accept warning messages without side effects during tests."""
        return None


def test_main_prints_rows_written_after_saved(monkeypatch, capsys, tmp_path: Path):
    """Show the saved path first, then row count and file size details."""
    monkeypatch.chdir(tmp_path)
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
        lambda ticker, logger=None: frames[ticker],
    )

    written = {}

    def stub_write_options_csv(ticker_frames, output_path):
        written["rows"] = sum(len(frame) for frame in ticker_frames)
        written["path"] = output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("x" * 2048, encoding="utf-8")

    monkeypatch.setattr(main, "write_options_csv", stub_write_options_csv)

    main.main()

    stdout = capsys.readouterr().out
    assert f"Saved: {written['path']}" in stdout
    assert "Rows written: 3 | File size: 2.0 KB" in stdout
    assert stdout.index(f"Saved: {written['path']}") < stdout.index(
        "Rows written: 3 | File size: 2.0 KB"
    )

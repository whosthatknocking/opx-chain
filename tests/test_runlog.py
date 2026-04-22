"""Run-log tests covering shared logging between the app and yfinance."""

import logging
from pathlib import Path

from conftest import make_runtime_config
from opx_chain.runlog import create_run_logger


def test_create_run_logger_routes_yfinance_errors_to_run_log(tmp_path, monkeypatch):
    """yfinance errors should be written into the shared run log file."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "opx_chain.runlog.get_runtime_config",
        lambda: make_runtime_config(
            data_provider="yfinance",
            config_path=Path("/tmp/opx-test.toml"),
        ),
    )

    def stub_provider():
        """Return a provider stub exposing yfinance logger routing."""
        return type("StubProvider", (), {"external_logger_names": ("yfinance",)})()

    monkeypatch.setattr(
        "opx_chain.runlog.get_data_provider",
        stub_provider,
    )

    logger, log_path = create_run_logger()
    logging.getLogger("yfinance").error("remote request failed for TSLA")

    for handler in logger.handlers:
        handler.flush()

    contents = log_path.read_text(encoding="utf-8")
    assert "run_started" in contents
    assert "remote request failed for TSLA" in contents
    assert log_path.name == "opx_runs.log"

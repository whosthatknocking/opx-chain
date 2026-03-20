import logging

from options_fetcher.runlog import create_run_logger


def test_create_run_logger_routes_yfinance_errors_to_run_log(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    logger, log_path = create_run_logger()
    logging.getLogger("yfinance").error("remote request failed for TSLA")

    for handler in logger.handlers:
        handler.flush()

    contents = log_path.read_text(encoding="utf-8")
    assert "run_started" in contents
    assert "remote request failed for TSLA" in contents

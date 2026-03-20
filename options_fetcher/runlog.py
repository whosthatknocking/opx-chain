import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from options_fetcher.config import SCRIPT_VERSION


def configure_yfinance_logger(file_handler):
    """Route yfinance errors into the same append-only run log."""
    yf_logger = logging.getLogger("yfinance")
    yf_logger.setLevel(logging.ERROR)
    yf_logger.handlers.clear()
    yf_logger.propagate = False
    yf_logger.addHandler(file_handler)


def create_run_logger():
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    log_path = logs_dir / "options_fetcher_runs.log"

    logger = logging.getLogger("options_fetcher.run")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)sZ | %(levelname)s | %(message)s")
    formatter.converter = time.gmtime
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    configure_yfinance_logger(file_handler)

    logger.info("=" * 80)
    logger.info("run_started run_id=%s script_version=%s", timestamp, SCRIPT_VERSION)
    return logger, log_path

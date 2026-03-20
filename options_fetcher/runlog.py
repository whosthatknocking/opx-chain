"""Run-log configuration for fetcher execution and vendor error capture."""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from options_fetcher.config import SCRIPT_VERSION
from options_fetcher.providers import get_data_provider


def configure_external_loggers(file_handler):
    """Route configured provider-library errors into the same append-only run log."""
    provider = get_data_provider()
    for logger_name in provider.external_logger_names:
        provider_logger = logging.getLogger(logger_name)
        provider_logger.setLevel(logging.ERROR)
        provider_logger.handlers.clear()
        provider_logger.propagate = False
        provider_logger.addHandler(file_handler)


def create_run_logger():
    """Create the append-only run logger and return it with its file path."""
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
    configure_external_loggers(file_handler)

    logger.info("=" * 80)
    logger.info("run_started run_id=%s script_version=%s", timestamp, SCRIPT_VERSION)
    return logger, log_path

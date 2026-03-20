import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from options_fetcher.config import SCRIPT_VERSION


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

    logger.info("=" * 80)
    logger.info("run_started run_id=%s script_version=%s", timestamp, SCRIPT_VERSION)
    return logger, log_path

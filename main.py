"""CLI entrypoint for fetching option chains and writing the export CSV."""

from datetime import datetime
import fcntl
from pathlib import Path

from opx.config import describe_runtime_config, get_runtime_config
from opx.export import write_options_csv
from opx.fetch import fetch_ticker_option_chain
from opx.runlog import create_run_logger

OUTPUTS_DIR = Path("outputs")
LOCKS_DIR = Path("logs")
FETCHER_LOCK_PATH = LOCKS_DIR / "fetcher.lock"


def format_file_size(byte_count):
    """Format byte counts into a small human-readable string."""
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.1f} KB"
    return f"{byte_count / (1024 * 1024):.1f} MB"


def acquire_fetcher_lock():
    """Acquire an exclusive non-blocking lock for the fetcher process."""
    LOCKS_DIR.mkdir(exist_ok=True)
    handle = FETCHER_LOCK_PATH.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.write(f"{FETCHER_LOCK_PATH}\n")
    handle.flush()
    return handle


def release_fetcher_lock(lock_handle):
    """Close the lock handle and remove the lock file path after the run ends."""
    try:
        lock_handle.close()
    finally:
        try:
            FETCHER_LOCK_PATH.unlink()
        except FileNotFoundError:
            pass


def main():
    """Fetch configured tickers and write the consolidated CSV output."""
    lock_handle = acquire_fetcher_lock()
    if lock_handle is None:
        print(f"Another fetcher run is already active: {FETCHER_LOCK_PATH}")
        return 1

    logger = None
    try:
        config = get_runtime_config()
        logger, log_path = create_run_logger()
        print(f"Today: {config.today}")
        print(f"Max expiration: {config.max_expiration}")
        print(f"Log: {log_path}")
        print("Resolved config:")
        for line in describe_runtime_config(config):
            print(f"  {line}")
        if config.config_warnings:
            print("Config fallbacks:")
            for warning in config.config_warnings:
                print(f"  {warning}")
        logger.info(
            "run_context today=%s max_expiration=%s provider=%s config_path=%s",
            config.today,
            config.max_expiration,
            config.data_provider,
            config.config_path,
        )
        for line in describe_runtime_config(config):
            logger.info("config_applied %s", line)
        for warning in config.config_warnings:
            logger.warning("config_fallback %s", warning)

        ticker_frames = []
        for ticker in config.tickers:
            print(f"Loading {ticker}")
            ticker_df = fetch_ticker_option_chain(ticker, logger=logger)
            if not ticker_df.empty:
                ticker_frames.append(ticker_df)

        if not ticker_frames:
            print("No data fetched.")
            logger.warning("run_finished no_data_fetched=true")
            return 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUTS_DIR / f"options_engine_output_{timestamp}.csv"
        row_count = sum(len(frame) for frame in ticker_frames)
        write_options_csv(ticker_frames, output_path=output_path)
        file_size_bytes = output_path.stat().st_size
        logger.info(
            "run_finished output_path=%s ticker_frames=%s rows_written=%s file_size_bytes=%s",
            output_path,
            len(ticker_frames),
            row_count,
            file_size_bytes,
        )
        print(f"\nSaved: {output_path}")
        print(f"Rows written: {row_count} | File size: {format_file_size(file_size_bytes)}")
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted.")
        if logger:
            logger.warning("run_finished interrupted=true")
        return 130
    finally:
        release_fetcher_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())

"""CLI entrypoint for fetching option chains and writing the export CSV."""

import argparse
from dataclasses import replace
from datetime import datetime
import fcntl
import os
from pathlib import Path

import pandas as pd

from opx.config import describe_runtime_config, get_runtime_config, set_runtime_config_override
from opx.export import write_options_csv
from opx.fetch import fetch_ticker_option_chain
from opx.runlog import create_run_logger
from opx.validate import emit_validation_report, validate_export_frame

OUTPUTS_DIR = Path("output")
LOCKS_DIR = Path("logs")
FETCHER_LOCK_PATH = LOCKS_DIR / "fetcher.lock"


def parse_args(argv=None):
    """Parse fetcher CLI arguments."""
    if argv is None and "PYTEST_CURRENT_TEST" in os.environ:
        argv = []
    parser = argparse.ArgumentParser(
        prog="opx-fetcher",
        description="Fetch option chains and write a consolidated CSV export.",
    )
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        "--enable-filters",
        action="store_true",
        help="Force shared post-download filters on for this run.",
    )
    filter_group.add_argument(
        "--disable-filters",
        action="store_true",
        help="Force shared post-download filters off for this run.",
    )
    return parser.parse_args(argv)


def apply_cli_overrides(config, args):
    """Apply one-off CLI overrides on top of the resolved runtime config."""
    if args.enable_filters:
        return replace(config, enable_filters=True), "filters_enable=true"
    if args.disable_filters:
        return replace(config, enable_filters=False), "filters_enable=false"
    return config, None


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


def main(argv=None):  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    """Fetch configured tickers and write the consolidated CSV output."""
    args = parse_args(argv)
    lock_handle = acquire_fetcher_lock()
    if lock_handle is None:
        print(f"Another fetcher run is already active: {FETCHER_LOCK_PATH}")
        return 1

    logger = None
    try:
        config, cli_override = apply_cli_overrides(get_runtime_config(), args)
        set_runtime_config_override(config)
        logger, log_path = create_run_logger()
        print(f"Today: {config.today}")
        print(f"Max expiration: {config.max_expiration}")
        print(f"Log: {log_path}")
        if cli_override:
            print("CLI overrides:")
            print(f"  {cli_override}")
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
        if cli_override:
            logger.info("cli_override %s", cli_override)
        for line in describe_runtime_config(config):
            logger.info("config_applied %s", line)
        for warning in config.config_warnings:
            logger.warning("config_fallback %s", warning)

        ticker_frames = []
        validation_findings = []
        filtered_row_counts = []
        for ticker in config.tickers:
            print(f"Loading {ticker}")
            ticker_df = fetch_ticker_option_chain(
                ticker,
                logger=logger,
                validation_findings=validation_findings,
                filtered_row_counts=filtered_row_counts,
            )
            if not ticker_df.empty:
                ticker_frames.append(ticker_df)

        filtered_out_rows = sum(filtered_row_counts)
        print("Filter summary:")
        print(f"  filtered_out_rows: {filtered_out_rows}")
        if logger:
            logger.info("filter_summary filtered_out_rows=%s", filtered_out_rows)

        if not ticker_frames:
            print("No data fetched.")
            logger.warning("run_finished no_data_fetched=true")
            return 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUTS_DIR / f"options_engine_output_{timestamp}.csv"
        combined = pd.concat(ticker_frames, ignore_index=True)
        if config.enable_validation:
            validation_findings.extend(validate_export_frame(combined))
            emit_validation_report(validation_findings, logger=logger)
        row_count = len(combined)
        write_options_csv([combined], output_path=output_path)
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
        set_runtime_config_override(None)
        release_fetcher_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())

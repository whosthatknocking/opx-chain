"""CLI entrypoint for fetching option chains and writing the export CSV."""

import argparse
from dataclasses import replace
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path

import pandas as pd

from opx_chain import SCHEMA_VERSION
from opx_chain.config import (
    describe_runtime_config, get_runtime_config, set_runtime_config_override,
)
from opx_chain.export import prepare_export_frame, write_options_csv
from opx_chain.fetch import fetch_ticker_option_chain
from opx_chain.positions import DEFAULT_POSITIONS_PATH, load_positions
from opx_chain.runlog import create_run_logger
from opx_chain.storage.factory import get_storage_backend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary, TickerFetchResult
from opx_chain.validate import emit_validation_report, validate_export_frame

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
    parser.add_argument(
        "--positions",
        type=Path,
        default=None,
        help="Path to positions CSV. Defaults to data/positions.csv.",
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


def _config_fingerprint(config) -> str:
    """Return a SHA-256 hex digest of the config fields that affect fetch output."""
    fields = {
        "provider": config.data_provider,
        "tickers": sorted(config.tickers),
        "max_expiration_weeks": config.max_expiration_weeks,
        "enable_filters": config.enable_filters,
        "min_bid": config.min_bid,
        "min_open_interest": config.min_open_interest,
        "min_volume": config.min_volume,
        "max_spread_pct_of_mid": config.max_spread_pct_of_mid,
        "max_strike_distance_pct": config.max_strike_distance_pct,
        "option_score_income_weight": config.option_score_income_weight,
        "option_score_liquidity_weight": config.option_score_liquidity_weight,
        "option_score_risk_weight": config.option_score_risk_weight,
        "option_score_efficiency_weight": config.option_score_efficiency_weight,
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def _positions_fingerprint(positions_path: Path) -> str:
    """Return SHA-256 of the positions file bytes, or empty string if absent."""
    if not positions_path.exists():
        return ""
    return hashlib.sha256(positions_path.read_bytes()).hexdigest()


def acquire_fetcher_lock():
    """Acquire an exclusive non-blocking lock for the fetcher process."""
    LOCKS_DIR.mkdir(exist_ok=True)
    handle = FETCHER_LOCK_PATH.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
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


def _do_fetch_with_lock_held(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    config,
    positions_path: Path | None,
    cli_override: str | None,
) -> None:
    """Execute the fetch pipeline. Lock must already be held by caller. Raises on failure."""
    logger = None
    storage = None
    run_id = None
    try:
        storage = get_storage_backend(config)
        logger, log_path = create_run_logger()
        print(f"Today: {config.today}  Log: {log_path}")
        if cli_override:
            print(f"CLI override: {cli_override}")
        runs_today = storage.count_runs_today(config.data_provider) if storage else 0
        print("Config:")
        for line in describe_runtime_config(config):
            print(f"  {line}")
        if runs_today > 0:
            print(
                f"  Runs today ({config.data_provider}): {runs_today}"
                f"  (this will be run {runs_today + 1})"
            )
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

        resolved_positions_path = (positions_path or DEFAULT_POSITIONS_PATH).expanduser()
        logger.info("positions path: %s", resolved_positions_path)
        position_set = load_positions(resolved_positions_path)
        extra_tickers = tuple(
            t for t in sorted(position_set.stock_tickers) if t not in set(config.tickers)
        )
        effective_tickers = config.tickers + extra_tickers
        if resolved_positions_path.exists():
            print(
                f"Positions ({resolved_positions_path}): "
                f"{len(position_set.stock_tickers)} stocks, "
                f"{len(position_set.option_keys)} options"
            )
        else:
            print(f"Positions ({resolved_positions_path}): file not found, skipping")
        if extra_tickers:
            print(f"  Added from positions: {', '.join(extra_tickers)}")
        logger.info(
            "positions stocks=%s options=%s extra_tickers=%s",
            len(position_set.stock_tickers),
            len(position_set.option_keys),
            len(extra_tickers),
        )

        if storage is not None:
            run_id = storage.create_run(RunContext(
                provider=config.data_provider,
                tickers=effective_tickers,
                config_fingerprint=_config_fingerprint(config),
                positions_fingerprint=_positions_fingerprint(resolved_positions_path),
            ))

        ticker_frames = []
        validation_findings = []
        filtered_row_counts = []
        for ticker in effective_tickers:
            counts_before = len(filtered_row_counts)
            ticker_df = fetch_ticker_option_chain(
                ticker,
                logger=logger,
                validation_findings=validation_findings,
                filtered_row_counts=filtered_row_counts,
                position_set=position_set,
            )
            if not ticker_df.empty:
                ticker_frames.append(ticker_df)
            if storage is not None and run_id is not None:
                filtered_this = sum(filtered_row_counts[counts_before:])
                kept = len(ticker_df)
                exp_count = (
                    int(ticker_df["expiration_date"].nunique())
                    if kept and "expiration_date" in ticker_df.columns else 0
                )
                storage.record_ticker_result(run_id, TickerFetchResult(
                    ticker=ticker,
                    raw_row_count=kept + filtered_this,
                    normalized_row_count=kept + filtered_this,
                    kept_row_count=kept,
                    filtered_row_count=filtered_this,
                    expiration_count=exp_count,
                    status="ok" if not ticker_df.empty else "skipped",
                ))

        filtered_out_rows = sum(filtered_row_counts)
        if logger:
            logger.info("filter_summary filtered_out_rows=%s", filtered_out_rows)

        if not ticker_frames:
            print("No data fetched.")
            logger.warning("run_finished no_data_fetched=true")
            if storage is not None and run_id is not None:
                storage.fail_run(run_id, "no data fetched")
                run_id = None
            raise RuntimeError("No data fetched.")

        combined = pd.concat(ticker_frames, ignore_index=True)
        if config.enable_validation:
            validation_findings.extend(validate_export_frame(combined))
            emit_validation_report(validation_findings, logger=logger)
        row_count = len(combined)

        write_csv = storage is None or config.storage_write_legacy_csv
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUTS_DIR / f"options_engine_output_{timestamp}.csv"
        if write_csv:
            export_df = write_options_csv([combined], output_path=output_path)
            file_size_bytes = output_path.stat().st_size
        else:
            export_df = prepare_export_frame([combined])
            file_size_bytes = 0

        dataset_record = None
        if storage is not None and run_id is not None:
            dataset_record = storage.write_dataset(run_id, DatasetWrite(
                data=export_df,
                provider=config.data_provider,
                schema_version=SCHEMA_VERSION,
                format=config.storage_dataset_format,
            ))
            storage.finalize_run(run_id, RunSummary(status="complete"))

        print()
        if write_csv:
            print(f"Saved: {output_path}")
        if dataset_record is not None:
            artifact_path = Path(dataset_record.location)
            artifact_size = (
                format_file_size(artifact_path.stat().st_size)
                if artifact_path.exists() else "unknown size"
            )
            run_short = run_id[:8] if run_id else "?"
            print(
                f"Storage: run={run_short}  "
                f"artifact={artifact_path}  {artifact_size}"
            )

        if write_csv:
            file_size = format_file_size(file_size_bytes)
            summary = f"rows={row_count}  size={file_size}  dropped={filtered_out_rows}"
        else:
            summary = f"rows={row_count}  dropped={filtered_out_rows}"
        print(summary)

        logger.info(
            "run_finished ticker_frames=%s rows_written=%s file_size_bytes=%s"
            " legacy_csv=%s run_id=%s",
            len(ticker_frames),
            row_count,
            file_size_bytes,
            write_csv,
            run_id,
        )
    except KeyboardInterrupt:
        print("\nInterrupted.")
        if logger:
            logger.warning("run_finished interrupted=true")
        if storage is not None and run_id is not None:
            storage.finalize_run(
                run_id, RunSummary(status="interrupted", error_summary="interrupted")
            )
        raise
    except Exception as exc:
        print(f"\nFatal error: {exc}")
        if logger:
            logger.exception("run_finished fatal error: %s", exc)
        if storage is not None and run_id is not None:
            try:
                storage.fail_run(run_id, str(exc))
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        raise


def run_fetch(positions_path: Path | None = None) -> None:
    """Trigger a fresh option-chain fetch and write the result to storage.

    This is the programmatic entry point for downstream consumers (e.g.
    opx-strategy stage 3) that import opx_chain directly rather than
    invoking opx-fetcher as a subprocess.

    Raises RuntimeError if another fetch run is already active.
    Raises RuntimeError if the fetch produces no data.
    Propagates any provider-level exception on fatal failure.
    """
    lock_handle = acquire_fetcher_lock()
    if lock_handle is None:
        raise RuntimeError(f"Another fetcher run is already active: {FETCHER_LOCK_PATH}")
    try:
        config = get_runtime_config()
        set_runtime_config_override(config)
        _do_fetch_with_lock_held(config, positions_path, cli_override=None)
    finally:
        set_runtime_config_override(None)
        release_fetcher_lock(lock_handle)


def main(argv=None):
    """Fetch configured tickers and write the consolidated CSV output."""
    args = parse_args(argv)
    lock_handle = acquire_fetcher_lock()
    if lock_handle is None:
        print(f"Another fetcher run is already active: {FETCHER_LOCK_PATH}")
        return 1
    try:
        config, cli_override = apply_cli_overrides(get_runtime_config(), args)
        set_runtime_config_override(config)
        _do_fetch_with_lock_held(config, args.positions, cli_override=cli_override)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:  # pylint: disable=broad-exception-caught
        return 1
    finally:
        set_runtime_config_override(None)
        release_fetcher_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())

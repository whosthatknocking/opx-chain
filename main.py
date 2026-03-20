"""CLI entrypoint for fetching option chains and writing the export CSV."""

from datetime import datetime
from pathlib import Path

from options_fetcher.config import MAX_EXPIRATION, TICKERS, today
from options_fetcher.export import write_options_csv
from options_fetcher.fetch import fetch_ticker_option_chain
from options_fetcher.runlog import create_run_logger

OUTPUTS_DIR = Path("outputs")


def format_file_size(byte_count):
    """Format byte counts into a small human-readable string."""
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.1f} KB"
    return f"{byte_count / (1024 * 1024):.1f} MB"


def main():
    """Fetch configured tickers and write the consolidated CSV output."""
    logger, log_path = create_run_logger()
    print(f"Today: {today}")
    print(f"Max expiration: {MAX_EXPIRATION}")
    print(f"Log: {log_path}")
    logger.info("run_context today=%s max_expiration=%s", today, MAX_EXPIRATION)

    ticker_frames = []
    for ticker in TICKERS:
        print(f"Loading {ticker}")
        ticker_df = fetch_ticker_option_chain(ticker, logger=logger)
        if not ticker_df.empty:
            ticker_frames.append(ticker_df)

    if not ticker_frames:
        print("No data fetched.")
        logger.warning("run_finished no_data_fetched=true")
        raise SystemExit(0)

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


if __name__ == "__main__":
    main()

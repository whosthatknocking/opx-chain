from datetime import datetime
from pathlib import Path

from options_fetcher.config import MAX_EXPIRATION, TICKERS, today
from options_fetcher.export import write_options_csv
from options_fetcher.fetch import fetch_ticker_option_chain
from options_fetcher.runlog import create_run_logger

OUTPUTS_DIR = Path("outputs")


def main():
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
    write_options_csv(ticker_frames, output_path=output_path)
    logger.info("run_finished output_path=%s ticker_frames=%s", output_path, len(ticker_frames))
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()

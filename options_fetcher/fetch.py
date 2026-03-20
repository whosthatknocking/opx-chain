"""Fetch orchestration using the configured market-data provider."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from options_fetcher.config import (
    MAX_EXPIRATION,
    STALE_QUOTE_SECONDS,
    today,
)
from options_fetcher.metrics import add_expected_move_by_expiration
from options_fetcher.normalize import enrich_option_frame
from options_fetcher.providers import get_data_provider


def append_underlying_snapshot_fields(df, snapshot, fetched_at):
    """Add underlying snapshot metadata to each option row."""
    df["underlying_price_time"] = snapshot["underlying_price_time"]
    df["underlying_market_state"] = snapshot["underlying_market_state"]
    df["underlying_day_change_pct"] = snapshot["underlying_day_change_pct"]
    df["historical_volatility"] = snapshot["historical_volatility"]
    df["vix_level"] = snapshot["vix_level"]
    df["vix_quote_time"] = snapshot["vix_quote_time"]
    df["underlying_price_age_seconds"] = (
        (fetched_at - snapshot["underlying_price_time"]).total_seconds()
        if pd.notna(snapshot["underlying_price_time"])
        else np.nan
    )
    df["is_stale_underlying_price"] = np.where(
        pd.notna(df["underlying_price_age_seconds"]),
        df["underlying_price_age_seconds"] > STALE_QUOTE_SECONDS,
        None,
    )
    return df


def fetch_ticker_option_chain(  # pylint: disable=too-many-locals,broad-exception-caught
    ticker,
    logger=None,
):
    """Fetch and normalize all near-term option chains for one ticker."""
    try:
        fetched_at = pd.Timestamp.now(tz=timezone.utc)
        provider = get_data_provider()
        snapshot = provider.load_underlying_snapshot(ticker)
        underlying_price = snapshot["underlying_price"]

        if pd.isna(underlying_price) or underlying_price <= 0:
            if logger:
                logger.warning(
                    "ticker=%s status=skipped reason=invalid_underlying_price",
                    ticker,
                )
            return pd.DataFrame()

        rows = []
        raw_contract_count = 0
        raw_expiration_count = 0
        for expiration_date in provider.list_option_expirations(ticker):
            if expiration_date > MAX_EXPIRATION:
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            if (exp_date - today).days <= 0:
                continue

            chain = provider.load_option_chain(ticker, expiration_date)
            expiration_raw_count = len(chain.calls) + len(chain.puts)
            raw_contract_count += expiration_raw_count
            raw_expiration_count += 1
            if logger:
                logger.info(
                    (
                        "ticker=%s expiration=%s status=raw_yfinance_rows "
                        "call_rows=%s put_rows=%s total_rows=%s"
                    ),
                    ticker,
                    expiration_date,
                    len(chain.calls),
                    len(chain.puts),
                    expiration_raw_count,
                )
            for option_type, option_frame in [("call", chain.calls), ("put", chain.puts)]:
                vendor_normalized = provider.normalize_option_frame(
                    df=option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
                )
                normalized = enrich_option_frame(
                    df=vendor_normalized,
                    underlying_price=underlying_price,
                    fetched_at=fetched_at,
                )
                rows.append(
                    append_underlying_snapshot_fields(normalized, snapshot, fetched_at)
                )

        if not rows:
            if logger:
                logger.warning(
                    (
                        "ticker=%s status=ok rows=0 expirations=0 "
                        "raw_yfinance_rows=%s raw_expirations=%s"
                    ),
                    ticker,
                    raw_contract_count,
                    raw_expiration_count,
                )
            return pd.DataFrame()

        combined = pd.concat(rows, ignore_index=True)
        combined = add_expected_move_by_expiration(combined)
        if logger:
            logger.info(
                (
                    "ticker=%s status=ok fetched_at=%s rows=%s expirations=%s "
                    "raw_yfinance_rows=%s raw_expirations=%s"
                ),
                ticker,
                fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                len(combined),
                combined["expiration_date"].nunique(),
                raw_contract_count,
                raw_expiration_count,
            )
        return combined

    except Exception as exc:
        print(f"{ticker} error: {exc}")
        if logger:
            logger.exception("ticker=%s status=error message=%s", ticker, exc)
        return pd.DataFrame()

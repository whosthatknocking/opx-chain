"""Normalization and early filtering for raw vendor option-chain rows."""

import pandas as pd

from options_fetcher.config import (
    MAX_SPREAD_PCT_OF_MID,
    MAX_STRIKE_DISTANCE_PCT,
    RISK_FREE_RATE,
    today,
)
from options_fetcher.metrics import (
    add_derived_pricing_metrics,
    add_quote_quality_metrics,
    add_screening_and_freshness_flags,
)


def normalize_vendor_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    df,
    underlying_price,
    expiration_date,
    option_type,
    ticker,
    data_source,
):
    """Normalize vendor columns into a stable schema before deriving metrics."""
    df = df.copy()
    df = df.rename(
        columns={
            "contractSymbol": "contract_symbol",
            "lastTradeDate": "option_quote_time",
            "lastPrice": "last_trade_price",
            "openInterest": "open_interest",
            "impliedVolatility": "implied_volatility",
            "inTheMoney": "is_in_the_money",
            "percentChange": "percent_change",
            "contractSize": "contract_size",
        }
    )

    expiration_ts = pd.Timestamp(expiration_date)
    days_to_expiration = (expiration_ts.date() - today).days
    time_to_expiration_years = days_to_expiration / 365.0

    df["option_type"] = option_type
    df["underlying_symbol"] = ticker
    df["expiration_date"] = expiration_date
    df["days_to_expiration"] = days_to_expiration
    df["time_to_expiration_years"] = time_to_expiration_years
    df["data_source"] = data_source
    df["risk_free_rate_used"] = RISK_FREE_RATE
    df["underlying_price"] = underlying_price

    numeric_columns = [
        "bid",
        "ask",
        "strike",
        "open_interest",
        "volume",
        "last_trade_price",
        "implied_volatility",
        "change",
        "percent_change",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["option_quote_time"] = pd.to_datetime(df["option_quote_time"], utc=True, errors="coerce")
    return df


def filter_strikes_near_spot(df, underlying_price):
    """Keep only strikes within the configured percentage band around spot."""
    if pd.isna(underlying_price) or underlying_price <= 0:
        return df

    min_strike = underlying_price * (1 - MAX_STRIKE_DISTANCE_PCT)
    max_strike = underlying_price * (1 + MAX_STRIKE_DISTANCE_PCT)
    return df[df["strike"].between(min_strike, max_strike, inclusive="both")].copy()


def filter_zero_bid_quotes(df):
    """Exclude contracts with an explicit zero bid from the fetched dataset."""
    return df[df["bid"] != 0].copy()


def filter_wide_spread_quotes(df):
    """Exclude contracts whose spread exceeds the configured share of mid price."""
    return df[df["bid_ask_spread_pct_of_mid"] < MAX_SPREAD_PCT_OF_MID].copy()


def enrich_option_frame(df, underlying_price, fetched_at):
    """Add derived metrics and quality flags to an already normalized frame."""
    df = filter_zero_bid_quotes(df)
    df = filter_strikes_near_spot(df, underlying_price)
    df = add_quote_quality_metrics(df, underlying_price)
    df = filter_wide_spread_quotes(df)
    df = add_derived_pricing_metrics(df, underlying_price)
    df = add_screening_and_freshness_flags(df, fetched_at)
    return df

"""Normalization and early filtering for raw vendor option-chain rows."""

import pandas as pd

from opx.config import get_runtime_config
from opx.metrics import (
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
    config = get_runtime_config()
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
    days_to_expiration = (expiration_ts.date() - config.today).days
    time_to_expiration_years = days_to_expiration / 365.0

    df["option_type"] = option_type
    if "underlying_symbol" not in df.columns:
        df["underlying_symbol"] = ticker
    else:
        df["underlying_symbol"] = df["underlying_symbol"].fillna(ticker)
    df["expiration_date"] = expiration_date
    df["days_to_expiration"] = days_to_expiration
    df["time_to_expiration_years"] = time_to_expiration_years
    df["data_source"] = data_source
    df["risk_free_rate_used"] = config.risk_free_rate
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
    config = get_runtime_config()
    if pd.isna(underlying_price) or underlying_price <= 0:
        return df

    min_strike = underlying_price * (1 - config.max_strike_distance_pct)
    max_strike = underlying_price * (1 + config.max_strike_distance_pct)
    return df[df["strike"].between(min_strike, max_strike, inclusive="both")].copy()


def filter_zero_bid_quotes(df):
    """Exclude contracts with an explicit zero bid from the fetched dataset."""
    return df[df["bid"] != 0].copy()


def filter_wide_spread_quotes(df):
    """Exclude contracts whose spread exceeds the configured share of mid price."""
    config = get_runtime_config()
    return df[df["bid_ask_spread_pct_of_mid"] <= config.max_spread_pct_of_mid].copy()


def enrich_option_frame(df, underlying_price, fetched_at):
    """Add derived metrics and quality flags to an already normalized frame."""
    df = add_quote_quality_metrics(df, underlying_price)
    df = add_derived_pricing_metrics(df, underlying_price)
    df = add_screening_and_freshness_flags(df, fetched_at)
    return df


def _matches_any_position(df, option_keys):
    """Return a boolean mask for rows matching any portfolio option position."""
    mask = pd.Series(False, index=df.index)
    if not option_keys:
        return mask
    required = {"underlying_symbol", "expiration_date", "option_type", "strike"}
    if not required.issubset(df.columns):
        return mask
    for key in option_keys:
        row_mask = (
            (df["underlying_symbol"] == key.ticker)
            & (df["expiration_date"] == key.expiration_date)
            & (df["option_type"] == key.option_type)
            & ((df["strike"] - key.strike).abs() < 0.005)
        )
        mask |= row_mask
    return mask


def apply_post_download_filters(df, underlying_price, position_keys=None):
    """Apply the shared post-download filters after validation has run."""
    config = get_runtime_config()
    if not config.enable_filters:
        return df

    # Portfolio position rows bypass all quality filters.
    if position_keys:
        position_mask = _matches_any_position(df, position_keys)
        position_rows = df[position_mask]
        to_filter = df[~position_mask]
    else:
        position_rows = pd.DataFrame()
        to_filter = df

    filtered = filter_zero_bid_quotes(to_filter)
    filtered = filter_strikes_near_spot(filtered, underlying_price)
    filtered = filter_wide_spread_quotes(filtered)

    if position_rows.empty:
        return filtered
    return pd.concat([filtered, position_rows], ignore_index=True)

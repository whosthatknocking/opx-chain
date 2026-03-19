import pandas as pd

from options_fetcher_app.config import DATA_SOURCE, RISK_FREE_RATE, today
from options_fetcher_app.metrics import (
    add_derived_pricing_metrics,
    add_quote_quality_metrics,
    add_screening_and_freshness_flags,
)


def normalize_vendor_option_frame(df, underlying_price, expiration_date, option_type, ticker, fetched_at):
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
    df["data_source"] = DATA_SOURCE
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


def enrich_option_frame(df, underlying_price, expiration_date, option_type, ticker, fetched_at):
    """Normalize the vendor frame, then add derived metrics and quality flags."""
    df = normalize_vendor_option_frame(
        df=df,
        underlying_price=underlying_price,
        expiration_date=expiration_date,
        option_type=option_type,
        ticker=ticker,
        fetched_at=fetched_at,
    )
    df = add_quote_quality_metrics(df, underlying_price)
    df = add_derived_pricing_metrics(df, underlying_price)
    df = add_screening_and_freshness_flags(df, fetched_at)
    return df

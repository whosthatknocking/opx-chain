"""Remote fetch helpers for Yahoo Finance option and underlying data."""

from functools import lru_cache
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

from options_fetcher.config import (
    HV_LOOKBACK_DAYS,
    MAX_EXPIRATION,
    STALE_QUOTE_SECONDS,
    TRADING_DAYS_PER_YEAR,
    today,
)
from options_fetcher.metrics import add_expected_move_by_expiration
from options_fetcher.normalize import enrich_option_frame
from options_fetcher.utils import coerce_float, normalize_timestamp


def normalize_market_state(value):
    """Collapse duplicated vendor market-state strings such as POSTPOST -> POST."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None

    half = len(normalized) // 2
    if len(normalized) % 2 == 0 and normalized[:half] == normalized[half:]:
        return normalized[:half]
    return normalized


def compute_historical_volatility(stock):  # pylint: disable=broad-exception-caught
    """Compute trailing annualized realized volatility from daily closes."""
    lookback_period = f"{max(HV_LOOKBACK_DAYS * 3, 90)}d"
    try:
        history = stock.history(period=lookback_period, interval="1d", auto_adjust=False)
    except Exception:  # pylint: disable=broad-exception-caught
        return np.nan
    if history.empty:
        return np.nan

    close_column = "Adj Close" if "Adj Close" in history.columns else "Close"
    closes = pd.to_numeric(history[close_column], errors="coerce").dropna()
    log_returns = np.log(closes / closes.shift(1)).dropna()
    if len(log_returns) < HV_LOOKBACK_DAYS:
        return np.nan

    recent_returns = log_returns.tail(HV_LOOKBACK_DAYS)
    return recent_returns.std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)


@lru_cache(maxsize=1)
def load_vix_snapshot():
    """Load the latest VIX snapshot once per run."""
    try:  # pylint: disable=broad-exception-caught
        vix = yf.Ticker("^VIX")
        fast_info = getattr(vix, "fast_info", {}) or {}
        info = vix.info
    except Exception:  # pylint: disable=broad-exception-caught
        fast_info = {}
        info = {}

    vix_level = coerce_float(
        fast_info.get("lastPrice")
        or info.get("regularMarketPrice")
        or info.get("previousClose")
    )
    vix_quote_time = normalize_timestamp(info.get("regularMarketTime"))
    return {
        "vix_level": vix_level,
        "vix_quote_time": vix_quote_time,
    }


def load_underlying_snapshot(stock):  # pylint: disable=broad-exception-caught
    """Load the underlying snapshot once per ticker and reuse it for each expiration."""
    fast_info = getattr(stock, "fast_info", {}) or {}
    try:
        info = stock.info
    except Exception:  # pylint: disable=broad-exception-caught
        info = {}

    last_price = coerce_float(
        fast_info.get("lastPrice")
        or info.get("regularMarketPrice")
        or info.get("previousClose")
    )
    previous_close = coerce_float(
        fast_info.get("previousClose")
        or info.get("previousClose")
    )

    if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
        underlying_day_change_pct = (last_price - previous_close) / previous_close
    else:
        underlying_day_change_pct = np.nan

    vix_snapshot = load_vix_snapshot()

    return {
        "underlying_price": last_price,
        "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
        "underlying_market_state": normalize_market_state(info.get("marketState")),
        "underlying_day_change_pct": underlying_day_change_pct,
        "historical_volatility": compute_historical_volatility(stock),
        "vix_level": vix_snapshot["vix_level"],
        "vix_quote_time": vix_snapshot["vix_quote_time"],
    }


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
        stock = yf.Ticker(ticker)
        snapshot = load_underlying_snapshot(stock)
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
        for expiration_date in stock.options:
            if expiration_date > MAX_EXPIRATION:
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            if (exp_date - today).days <= 0:
                continue

            chain = stock.option_chain(expiration_date)
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
                normalized = enrich_option_frame(
                    df=option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
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

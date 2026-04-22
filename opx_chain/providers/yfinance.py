"""Yahoo Finance provider implementation."""

# pylint: disable=duplicate-code

from __future__ import annotations

import numpy as np
import pandas as pd
import yfinance as yf

from opx_chain.config import get_runtime_config
from opx_chain.providers.base import DataProvider, OptionChainFrames, normalize_provider_frame
from opx_chain.utils import coerce_float, normalize_timestamp


def _first_non_missing(*values):
    """Return the first value that is not None/NaN, preserving legitimate zeroes."""
    for value in values:
        if value is not None and not pd.isna(value):
            return value
    return None


def compute_historical_volatility(stock):  # pylint: disable=broad-exception-caught
    """Compute trailing annualized realized volatility from daily closes."""
    config = get_runtime_config()
    lookback_period = f"{max(config.hv_lookback_days * 3, 90)}d"
    try:
        history = stock.history(period=lookback_period, interval="1d", auto_adjust=False)
    except Exception:  # pylint: disable=broad-exception-caught
        return np.nan
    if history.empty:
        return np.nan

    close_column = "Adj Close" if "Adj Close" in history.columns else "Close"
    closes = pd.to_numeric(history[close_column], errors="coerce").dropna()
    log_returns = np.log(closes / closes.shift(1)).dropna()
    if len(log_returns) < config.hv_lookback_days:
        return np.nan

    recent_returns = log_returns.tail(config.hv_lookback_days)
    return recent_returns.std(ddof=1) * np.sqrt(config.trading_days_per_year)


class YFinanceProvider(DataProvider):
    """Market-data provider backed by yfinance/Yahoo Finance."""

    name = "yfinance"

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose yfinance's logger so runlog can capture vendor errors."""
        return ("yfinance",)

    def load_underlying_snapshot(self, ticker: str) -> dict:  # pylint: disable=broad-exception-caught
        """Load the underlying snapshot once per ticker and reuse it for each expiration."""
        stock = yf.Ticker(ticker)
        fast_info = getattr(stock, "fast_info", {}) or {}
        try:
            info = stock.info
        except Exception:  # pylint: disable=broad-exception-caught
            info = {}
        self.debug_dump_payload(
            ticker,
            "underlying_snapshot",
            {"fast_info": fast_info, "info": info},
        )

        last_price = coerce_float(
            _first_non_missing(
                fast_info.get("lastPrice"),
                info.get("regularMarketPrice"),
                info.get("previousClose"),
            )
        )
        previous_close = coerce_float(
            _first_non_missing(
                fast_info.get("previousClose"),
                info.get("previousClose"),
            )
        )

        if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
            underlying_day_change_pct = (last_price - previous_close) / previous_close
        else:
            underlying_day_change_pct = np.nan

        return {
            "underlying_price": last_price,
            "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": compute_historical_volatility(stock),
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return option expiration strings available from yfinance."""
        stock = yf.Ticker(ticker)
        expirations = list(stock.options)
        self.debug_dump_payload(ticker, "expirations", expirations)
        return expirations

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load one yfinance option chain and return its raw call/put frames."""
        stock = yf.Ticker(ticker)
        chain = stock.option_chain(expiration_date)
        self.debug_dump_payload(
            ticker,
            f"option_chain_{expiration_date}",
            {"calls": chain.calls, "puts": chain.puts},
        )
        return OptionChainFrames(calls=chain.calls, puts=chain.puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a yfinance frame into the canonical options schema."""
        return normalize_provider_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )

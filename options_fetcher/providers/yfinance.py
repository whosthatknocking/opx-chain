"""Yahoo Finance provider implementation."""

from __future__ import annotations

from functools import lru_cache

import numpy as np
import pandas as pd
import yfinance as yf

from options_fetcher.config import HV_LOOKBACK_DAYS, TRADING_DAYS_PER_YEAR
from options_fetcher.normalize import normalize_vendor_option_frame
from options_fetcher.providers.base import DataProvider, OptionChainFrames
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


class YFinanceProvider(DataProvider):
    """Market-data provider backed by yfinance/Yahoo Finance."""

    name = "yfinance"

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose yfinance's logger so runlog can capture vendor errors."""
        return ("yfinance",)

    @lru_cache(maxsize=1)
    def load_vix_snapshot(self) -> dict:
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

    def load_underlying_snapshot(self, ticker: str) -> dict:  # pylint: disable=broad-exception-caught
        """Load the underlying snapshot once per ticker and reuse it for each expiration."""
        stock = yf.Ticker(ticker)
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

        vix_snapshot = self.load_vix_snapshot()

        return {
            "underlying_price": last_price,
            "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
            "underlying_market_state": normalize_market_state(info.get("marketState")),
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": compute_historical_volatility(stock),
            "vix_level": vix_snapshot["vix_level"],
            "vix_quote_time": vix_snapshot["vix_quote_time"],
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return option expiration strings available from yfinance."""
        stock = yf.Ticker(ticker)
        return list(stock.options)

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load one yfinance option chain and return its raw call/put frames."""
        stock = yf.Ticker(ticker)
        chain = stock.option_chain(expiration_date)
        return OptionChainFrames(calls=chain.calls, puts=chain.puts)

    def normalize_option_frame(
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a yfinance frame into the canonical options schema."""
        return normalize_vendor_option_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )

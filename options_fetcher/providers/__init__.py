"""Provider selection helpers for the market-data layer."""

from options_fetcher.config import DATA_PROVIDER
from options_fetcher.providers.base import DataProvider
from options_fetcher.providers.yfinance import YFinanceProvider


def get_data_provider() -> DataProvider:
    """Return the configured market-data provider implementation."""
    if DATA_PROVIDER == "yfinance":
        return YFinanceProvider()
    raise ValueError(f"Unsupported data provider: {DATA_PROVIDER}")

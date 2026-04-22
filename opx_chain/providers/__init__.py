"""Provider selection helpers for the market-data layer."""

from opx_chain.config import get_runtime_config
from opx_chain.providers.massive import MassiveProvider
from opx_chain.providers.marketdata import MarketDataProvider
from opx_chain.providers.base import DataProvider
from opx_chain.providers.yfinance import YFinanceProvider


PROVIDER_FACTORIES = {
    "yfinance": YFinanceProvider,
    "massive": MassiveProvider,
    "marketdata": MarketDataProvider,
}


def get_data_provider() -> DataProvider:
    """Return the configured market-data provider implementation."""
    provider_name = get_runtime_config().data_provider
    try:
        return PROVIDER_FACTORIES[provider_name]()
    except KeyError as exc:
        raise ValueError(f"Unsupported data provider: {provider_name}") from exc

"""Provider selection helpers for the market-data layer."""

from opx.config import get_runtime_config
from opx.providers.base import DataProvider
from opx.providers.yfinance import YFinanceProvider


class MassiveProvider(DataProvider):
    """Placeholder provider used until the Massive implementation lands."""

    name = "massive"

    def load_vix_snapshot(self) -> dict:
        raise NotImplementedError("Provider 'massive' is not implemented yet.")

    def load_underlying_snapshot(self, ticker: str) -> dict:
        raise NotImplementedError("Provider 'massive' is not implemented yet.")

    def list_option_expirations(self, ticker: str) -> list[str]:
        raise NotImplementedError("Provider 'massive' is not implemented yet.")

    def load_option_chain(self, ticker: str, expiration_date: str):
        raise NotImplementedError("Provider 'massive' is not implemented yet.")

    def normalize_option_frame(
        self,
        df,
        underlying_price,
        expiration_date,
        option_type,
        ticker,
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments
        raise NotImplementedError("Provider 'massive' is not implemented yet.")


PROVIDER_FACTORIES = {
    "yfinance": YFinanceProvider,
    "massive": MassiveProvider,
}


def get_data_provider() -> DataProvider:
    """Return the configured market-data provider implementation."""
    provider_name = get_runtime_config().data_provider
    try:
        return PROVIDER_FACTORIES[provider_name]()
    except KeyError as exc:
        raise ValueError(f"Unsupported data provider: {provider_name}") from exc

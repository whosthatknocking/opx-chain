"""Provider abstractions for loading market data from different vendors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class OptionChainFrames:
    """Vendor option-chain payload split into calls and puts."""

    calls: pd.DataFrame
    puts: pd.DataFrame


class DataProvider(ABC):
    """Abstract market-data provider used by the fetch pipeline."""

    name: str

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Logger names used by vendor libraries that should be routed to the run log."""
        return ()

    @abstractmethod
    def load_vix_snapshot(self) -> dict:
        """Load the latest market-volatility snapshot for the run."""

    @abstractmethod
    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Load the current underlying snapshot for one ticker."""

    @abstractmethod
    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return available option expiration strings for a ticker."""

    @abstractmethod
    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load the raw option chain for one ticker and expiration."""

    @abstractmethod
    # The provider contract needs these canonical normalization inputs.
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def normalize_option_frame(
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Map one vendor-specific option frame into the canonical schema."""

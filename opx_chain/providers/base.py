"""Provider abstractions for loading market data from different vendors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import math
from numbers import Integral
from pathlib import Path
import random
import re
import threading
import time

import numpy as np
import pandas as pd

from opx_chain.config import get_runtime_config
from opx_chain.normalize import normalize_vendor_option_frame
from opx_chain.json_utils import to_python_scalar
from opx_chain.storage.atomic import atomic_write_text
from opx_chain.timestamps import format_utc_compact, format_utc_z_seconds


class ProviderAuthenticationError(RuntimeError):
    """Raised when provider authentication fails and the run should stop clearly."""


class ProviderQuotaError(RuntimeError):
    """Raised when the provider rejects the request due to a quota or rate limit."""


TRANSIENT_BASE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TimeoutError,
    ConnectionError,
)


def empty_underlying_snapshot() -> dict:
    """Return the canonical missing underlying snapshot payload."""
    return {
        "underlying_price": np.nan,
        "underlying_price_time": pd.NaT,
        "underlying_day_change_pct": np.nan,
        "historical_volatility": np.nan,
    }


def positive_int_arg(value: object, *, name: str) -> int:
    """Validate a public provider integer argument."""
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a positive integer")
    resolved = int(value)
    if resolved <= 0:
        raise ValueError(f"{name} must be positive")
    return resolved


def date_arg(value: object, *, name: str) -> date:
    """Validate a public provider date argument before any provider call."""
    if isinstance(value, datetime):
        raise ValueError(f"{name} must be a date, not a datetime")
    if isinstance(value, date):
        return value
    raise ValueError(f"{name} must be a date")


def compute_backoff_delay(
    attempt: int,
    base_seconds: float,
    *,
    max_seconds: float = 60.0,
    jitter_range: tuple[float, float] = (0.7, 1.3),
) -> float:
    """Return capped exponential backoff with jitter for transient provider retries."""
    try:
        base_value = float(base_seconds)
    except (TypeError, ValueError):
        return 0.0
    try:
        max_value = float(max_seconds)
    except (TypeError, ValueError):
        max_value = 60.0
    if not math.isfinite(base_value) or base_value <= 0:
        return 0.0
    if not math.isfinite(max_value) or max_value <= 0:
        max_value = 60.0
    base_delay = base_value * (2 ** max(0, attempt))
    low, high = jitter_range
    if high <= 0 or low <= 0 or high < low:
        return min(base_delay, max_value)
    return min(base_delay * random.uniform(low, high), max_value)


class RequestThrottle:  # pylint: disable=too-few-public-methods
    """Thread-safe request pacing helper shared by provider wrappers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_request_started_at: float | None = None

    def wait(self, interval_seconds: float) -> None:
        """Sleep until the configured minimum interval has elapsed."""
        try:
            interval = float(interval_seconds)
        except (TypeError, ValueError):
            interval = 0.0
        if not math.isfinite(interval) or interval <= 0:
            interval = 0.0
        with self._lock:
            if self._last_request_started_at is not None and interval > 0:
                elapsed = time.monotonic() - self._last_request_started_at
                remaining = interval - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_request_started_at = time.monotonic()


def is_provider_quota_error(exc: Exception) -> bool:
    """Return True when a provider exception represents quota/rate-limit exhaustion."""
    status_code = (
        getattr(exc, "status_code", None)
        or getattr(exc, "status", None)
        or getattr(getattr(exc, "response", None), "status_code", None)
        or getattr(getattr(exc, "response", None), "status", None)
    )
    if str(status_code) == "429":
        return True
    message = str(exc).lower()
    quota_patterns = (
        r"\bapi\s+quota\b",
        r"\bprovider\s+quota\b",
        r"\brequest\s+quota\b",
        r"\bquota\s*/\s*rate\s*limit\b",
    )
    if any(re.search(pattern, message) for pattern in quota_patterns):
        return True
    return any(
        token in message
        for token in (
            "429",
            "rate limit",
            "rate-limit",
            "rate limited",
            "request limit",
            "too many requests",
        )
    )


@dataclass(frozen=True)
class OptionChainFrames:
    """Vendor option-chain payload split into calls and puts."""

    calls: pd.DataFrame
    puts: pd.DataFrame


def _to_json_ready(value):  # pylint: disable=too-many-return-statements
    """Convert provider payloads into JSON-serializable structures."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(float(value)) else None
    try:
        missing = pd.isna(value)
        if isinstance(missing, (bool, np.bool_)) and bool(missing):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_ready(item) for item in value]
    if isinstance(value, pd.DataFrame):
        records = value.where(pd.notna(value), None).to_dict(orient="records")
        return [_to_json_ready(record) for record in records]
    if isinstance(value, pd.Series):
        return _to_json_ready(value.where(pd.notna(value), None).to_dict())
    scalar = to_python_scalar(value)
    if scalar is not value:
        return scalar
    if hasattr(value, "__dict__"):
        return {
            key: _to_json_ready(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


class DataProvider(ABC):
    """Abstract market-data provider used by the fetch pipeline."""

    name: str

    def prepare_ticker_fetch(self, ticker: str) -> None:  # pylint: disable=unused-argument
        """Reset provider-local per-ticker state before a new fetch pipeline call."""

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Logger names used by vendor libraries that should be routed to the run log."""
        return ()

    def debug_dump_payload(self, ticker: str, label: str, payload) -> Path | None:
        """Write a raw provider payload dump when shared debug mode is enabled."""
        config = get_runtime_config()
        if not config.debug_dump_provider_payload:
            return None
        dump_dir = Path(config.debug_dump_dir)
        dump_dir.mkdir(parents=True, exist_ok=True)
        fetched_at = datetime.now(timezone.utc)
        timestamp = format_utc_compact(fetched_at)
        safe_label = label.replace(" ", "_")
        dump_path = dump_dir / f"{self.name}_{ticker.upper()}_{safe_label}_{timestamp}.json"
        debug_payload = {
            "provider": self.name,
            "ticker": ticker.upper(),
            "label": label,
            "fetched_at": format_utc_z_seconds(fetched_at),
            "payload": _to_json_ready(payload),
        }
        atomic_write_text(
            dump_path,
            json.dumps(debug_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(f"{self.name} debug: dumped {label} payload to {dump_path}")
        return dump_path

    def load_ticker_events(self, ticker: str) -> dict:  # pylint: disable=unused-argument
        """Return corporate event data for a ticker. Override for providers that support it."""
        return {
            "next_earnings_date": None,
            "next_earnings_date_is_estimated": None,
            "next_earnings_date_source": None,
            "next_earnings_date_confidence": None,
            "next_ex_div_date": None,
            "next_ex_div_date_source": None,
            "next_ex_div_date_confidence": None,
            "dividend_amount": np.nan,
        }

    def load_price_history(
        self,
        ticker: str,  # pylint: disable=unused-argument
        *,
        lookback_days: int,  # pylint: disable=unused-argument
    ) -> pd.DataFrame:
        """Return daily OHLCV history for optional price context, when supported."""
        return pd.DataFrame()

    def load_benchmark_price_history(self, request):  # pylint: disable=unused-argument
        """Return strict bounded benchmark rows only when adjustment basis is proven.

        Called by the approved public benchmark producer, not fetch orchestration.
        Ordinary OHLCV history does not imply this capability.
        """
        # Keep the optional producer out of ordinary provider initialization.
        from opx_chain.benchmark_series import BenchmarkSeriesError  # pylint: disable=import-outside-toplevel
        raise BenchmarkSeriesError("UNSUPPORTED_PROVIDER")

    def load_historical_option_chain_frame(
        self,
        ticker: str,  # pylint: disable=unused-argument
        *,
        observation_date: date,  # pylint: disable=unused-argument
    ) -> pd.DataFrame:
        """Return a historical option-chain frame for IV-history seeding, when supported."""
        return pd.DataFrame()

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


# pylint: disable=too-many-arguments
def normalize_provider_frame(
    *,
    df: pd.DataFrame,
    underlying_price: float,
    expiration_date: str,
    option_type: str,
    ticker: str,
    data_source: str,
) -> pd.DataFrame:
    """Apply the shared canonical vendor normalization for one provider frame."""
    return normalize_vendor_option_frame(
        df=df,
        underlying_price=underlying_price,
        expiration_date=expiration_date,
        option_type=option_type,
        ticker=ticker,
        data_source=data_source,
    )

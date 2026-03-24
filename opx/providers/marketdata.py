"""Market Data provider implementation backed by the official SDK."""

# pylint: disable=missing-kwoa

from __future__ import annotations

from functools import lru_cache
import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from marketdata.client import MarketDataClient
from marketdata.input_types.base import Mode, OutputFormat
from marketdata.sdk_error import MarketDataClientErrorResult

from opx.config import SCRIPT_VERSION, get_provider_credentials, get_runtime_config
from opx.providers.base import (
    DataProvider,
    OptionChainFrames,
    ProviderAuthenticationError,
    normalize_provider_frame,
)
from opx.utils import coerce_float, normalize_timestamp

CALLER_USER_AGENT = f"opx/{SCRIPT_VERSION}"
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0


class OpxMarketDataClient(MarketDataClient):  # pylint: disable=too-few-public-methods
    """Disable the SDK startup rate-limit probe so provider init does not spend an API call."""

    def _setup_rate_limits(self):
        self.rate_limits = None

    def _check_rate_limits(self, raise_error: bool = True):
        return None


def _as_dict(value: Any) -> dict[str, Any]:
    """Convert SDK dataclass-like results into a plain dict."""
    if isinstance(value, dict):
        return value
    return {
        key: item
        for key, item in vars(value).items()
        if not key.startswith("_")
    }


def _count_payload_rows(payload: Any) -> int:
    """Return the row count for the known Market Data response shapes."""
    if not isinstance(payload, dict):
        return 0
    for key in ("optionSymbol", "expirations", "symbol"):
        values = payload.get(key)
        if isinstance(values, list):
            return len(values)
    return 0


def _normalize_marketdata_expiration_series(series: pd.Series) -> pd.Series:
    """Normalize Market Data expiration values into YYYY-MM-DD strings."""
    if pd.api.types.is_datetime64_any_dtype(series):
        timestamps = pd.to_datetime(series, utc=True, errors="coerce")
    elif pd.api.types.is_numeric_dtype(series):
        timestamps = pd.to_datetime(series, unit="s", utc=True, errors="coerce")
    else:
        timestamps = pd.to_datetime(series, utc=True, errors="coerce")
    return timestamps.dt.strftime("%Y-%m-%d")


class MarketDataProvider(DataProvider):
    """Market-data provider backed by the official Market Data Python SDK."""

    name = "marketdata"

    def __init__(self) -> None:
        self._debug_call_sequence = 0
        self._active_debug_ticker: str | None = None
        self._last_request_started_at: float | None = None

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose SDK logs so the run log can capture provider-library messages."""
        return ("marketdata.logger",)

    def _api_token(self) -> str:
        credentials = get_provider_credentials(self.name)
        return credentials["api_token"]

    def _mode(self) -> Mode | None:
        """Return the configured Market Data mode enum, if set."""
        mode = get_runtime_config().marketdata_mode
        return None if mode is None else Mode(mode)

    def _max_retries(self) -> int:
        """Return the configured Market Data retry count for 429 responses."""
        return get_runtime_config().marketdata_max_retries

    def _request_interval_seconds(self) -> float:
        """Return the configured minimum spacing between Market Data HTTP requests."""
        return get_runtime_config().marketdata_request_interval_seconds

    @lru_cache(maxsize=1)
    def _client(self) -> OpxMarketDataClient:
        """Construct the official Market Data client once per provider instance."""
        client = OpxMarketDataClient(
            token=self._api_token(),
            logger=logging.getLogger("marketdata.logger"),
        )
        client.headers["User-Agent"] = CALLER_USER_AGENT
        client.client.headers["User-Agent"] = CALLER_USER_AGENT
        client._make_request = self._wrap_logged_request(  # pylint: disable=protected-access
            client._make_request  # pylint: disable=protected-access
        )  # type: ignore[method-assign]
        return client

    def _wrap_logged_request(self, wrapped_request):
        """Apply pacing, retry 429s, and log each Market Data HTTP response."""

        def logged_request(method, url, *args, **kwargs):
            endpoint_label = self._classify_endpoint(url)
            for attempt in range(self._max_retries() + 1):
                self._sleep_for_request_interval()
                response = wrapped_request(method, url, *args, **kwargs)
                decoded = self._decode_response_json(response)
                self._debug_call_sequence += 1
                results_count = _count_payload_rows(decoded)
                self._dump_debug_payload(url, method, endpoint_label, response, decoded)
                print(
                    (
                        f"marketdata api: {endpoint_label} status={response.status_code} "
                        f"results_count={results_count}"
                    )
                )
                if response.status_code != 429 or attempt == self._max_retries():
                    return response

                retry_delay = self._retry_delay_seconds(response, attempt)
                print(
                    f"marketdata api: {endpoint_label} rate_limit_retry_in="
                    f"{retry_delay:.2f}s attempt={attempt + 1}/{self._max_retries()}"
                )
                time.sleep(retry_delay)

            return response

        return logged_request

    def _sleep_for_request_interval(self) -> None:
        """Respect the configured minimum spacing between HTTP requests."""
        interval_seconds = self._request_interval_seconds()
        if self._last_request_started_at is not None and interval_seconds > 0:
            elapsed = time.monotonic() - self._last_request_started_at
            remaining = interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_started_at = time.monotonic()

    @staticmethod
    def _decode_response_json(response):
        """Decode a JSON response body when available."""
        try:
            return response.json()
        except (ValueError, TypeError, AttributeError):
            return None

    def _dump_debug_payload(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, url, method, endpoint_label, response, decoded
    ) -> None:
        """Persist the raw provider response when debug dumping is enabled."""
        if not self._active_debug_ticker:
            return
        self.debug_dump_payload(
            self._active_debug_ticker,
            f"{endpoint_label}_{self._debug_call_sequence:03d}",
            {
                "method": method,
                "status": response.status_code,
                "url": url,
                "decoded_response": decoded,
            },
        )

    @staticmethod
    def _retry_delay_seconds(response, attempt: int) -> float:
        """Use Retry-After when present, otherwise exponential backoff."""
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After")
        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except (TypeError, ValueError):
                pass
        return DEFAULT_RETRY_BACKOFF_SECONDS * (2 ** attempt)

    @staticmethod
    def _classify_endpoint(url: str) -> str:
        """Reduce SDK URL paths to a stable progress label."""
        if "options/chain/" in url:
            return "options_chain"
        return "request"

    @staticmethod
    def _raise_if_error(result, *, context: str):
        """Convert SDK error results into provider exceptions."""
        if not isinstance(result, MarketDataClientErrorResult):
            return result

        error = result.error
        message = getattr(error, "message", str(error))
        status_code = getattr(error, "status_code", 0)
        normalized = message.lower()
        if status_code in {401, 403} or any(
            token in normalized for token in ("unauthorized", "forbidden", "token", "auth")
        ):
            raise ProviderAuthenticationError(
                "Market Data authentication failed. Check [providers.marketdata] api_token "
                "in ~/.config/opx/config.toml."
            )
        raise RuntimeError(f"Market Data {context} failed: {message}")

    @lru_cache(maxsize=32)
    def _chain_frame(self, ticker: str) -> pd.DataFrame:
        """Load the full option chain once and split/filter it in memory."""
        self._debug_call_sequence = 0
        self._active_debug_ticker = ticker.upper()
        try:
            result = self._client().options.chain(
                ticker.upper(),
                expiration="all",
                output_format=OutputFormat.INTERNAL,
                mode=self._mode(),
            )  # pylint: disable=missing-kwoa
            chain = self._raise_if_error(result, context="options chain request")
            payload = {
                key: value
                for key, value in _as_dict(chain).items()
                if key != "s"
            }
            if not payload:
                return pd.DataFrame()
            frame = pd.DataFrame(payload)
            if "expiration" in frame.columns:
                frame["expiration_date"] = _normalize_marketdata_expiration_series(
                    frame["expiration"]
                )
            return frame
        finally:
            self._active_debug_ticker = None

    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Load the underlying snapshot from the cached Market Data chain payload."""
        chain_frame = self._chain_frame(ticker)
        underlying_price = coerce_float(
            chain_frame["underlyingPrice"].dropna().iloc[0]
            if "underlyingPrice" in chain_frame.columns and not chain_frame.empty
            else np.nan
        )
        option_quote_time = normalize_timestamp(
            chain_frame["updated"].dropna().max()
            if "updated" in chain_frame.columns and not chain_frame["updated"].dropna().empty
            else None
        )

        return {
            "underlying_price": underlying_price,
            "underlying_price_time": option_quote_time,
            "underlying_day_change_pct": np.nan,
            "historical_volatility": np.nan,
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return distinct expiration dates present in the full chain payload."""
        frame = self._chain_frame(ticker)
        if frame.empty or "expiration_date" not in frame.columns:
            return []
        expirations = frame["expiration_date"].dropna().astype(str).unique().tolist()
        return sorted(expirations)

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Filter the cached chain payload down to one expiration and split by side."""
        frame = self._chain_frame(ticker)
        if frame.empty:
            return OptionChainFrames(calls=pd.DataFrame(), puts=pd.DataFrame())

        scoped = frame.loc[frame["expiration_date"] == expiration_date].copy()
        if "contract_size" not in scoped.columns:
            scoped["contract_size"] = "REGULAR"

        calls = scoped.loc[scoped["side"] == "call"].copy()
        puts = scoped.loc[scoped["side"] == "put"].copy()
        return OptionChainFrames(calls=calls, puts=puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a Market Data options-chain frame into the canonical schema."""
        normalized = df.rename(
            columns={
                "optionSymbol": "contract_symbol",
                "underlying": "underlying_symbol",
                "updated": "option_quote_time",
                "last": "last_trade_price",
                "openInterest": "open_interest",
                "inTheMoney": "is_in_the_money",
                "iv": "implied_volatility",
            }
        )
        return normalize_provider_frame(
            df=normalized,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )

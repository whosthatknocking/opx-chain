"""Massive provider implementation backed by the official Massive client."""

# pylint: disable=duplicate-code

from __future__ import annotations

from functools import lru_cache
import json
import time
from typing import Any

import numpy as np
import pandas as pd
from massive import RESTClient

from opx.config import (
    DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT,
    SCRIPT_VERSION,
    get_provider_credentials,
    get_runtime_config,
)
from opx.providers.base import (
    DataProvider,
    OptionChainFrames,
    ProviderAuthenticationError,
    normalize_provider_frame,
)
from opx.utils import coerce_float, normalize_timestamp

MAX_RETRIES = 3
BACKOFF_SECONDS = 1.0
DEFAULT_SNAPSHOT_PAGE_LIMIT = DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT
CALLER_USER_AGENT = f"opx/{SCRIPT_VERSION}"


def _coalesce(*values: Any) -> Any:
    """Return the first value that is not None and not NaN-like."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and np.isnan(value):
            continue
        return value
    return None


def _get_field(value: Any, *path: str) -> Any:
    """Read nested attributes or dict keys without assuming one payload shape."""
    current = value
    for part in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def _normalize_contract_type(value: Any) -> str | None:
    """Map Massive contract types to canonical call/put labels."""
    normalized = str(value).strip().lower() if value is not None else ""
    if normalized in {"call", "c"}:
        return "call"
    if normalized in {"put", "p"}:
        return "put"
    return None


def _normalize_contract_symbol(value: Any) -> str | None:
    """Normalize Massive option symbols into the canonical contract identifier."""
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized.startswith("O:"):
        return normalized[2:]
    return normalized or None


def _compute_is_in_the_money(result: Any, option_type: str | None) -> bool | None:
    """Infer in-the-money state from the snapshot underlying price and strike."""
    if option_type is None:
        return None
    underlying_price = coerce_float(
        _coalesce(
            _get_field(result, "underlying_asset", "price"),
            _get_field(result, "underlying_asset", "value"),
        )
    )
    strike_price = coerce_float(_get_field(result, "details", "strike_price"))
    if pd.isna(underlying_price) or pd.isna(strike_price):
        return None
    if option_type == "call":
        return bool(underlying_price > strike_price)
    if option_type == "put":
        return bool(underlying_price < strike_price)
    return None

class MassiveProvider(DataProvider):
    """Market-data provider backed by the official Massive/Polygon REST client."""

    name = "massive"

    def __init__(self) -> None:
        self._last_request_started_at: float | None = None
        self._api_page_count = 0
        self._api_result_count = 0
        self._debug_call_sequence = 0
        self._active_debug_ticker: str | None = None

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose urllib3 logging used underneath the official client."""
        return ("urllib3",)

    def _api_key(self) -> str:
        credentials = get_provider_credentials(self.name)
        return credentials["api_key"]

    @lru_cache(maxsize=1)
    def _client(self) -> RESTClient:
        """Construct the official Massive REST client once per provider instance."""
        client = RESTClient(api_key=self._api_key(), retries=MAX_RETRIES, pagination=True)
        client.headers["User-Agent"] = CALLER_USER_AGENT
        client.client.headers["User-Agent"] = CALLER_USER_AGENT
        client.client.request = self._wrap_logged_request(client.client.request)
        client._get = self._wrap_rate_limited_get(client._get)  # pylint: disable=protected-access
        return client

    def _snapshot_page_limit(self) -> int:
        """Return the configured Massive snapshot page size."""
        return get_runtime_config().massive_snapshot_page_limit

    def _request_interval_seconds(self) -> float:
        """Return the configured minimum spacing between Massive HTTP requests."""
        return get_runtime_config().massive_request_interval_seconds

    def _wrap_rate_limited_get(self, wrapped_get):
        """Enforce minimum spacing between underlying Massive client HTTP calls."""

        def rate_limited_get(*args, **kwargs):
            interval_seconds = self._request_interval_seconds()
            if self._last_request_started_at is not None and interval_seconds > 0:
                elapsed = time.monotonic() - self._last_request_started_at
                remaining = interval_seconds - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_request_started_at = time.monotonic()
            return wrapped_get(*args, **kwargs)

        return rate_limited_get

    def _wrap_logged_request(self, wrapped_request):
        """Print Massive API call status and payload counts for each HTTP request."""

        def logged_request(method, url, *args, **kwargs):
            try:
                response = wrapped_request(method, url, *args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print(f"massive api: snapshot_chain error={exc}")
                raise
            payload_bits = []
            decoded = None
            response_data = getattr(response, "data", None)
            if response_data:
                try:
                    decoded = json.loads(response_data.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
                    decoded = None
                if isinstance(decoded, dict):
                    results = decoded.get("results")
                    if isinstance(results, list):
                        page_result_count = len(results)
                        self._api_page_count += 1
                        self._api_result_count += page_result_count
                        payload_bits.append(f"page={self._api_page_count}")
                        payload_bits.append(f"results_count={page_result_count}")
                        payload_bits.append(f"results_total={self._api_result_count}")
                    if "next_url" in decoded:
                        payload_bits.append("has_next_page=true")
                    else:
                        payload_bits.append("has_next_page=false")
            self._debug_call_sequence += 1
            if self._active_debug_ticker:
                self.debug_dump_payload(
                    self._active_debug_ticker,
                    f"snapshot_chain_page_{self._debug_call_sequence:03d}",
                    {
                        "method": method,
                        "status": getattr(response, "status", None),
                        "url": url,
                        "page": self._debug_call_sequence,
                        "results_total_so_far": self._api_result_count,
                        "decoded_response": decoded,
                    },
                )
            suffix = "" if not payload_bits else " " + " ".join(payload_bits)
            print(f"massive api: snapshot_chain status={response.status}{suffix}")
            return response

        return logged_request

    def _fetch_snapshot_results(self, ticker: str) -> tuple[Any, ...]:
        """Load the per-ticker snapshot chain via the single Massive collection call."""
        last_error: Exception | None = None
        self._api_page_count = 0
        self._api_result_count = 0
        self._debug_call_sequence = 0
        self._active_debug_ticker = ticker.upper()

        try:
            for attempt in range(MAX_RETRIES + 1):
                try:
                    results = self._client().list_snapshot_options_chain(
                        ticker.upper(),
                        params={"limit": self._snapshot_page_limit()},
                    )
                    return tuple(results)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    last_error = exc
                    message = str(exc).lower()
                    if "401" in message or "403" in message or "auth" in message:
                        raise ProviderAuthenticationError(
                            "Massive authentication failed. Check [providers.massive] api_key "
                            "in ~/.config/opx/config.toml."
                        ) from exc
                    if attempt == MAX_RETRIES:
                        raise
                    time.sleep(BACKOFF_SECONDS * (2 ** attempt))

            raise RuntimeError(
                "Massive snapshot request failed without a response."
            ) from last_error
        finally:
            self._active_debug_ticker = None

    @lru_cache(maxsize=32)
    def _snapshot_results(self, ticker: str) -> tuple[Any, ...]:
        """Cache snapshot results once per ticker for the current process."""
        return self._fetch_snapshot_results(ticker)

    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Infer the underlying snapshot from the option snapshot payload."""
        results = self._snapshot_results(ticker)
        if not results:
            return {
                "underlying_price": np.nan,
                "underlying_price_time": pd.NaT,
                "underlying_day_change_pct": np.nan,
                "historical_volatility": np.nan,
            }

        first = results[0]
        underlying_price = coerce_float(
            _coalesce(
                _get_field(first, "underlying_asset", "price"),
                _get_field(first, "underlying_asset", "value"),
            )
        )
        underlying_price_time = normalize_timestamp(
            _coalesce(
                _get_field(first, "underlying_asset", "last_updated"),
                _get_field(first, "day", "last_updated"),
                _get_field(first, "last_trade", "sip_timestamp"),
                _get_field(first, "last_quote", "last_updated"),
                _get_field(first, "last_quote", "sip_timestamp"),
            )
        )
        previous_close = coerce_float(_get_field(first, "day", "previous_close"))
        if pd.notna(underlying_price) and pd.notna(previous_close) and previous_close > 0:
            underlying_day_change_pct = (underlying_price - previous_close) / previous_close
        else:
            underlying_day_change_pct = np.nan
        return {
            "underlying_price": underlying_price,
            "underlying_price_time": underlying_price_time,
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": np.nan,
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return distinct expiration dates present in the Massive snapshot payload."""
        expirations = {
            _get_field(result, "details", "expiration_date")
            for result in self._snapshot_results(ticker)
        }
        return sorted(expiration for expiration in expirations if expiration)

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Filter the snapshot payload down to one expiration and split by side."""
        rows = []
        for result in self._snapshot_results(ticker):
            if _get_field(result, "details", "expiration_date") != expiration_date:
                continue
            option_type = _normalize_contract_type(_get_field(result, "details", "contract_type"))
            if option_type is None:
                continue
            row = {
                "contract_symbol": _normalize_contract_symbol(
                    _coalesce(
                        _get_field(result, "details", "ticker"),
                        _get_field(result, "ticker"),
                    )
                ),
                "underlying_symbol": _coalesce(
                    _get_field(result, "underlying_asset", "ticker"),
                    ticker.upper(),
                ),
                "option_type": option_type,
                "strike": _get_field(result, "details", "strike_price"),
                "expiration_date": expiration_date,
                "contract_size": _coalesce(
                    _get_field(result, "details", "shares_per_contract"),
                    "REGULAR",
                ),
                "option_quote_time": _coalesce(
                    _get_field(result, "last_quote", "last_updated"),
                    _get_field(result, "last_quote", "sip_timestamp"),
                    _get_field(result, "last_trade", "sip_timestamp"),
                    _get_field(result, "day", "last_updated"),
                ),
                "bid": _coalesce(
                    _get_field(result, "last_quote", "bid"),
                    _get_field(result, "last_quote", "bid_price"),
                ),
                "ask": _coalesce(
                    _get_field(result, "last_quote", "ask"),
                    _get_field(result, "last_quote", "ask_price"),
                ),
                "last_trade_price": _coalesce(
                    _get_field(result, "last_trade", "price"),
                    _get_field(result, "day", "close"),
                ),
                "volume": _get_field(result, "day", "volume"),
                "open_interest": _get_field(result, "open_interest"),
                "implied_volatility": coerce_float(_get_field(result, "implied_volatility")),
                "change": _get_field(result, "day", "change"),
                "percent_change": _get_field(result, "day", "change_percent"),
                "is_in_the_money": _compute_is_in_the_money(result, option_type),
                "delta": _get_field(result, "greeks", "delta"),
                "gamma": _get_field(result, "greeks", "gamma"),
                "theta": _get_field(result, "greeks", "theta"),
                "vega": _get_field(result, "greeks", "vega"),
            }
            rows.append(row)

        frame = pd.DataFrame(rows)
        if frame.empty:
            empty = pd.DataFrame()
            return OptionChainFrames(calls=empty, puts=empty)

        calls = frame[frame["option_type"] == "call"].copy()
        puts = frame[frame["option_type"] == "put"].copy()
        return OptionChainFrames(calls=calls, puts=puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a Massive frame into the canonical options schema."""
        frame = normalize_provider_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )
        for greek_column in ["delta", "gamma", "theta", "vega"]:
            if greek_column in frame.columns:
                frame[greek_column] = pd.to_numeric(frame[greek_column], errors="coerce")
        return frame

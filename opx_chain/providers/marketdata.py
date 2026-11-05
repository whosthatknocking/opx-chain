"""Market Data provider implementation backed by the official SDK."""

# pylint: disable=missing-kwoa,too-many-lines

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import math
import time
from typing import Any

import httpx
import numpy as np
import pandas as pd
from marketdata.client import MarketDataClient
from marketdata.input_types.base import Mode, OutputFormat
from marketdata.sdk_error import MarketDataClientErrorResult
from scipy.optimize import brentq
from scipy.stats import norm

from opx_chain._integrity_validation import (
    provider_payload_to_frame,
    validate_option_chain_provider_response,
)
from opx_chain.config import (
    SCRIPT_VERSION,
    get_provider_credentials,
    get_runtime_config,
)
from opx_chain.json_utils import loads_strict_json, sanitize_json_payload
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT, OPTION_TYPES
from opx_chain.paths import get_default_config_path
from opx_chain.providers.base import (
    DataProvider,
    OptionChainFrames,
    ProviderAuthenticationError,
    ProviderQuotaError,
    RequestThrottle,
    TRANSIENT_BASE_EXCEPTIONS,
    compute_backoff_delay,
    empty_underlying_snapshot,
    date_arg,
    is_provider_quota_error,
    normalize_provider_frame,
    positive_int_arg,
)
from opx_chain.providers._dates import parse_event_date as _parse_event_date
from opx_chain.runlog import get_logger, logger_name
from opx_chain.schema import SOURCE_QUOTE_COLUMN
from opx_chain.utils import coerce_float, finite_float_or_none, normalize_timestamp

CALLER_USER_AGENT = f"opx-chain/{SCRIPT_VERSION}"
_SDK_LOGGER_SUFFIX = "providers.marketdata.sdk"
_SDK_LOGGER_NAME = logger_name(_SDK_LOGGER_SUFFIX)
_LOGGER = get_logger("providers.marketdata")
TRANSIENT_REQUEST_EXCEPTIONS = (
    *TRANSIENT_BASE_EXCEPTIONS,
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
)
_MIN_DERIVED_IV = 0.0001
_MAX_DERIVED_IV = 5.0


@dataclass(frozen=True)
class _EventDateMetadata:
    """Canonical event date plus provider confidence metadata."""

    value: str | None
    is_estimated: bool | None
    source: str | None
    confidence: str | None


_BLANK_EVENT_DATE = _EventDateMetadata(None, None, None, None)


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


class _MarketDataErrorForClassification(RuntimeError):
    """Adapter that lets MarketData-specific errors use the shared classifier."""

    def __init__(self, message: str, status_code: Any) -> None:
        super().__init__(message or f"HTTP {status_code}")
        self.status_code = status_code


def _is_marketdata_quota_error(message: str, status_code: Any) -> bool:
    """Return True when a MarketData error is quota/rate-limit related."""
    return is_provider_quota_error(
        _MarketDataErrorForClassification(message, status_code)
    )


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
    return series.map(_parse_event_date).map(
        lambda value: value.isoformat() if value is not None else np.nan
    )


def _black_scholes_price(  # pylint: disable=too-many-arguments
    option_type: str,
    *,
    spot: float,
    strike: float,
    years: float,
    risk_free_rate: float,
    sigma: float,
) -> float:
    """Return the Black-Scholes option value for one row."""
    sqrt_years = math.sqrt(years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate + 0.5 * sigma * sigma) * years
    ) / (sigma * sqrt_years)
    d2 = d1 - sigma * sqrt_years
    discounted_strike = strike * math.exp(-risk_free_rate * years)
    if option_type == OPTION_TYPE_CALL:
        return (spot * norm.cdf(d1)) - (discounted_strike * norm.cdf(d2))
    return (discounted_strike * norm.cdf(-d2)) - (spot * norm.cdf(-d1))


def _black_scholes_delta(  # pylint: disable=too-many-arguments
    option_type: str,
    *,
    spot: float,
    strike: float,
    years: float,
    risk_free_rate: float,
    sigma: float,
) -> float:
    """Return the Black-Scholes delta for one row."""
    sqrt_years = math.sqrt(years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate + 0.5 * sigma * sigma) * years
    ) / (sigma * sqrt_years)
    if option_type == OPTION_TYPE_CALL:
        return float(norm.cdf(d1))
    return float(norm.cdf(d1) - 1.0)


def _option_intrinsic_value(option_type: str, *, spot: float, strike: float) -> float:
    if option_type == OPTION_TYPE_CALL:
        return max(spot - strike, 0.0)
    return max(strike - spot, 0.0)


def _derived_historical_iv(  # pylint: disable=too-many-arguments,too-many-boolean-expressions
    option_type: str,
    *,
    price: float,
    spot: float,
    strike: float,
    days_to_expiration: float,
    risk_free_rate: float,
) -> float:
    """Solve implied volatility from historical quote price when vendor IV is absent."""
    years = days_to_expiration / 365.0
    if (
        option_type not in OPTION_TYPES
        or not np.isfinite(price)
        or not np.isfinite(spot)
        or not np.isfinite(strike)
        or not np.isfinite(years)
        or price <= 0
        or spot <= 0
        or strike <= 0
        or years <= 0
    ):
        return np.nan
    intrinsic = _option_intrinsic_value(option_type, spot=spot, strike=strike)
    if price <= intrinsic + 1e-8:
        return np.nan

    def objective(sigma: float) -> float:
        return _black_scholes_price(
            option_type,
            spot=spot,
            strike=strike,
            years=years,
            risk_free_rate=risk_free_rate,
            sigma=sigma,
        ) - price

    try:
        low_value = objective(_MIN_DERIVED_IV)
        high_value = objective(_MAX_DERIVED_IV)
        if not np.isfinite(low_value) or not np.isfinite(high_value):
            return np.nan
        if low_value * high_value > 0:
            return np.nan
        return float(brentq(objective, _MIN_DERIVED_IV, _MAX_DERIVED_IV, maxiter=100))
    except (ValueError, OverflowError, ZeroDivisionError):
        return np.nan


def _historical_reference_prices(frame: pd.DataFrame) -> pd.Series:
    """Return the quote price used to derive historical IV."""
    index = frame.index
    mid = pd.to_numeric(frame.get("mid", pd.Series(np.nan, index=index)), errors="coerce")
    bid = pd.to_numeric(frame.get("bid", pd.Series(np.nan, index=index)), errors="coerce")
    ask = pd.to_numeric(frame.get("ask", pd.Series(np.nan, index=index)), errors="coerce")
    last = pd.to_numeric(frame.get("last", pd.Series(np.nan, index=index)), errors="coerce")
    calculated_mid = ((bid + ask) / 2).where(
        bid.notna() & ask.notna() & (bid >= 0) & (ask >= bid)
    )
    price = mid.where(mid > 0, calculated_mid)
    return price.where(price > 0, last)


def _fill_historical_risk_model(  # pylint: disable=too-many-locals,too-many-boolean-expressions
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Fill missing historical IV/delta from option quote price when possible."""
    required = {"option_type", "strike", "underlying_price", "days_to_expiration"}
    if frame.empty or not required.issubset(frame.columns):
        return frame
    result = frame.copy()
    risk_free_rate = get_runtime_config().risk_free_rate
    prices = _historical_reference_prices(result)
    existing_iv = pd.to_numeric(
        result.get("implied_volatility", pd.Series(np.nan, index=result.index)),
        errors="coerce",
    )
    existing_delta = pd.to_numeric(
        result.get("delta", pd.Series(np.nan, index=result.index)),
        errors="coerce",
    )
    strikes = pd.to_numeric(result["strike"], errors="coerce")
    spots = pd.to_numeric(result["underlying_price"], errors="coerce")
    days = pd.to_numeric(result["days_to_expiration"], errors="coerce")
    option_types = result["option_type"].astype(str).str.strip().str.lower()

    derived_iv: list[float] = []
    derived_delta: list[float] = []
    for idx in result.index:
        iv_value = existing_iv.loc[idx]
        if not np.isfinite(iv_value) or iv_value <= 0:
            iv_value = _derived_historical_iv(
                option_types.loc[idx],
                price=float(prices.loc[idx]),
                spot=float(spots.loc[idx]),
                strike=float(strikes.loc[idx]),
                days_to_expiration=float(days.loc[idx]),
                risk_free_rate=risk_free_rate,
            )
        derived_iv.append(iv_value)
        delta_value = existing_delta.loc[idx]
        years = float(days.loc[idx]) / 365.0
        if (
            (not np.isfinite(delta_value))
            and np.isfinite(iv_value)
            and iv_value > 0
            and years > 0
            and np.isfinite(spots.loc[idx])
            and np.isfinite(strikes.loc[idx])
        ):
            delta_value = _black_scholes_delta(
                option_types.loc[idx],
                spot=float(spots.loc[idx]),
                strike=float(strikes.loc[idx]),
                years=years,
                risk_free_rate=risk_free_rate,
                sigma=float(iv_value),
            )
        derived_delta.append(delta_value)

    result["implied_volatility"] = pd.Series(derived_iv, index=result.index, dtype=float)
    result["delta"] = pd.Series(derived_delta, index=result.index, dtype=float)
    return result


def _row_value(values: Any, index: int) -> Any:
    """Return an indexed provider list value, or None when the row has no value."""
    try:
        return values[index] if index < len(values) else None
    except TypeError:
        return None


def _has_reported_eps(value: Any) -> bool:
    """Return True when Market Data says this earnings period has reported."""
    if value is None:
        return False
    try:
        return not pd.isna(value)
    except (TypeError, ValueError):
        return True


class MarketDataProvider(DataProvider):
    """Market-data provider backed by the official Market Data Python SDK."""

    name = "marketdata"

    def __init__(self) -> None:
        self._debug_call_sequence = 0
        self._active_debug_ticker: str | None = None
        self._request_throttle = RequestThrottle()
        self._client_cache_key: tuple[str] | None = None
        self._client_cache: OpxMarketDataClient | None = None
        self._chain_frame_cache: dict[tuple[str, Mode | None, str | None], pd.DataFrame] = {}
        self._stock_quote_snapshot_cache: dict[tuple[str, Mode | None], dict | None] = {}

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose SDK logs so the run log can capture provider-library messages."""
        return (_SDK_LOGGER_NAME,)

    def prepare_ticker_fetch(self, ticker: str) -> None:  # pylint: disable=unused-argument
        """Clear process-local ticker caches before a new fetch pipeline call."""
        self._chain_frame_cache.clear()
        self._stock_quote_snapshot_cache.clear()

    def _api_token(self) -> str:
        credentials = get_provider_credentials(self.name)
        return credentials["api_token"]

    def _mode(self) -> Mode | None:
        """Return the configured Market Data mode enum, if set."""
        mode = get_runtime_config().marketdata_mode
        return None if mode is None else Mode(mode)

    def _max_retries(self) -> int:
        """Return the configured Market Data retry count for transient responses."""
        return get_runtime_config().marketdata_max_retries

    def _request_interval_seconds(self) -> float:
        """Return the configured minimum spacing between Market Data HTTP requests."""
        return get_runtime_config().marketdata_request_interval_seconds

    def _backoff_seconds(self) -> float:
        """Return the configured Market Data retry backoff base."""
        return get_runtime_config().marketdata_backoff_seconds

    def _raw_endpoint_url(self, endpoint: str, mode: Mode | None = None) -> str:
        """Return a raw SDK endpoint URL with configured mode applied when needed."""
        if mode is None:
            return endpoint
        separator = "&" if "?" in endpoint else "?"
        return f"{endpoint}{separator}mode={mode.value}"

    def _client(self) -> OpxMarketDataClient:
        """Construct the official Market Data client once per provider instance."""
        cache_key = (self._api_token(),)
        if self._client_cache_key == cache_key and self._client_cache is not None:
            return self._client_cache

        client = OpxMarketDataClient(
            token=cache_key[0],
            logger=get_logger(_SDK_LOGGER_SUFFIX),
        )
        client.headers["User-Agent"] = CALLER_USER_AGENT
        client.client.headers["User-Agent"] = CALLER_USER_AGENT
        client._make_request = self._wrap_logged_request(  # pylint: disable=protected-access
            client._make_request  # pylint: disable=protected-access
        )  # type: ignore[method-assign]
        self._client_cache_key = cache_key
        self._client_cache = client
        return client

    def _wrap_logged_request(self, wrapped_request):
        """Apply pacing, retry transient failures, and log Market Data responses."""

        def logged_request(method, url, *args, **kwargs):
            endpoint_label = self._classify_endpoint(url)
            for attempt in range(self._max_retries() + 1):
                self._sleep_for_request_interval()
                try:
                    response = wrapped_request(method, url, *args, **kwargs)
                except TRANSIENT_REQUEST_EXCEPTIONS as exc:
                    if attempt == self._max_retries():
                        raise
                    retry_delay = compute_backoff_delay(attempt, self._backoff_seconds())
                    print(
                        f"marketdata api: {endpoint_label} transient_retry_in="
                        f"{retry_delay:.2f}s attempt={attempt + 1}/{self._max_retries()} "
                        f"error_type={type(exc).__name__} error={exc}"
                    )
                    time.sleep(retry_delay)
                    continue

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
                if not self._is_retryable_response(response) or attempt == self._max_retries():
                    return response

                retry_delay = self._retry_delay_seconds(response, attempt)
                retry_label = (
                    "rate_limit_retry_in="
                    if response.status_code == 429
                    else "transient_retry_in="
                )
                print(
                    f"marketdata api: {endpoint_label} {retry_label}"
                    f"{retry_delay:.2f}s attempt={attempt + 1}/{self._max_retries()} "
                    f"status={response.status_code}"
                )
                time.sleep(retry_delay)

            return response

        return logged_request

    @staticmethod
    def _is_retryable_response(response) -> bool:
        """Return True for transient HTTP statuses worth retrying."""
        status_code = getattr(response, "status_code", 0)
        return status_code in {408, 429} or status_code >= 500

    def _sleep_for_request_interval(self) -> None:
        """Respect the configured minimum spacing between HTTP requests."""
        self._request_throttle.wait(self._request_interval_seconds())

    @staticmethod
    def _decode_response_json(response):
        """Decode a JSON response body when available."""
        try:
            return loads_strict_json(response.text)
        except (ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _is_no_data_response(response) -> bool:
        """Return True for Market Data's expected empty-result response shape."""
        payload = MarketDataProvider._decode_response_json(response)
        if not isinstance(payload, dict):
            return False
        return str(payload.get("s") or "").lower() == "no_data"

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

    def _retry_delay_seconds(self, response, attempt: int) -> float:
        """Use Retry-After when present, otherwise exponential backoff."""
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After")
        if retry_after is not None:
            try:
                retry_delay = float(retry_after)
            except (TypeError, ValueError):
                retry_at = self._parse_retry_after_http_date(retry_after)
                if retry_at is not None and math.isfinite(retry_at):
                    return retry_at
            else:
                if math.isfinite(retry_delay):
                    return max(retry_delay, 0.0)
        return compute_backoff_delay(attempt, self._backoff_seconds())

    @staticmethod
    def _parse_retry_after_http_date(retry_after: str) -> float | None:
        """Return seconds until an HTTP-date Retry-After value, when parseable."""
        try:
            retry_at = parsedate_to_datetime(retry_after)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        retry_at = retry_at.astimezone(timezone.utc)
        return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)

    @staticmethod
    def _classify_endpoint(url: str) -> str:
        """Reduce SDK URL paths to a stable progress label."""
        if "options/chain/" in url:
            return "options_chain"
        if "stocks/candles/" in url:
            return "stocks_candles"
        if "stocks/quotes/" in url:
            return "stocks_quotes"
        if "stocks/earnings/" in url:
            return "stocks_earnings"
        if "stocks/dividends/" in url:
            return "stocks_dividends"
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
                f"in {get_default_config_path()}."
            )
        if _is_marketdata_quota_error(message, status_code):
            raise ProviderQuotaError(f"Market Data {context} failed: {message}")
        raise RuntimeError(f"Market Data {context} failed: {message}")

    @staticmethod
    def _raise_raw_response_if_error(response, *, context: str) -> None:
        """Convert raw HTTP responses into provider exceptions."""
        status_code = getattr(response, "status_code", 200)
        if status_code < 400:
            return
        payload = MarketDataProvider._decode_response_json(response)
        message = ""
        if isinstance(payload, dict):
            message = str(
                payload.get("message")
                or payload.get("errmsg")
                or payload.get("error")
                or payload.get("s")
                or ""
            )
        normalized = message.lower()
        if status_code in {401, 403} or any(
            token in normalized for token in ("unauthorized", "forbidden", "token", "auth")
        ):
            raise ProviderAuthenticationError(
                "Market Data authentication failed. Check [providers.marketdata] api_token "
                f"in {get_default_config_path()}."
            )
        if _is_marketdata_quota_error(message, status_code):
            detail = message or f"HTTP {status_code}"
            raise ProviderQuotaError(f"Market Data {context} failed: {detail}")
        raise RuntimeError(f"Market Data {context} failed: {message or f'HTTP {status_code}'}")

    def _chain_frame(  # pylint: disable=too-many-locals
        self,
        ticker: str,
        mode: Mode | None,
        *,
        chain_date: date | None = None,
    ) -> pd.DataFrame:
        """Load the full option chain once and split/filter it in memory."""
        ticker_key = ticker.upper()
        cache_key = (ticker_key, mode, chain_date.isoformat() if chain_date else None)
        cached = self._chain_frame_cache.get(cache_key)
        if cached is not None:
            return cached

        self._debug_call_sequence = 0
        self._active_debug_ticker = ticker_key
        try:
            chain_kwargs = {
                "expiration": "all",
                "output_format": OutputFormat.INTERNAL,
                "mode": mode,
            }
            if chain_date is not None:
                chain_kwargs["date"] = chain_date.isoformat()
            result = self._client().options.chain(
                ticker_key,
                **chain_kwargs,
            )  # pylint: disable=missing-kwoa
            chain = self._raise_if_error(result, context="options chain request")
            payload = {
                key: value
                for key, value in _as_dict(chain).items()
                if key != "s"
            }
            if not payload:
                return pd.DataFrame()
            frame = provider_payload_to_frame(
                payload,
                ticker=ticker_key,
                provider=self.name,
            )
            raw_sides = frame.get("side", pd.Series(None, index=frame.index))
            normalized_sides = raw_sides.astype("string").str.strip().str.lower()
            call_mask = normalized_sides.isin((OPTION_TYPE_CALL, "c"))
            put_mask = normalized_sides.isin((OPTION_TYPE_PUT, "p"))
            invalid_side_rows = frame.loc[~(call_mask | put_mask)]
            if not invalid_side_rows.empty:
                validate_option_chain_provider_response(
                    invalid_side_rows,
                    pd.DataFrame(),
                    ticker=ticker_key,
                    provider=self.name,
                )
            validate_option_chain_provider_response(
                frame.loc[call_mask],
                frame.loc[put_mask],
                ticker=ticker_key,
                provider=self.name,
            )
            if "expiration" in frame.columns:
                frame["expiration_date"] = _normalize_marketdata_expiration_series(
                    frame["expiration"]
                )
            self._chain_frame_cache[cache_key] = frame
            return frame
        finally:
            self._active_debug_ticker = None

    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Load the underlying snapshot from the cached Market Data chain payload."""
        mode = self._mode()
        quote_snapshot = self._fetch_stock_quote_snapshot(ticker, mode)
        if quote_snapshot is not None:
            return quote_snapshot

        chain_frame = self._chain_frame(ticker, mode)
        return self._snapshot_from_chain_frame(chain_frame)

    @staticmethod
    def _snapshot_from_chain_frame(chain_frame: pd.DataFrame) -> dict:
        """Build a consistent underlying snapshot from one chain row."""
        if chain_frame.empty or "underlyingPrice" not in chain_frame.columns:
            return empty_underlying_snapshot()

        candidates = chain_frame.loc[chain_frame["underlyingPrice"].notna()].copy()
        if candidates.empty:
            return empty_underlying_snapshot()

        if "updated" in candidates.columns:
            candidates["_updated_ts"] = candidates["updated"].map(normalize_timestamp)
            candidates = candidates.sort_values(
                by="_updated_ts",
                ascending=False,
                na_position="last",
            )
        best_row = candidates.iloc[0]
        option_quote_time = normalize_timestamp(best_row.get("updated"))

        return {
            "underlying_price": coerce_float(best_row.get("underlyingPrice")),
            "underlying_price_time": option_quote_time,
            "underlying_day_change_pct": np.nan,
            "historical_volatility": np.nan,
        }

    def _fetch_stock_quote_snapshot(self, ticker: str, mode: Mode | None) -> dict | None:
        """Load a stock quote snapshot so spot price and change stay internally consistent."""
        ticker_key = ticker.upper()
        cache_key = (ticker_key, mode)
        if cache_key in self._stock_quote_snapshot_cache:
            return self._stock_quote_snapshot_cache[cache_key]

        self._active_debug_ticker = ticker_key
        try:
            response = self._client()._make_request(  # pylint: disable=protected-access
                method="GET",
                url=self._raw_endpoint_url(f"stocks/quotes/{ticker_key}/", mode),
            )
            self._raise_raw_response_if_error(response, context="stock quote request")
            quote_data = self._decode_response_json(response)
            if not isinstance(quote_data, dict):
                self._stock_quote_snapshot_cache[cache_key] = None
                return None
            best_quote = self._select_best_quote_row(quote_data)
            if best_quote is None:
                self._stock_quote_snapshot_cache[cache_key] = None
                return None
            snapshot = {
                "underlying_price": best_quote["underlying_price"],
                "underlying_price_time": best_quote["underlying_price_time"],
                "underlying_day_change_pct": best_quote["underlying_day_change_pct"],
                "historical_volatility": np.nan,
            }
            self._stock_quote_snapshot_cache[cache_key] = snapshot
            return snapshot
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            self._stock_quote_snapshot_cache[cache_key] = None
            return None
        finally:
            self._active_debug_ticker = None

    @staticmethod
    def _select_best_quote_row(quote_data: dict[str, Any]) -> dict[str, Any] | None:
        """Pick the most recent usable stock-quote row and keep its fields paired."""
        row_count = max(
            (
                len(values)
                for values in quote_data.values()
                if isinstance(values, list)
            ),
            default=0,
        )
        best_quote = None
        for index in range(row_count):
            last_values = quote_data.get("last") or []
            price = coerce_float(last_values[index] if index < len(last_values) else None)
            if pd.isna(price):
                continue
            updated_values = quote_data.get("updated") or []
            change_pct_values = quote_data.get("changepct") or []
            quote_time = normalize_timestamp(
                updated_values[index] if index < len(updated_values) else None
            )
            quote_row = {
                "underlying_price": price,
                "underlying_price_time": quote_time,
                "underlying_day_change_pct": coerce_float(
                    change_pct_values[index] if index < len(change_pct_values) else np.nan
                ),
            }
            if best_quote is None:
                best_quote = quote_row
                continue
            best_time = best_quote["underlying_price_time"]
            if pd.isna(best_time) and not pd.isna(quote_time):
                best_quote = quote_row
            elif not pd.isna(quote_time) and quote_time > best_time:
                best_quote = quote_row
        return best_quote

    def _fetch_next_earnings_event(self, ticker: str, today: date) -> _EventDateMetadata:
        """Return the next upcoming earnings date and provider confidence metadata.

        Market Data exposes `date` as the fiscal period end for the earnings
        report. Use `reportDate` as the event date and mark future rows as
        estimated until the provider supplies reported EPS, at which point the
        row is historical and should not be selected as an upcoming event.
        """
        try:
            result = self._client().stocks.earnings(
                ticker.upper(),
                output_format=OutputFormat.INTERNAL,
                mode=self._mode(),
            )
            earnings_data = self._raise_if_error(result, context="earnings request")
            report_dates = getattr(earnings_data, "reportDate", None) or []
            reported_eps = getattr(earnings_data, "reportedEPS", None) or []
            row_count = max(len(report_dates), len(reported_eps))
            upcoming: list[_EventDateMetadata] = []
            for idx in range(row_count):
                if _has_reported_eps(_row_value(reported_eps, idx)):
                    continue
                estimated_date = _parse_event_date(_row_value(report_dates, idx))
                if estimated_date is not None and estimated_date >= today:
                    upcoming.append(
                        _EventDateMetadata(
                            estimated_date.isoformat(),
                            True,
                            "marketdata.reportDate",
                            "estimated",
                        )
                    )
            if not upcoming:
                return _BLANK_EVENT_DATE
            return min(upcoming, key=lambda item: item.value or "")
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception as exc:  # pylint: disable=broad-exception-caught
            raise RuntimeError(
                f"MarketData earnings request failed for {ticker.upper()}"
            ) from exc

    def _fetch_next_dividend(
        self,
        ticker: str,
        today: date,
    ) -> tuple[_EventDateMetadata, float]:
        """Return the next upcoming ex-dividend date metadata and amount."""
        try:
            response = self._client()._make_request(  # pylint: disable=protected-access
                method="GET",
                url=self._raw_endpoint_url(f"stocks/dividends/{ticker.upper()}/", self._mode()),
                raise_for_status=False,
                retry_status_codes=[],
            )
            if self._is_no_data_response(response):
                return _BLANK_EVENT_DATE, np.nan
            self._raise_raw_response_if_error(response, context="dividends request")
            div_data = self._decode_response_json(response) or {}
            ex_dates = div_data.get("exDate") or []
            amounts = div_data.get("amount") or []
            upcoming_divs = sorted(
                (
                    (d, _row_value(amounts, idx))
                    for idx, raw in enumerate(ex_dates)
                    if (d := _parse_event_date(raw)) is not None and d >= today
                ),
                key=lambda item: item[0],
            )
            if not upcoming_divs:
                return _BLANK_EVENT_DATE, np.nan
            next_date, next_amount = upcoming_divs[0]
            event = _EventDateMetadata(
                next_date.isoformat(),
                False,
                "marketdata.exDate",
                "confirmed",
            )
            amount = finite_float_or_none(next_amount)
            return event, np.nan if amount is None else amount
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception as exc:  # pylint: disable=broad-exception-caught
            raise RuntimeError(
                f"MarketData dividends request failed for {ticker.upper()}"
            ) from exc

    def load_ticker_events(self, ticker: str) -> dict:
        """Fetch upcoming earnings and dividend event data from the Market Data API."""
        today = get_runtime_config().today
        ticker_upper = ticker.upper()
        try:
            next_earnings = self._fetch_next_earnings_event(ticker, today)
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOGGER.warning(
                "marketdata_event_degraded ticker=%s event=earnings reason=%s",
                ticker_upper,
                exc,
            )
            next_earnings = _BLANK_EVENT_DATE
        try:
            next_ex_div, dividend_amount = self._fetch_next_dividend(ticker, today)
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOGGER.warning(
                "marketdata_event_degraded ticker=%s event=dividends reason=%s",
                ticker_upper,
                exc,
            )
            next_ex_div, dividend_amount = _BLANK_EVENT_DATE, np.nan
        return {
            "next_earnings_date": next_earnings.value,
            "next_earnings_date_is_estimated": next_earnings.is_estimated,
            "next_earnings_date_source": next_earnings.source,
            "next_earnings_date_confidence": next_earnings.confidence,
            "next_ex_div_date": next_ex_div.value,
            "next_ex_div_date_source": next_ex_div.source,
            "next_ex_div_date_confidence": next_ex_div.confidence,
            "dividend_amount": dividend_amount,
        }

    def load_benchmark_price_history(self, request):
        """Return bounded split-only daily SPY history for the benchmark producer."""
        # The optional worker imports producer types; initialize it only on use.
        from opx_chain.providers.benchmark_history import load_marketdata_spy  # pylint: disable=import-outside-toplevel
        return load_marketdata_spy(request)

    def load_price_history(self, ticker: str, *, lookback_days: int) -> pd.DataFrame:
        """Load daily stock candles for optional price-context enrichment."""
        resolved_lookback = positive_int_arg(lookback_days, name="lookback_days")
        self._active_debug_ticker = ticker.upper()
        try:
            result = self._client().stocks.candles(
                ticker.upper(),
                resolution="D",
                countback=resolved_lookback,
                adjust_splits=True,
                output_format=OutputFormat.JSON,
                mode=self._mode(),
            )
            candles_data = self._raise_if_error(result, context="stock candles request")
            if not isinstance(candles_data, dict) or not candles_data:
                return pd.DataFrame()
            self.debug_dump_payload(ticker, "price_history", candles_data)
            return pd.DataFrame(candles_data)
        finally:
            self._active_debug_ticker = None

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return distinct expiration dates present in the full chain payload."""
        frame = self._chain_frame(ticker, self._mode())
        if frame.empty or "expiration_date" not in frame.columns:
            return []
        expirations = frame["expiration_date"].dropna().astype(str).unique().tolist()
        return sorted(expirations)

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Filter the cached chain payload down to one expiration and split by side."""
        frame = self._chain_frame(ticker, self._mode())
        if frame.empty:
            return OptionChainFrames(calls=pd.DataFrame(), puts=pd.DataFrame())

        scoped = frame.loc[frame["expiration_date"] == expiration_date].copy()
        if "contract_size" not in scoped.columns:
            scoped["contract_size"] = "REGULAR"

        calls = scoped.loc[scoped["side"] == OPTION_TYPE_CALL].copy()
        puts = scoped.loc[scoped["side"] == OPTION_TYPE_PUT].copy()
        return OptionChainFrames(calls=calls, puts=puts)

    @staticmethod
    def _iv_history_frame(
        frame: pd.DataFrame,
        *,
        ticker: str,
        observation_date: date,
    ) -> pd.DataFrame:
        """Map a raw Market Data chain payload into IV-history input columns."""
        if frame.empty:
            return pd.DataFrame()
        result = frame.copy()
        if "expiration_date" not in result.columns and "expiration" in result.columns:
            result["expiration_date"] = _normalize_marketdata_expiration_series(
                result["expiration"]
            )
        renamed = result.rename(
            columns={
                "underlying": "underlying_symbol",
                "underlyingPrice": "underlying_price",
                "updated": "option_quote_time",
                "iv": "implied_volatility",
                "side": "option_type",
            }
        )
        if "underlying_symbol" not in renamed.columns:
            renamed["underlying_symbol"] = ticker.upper()
        else:
            renamed["underlying_symbol"] = renamed["underlying_symbol"].fillna(
                ticker.upper()
            )
        if "days_to_expiration" not in renamed.columns:
            expirations = pd.to_datetime(
                renamed.get("expiration_date"),
                utc=False,
                errors="coerce",
            )
            renamed["days_to_expiration"] = [
                (expiration.date() - observation_date).days
                if not pd.isna(expiration)
                else np.nan
                for expiration in expirations
            ]
        if "option_quote_time" in renamed.columns:
            renamed["option_quote_time"] = renamed["option_quote_time"].map(
                normalize_timestamp
            )
        else:
            renamed["option_quote_time"] = pd.NaT
        renamed = _fill_historical_risk_model(renamed)
        columns = [
            column
            for column in (
                "underlying_symbol",
                "implied_volatility",
                "option_type",
                "days_to_expiration",
                "expiration_date",
                "delta",
                "option_quote_time",
            )
            if column in renamed.columns
        ]
        return renamed[columns].copy()

    def load_historical_option_chain_frame(
        self,
        ticker: str,
        *,
        observation_date: date,
    ) -> pd.DataFrame:
        """Fetch a historical Market Data option-chain snapshot for IV-history seeding."""
        resolved_observation_date = date_arg(
            observation_date,
            name="observation_date",
        )
        frame = self._chain_frame(
            ticker,
            self._mode(),
            chain_date=resolved_observation_date,
        )
        return self._iv_history_frame(
            frame,
            ticker=ticker,
            observation_date=resolved_observation_date,
        )

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
                "underlyingPrice": "underlying_price",
            }
        )
        if "underlyingPrice" in df.columns:
            # The chain's stock price belongs to this option snapshot, not the
            # independently fetched (potentially delayed) ticker quote.
            normalized = normalized.copy()
            normalized["underlying_price_time"] = normalized["option_quote_time"].map(
                normalize_timestamp
            ) if "option_quote_time" in normalized else pd.NaT
        result = normalize_provider_frame(
            df=normalized,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )
        native = "underlyingPrice" in df.columns
        result[SOURCE_QUOTE_COLUMN] = [sanitize_json_payload({
            "kind": "OPTION_CHAIN_SNAPSHOT" if native else "INDEPENDENT_STOCK_SNAPSHOT",
            "contract_symbol": row.get("optionSymbol", row.get("contract_symbol")),
            "underlying_price": row.get("underlyingPrice") if native
                else row.get("underlying_price", underlying_price),
            "underlying_price_time": normalize_timestamp(
                row.get("updated", row.get("option_quote_time"))) if native
                else row.get("underlying_price_time"),
            "option_quote_time": normalize_timestamp(
                row.get("updated", row.get("option_quote_time"))),
            "bid": row.get("bid"), "ask": row.get("ask"),
            "provider_intrinsic_value": row.get("intrinsicValue"),
            "provider_extrinsic_value": row.get("extrinsicValue"),
            "provider_delta": row.get("delta"), "provider_iv": row.get("iv"),
        }) for row in df.to_dict("records")]
        return result

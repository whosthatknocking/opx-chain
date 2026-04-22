"""Market Data provider implementation backed by the official SDK."""

# pylint: disable=missing-kwoa

from __future__ import annotations

from datetime import date, datetime, timezone
from functools import lru_cache
import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from marketdata.client import MarketDataClient
from marketdata.input_types.base import Mode, OutputFormat
from marketdata.sdk_error import MarketDataClientErrorResult

from opx_chain.config import (
    SCRIPT_VERSION,
    US_MARKET_TIMEZONE,
    get_provider_credentials,
    get_runtime_config,
)
from opx_chain.providers.base import (
    DataProvider,
    OptionChainFrames,
    ProviderAuthenticationError,
    normalize_provider_frame,
)
from opx_chain.utils import coerce_float, normalize_timestamp

CALLER_USER_AGENT = f"opx/{SCRIPT_VERSION}"
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0


def _parse_event_date(raw_date) -> date | None:
    """Convert Market Data date values into U.S. market-calendar dates."""
    if raw_date is None:
        return None
    parsed_date = None
    try:
        if pd.isna(raw_date):
            return None
        if isinstance(raw_date, (int, float, np.integer, np.floating)):
            parsed_date = datetime.fromtimestamp(
                float(raw_date),
                tz=timezone.utc,
            ).astimezone(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, str):
            parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
        elif isinstance(raw_date, datetime):
            if raw_date.tzinfo is None:
                parsed_date = raw_date.date()
            else:
                parsed_date = raw_date.astimezone(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, date):
            parsed_date = raw_date
    except (ValueError, TypeError, OSError):
        pass
    return parsed_date


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
    return series.map(_parse_event_date).map(
        lambda value: value.isoformat() if value is not None else np.nan
    )


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
                "in ~/.config/opx-chain/config.toml."
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
        quote_snapshot = self._fetch_stock_quote_snapshot(ticker)
        if quote_snapshot is not None:
            return quote_snapshot

        chain_frame = self._chain_frame(ticker)
        return self._snapshot_from_chain_frame(chain_frame)

    @staticmethod
    def _snapshot_from_chain_frame(chain_frame: pd.DataFrame) -> dict:
        """Build a consistent underlying snapshot from one chain row."""
        if chain_frame.empty or "underlyingPrice" not in chain_frame.columns:
            return {
                "underlying_price": np.nan,
                "underlying_price_time": pd.NaT,
                "underlying_day_change_pct": np.nan,
                "historical_volatility": np.nan,
            }

        candidates = chain_frame.loc[chain_frame["underlyingPrice"].notna()].copy()
        if candidates.empty:
            return {
                "underlying_price": np.nan,
                "underlying_price_time": pd.NaT,
                "underlying_day_change_pct": np.nan,
                "historical_volatility": np.nan,
            }

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

    @lru_cache(maxsize=32)
    def _fetch_stock_quote_snapshot(self, ticker: str) -> dict | None:
        """Load a stock quote snapshot so spot price and change stay internally consistent."""
        self._active_debug_ticker = ticker.upper()
        try:
            response = self._client()._make_request(  # pylint: disable=protected-access
                method="GET",
                url=f"stocks/quotes/{ticker.upper()}/",
            )
            if getattr(response, "status_code", 200) >= 400:
                return None
            quote_data = self._decode_response_json(response)
            if not isinstance(quote_data, dict):
                return None
            best_quote = self._select_best_quote_row(quote_data)
            if best_quote is None:
                return None
            return {
                "underlying_price": best_quote["underlying_price"],
                "underlying_price_time": best_quote["underlying_price_time"],
                "underlying_day_change_pct": best_quote["underlying_day_change_pct"],
                "historical_volatility": np.nan,
            }
        except Exception:  # pylint: disable=broad-exception-caught
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

    def _fetch_next_earnings_date(self, ticker: str, today: date) -> str | None:
        """Return the next upcoming earnings date as an ISO string, or None."""
        try:
            result = self._client().stocks.earnings(
                ticker.upper(),
                output_format=OutputFormat.INTERNAL,
                mode=self._mode(),
            )
            earnings_data = self._raise_if_error(result, context="earnings request")
            report_dates = getattr(earnings_data, "reportDate", None) or []
            upcoming = [
                d
                for raw in report_dates
                if (d := _parse_event_date(raw)) is not None and d >= today
            ]
            return min(upcoming).isoformat() if upcoming else None
        except Exception:  # pylint: disable=broad-exception-caught
            return None

    def _fetch_next_dividend(self, ticker: str, today: date) -> tuple[str | None, float]:
        """Return the next upcoming ex-dividend date and amount, or (None, NaN)."""
        try:
            response = self._client()._make_request(  # pylint: disable=protected-access
                method="GET",
                url=f"stocks/dividends/{ticker.upper()}/",
            )
            div_data = self._decode_response_json(response) or {}
            ex_dates = div_data.get("exDate") or []
            amounts = div_data.get("amount") or []
            upcoming_divs = sorted(
                (
                    (d, amt)
                    for raw, amt in zip(ex_dates, amounts)
                    if (d := _parse_event_date(raw)) is not None and d >= today
                ),
                key=lambda item: item[0],
            )
            if not upcoming_divs:
                return None, np.nan
            next_date, next_amount = upcoming_divs[0]
            try:
                return next_date.isoformat(), float(next_amount)
            except (TypeError, ValueError):
                return next_date.isoformat(), np.nan
        except Exception:  # pylint: disable=broad-exception-caught
            return None, np.nan

    def load_ticker_events(self, ticker: str) -> dict:
        """Fetch upcoming earnings and dividend event data from the Market Data API."""
        today = get_runtime_config().today
        next_earnings_date = self._fetch_next_earnings_date(ticker, today)
        next_ex_div_date, dividend_amount = self._fetch_next_dividend(ticker, today)
        return {
            "next_earnings_date": next_earnings_date,
            "next_earnings_date_is_estimated": True if next_earnings_date else None,
            "next_ex_div_date": next_ex_div_date,
            "dividend_amount": dividend_amount,
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

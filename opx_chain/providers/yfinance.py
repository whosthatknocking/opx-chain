"""Yahoo Finance provider implementation."""

# pylint: disable=duplicate-code

from __future__ import annotations

from datetime import date
import math
import time
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf
from requests import exceptions as requests_exceptions
from yfinance.exceptions import YFRateLimitError

try:
    from curl_cffi.requests import exceptions as curl_exceptions
except ImportError:  # pragma: no cover - yfinance currently depends on curl_cffi
    curl_exceptions = None

from opx_chain.config import get_runtime_config
from opx_chain.providers._yfinance_transport import yfinance_session
from opx_chain.providers.base import (
    DataProvider,
    OptionChainFrames,
    ProviderAuthenticationError,
    ProviderQuotaError,
    RequestThrottle,
    TRANSIENT_BASE_EXCEPTIONS,
    compute_backoff_delay,
    is_provider_quota_error,
    normalize_provider_frame,
    positive_int_arg,
)
from opx_chain.providers._dates import parse_event_date as _parse_event_date
from opx_chain.utils import (
    coerce_float,
    first_non_missing as _first_non_missing,
    normalize_timestamp,
)

_PRICE_HISTORY_CALENDAR_BUFFER_DAYS = 14
_TRADING_TO_CALENDAR_DAY_RATIO = 7 / 5


_transient_yfinance_exceptions: tuple[type[BaseException], ...] = (
    *TRANSIENT_BASE_EXCEPTIONS,
    YFRateLimitError,
    requests_exceptions.RequestException,
)
if curl_exceptions is not None:
    _transient_yfinance_exceptions = (
        *_transient_yfinance_exceptions,
        curl_exceptions.RequestException,
    )


def _is_retryable_yfinance_error(exc: Exception) -> bool:
    """Return True only for transient Yahoo/yfinance failures worth retrying."""
    return isinstance(exc, _transient_yfinance_exceptions) or is_provider_quota_error(exc)


def _flatten_calendar_values(value: Any) -> list[Any]:  # pylint: disable=too-many-return-statements
    """Flatten yfinance calendar payload shapes into a scalar list."""
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        return _flatten_calendar_values(value.to_numpy().ravel().tolist())
    if isinstance(value, pd.Series):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, pd.Index):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, np.ndarray):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, (list, tuple, set)):
        flattened: list[Any] = []
        for item in value:
            flattened.extend(_flatten_calendar_values(item))
        return flattened
    return [value]


def _extract_calendar_field(  # pylint: disable=too-many-return-statements
    calendar_payload: Any,
    field_name: str,
) -> list[Any]:
    """Extract one named field from a yfinance calendar payload."""
    if calendar_payload is None:
        return []
    if isinstance(calendar_payload, dict):
        if field_name in calendar_payload:
            return _flatten_calendar_values(calendar_payload[field_name])
        return []
    if isinstance(calendar_payload, pd.DataFrame):
        if field_name in calendar_payload.columns:
            return _flatten_calendar_values(calendar_payload[field_name])
        if field_name in calendar_payload.index:
            return _flatten_calendar_values(calendar_payload.loc[field_name])
        return []
    if isinstance(calendar_payload, pd.Series):
        if field_name in calendar_payload.index:
            return _flatten_calendar_values(calendar_payload.loc[field_name])
        return []
    return []


def _pick_next_future_date(raw_values: list[Any], today: date) -> date | None:
    """Return the earliest date on or after today from a list of raw Yahoo values."""
    upcoming = sorted(
        parsed
        for raw_value in raw_values
        if (parsed := _parse_event_date(raw_value)) is not None and parsed >= today
    )
    return upcoming[0] if upcoming else None


def _parse_yahoo_date_only_event(raw_date) -> date | None:
    """Parse Yahoo metadata fields that encode date-only values as UTC epochs."""
    if raw_date is None:
        return None
    if isinstance(raw_date, (int, float, np.integer, np.floating)):
        timestamp = normalize_timestamp(raw_date)
        if pd.isna(timestamp):
            return None
        return timestamp.date()
    return _parse_event_date(raw_date)


def _calendar_days_for_trading_lookback(lookback_days: int) -> int:
    """Return a Yahoo calendar-day span large enough for daily-bar coverage."""
    resolved_lookback = positive_int_arg(lookback_days, name="lookback_days")
    return max(
        resolved_lookback,
        math.ceil(resolved_lookback * _TRADING_TO_CALENDAR_DAY_RATIO)
        + _PRICE_HISTORY_CALENDAR_BUFFER_DAYS,
    )


def compute_historical_volatility(stock, load_history=None):  # pylint: disable=broad-exception-caught
    """Compute trailing annualized realized volatility from daily closes."""
    config = get_runtime_config()
    lookback_period = f"{max(config.hv_lookback_days * 3, 90)}d"
    history_loader = load_history or stock.history
    try:
        history = history_loader(period=lookback_period, interval="1d", auto_adjust=False)
    except (ProviderAuthenticationError, ProviderQuotaError):
        raise
    except Exception:  # pylint: disable=broad-exception-caught
        return np.nan
    if history.empty:
        return np.nan

    close_column = "Adj Close" if "Adj Close" in history.columns else "Close"
    closes = pd.to_numeric(history[close_column], errors="coerce").dropna()
    log_returns = np.log(closes / closes.shift(1)).dropna()
    if len(log_returns) < config.hv_lookback_days:
        return np.nan

    recent_returns = log_returns.tail(config.hv_lookback_days)
    return recent_returns.std(ddof=1) * np.sqrt(config.trading_days_per_year)


class YFinanceProvider(DataProvider):
    """Market-data provider backed by yfinance/Yahoo Finance."""

    name = "yfinance"

    def __init__(self) -> None:
        self._request_throttle = RequestThrottle()

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose yfinance's logger so runlog can capture vendor errors."""
        return ("yfinance",)

    def _request_interval_seconds(self) -> float:
        """Return the configured minimum spacing between Yahoo calls."""
        return get_runtime_config().yfinance_request_interval_seconds

    def _max_retries(self) -> int:
        """Return the configured Yahoo retry count."""
        return get_runtime_config().yfinance_max_retries

    def _backoff_seconds(self) -> float:
        """Return the configured Yahoo retry backoff base."""
        return get_runtime_config().yfinance_backoff_seconds

    def _sleep_for_request_interval(self) -> None:
        """Respect the configured minimum spacing between Yahoo calls."""
        self._request_throttle.wait(self._request_interval_seconds())

    def _call_yahoo(self, label: str, callback):
        """Apply pacing and retry configuration around one yfinance call."""
        max_retries = self._max_retries()
        for attempt in range(max_retries + 1):
            self._sleep_for_request_interval()
            try:
                return callback()
            except (ProviderAuthenticationError, ProviderQuotaError):
                raise
            except Exception as exc:  # pylint: disable=broad-exception-caught
                if not _is_retryable_yfinance_error(exc):
                    raise
                if attempt == max_retries:
                    if is_provider_quota_error(exc):
                        raise ProviderQuotaError(
                            f"Yahoo Finance {label} failed due to quota/rate limit: {exc}"
                        ) from exc
                    raise
                delay = compute_backoff_delay(attempt, self._backoff_seconds())
                print(
                    f"yfinance api: {label} retry_in={delay:.2f}s "
                    f"attempt={attempt + 1}/{max_retries} error={exc}"
                )
                time.sleep(delay)
        return callback()  # pragma: no cover - loop always returns or raises

    def _safe_info(self, ticker: str, stock) -> dict[str, Any]:
        """Return the yfinance info payload or an empty dict on failure."""
        return self._safe_yfinance_attr(
            f"{ticker} info",
            lambda: stock.info,
            default={},
            expected_type=dict,
        )

    def _safe_yfinance_attr(
        self,
        label: str,
        callback,
        *,
        default,
        expected_type=None,
    ):
        """Return a best-effort yfinance attribute value with optional type validation."""
        try:
            result = self._call_yahoo(label, callback)
        except (ProviderAuthenticationError, ProviderQuotaError):
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            return default
        if expected_type is None or isinstance(result, expected_type):
            return result
        return default

    def _safe_calendar(self, ticker: str, stock):
        """Return the yfinance calendar payload or None on failure."""
        return self._safe_yfinance_attr(
            f"{ticker} calendar",
            lambda: stock.calendar,
            default=None,
            expected_type=(dict, pd.DataFrame, pd.Series),
        )

    def _safe_dividends(self, ticker: str, stock) -> pd.Series:
        """Return the yfinance dividends series or an empty series on failure."""
        return self._safe_yfinance_attr(
            f"{ticker} dividends",
            lambda: stock.dividends,
            default=pd.Series(dtype="float64"),
            expected_type=pd.Series,
        )

    @staticmethod
    def _next_earnings_event(info: dict[str, Any], calendar_payload, today: date):
        """Return the next Yahoo earnings date and estimate flag when available."""
        info_dates = [
            _parse_event_date(info.get("earningsTimestampStart")),
            _parse_event_date(info.get("earningsTimestamp")),
            _parse_event_date(info.get("earningsTimestampEnd")),
        ]
        info_upcoming = sorted(d for d in info_dates if d is not None and d >= today)
        calendar_upcoming = _pick_next_future_date(
            _extract_calendar_field(calendar_payload, "Earnings Date"),
            today,
        )
        candidates = info_upcoming.copy()
        if calendar_upcoming is not None:
            candidates.append(calendar_upcoming)
        if not candidates:
            return None, None
        next_date = min(candidates)
        estimate_flag = None
        estimate_value = info.get("isEarningsDateEstimate")
        if next_date in info_upcoming and isinstance(estimate_value, bool):
            estimate_flag = estimate_value
        return next_date.isoformat(), estimate_flag

    @staticmethod
    def _next_dividend_event(
        info: dict[str, Any],
        calendar_payload,
        dividends: pd.Series,
        today: date,
    ):
        """Return the next Yahoo ex-dividend date and associated amount when available."""
        future_dividends: dict[date, float] = {}
        if not dividends.empty:
            for raw_date, raw_amount in dividends.items():
                ex_div_date = _parse_event_date(raw_date)
                if ex_div_date is None or ex_div_date < today:
                    continue
                amount = coerce_float(raw_amount)
                future_dividends[ex_div_date] = np.nan if pd.isna(amount) else float(amount)

        candidates: list[date] = list(future_dividends)
        info_ex_div_date = _parse_yahoo_date_only_event(info.get("exDividendDate"))
        if info_ex_div_date is not None and info_ex_div_date >= today:
            candidates.append(info_ex_div_date)
        if not candidates:
            calendar_ex_div_date = _pick_next_future_date(
                _extract_calendar_field(calendar_payload, "Ex-Dividend Date"),
                today,
            )
            if calendar_ex_div_date is not None:
                candidates.append(calendar_ex_div_date)
        if not candidates:
            return None, np.nan
        next_date = min(candidates)
        return next_date.isoformat(), future_dividends.get(next_date, np.nan)

    def load_underlying_snapshot(self, ticker: str) -> dict:  # pylint: disable=broad-exception-caught
        """Load the underlying snapshot once per ticker and reuse it for each expiration."""
        stock = yf.Ticker(ticker, session=yfinance_session())
        fast_info = self._call_yahoo(
            f"{ticker} fast_info",
            lambda: getattr(stock, "fast_info", {}) or {},
        )
        info = self._safe_info(ticker, stock)
        self.debug_dump_payload(
            ticker,
            "underlying_snapshot",
            {"fast_info": fast_info, "info": info},
        )

        last_price = coerce_float(
            _first_non_missing(
                fast_info.get("lastPrice"),
                info.get("regularMarketPrice"),
                info.get("previousClose"),
            )
        )
        previous_close = coerce_float(
            _first_non_missing(
                fast_info.get("previousClose"),
                info.get("previousClose"),
            )
        )

        if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
            underlying_day_change_pct = (last_price - previous_close) / previous_close
        else:
            underlying_day_change_pct = np.nan

        return {
            "underlying_price": last_price,
            "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": compute_historical_volatility(
                stock,
                load_history=lambda **kwargs: self._call_yahoo(
                    f"{ticker} history",
                    lambda: stock.history(**kwargs),
                ),
            ),
        }

    def load_ticker_events(self, ticker: str) -> dict:
        """Load best-effort earnings and dividend events from Yahoo metadata."""
        stock = yf.Ticker(ticker, session=yfinance_session())
        info = self._safe_info(ticker, stock)
        calendar_payload = self._safe_calendar(ticker, stock)
        dividends = self._safe_dividends(ticker, stock)
        self.debug_dump_payload(
            ticker,
            "ticker_events",
            {
                "info": info,
                "calendar": calendar_payload,
                "dividends": dividends,
            },
        )
        today = get_runtime_config().today
        next_earnings_date, is_estimated = self._next_earnings_event(info, calendar_payload, today)
        next_ex_div_date, dividend_amount = self._next_dividend_event(
            info,
            calendar_payload,
            dividends,
            today,
        )
        earnings_confidence = None
        if next_earnings_date:
            earnings_confidence = (
                "estimated"
                if is_estimated is True
                else "confirmed"
                if is_estimated is False
                else "unknown"
            )
        return {
            "next_earnings_date": next_earnings_date,
            "next_earnings_date_is_estimated": is_estimated,
            "next_earnings_date_source": "yfinance" if next_earnings_date else None,
            "next_earnings_date_confidence": earnings_confidence,
            "next_ex_div_date": next_ex_div_date,
            "next_ex_div_date_source": "yfinance" if next_ex_div_date else None,
            "next_ex_div_date_confidence": "confirmed" if next_ex_div_date else None,
            "dividend_amount": dividend_amount,
        }

    def load_analyst_forecast(self, ticker: str) -> dict:
        """Load structured analyst price targets and recommendation summary."""
        stock = yf.Ticker(ticker, session=yfinance_session())
        price_targets = self._safe_yfinance_attr(
            f"{ticker} analyst price targets",
            stock.get_analyst_price_targets,
            default={},
            expected_type=(dict, pd.Series),
        )
        if isinstance(price_targets, pd.Series):
            price_targets = price_targets.to_dict()
        if not price_targets:
            price_targets = self._safe_yfinance_attr(
                f"{ticker} analyst_price_targets",
                lambda: stock.analyst_price_targets,
                default={},
                expected_type=(dict, pd.Series),
            )
            if isinstance(price_targets, pd.Series):
                price_targets = price_targets.to_dict()
        recommendations_summary = self._safe_yfinance_attr(
            f"{ticker} recommendations summary",
            stock.get_recommendations_summary,
            default=None,
            expected_type=(pd.DataFrame, pd.Series, dict),
        )
        if recommendations_summary is None:
            recommendations_summary = self._safe_yfinance_attr(
                f"{ticker} recommendations_summary",
                lambda: stock.recommendations_summary,
                default=None,
                expected_type=(pd.DataFrame, pd.Series, dict),
            )
        self.debug_dump_payload(
            ticker,
            "analyst_forecast",
            {
                "price_targets": price_targets,
                "recommendations_summary": recommendations_summary,
            },
        )
        return {
            "price_targets": price_targets if isinstance(price_targets, dict) else {},
            "recommendations_summary": recommendations_summary,
        }

    def load_price_history(self, ticker: str, *, lookback_days: int) -> pd.DataFrame:
        """Load daily OHLCV history for optional price-context enrichment."""
        calendar_days = _calendar_days_for_trading_lookback(lookback_days)
        stock = yf.Ticker(ticker, session=yfinance_session())
        history = self._call_yahoo(
            f"{ticker} price history",
            lambda: stock.history(
                period=f"{calendar_days}d",
                interval="1d",
                auto_adjust=True,
            ),
        )
        self.debug_dump_payload(ticker, "price_history", history)
        return history if isinstance(history, pd.DataFrame) else pd.DataFrame()

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return option expiration strings available from yfinance."""
        stock = yf.Ticker(ticker, session=yfinance_session())
        expirations = list(self._call_yahoo(f"{ticker} expirations", lambda: stock.options))
        self.debug_dump_payload(ticker, "expirations", expirations)
        return expirations

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load one yfinance option chain and return its raw call/put frames."""
        stock = yf.Ticker(ticker, session=yfinance_session())
        chain = self._call_yahoo(
            f"{ticker} option_chain {expiration_date}",
            lambda: stock.option_chain(expiration_date),
        )
        self.debug_dump_payload(
            ticker,
            f"option_chain_{expiration_date}",
            {"calls": chain.calls, "puts": chain.puts},
        )
        return OptionChainFrames(calls=chain.calls, puts=chain.puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a yfinance frame into the canonical options schema."""
        return normalize_provider_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )

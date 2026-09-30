"""Fetch orchestration using the configured market-data provider."""

from datetime import datetime
from numbers import Integral
from numbers import Real
import pickle

import numpy as np
import pandas as pd

from opx_chain._integrity_validation import (
    validate_option_chain_frame,
    validate_option_chain_provider_response,
)
from opx_chain.config import get_runtime_config
from opx_chain.error_summary import compact_exception_summary
from opx_chain.json_utils import dumps_strict_json, loads_strict_json, to_python_scalar
from opx_chain.integrity import (
    OptionChainDataIntegrityError,
    OptionChainIntegrityBoundary,
)
from opx_chain.metrics import (
    add_expected_move_by_expiration,
    add_iv_state_level,
    add_iv_state_term,
    add_listed_strike_increment,
    add_theta_efficiency_below_p25,
)
from opx_chain.normalize import apply_post_download_filters, enrich_option_frame
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT
from opx_chain.positions import EMPTY_POSITION_SET, PositionSet
from opx_chain.price_context import (
    PriceContextStatus,
    blank_price_context,
    compute_price_context,
)
from opx_chain.price_history import reconcile_price_history
from opx_chain.quote_quarantine import quarantine_unusable_quotes
from opx_chain.providers.base import (
    OptionChainFrames,
    ProviderAuthenticationError,
    ProviderQuotaError,
)
from opx_chain.providers import get_data_provider
from opx_chain.runlog import get_logger
from opx_chain.runtime_args import strict_bool_arg
from opx_chain.storage.cache import get_provider_cache
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import format_utc_z_seconds, utc_now_timestamp
from opx_chain.utils import is_finite_positive_number
from opx_chain.validate import validate_option_rows

_JSON_TIMESTAMP_KEY = "__opx_pd_timestamp__"
_JSON_NAT_KEY = "__opx_pd_nat__"
_LOGGER = get_logger("fetch")


def _non_negative_int_arg(value, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a non-negative integer")
    resolved = int(value)
    if resolved < 0:
        raise ValueError(f"{name} must be non-negative")
    return resolved


def _with_fetch_counts(
    df: pd.DataFrame,
    *,
    raw_row_count: int,
    normalized_row_count: int,
    filtered_row_count: int,
    raw_expiration_count: int,
) -> pd.DataFrame:
    """Attach non-column fetch diagnostics used by storage metadata writers."""
    df.attrs["raw_row_count"] = raw_row_count
    df.attrs["normalized_row_count"] = normalized_row_count
    df.attrs["filtered_row_count"] = filtered_row_count
    df.attrs["raw_expiration_count"] = raw_expiration_count
    return df


def _with_fetch_status(
    df: pd.DataFrame,
    status: str,
    error_summary: str | None = None,
) -> pd.DataFrame:
    """Attach ticker-level fetch outcome diagnostics for storage metadata."""
    df.attrs["fetch_status"] = status
    if error_summary is not None:
        df.attrs["fetch_error_summary"] = error_summary
    return df


def _exception_summary(exc: Exception) -> str:
    """Return a compact exception summary for ticker-level storage metadata."""
    return compact_exception_summary(exc, include_type=True)


def _cache_get_json(cache, key: str) -> dict | None:
    """Return a cached dict if the key is present and unexpired, else None."""
    data = cache.get(key)
    if data is None:
        return None
    try:
        restored = _restore_cached_json_value(loads_strict_json(data.decode()))
        if not isinstance(restored, dict):
            raise ValueError("cached JSON payload must be an object")
        return restored
    except (TypeError, UnicodeDecodeError, ValueError):
        try:
            cache.invalidate(key)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None


def _cache_put_json(cache, key: str, value: dict, ttl: int, logger=None) -> None:
    """Serialise value to JSON and store in cache."""
    try:
        cache.put(
            key,
            dumps_strict_json(_prepare_cached_json_value(value)).encode(),
            ttl,
        )
    except (TypeError, ValueError) as exc:
        message = f"cache put skipped for key={key}: {exc}"
        if logger:
            logger.warning(message)
        else:
            _LOGGER.warning(message)


def _prepare_cached_json_value(value):
    """Convert pandas/numpy scalar values into JSON-safe cache values."""
    prepared = value
    is_nat = value is pd.NaT
    if isinstance(value, pd.Timestamp):
        is_nat = pd.isna(value)
        prepared = {_JSON_TIMESTAMP_KEY: value.isoformat()} if not is_nat else prepared
    elif isinstance(value, dict):
        prepared = {key: _prepare_cached_json_value(item) for key, item in value.items()}
    elif isinstance(value, (list, tuple)):
        prepared = [_prepare_cached_json_value(item) for item in value]
    elif isinstance(value, np.generic):
        prepared = to_python_scalar(value)
    if isinstance(prepared, Real) and not isinstance(prepared, bool):
        return prepared if np.isfinite(float(prepared)) else None
    return {_JSON_NAT_KEY: True} if is_nat else prepared


def _restore_cached_json_value(value):
    """Restore pandas scalar values from their JSON-safe cache representation."""
    if isinstance(value, dict):
        if set(value) == {_JSON_NAT_KEY}:
            if value[_JSON_NAT_KEY] is not True:
                raise ValueError("invalid cached pandas NaT marker")
            return pd.NaT
        if set(value) == {_JSON_TIMESTAMP_KEY}:
            timestamp_value = value[_JSON_TIMESTAMP_KEY]
            if not isinstance(timestamp_value, str):
                raise ValueError("invalid cached pandas timestamp marker")
            timestamp = pd.Timestamp(timestamp_value)
            if pd.isna(timestamp):
                raise ValueError("invalid cached pandas timestamp marker")
            return timestamp
        return {key: _restore_cached_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_restore_cached_json_value(item) for item in value]
    return value


def _cache_get_chain(cache, key: str) -> OptionChainFrames | None:
    """Return a cached OptionChainFrames if present and unexpired, else None."""
    data = cache.get(key)
    if data is None:
        return None
    try:
        value = pickle.loads(data)  # nosec pickle — local filesystem cache only
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            cache.invalidate(key)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None
    if not isinstance(value, OptionChainFrames):
        try:
            cache.invalidate(key)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None
    return value


def _cache_put_chain(cache, key: str, value: OptionChainFrames, ttl: int, logger=None) -> None:
    """Pickle an OptionChainFrames and store in cache."""
    try:
        cache.put(key, pickle.dumps(value), ttl)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        message = f"cache put skipped for key={key}: {exc}"
        if logger:
            logger.warning(message)
        else:
            _LOGGER.warning(message)


def _provider_cache_scope(provider_name: str, config) -> str:
    """Return the provider cache namespace for response-shaping provider config."""
    if provider_name == "marketdata":
        return f"{provider_name}:mode={config.marketdata_mode or 'default'}"
    return provider_name


def _emit_fetch_info(message, logger=None):
    """Print a fetch-progress message and mirror it to the run log when available."""
    print(message)
    if logger:
        logger.info(message)


def _frame_value_count(frame, column):
    """Count non-null values for one column without assuming the column exists."""
    columns = (column,) if isinstance(column, str) else tuple(column)
    available_columns = [name for name in columns if name in frame.columns]
    if not available_columns:
        return 0
    if len(available_columns) == 1:
        return int(frame[available_columns[0]].notna().sum())
    return int(frame[available_columns].notna().any(axis=1).sum())


def append_underlying_snapshot_fields(df, snapshot, fetched_at, stale_quote_seconds):
    """Add underlying snapshot metadata to each option row."""
    df["underlying_price_time"] = snapshot["underlying_price_time"]
    df["underlying_day_change_pct"] = snapshot["underlying_day_change_pct"]
    df["historical_volatility"] = snapshot["historical_volatility"]
    df["underlying_price_age_seconds"] = (
        (fetched_at - snapshot["underlying_price_time"]).total_seconds()
        if pd.notna(snapshot["underlying_price_time"])
        else np.nan
    )
    df["is_stale_underlying_price"] = np.where(
        pd.notna(df["underlying_price_age_seconds"]),
        (df["underlying_price_age_seconds"] < 0)
        | (df["underlying_price_age_seconds"] > stale_quote_seconds),
        None,
    )
    return df


def append_ticker_event_fields(df, events, today):
    """Broadcast per-ticker corporate event data to all option rows."""
    df["next_earnings_date"] = events.get("next_earnings_date")
    df["next_earnings_date_is_estimated"] = events.get("next_earnings_date_is_estimated")
    df["next_earnings_date_source"] = events.get("next_earnings_date_source")
    df["next_earnings_date_confidence"] = events.get("next_earnings_date_confidence")
    df["next_ex_div_date"] = events.get("next_ex_div_date")
    df["next_ex_div_date_source"] = events.get("next_ex_div_date_source")
    df["next_ex_div_date_confidence"] = events.get("next_ex_div_date_confidence")
    df["dividend_amount"] = events.get("dividend_amount", np.nan)

    earnings_date_str = events.get("next_earnings_date")
    if earnings_date_str:
        try:
            earnings_date = datetime.strptime(earnings_date_str, "%Y-%m-%d").date()
            df["days_to_earnings"] = (earnings_date - today).days
        except (ValueError, TypeError):
            df["days_to_earnings"] = np.nan
    else:
        df["days_to_earnings"] = np.nan

    ex_div_date_str = events.get("next_ex_div_date")
    if ex_div_date_str:
        try:
            ex_div_date = datetime.strptime(ex_div_date_str, "%Y-%m-%d").date()
            df["days_to_ex_div"] = (ex_div_date - today).days
        except (ValueError, TypeError):
            df["days_to_ex_div"] = np.nan
    else:
        df["days_to_ex_div"] = np.nan

    return df


def fetch_ticker_price_context(  # pylint: disable=too-many-arguments
    ticker,
    *,
    provider=None,
    logger=None,
    cache=None,
    config=None,
    store=None,
):
    """Reconcile stored daily OHLCV history and compute optional price context."""
    del cache  # Price context is derived from the durable price-history store.
    config = config or get_runtime_config()
    provider = provider or get_data_provider()
    max_age_days = _non_negative_int_arg(
        config.price_context_max_age_days,
        name="price_context_max_age_days",
    )

    try:
        result = reconcile_price_history(
            ticker=ticker,
            provider=provider,
            config=config,
            logger=logger,
            store=store,
        )
        if result.error_summary is not None and result.history.empty:
            return blank_price_context(source=provider.name, status=PriceContextStatus.ERROR)
        context = compute_price_context(
            result.history,
            source=provider.name,
            today=config.today,
            max_age_days=max_age_days,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        context = blank_price_context(source=provider.name, status=PriceContextStatus.ERROR)
        message = f"{ticker}: price_context skipped  error={_exception_summary(exc)}"
        if logger:
            logger.warning(message)
        else:
            _LOGGER.warning(message)
        print(message)
    return context


def _normalize_requested_ticker(value) -> str:
    if not isinstance(value, str):
        raise ValueError("ticker must be a non-empty string")
    ticker = value.strip().upper()
    if not ticker:
        raise ValueError("ticker must be a non-empty string")
    if not is_valid_ticker(ticker):
        raise ValueError("ticker must be a valid stock ticker symbol")
    return ticker


def fetch_ticker_option_chain(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements,broad-exception-caught
    ticker,
    logger=None,
    validation_findings=None,
    filtered_row_counts=None,
    position_set: PositionSet | None = None,
    skip_events: bool = False,
):
    """Fetch and normalize all near-term option chains for one ticker."""
    ticker = _normalize_requested_ticker(ticker)
    skip_events = strict_bool_arg(skip_events, name="skip_events")
    provider = None
    try:
        config = get_runtime_config()
        cache = get_provider_cache(config)
        fetch_started_at = utc_now_timestamp()
        provider = get_data_provider()
        prepare_ticker_fetch = getattr(provider, "prepare_ticker_fetch", None)
        if callable(prepare_ticker_fetch):
            prepare_ticker_fetch(ticker)
        _emit_fetch_info(f"Loading {ticker}  ({provider.name})", logger=logger)
        cache_scope = _provider_cache_scope(provider.name, config)
        snap_key = f"snapshot:{cache_scope}:{ticker}"
        snapshot = _cache_get_json(cache, snap_key)
        if snapshot is None:
            snapshot = provider.load_underlying_snapshot(ticker)
            _cache_put_json(cache, snap_key, snapshot, config.provider_snapshot_ttl, logger=logger)
        underlying_price = snapshot["underlying_price"]
        snap_time = snapshot["underlying_price_time"]
        _emit_fetch_info(
            f"{ticker}: snapshot  price={underlying_price}  time={snap_time}",
            logger=logger,
        )

        if not is_finite_positive_number(underlying_price):
            _emit_fetch_info(
                f"{ticker}: skipped because underlying price is missing or invalid",
                logger=logger,
            )
            if logger:
                logger.warning(
                    "ticker=%s status=skipped reason=invalid_underlying_price",
                    ticker,
                )
            return _with_fetch_status(
                _with_fetch_counts(
                    pd.DataFrame(),
                    raw_row_count=0,
                    normalized_row_count=0,
                    filtered_row_count=0,
                    raw_expiration_count=0,
                ),
                "skipped",
            )

        all_normalized_rows = []
        raw_contract_count = 0
        raw_expiration_count = 0
        available_expirations = provider.list_option_expirations(ticker)
        usable_expirations = []
        skipped_for_max_expiration = 0
        skipped_for_past_expiration = 0
        positions = position_set or EMPTY_POSITION_SET
        for expiration_date in available_expirations:
            if config.max_expiration is not None and expiration_date > config.max_expiration:
                skipped_for_max_expiration += 1
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            days_until = (exp_date - config.today).days
            # Keep today's expiration for any portfolio exposure to the ticker; drop past.
            min_days = 0 if ticker in positions.tickers else 1
            if days_until < min_days:
                skipped_for_past_expiration += 1
                continue
            usable_expirations.append(expiration_date)

        skipped_total = skipped_for_max_expiration + skipped_for_past_expiration
        exp_msg = (
            f"{ticker}: expirations  usable={len(usable_expirations)}"
            f"/{len(available_expirations)}"
        )
        if skipped_total:
            exp_msg += f"  skipped={skipped_total}"
        _emit_fetch_info(exp_msg, logger=logger)

        if skip_events:
            events = {}
            _emit_fetch_info(
                f"{ticker}: events skipped by caller",
                logger=logger,
            )
        else:
            events_key = f"events:{cache_scope}:{ticker}"
            events = _cache_get_json(cache, events_key)
            if events is None:
                events = provider.load_ticker_events(ticker)
                _cache_put_json(
                    cache,
                    events_key,
                    events,
                    config.provider_events_ttl,
                    logger=logger,
                )
            earnings = events.get("next_earnings_date") or "none"
            ex_div = events.get("next_ex_div_date") or "none"
            _emit_fetch_info(
                f"{ticker}: events  earnings={earnings}  ex_div={ex_div}",
                logger=logger,
            )
        for expiration_date in usable_expirations:
            chain_key = f"chain:{cache_scope}:{ticker}:{expiration_date}"
            chain = _cache_get_chain(cache, chain_key)
            if chain is None:
                chain = provider.load_option_chain(ticker, expiration_date)
                _cache_put_chain(cache, chain_key, chain, config.provider_chain_ttl, logger=logger)
            validate_option_chain_provider_response(
                chain.calls,
                chain.puts,
                ticker=ticker,
                provider=provider.name,
            )
            expiration_raw_count = len(chain.calls) + len(chain.puts)
            raw_contract_count += expiration_raw_count
            raw_expiration_count += 1
            call_bid_count = _frame_value_count(chain.calls, "bid")
            put_bid_count = _frame_value_count(chain.puts, "bid")
            call_ask_count = _frame_value_count(chain.calls, "ask")
            put_ask_count = _frame_value_count(chain.puts, "ask")
            last_trade_columns = ("last_trade_price", "lastPrice", "last")
            call_trade_count = _frame_value_count(chain.calls, last_trade_columns)
            put_trade_count = _frame_value_count(chain.puts, last_trade_columns)
            _emit_fetch_info(
                f"{ticker}: chain  {expiration_date}  rows={expiration_raw_count}",
                logger=logger,
            )
            if logger:
                logger.info(
                    (
                        "ticker=%s provider=%s expiration=%s status=raw_provider_rows "
                        "call_rows=%s put_rows=%s total_rows=%s "
                        "call_bid_rows=%s put_bid_rows=%s call_ask_rows=%s put_ask_rows=%s "
                        "call_trade_rows=%s put_trade_rows=%s"
                    ),
                    ticker,
                    provider.name,
                    expiration_date,
                    len(chain.calls),
                    len(chain.puts),
                    expiration_raw_count,
                    call_bid_count,
                    put_bid_count,
                    call_ask_count,
                    put_ask_count,
                    call_trade_count,
                    put_trade_count,
                )
            for option_type, option_frame in [
                (OPTION_TYPE_CALL, chain.calls),
                (OPTION_TYPE_PUT, chain.puts),
            ]:
                if option_frame.empty:
                    continue
                vendor_normalized = provider.normalize_option_frame(
                    df=option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
                )
                all_normalized_rows.append(vendor_normalized)

        if not all_normalized_rows:
            _emit_fetch_info(
                f"{ticker}: provider returned no usable option frames",
                logger=logger,
            )
            if logger:
                logger.warning(
                    (
                        "ticker=%s provider=%s status=skipped rows=0 expirations=0 "
                        "raw_provider_rows=%s raw_expirations=%s"
                    ),
                    ticker,
                    provider.name,
                    raw_contract_count,
                    raw_expiration_count,
                )
            return _with_fetch_status(
                _with_fetch_counts(
                    pd.DataFrame(),
                    raw_row_count=raw_contract_count,
                    normalized_row_count=0,
                    filtered_row_count=0,
                    raw_expiration_count=raw_expiration_count,
                ),
                "skipped",
            )

        # Validate every complete normalized row before enrichment or filtering.
        all_normalized = pd.concat(all_normalized_rows, ignore_index=True)
        pre_filter_count = len(all_normalized)
        _emit_fetch_info(
            f"{ticker}: normalize  rows={pre_filter_count}",
            logger=logger,
        )
        all_normalized, quote_report = quarantine_unusable_quotes(
            all_normalized, provider=provider, ticker=ticker,
            underlying_price=underlying_price,
        )
        if quote_report:
            _emit_fetch_info(f"{ticker}: quote_quality_warning {quote_report}", logger=logger)
        # Row-scope counts describe validated canonical rows entering ordinary
        # screening; excluded quote counts live in the durable quality report.
        pre_filter_count = len(all_normalized)
        validate_option_chain_frame(
            all_normalized,
            boundary=OptionChainIntegrityBoundary.PRE_FILTER,
            requested_tickers=(ticker,),
            provider=provider.name,
        )

        # Assess all received/cached quotes after acquisition and quote refresh.
        # Fetch-start time can legitimately precede a quote received in this call.
        fetched_at = utc_now_timestamp()
        if logger:
            logger.info(
                "ticker=%s fetch_started_at=%s freshness_assessed_at=%s",
                ticker, format_utc_z_seconds(fetch_started_at),
                format_utc_z_seconds(fetched_at),
            )
        # Pre-filter cross-row enrichment on the full unfiltered chain.
        all_normalized = append_ticker_event_fields(
            all_normalized,
            events,
            config.today,
        )
        all_normalized = enrich_option_frame(
            df=all_normalized,
            underlying_price=underlying_price,
            fetched_at=fetched_at,
        )
        all_normalized = append_underlying_snapshot_fields(
            all_normalized,
            snapshot,
            fetched_at,
            config.stale_quote_seconds,
        )
        if config.enable_validation and validation_findings is not None:
            validation_findings.extend(validate_option_rows(all_normalized))
        all_normalized = add_iv_state_level(all_normalized)
        all_normalized = add_iv_state_term(all_normalized)
        all_normalized = add_listed_strike_increment(all_normalized)

        combined = apply_post_download_filters(
            all_normalized, underlying_price,
            position_keys=(position_set or EMPTY_POSITION_SET).option_keys,
        )
        dropped_rows = pre_filter_count - len(combined)
        if filtered_row_counts is not None:
            filtered_row_counts.append(dropped_rows)

        if combined.empty and raw_contract_count > 0:
            _emit_fetch_info(
                (
                    f"{ticker}: all provider rows were filtered out by the shared "
                    "normalization and screening pipeline"
                ),
                logger=logger,
            )
            combined = _with_fetch_counts(
                combined,
                raw_row_count=raw_contract_count,
                normalized_row_count=pre_filter_count,
                filtered_row_count=dropped_rows,
                raw_expiration_count=raw_expiration_count,
            )
            if logger:
                logger.info(
                    (
                        "ticker=%s provider=%s status=skipped fetched_at=%s "
                        "rows=0 expirations=0 raw_provider_rows=%s raw_expirations=%s "
                        "reason=all_rows_filtered"
                    ),
                    ticker,
                    provider.name,
                    format_utc_z_seconds(fetched_at),
                    raw_contract_count,
                    raw_expiration_count,
            )
            return _with_fetch_status(
                combined, "skipped", quote_report,
            )

        _emit_fetch_info(
            f"{ticker}: filter  rows={len(combined)}  dropped={dropped_rows}",
            logger=logger,
        )
        exp_count = combined["expiration_date"].nunique() if not combined.empty else 0
        _emit_fetch_info(
            f"{ticker}: done  rows={len(combined)}"
            f"  expirations={exp_count}  raw={raw_contract_count}",
            logger=logger,
        )

        # Post-filter enrichment on surviving rows.
        combined = add_theta_efficiency_below_p25(combined)
        combined = add_expected_move_by_expiration(combined)
        combined = _with_fetch_counts(
            combined,
            raw_row_count=raw_contract_count,
            normalized_row_count=pre_filter_count,
            filtered_row_count=dropped_rows,
            raw_expiration_count=raw_expiration_count,
        )
        if logger:
            logger.info(
                (
                    "ticker=%s provider=%s status=ok fetched_at=%s rows=%s expirations=%s "
                    "raw_provider_rows=%s raw_expirations=%s"
                ),
                ticker,
                provider.name,
                format_utc_z_seconds(fetched_at),
                len(combined),
                combined["expiration_date"].nunique(),
                raw_contract_count,
                raw_expiration_count,
            )
        if quote_report:
            return _with_fetch_status(combined, "ok_with_warnings", quote_report)
        return combined

    except (
        OptionChainDataIntegrityError,
        ProviderAuthenticationError,
        ProviderQuotaError,
    ) as exc:
        print(f"{ticker} error: {exc}")
        if logger:
            logger.exception(
                "ticker=%s provider=%s status=error message=%s",
                ticker,
                getattr(provider, "name", "unknown"),
                exc,
            )
        raise

    except Exception as exc:
        print(f"{ticker} error: {exc}")
        if logger:
            logger.exception(
                "ticker=%s provider=%s status=error message=%s",
                ticker,
                getattr(provider, "name", "unknown"),
                exc,
            )
        return _with_fetch_status(
            _with_fetch_counts(
                pd.DataFrame(),
                raw_row_count=0,
                normalized_row_count=0,
                filtered_row_count=0,
                raw_expiration_count=0,
            ),
            "error",
            _exception_summary(exc),
        )

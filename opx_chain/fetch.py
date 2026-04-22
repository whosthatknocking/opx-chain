"""Fetch orchestration using the configured market-data provider."""

from datetime import datetime, timezone
import json
import pickle

import numpy as np
import pandas as pd

from opx_chain.config import get_runtime_config
from opx_chain.metrics import (
    add_expected_move_by_expiration,
    add_iv_state_level,
    add_iv_state_term,
    add_listed_strike_increment,
    add_theta_efficiency_below_p25,
)
from opx_chain.normalize import apply_post_download_filters, enrich_option_frame
from opx_chain.positions import EMPTY_POSITION_SET, PositionSet
from opx_chain.providers.base import (
    OptionChainFrames,
    ProviderAuthenticationError,
    ProviderQuotaError,
)
from opx_chain.providers import get_data_provider
from opx_chain.storage.cache import get_provider_cache
from opx_chain.validate import validate_option_rows


def _cache_get_json(cache, key: str) -> dict | None:
    """Return a cached dict if the key is present and unexpired, else None."""
    data = cache.get(key)
    if data is None:
        return None
    try:
        return json.loads(data)
    except (json.JSONDecodeError, ValueError):
        return None


def _cache_put_json(cache, key: str, value: dict, ttl: int) -> None:
    """Serialise value to JSON and store in cache. Silently skips on serialisation error."""
    try:
        cache.put(key, json.dumps(value).encode(), ttl)
    except (TypeError, ValueError):
        pass


def _cache_get_chain(cache, key: str) -> OptionChainFrames | None:
    """Return a cached OptionChainFrames if present and unexpired, else None."""
    data = cache.get(key)
    if data is None:
        return None
    try:
        return pickle.loads(data)  # nosec pickle — local filesystem cache only
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def _cache_put_chain(cache, key: str, value: OptionChainFrames, ttl: int) -> None:
    """Pickle an OptionChainFrames and store in cache."""
    try:
        cache.put(key, pickle.dumps(value), ttl)
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def _emit_fetch_info(message, logger=None):
    """Print a fetch-progress message and mirror it to the run log when available."""
    print(message)
    if logger:
        logger.info(message)


def _frame_value_count(frame, column):
    """Count non-null values for one column without assuming the column exists."""
    if column not in frame.columns:
        return 0
    return int(frame[column].notna().sum())


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
        df["underlying_price_age_seconds"] > stale_quote_seconds,
        np.nan,
    )
    return df


def append_ticker_event_fields(df, events, today):
    """Broadcast per-ticker corporate event data to all option rows."""
    df["next_earnings_date"] = events.get("next_earnings_date")
    df["next_earnings_date_is_estimated"] = events.get("next_earnings_date_is_estimated")
    df["next_ex_div_date"] = events.get("next_ex_div_date")
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


def fetch_ticker_option_chain(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements,broad-exception-caught
    ticker,
    logger=None,
    validation_findings=None,
    filtered_row_counts=None,
    position_set: PositionSet | None = None,
):
    """Fetch and normalize all near-term option chains for one ticker."""
    provider = None
    try:
        config = get_runtime_config()
        cache = get_provider_cache(config)
        fetched_at = pd.Timestamp.now(tz=timezone.utc)
        provider = get_data_provider()
        _emit_fetch_info(f"Loading {ticker}  ({provider.name})", logger=logger)
        snap_key = f"snapshot:{provider.name}:{ticker}"
        snapshot = _cache_get_json(cache, snap_key)
        if snapshot is None:
            snapshot = provider.load_underlying_snapshot(ticker)
            _cache_put_json(cache, snap_key, snapshot, config.provider_snapshot_ttl)
        underlying_price = snapshot["underlying_price"]
        snap_time = snapshot["underlying_price_time"]
        _emit_fetch_info(
            f"{ticker}: snapshot  price={underlying_price}  time={snap_time}",
            logger=logger,
        )

        if pd.isna(underlying_price) or underlying_price <= 0:
            _emit_fetch_info(
                f"{ticker}: skipped because underlying price is missing or invalid",
                logger=logger,
            )
            if logger:
                logger.warning(
                    "ticker=%s status=skipped reason=invalid_underlying_price",
                    ticker,
                )
            return pd.DataFrame()

        all_normalized_rows = []
        raw_contract_count = 0
        raw_expiration_count = 0
        available_expirations = provider.list_option_expirations(ticker)
        usable_expirations = []
        skipped_for_max_expiration = 0
        skipped_for_past_expiration = 0
        for expiration_date in available_expirations:
            if config.max_expiration is not None and expiration_date > config.max_expiration:
                skipped_for_max_expiration += 1
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            days_until = (exp_date - config.today).days
            # Keep today's expiration for portfolio stock tickers (days == 0); drop past.
            positions = position_set or EMPTY_POSITION_SET
            min_days = 0 if ticker in positions.stock_tickers else 1
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

        events_key = f"events:{provider.name}:{ticker}"
        events = _cache_get_json(cache, events_key)
        if events is None:
            events = provider.load_ticker_events(ticker)
            _cache_put_json(cache, events_key, events, config.provider_events_ttl)
        earnings = events.get("next_earnings_date") or "none"
        ex_div = events.get("next_ex_div_date") or "none"
        _emit_fetch_info(
            f"{ticker}: events  earnings={earnings}  ex_div={ex_div}",
            logger=logger,
        )

        for expiration_date in usable_expirations:
            chain_key = f"chain:{provider.name}:{ticker}:{expiration_date}"
            chain = _cache_get_chain(cache, chain_key)
            if chain is None:
                chain = provider.load_option_chain(ticker, expiration_date)
                _cache_put_chain(cache, chain_key, chain, config.provider_chain_ttl)
            expiration_raw_count = len(chain.calls) + len(chain.puts)
            raw_contract_count += expiration_raw_count
            raw_expiration_count += 1
            call_bid_count = _frame_value_count(chain.calls, "bid")
            put_bid_count = _frame_value_count(chain.puts, "bid")
            call_ask_count = _frame_value_count(chain.calls, "ask")
            put_ask_count = _frame_value_count(chain.puts, "ask")
            call_trade_count = _frame_value_count(chain.calls, "last_trade_price")
            put_trade_count = _frame_value_count(chain.puts, "last_trade_price")
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
            for option_type, option_frame in [("call", chain.calls), ("put", chain.puts)]:
                if option_frame.empty:
                    continue
                vendor_normalized = provider.normalize_option_frame(
                    df=option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
                )
                vendor_normalized = append_ticker_event_fields(
                    vendor_normalized, events, config.today
                )
                normalized = enrich_option_frame(
                    df=vendor_normalized,
                    underlying_price=underlying_price,
                    fetched_at=fetched_at,
                )
                normalized = append_underlying_snapshot_fields(
                    normalized,
                    snapshot,
                    fetched_at,
                    config.stale_quote_seconds,
                )
                if config.enable_validation and validation_findings is not None:
                    validation_findings.extend(validate_option_rows(normalized))
                all_normalized_rows.append(normalized)

        if not all_normalized_rows:
            _emit_fetch_info(
                f"{ticker}: provider returned no usable option frames",
                logger=logger,
            )
            if logger:
                logger.warning(
                    (
                        "ticker=%s provider=%s status=ok rows=0 expirations=0 "
                        "raw_provider_rows=%s raw_expirations=%s"
                    ),
                    ticker,
                    provider.name,
                    raw_contract_count,
                    raw_expiration_count,
                )
            return pd.DataFrame()

        # Pre-filter cross-row enrichment on the full unfiltered chain.
        all_normalized = pd.concat(all_normalized_rows, ignore_index=True)
        pre_filter_count = len(all_normalized)
        _emit_fetch_info(
            f"{ticker}: normalize  rows={pre_filter_count}",
            logger=logger,
        )
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
        else:
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
        if logger:
            logger.info(
                (
                    "ticker=%s provider=%s status=ok fetched_at=%s rows=%s expirations=%s "
                    "raw_provider_rows=%s raw_expirations=%s"
                ),
                ticker,
                provider.name,
                fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                len(combined),
                combined["expiration_date"].nunique(),
                raw_contract_count,
                raw_expiration_count,
            )
        return combined

    except (ProviderAuthenticationError, ProviderQuotaError) as exc:
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
        return pd.DataFrame()

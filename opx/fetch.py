"""Fetch orchestration using the configured market-data provider."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from opx.config import get_runtime_config
from opx.metrics import add_expected_move_by_expiration
from opx.normalize import apply_post_download_filters, enrich_option_frame
from opx.providers.base import ProviderAuthenticationError
from opx.providers import get_data_provider
from opx.validate import validate_option_rows


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
        None,
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
):
    """Fetch and normalize all near-term option chains for one ticker."""
    provider = None
    try:
        config = get_runtime_config()
        fetched_at = pd.Timestamp.now(tz=timezone.utc)
        provider = get_data_provider()
        _emit_fetch_info(f"{ticker}: fetch start provider={provider.name}", logger=logger)
        snapshot = provider.load_underlying_snapshot(ticker)
        underlying_price = snapshot["underlying_price"]
        _emit_fetch_info(
            (
                f"{ticker}: underlying snapshot price={underlying_price} "
                f"quote_time={snapshot['underlying_price_time']}"
            ),
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

        rows = []
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
            if (exp_date - config.today).days <= 0:
                skipped_for_past_expiration += 1
                continue
            usable_expirations.append(expiration_date)

        _emit_fetch_info(
            (
                f"{ticker}: expirations available={len(available_expirations)} "
                f"usable={len(usable_expirations)} "
                f"skipped_max_expiration={skipped_for_max_expiration} "
                f"skipped_expired={skipped_for_past_expiration}"
            ),
            logger=logger,
        )

        events = provider.load_ticker_events(ticker)
        _emit_fetch_info(
            (
                f"{ticker}: events next_earnings={events.get('next_earnings_date')} "
                f"next_ex_div={events.get('next_ex_div_date')} "
                f"dividend_amount={events.get('dividend_amount')}"
            ),
            logger=logger,
        )

        for expiration_date in usable_expirations:
            chain = provider.load_option_chain(ticker, expiration_date)
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
                (
                    f"{ticker}: expiration={expiration_date} raw call_rows={len(chain.calls)} "
                    f"put_rows={len(chain.puts)} total_rows={expiration_raw_count} "
                    f"call_bid_rows={call_bid_count} put_bid_rows={put_bid_count} "
                    f"call_ask_rows={call_ask_count} put_ask_rows={put_ask_count} "
                    f"call_trade_rows={call_trade_count} put_trade_rows={put_trade_count}"
                ),
                logger=logger,
            )
            _emit_fetch_info(
                (
                    f"{ticker}: progress expirations_processed={raw_expiration_count}/"
                    f"{len(usable_expirations)} raw_rows_so_far={raw_contract_count}"
                ),
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
                filtered = apply_post_download_filters(normalized, underlying_price)
                dropped_rows = len(vendor_normalized) - len(filtered)
                if filtered_row_counts is not None:
                    filtered_row_counts.append(dropped_rows)
                _emit_fetch_info(
                    (
                        f"{ticker}: expiration={expiration_date} side={option_type} "
                        f"normalized_rows={len(vendor_normalized)} "
                        f"post_filter_rows={len(filtered)} "
                        f"dropped_rows={dropped_rows}"
                    ),
                    logger=logger,
                )
                rows.append(filtered)

        if not rows:
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

        combined = pd.concat(rows, ignore_index=True)
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
                (
                    f"{ticker}: fetch complete rows={len(combined)} "
                    "expirations="
                    f"{combined['expiration_date'].nunique() if not combined.empty else 0} "
                    f"raw_provider_rows={raw_contract_count}"
                ),
                logger=logger,
            )
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

    except ProviderAuthenticationError as exc:
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

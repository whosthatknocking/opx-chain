"""CSV export helpers for the normalized options dataset."""

from pathlib import Path

import pandas as pd


COLUMN_ORDER = [
    "underlying_symbol",
    "contract_symbol",
    "option_type",
    "expiration_date",
    "days_to_expiration",
    "time_to_expiration_years",
    "strike",
    "underlying_price",
    "underlying_market_state",
    "underlying_day_change_pct",
    "historical_volatility",
    "vix_level",
    "vix_quote_time",
    "underlying_price_time",
    "underlying_price_age_seconds",
    "is_stale_underlying_price",
    "bid",
    "ask",
    "last_trade_price",
    "mark_price_mid",
    "premium_reference_price",
    "premium_reference_method",
    "bid_ask_spread",
    "bid_ask_spread_pct_of_mid",
    "spread_to_strike_pct",
    "spread_to_bid_pct",
    "volume",
    "open_interest",
    "oi_to_volume_ratio",
    "implied_volatility",
    "change",
    "percent_change",
    "option_quote_time",
    "quote_age_seconds",
    "is_stale_quote",
    "is_in_the_money",
    "strike_minus_spot",
    "strike_vs_spot_pct",
    "strike_distance_pct",
    "itm_amount",
    "otm_pct",
    "intrinsic_value",
    "extrinsic_value_bid",
    "extrinsic_value_mid",
    "extrinsic_value_ask",
    "extrinsic_pct_mid",
    "has_negative_extrinsic_mid",
    "premium_to_strike",
    "premium_to_strike_bid",
    "premium_to_strike_annualized",
    "premium_per_day",
    "estimated_margin_requirement",
    "return_on_margin",
    "return_on_margin_annualized",
    "break_even_if_short",
    "expected_move",
    "expected_move_pct",
    "expected_move_lower_bound",
    "expected_move_upper_bound",
    "delta",
    "delta_abs",
    "delta_itm_proxy",
    "probability_itm",
    "gamma",
    "vega",
    "vega_per_day",
    "theta",
    "theta_to_premium_ratio",
    "has_valid_underlying",
    "has_valid_strike",
    "has_valid_quote",
    "has_valid_iv",
    "has_valid_greeks",
    "bid_le_ask",
    "has_nonzero_bid",
    "has_nonzero_ask",
    "has_crossed_or_locked_market",
    "is_wide_market",
    "days_bucket",
    "near_expiry_near_money_flag",
    "passes_primary_screen",
    "quote_quality_score",
    "contract_size",
    "data_source",
    "risk_free_rate_used",
]
UNWANTED_EXPORT_COLUMNS = {
    "currency",
    "underlying_currency",
    "roll_from_expiration_date",
    "roll_days_added",
    "roll_from_premium_reference_price",
    "roll_net_credit",
    "roll_yield",
    "fetch_status",
    "fetch_error",
    "script_version",
    "fetched_at",
}


def format_export_timestamps(df):
    """Format timestamps consistently so the CSV stays stable across runs."""
    for column in ["option_quote_time", "underlying_price_time", "vix_quote_time"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=True, errors="coerce").dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
    return df


def drop_unwanted_columns(df):
    """Remove transient runtime columns that should not be persisted to CSV."""
    existing = [column for column in df.columns if column in UNWANTED_EXPORT_COLUMNS]
    return df.drop(columns=existing) if existing else df


def reorder_export_columns(df):
    """Pin a stable schema while preserving any unexpected source columns at the end."""
    ordered = [column for column in COLUMN_ORDER if column in df.columns]
    extras = [column for column in df.columns if column not in ordered]
    return df[ordered + extras]


def write_options_csv(ticker_frames, output_path):
    """Combine fetched frames, format the schema, and write the final CSV."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.concat(ticker_frames, ignore_index=True)
    df = drop_unwanted_columns(df)
    df = format_export_timestamps(df)
    df = reorder_export_columns(df)
    df.to_csv(output_path, index=False)

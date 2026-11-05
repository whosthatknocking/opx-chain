"""Shared schema constants for canonical option data."""

INTEGER_DATASET_FIELDS = ("days_to_expiration",)
TIMESTAMP_FIELDS = ("option_quote_time", "underlying_price_time")
SOURCE_QUOTE_COLUMN = "_option_quote_source"

QUALITY_FLAG_FIELDS = (
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
)

BOOLEAN_FIELDS = (
    "is_stale_underlying_price",
    "next_earnings_date_is_estimated",
    "earnings_within_5d",
    "earnings_within_10d",
    "ex_div_within_3d",
    "is_stale_quote",
    "is_in_the_money",
    "has_negative_extrinsic_mid",
    "theta_efficiency_below_p25",
    *QUALITY_FLAG_FIELDS,
    "near_expiry_near_money_flag",
    "passes_primary_screen",
    "risk_model_inconsistent",
)

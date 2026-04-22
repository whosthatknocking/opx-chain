"""Shared schema constants for canonical option data."""

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

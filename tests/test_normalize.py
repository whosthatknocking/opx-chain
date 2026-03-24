"""Normalization filter tests for zero bids, strike bands, and spread limits."""

import pandas as pd
import pytest

from opx.normalize import (
    filter_strikes_near_spot,
    filter_wide_spread_quotes,
    filter_zero_bid_quotes,
)


def test_filter_zero_bid_quotes_excludes_only_explicit_zero_bid_rows():
    """Rows with NaN bids should remain while explicit zero bids are removed."""
    frame = pd.DataFrame(
        [
            {"contract_symbol": "ZERO", "bid": 0.0},
            {"contract_symbol": "VALID", "bid": 0.5},
            {"contract_symbol": "MISSING", "bid": float("nan")},
        ]
    )

    result = filter_zero_bid_quotes(frame)

    assert result["contract_symbol"].tolist() == ["VALID", "MISSING"]


def test_filter_strikes_near_spot_keeps_only_rows_within_configured_band():
    """Only strikes inside the configured percentage band should survive."""
    frame = pd.DataFrame(
        [
            {"strike": 69.9},
            {"strike": 70.0},
            {"strike": 100.0},
            {"strike": 130.0},
            {"strike": 130.1},
        ]
    )

    result = filter_strikes_near_spot(frame, underlying_price=100.0)

    assert result["strike"].tolist() == [70.0, 100.0, 130.0]


def test_filter_wide_spread_quotes_keeps_rows_at_the_cutoff(monkeypatch: pytest.MonkeyPatch):
    """The configured spread cutoff should exclude only rows above the threshold."""
    monkeypatch.setattr(
        "opx.normalize.get_runtime_config",
        lambda: type("Config", (), {"max_spread_pct_of_mid": 0.25})(),
    )
    frame = pd.DataFrame(
        [
            {"contract_symbol": "TIGHT", "bid_ask_spread_pct_of_mid": 0.10},
            {"contract_symbol": "EDGE", "bid_ask_spread_pct_of_mid": 0.25},
            {"contract_symbol": "WIDE", "bid_ask_spread_pct_of_mid": 0.30},
        ]
    )

    result = filter_wide_spread_quotes(frame)

    assert result["contract_symbol"].tolist() == ["TIGHT", "EDGE"]

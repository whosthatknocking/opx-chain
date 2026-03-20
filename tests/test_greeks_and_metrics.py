"""Unit tests for Black-Scholes greeks and expected-move calculations."""

import math

import pandas as pd

from options_fetcher.greeks import compute_greeks
from options_fetcher.metrics import add_expected_move_by_expiration


def test_compute_greeks_probability_itm_complements_for_matching_call_put():
    """Call and put ITM probabilities should complement each other at one strike."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.25,
                "option_type": "call",
            },
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.25,
                "option_type": "put",
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    call_probability = result.loc[0, "probability_itm"]
    put_probability = result.loc[1, "probability_itm"]

    assert math.isclose(call_probability + put_probability, 1.0, rel_tol=0, abs_tol=1e-9)
    assert call_probability > put_probability
    assert result["has_valid_greeks"].tolist() == [True, True]


def test_add_expected_move_by_expiration_uses_nearest_to_money_iv():
    """Expected move should use the average IV of the nearest-to-money contracts."""
    frame = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-04-17",
                "underlying_price": 100.0,
                "time_to_expiration_years": 0.25,
                "implied_volatility": 0.20,
                "strike_distance_pct": 0.10,
            },
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-04-17",
                "underlying_price": 100.0,
                "time_to_expiration_years": 0.25,
                "implied_volatility": 0.30,
                "strike_distance_pct": 0.01,
            },
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-04-17",
                "underlying_price": 100.0,
                "time_to_expiration_years": 0.25,
                "implied_volatility": 0.40,
                "strike_distance_pct": 0.01,
            },
        ]
    )

    result = add_expected_move_by_expiration(frame)

    expected_iv = (0.30 + 0.40) / 2
    expected_move = 100.0 * expected_iv * math.sqrt(0.25)

    assert result["expected_move"].notna().all()
    assert math.isclose(
        result.loc[0, "expected_move"], expected_move, rel_tol=0, abs_tol=1e-9
    )
    assert math.isclose(
        result.loc[0, "expected_move_pct"],
        expected_move / 100.0,
        rel_tol=0,
        abs_tol=1e-9,
    )
    assert math.isclose(
        result.loc[0, "expected_move_lower_bound"],
        100.0 - expected_move,
        rel_tol=0,
        abs_tol=1e-9,
    )
    assert math.isclose(
        result.loc[0, "expected_move_upper_bound"],
        100.0 + expected_move,
        rel_tol=0,
        abs_tol=1e-9,
    )

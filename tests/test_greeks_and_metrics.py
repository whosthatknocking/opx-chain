"""Unit tests for Black-Scholes greeks and expected-move calculations."""

import math

import pandas as pd
import pytest

from opx.greeks import compute_greeks
from opx.metrics import add_expected_move_by_expiration, add_option_score


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


def make_scored_row(**overrides):
    """Build one canonical row with the fields needed for option-score calculation."""
    row = {
        "option_type": "call",
        "premium_per_day": 0.04,
        "bid": 1.0,
        "ask": 1.1,
        "bid_ask_spread_pct_of_mid": 0.10,
        "open_interest": 800,
        "volume": 80,
        "delta_abs": 0.25,
        "days_to_expiration": 14,
        "strike": 100.0,
        "underlying_price": 102.0,
        "strike_distance_pct": 0.02,
    }
    row.update(overrides)
    return row


def make_score_config():
    """Build a minimal runtime-config stub for option-score tests."""
    return type(
        "Config",
        (),
        {
            "option_score_income_weight": 0.30,
            "option_score_liquidity_weight": 0.30,
            "option_score_risk_weight": 0.25,
            "option_score_efficiency_weight": 0.15,
        },
    )()


def test_add_option_score_returns_bounded_value(monkeypatch):
    """Option score should stay within 0-100 and reward stronger inputs."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(),
            make_scored_row(
                premium_per_day=0.01,
                bid_ask_spread_pct_of_mid=0.30,
                open_interest=50,
                volume=5,
                delta_abs=0.50,
                days_to_expiration=40,
                strike_distance_pct=0.40,
            ),
        ]
    )

    result = add_option_score(frame.copy())

    assert result["option_score"].between(0, 100).all()
    assert result.loc[0, "option_score"] > result.loc[1, "option_score"]


def test_add_option_score_returns_nan_when_required_inputs_are_missing(monkeypatch):
    """Option score should stay blank when required inputs are not available."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame([make_scored_row(delta_abs=None)])

    result = add_option_score(frame.copy())

    assert result["option_score"].iloc[0] == pytest.approx(float("nan"), nan_ok=True)

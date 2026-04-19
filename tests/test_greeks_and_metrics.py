"""Unit tests for Black-Scholes greeks and expected-move calculations."""

import math

import pandas as pd
import pytest

from opx.greeks import compute_greeks
from opx.metrics import (
    add_derived_pricing_metrics,
    add_event_risk_flags,
    add_expected_move_by_expiration,
    add_option_score,
    add_quote_quality_metrics,
    add_screening_and_freshness_flags,
)


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


def test_compute_greeks_adds_delta_safety_pct_from_delta_abs():
    """Delta safety should be the inverse absolute-delta percentage."""
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

    assert result.loc[0, "delta_safety_pct"] == pytest.approx(
        (1 - result.loc[0, "delta_abs"]) * 100
    )
    assert result.loc[1, "delta_safety_pct"] == pytest.approx(
        (1 - result.loc[1, "delta_abs"]) * 100
    )


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
        "implied_volatility": 0.30,
        "premium_per_day": 0.04,
        "iv_adjusted_premium_per_day": 0.04,
        "bid": 1.0,
        "ask": 1.1,
        "bid_ask_spread_pct_of_mid": 0.10,
        "spread_score": 85.0,
        "open_interest": 800,
        "volume": 80,
        "delta_abs": 0.25,
        "probability_itm": 0.22,
        "days_to_expiration": 14,
        "dte_score": 100.0,
        "theta_efficiency": 8.0,
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
            "min_bid": 0.50,
            "min_open_interest": 100,
            "min_volume": 10,
            "max_spread_pct_of_mid": 0.25,
            "risk_free_rate": 0.045,
            "stale_quote_seconds": 21600,
            "option_score_income_weight": 0.30,
            "option_score_liquidity_weight": 0.30,
            "option_score_risk_weight": 0.25,
            "option_score_efficiency_weight": 0.15,
        },
    )()


def test_add_derived_pricing_metrics_uses_expected_fill_rule_by_spread_threshold(monkeypatch):
    """Expected fill should switch from midpoint to bid-plus-quarter-spread above 10%."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
                {
                    "option_type": "call",
                    "bid": 1.00,
                    "ask": 1.10,
                    "last_trade_price": 1.10,
                    "implied_volatility": 0.30,
                    "strike": 100.0,
                    "volume": 20,
                    "open_interest": 100,
                    "days_to_expiration": 10,
                    "time_to_expiration_years": 10 / 365.0,
                },
                {
                    "option_type": "call",
                    "bid": 1.00,
                    "ask": 1.40,
                    "last_trade_price": 1.20,
                    "implied_volatility": 0.30,
                    "strike": 100.0,
                    "volume": 20,
                    "open_interest": 100,
                    "days_to_expiration": 10,
                    "time_to_expiration_years": 10 / 365.0,
                },
        ]
    )

    quoted = add_quote_quality_metrics(frame.copy(), underlying_price=100.0)
    result = add_derived_pricing_metrics(quoted, underlying_price=100.0)

    assert result.loc[0, "expected_fill_price"] == pytest.approx(1.05)
    assert result.loc[1, "expected_fill_price"] == pytest.approx(1.10)
    assert result.loc[0, "premium_per_day"] == pytest.approx(0.105)
    assert result.loc[1, "premium_per_day"] == pytest.approx(0.11)


def test_add_derived_pricing_metrics_falls_back_for_call_capital_required(monkeypatch):
    """Call capital should fall back from last trade to expected fill when needed."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            {
                "option_type": "call",
                "bid": 1.00,
                "ask": 1.40,
                "last_trade_price": float("nan"),
                "implied_volatility": 0.30,
                "strike": 100.0,
                "volume": 20,
                "open_interest": 100,
                "days_to_expiration": 10,
                "time_to_expiration_years": 10 / 365.0,
            }
        ]
    )

    quoted = add_quote_quality_metrics(frame.copy(), underlying_price=100.0)
    result = add_derived_pricing_metrics(quoted, underlying_price=100.0)

    assert result.loc[0, "expected_fill_price"] == pytest.approx(1.10)
    assert result.loc[0, "capital_required"] == pytest.approx(110.0)
    assert pd.notna(result.loc[0, "theta_efficiency"])


def test_add_screening_and_freshness_flags_uses_prompt_spread_and_dte_tiers(monkeypatch):
    """Spread and DTE scores should follow the prompt's execution scoring tiers."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    fetched_at = pd.Timestamp("2026-03-20T16:00:00Z")
    frame = pd.DataFrame(
        [
                {
                    "option_type": "call",
                    "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                    "days_to_expiration": 14,
                    "strike_distance_pct": 0.02,
                    "premium_per_day": 0.04,
                    "iv_adjusted_premium_per_day": 0.04,
                    "theta_efficiency": 8.0,
                    "bid": 1.0,
                    "ask": 1.1,
                    "strike": 100.0,
                    "underlying_price": 102.0,
                    "open_interest": 150,
                    "volume": 20,
                    "delta_abs": 0.25,
                    "probability_itm": 0.22,
                "has_valid_quote": True,
                "has_nonzero_bid": True,
                "has_nonzero_ask": True,
                "has_valid_iv": True,
                "has_valid_greeks": True,
                "has_crossed_or_locked_market": False,
                "bid_ask_spread_pct_of_mid": 0.08,
                },
                {
                    "option_type": "call",
                    "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                    "days_to_expiration": 45,
                    "strike_distance_pct": 0.02,
                    "premium_per_day": 0.02,
                    "iv_adjusted_premium_per_day": 0.02,
                    "theta_efficiency": 8.0,
                    "bid": 1.0,
                    "ask": 1.1,
                    "strike": 100.0,
                    "underlying_price": 102.0,
                    "open_interest": 150,
                    "volume": 20,
                    "delta_abs": 0.42,
                "probability_itm": 0.38,
                "has_valid_quote": True,
                "has_nonzero_bid": True,
                "has_nonzero_ask": True,
                "has_valid_iv": True,
                "has_valid_greeks": True,
                "has_crossed_or_locked_market": False,
                "bid_ask_spread_pct_of_mid": 0.20,
            },
        ]
    )

    result = add_screening_and_freshness_flags(frame.copy(), fetched_at=fetched_at)

    assert result.loc[0, "spread_score"] == pytest.approx(100.0)
    assert result.loc[1, "spread_score"] == pytest.approx(42.5)
    assert result.loc[0, "dte_score"] == pytest.approx(100.0)
    assert result.loc[1, "dte_score"] == pytest.approx(65.0)
    assert result.loc[0, "risk_level"] == "LOW"
    assert result.loc[1, "risk_level"] == "HIGH"


def test_add_option_score_returns_bounded_value(monkeypatch):
    """Option score should stay within 0-100 and reward stronger inputs."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(),
            make_scored_row(
                premium_per_day=0.01,
                iv_adjusted_premium_per_day=0.01,
                bid_ask_spread_pct_of_mid=0.30,
                spread_score=0.0,
                open_interest=50,
                volume=5,
                delta_abs=0.50,
                probability_itm=0.45,
                days_to_expiration=40,
                dte_score=65.0,
                theta_efficiency=1.0,
                strike_distance_pct=0.40,
            ),
        ]
    )

    result = add_option_score(frame.copy())

    assert result["option_score"].between(0, 100).all()
    assert result.loc[0, "option_score"] > result.loc[1, "option_score"]


def test_add_option_score_penalizes_near_useless_premium_per_day(monkeypatch):
    """Income scoring should zero out near-useless premium-per-day values below the floor."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(premium_per_day=0.009, iv_adjusted_premium_per_day=0.009),
            make_scored_row(premium_per_day=0.01, iv_adjusted_premium_per_day=0.01),
            make_scored_row(premium_per_day=0.03, iv_adjusted_premium_per_day=0.03),
        ]
    )

    result = add_option_score(frame.copy())

    assert result.loc[0, "option_score"] == pytest.approx(result.loc[1, "option_score"])
    assert result.loc[2, "option_score"] > result.loc[1, "option_score"]


def test_add_option_score_caps_income_component_at_point_zero_five(monkeypatch):
    """Income scoring should still cap at the premium-per-day ceiling."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(premium_per_day=0.05, iv_adjusted_premium_per_day=0.05),
            make_scored_row(premium_per_day=0.08, iv_adjusted_premium_per_day=0.08),
        ]
    )

    result = add_option_score(frame.copy())

    assert result.loc[0, "option_score"] == pytest.approx(result.loc[1, "option_score"])


def test_add_option_score_uses_prompt_execution_tiers(monkeypatch):
    """Score should prefer the prompt's best DTE and spread tiers over weaker execution."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(days_to_expiration=14, dte_score=100.0, spread_score=100.0),
            make_scored_row(days_to_expiration=28, dte_score=85.0, spread_score=85.0),
            make_scored_row(days_to_expiration=45, dte_score=65.0, spread_score=42.5),
            make_scored_row(days_to_expiration=4, dte_score=25.0, spread_score=85.0),
        ]
    )

    result = add_option_score(frame.copy())

    assert result.loc[0, "option_score"] > result.loc[1, "option_score"]
    assert result.loc[1, "option_score"] > result.loc[2, "option_score"]
    assert result.loc[0, "option_score"] > result.loc[3, "option_score"]


def test_add_option_score_rewards_higher_iv_adjusted_income(monkeypatch):
    """Higher IV-adjusted premium-per-day should improve the prompt-aligned score."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
            make_scored_row(premium_per_day=0.02, iv_adjusted_premium_per_day=0.02),
            make_scored_row(premium_per_day=0.02, iv_adjusted_premium_per_day=0.05),
            make_scored_row(premium_per_day=0.02, iv_adjusted_premium_per_day=0.08),
        ]
    )

    result = add_option_score(frame.copy())

    assert result.loc[1, "option_score"] > result.loc[0, "option_score"]
    assert result.loc[1, "option_score"] == pytest.approx(result.loc[2, "option_score"])


def test_add_option_score_assigns_final_score_adjustment(monkeypatch):
    """Score validation should adjust the row-level final score when alignment is weak or strong."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
                make_scored_row(
                    iv_adjusted_premium_per_day=0.01,
                    spread_score=100.0,
                    theta_efficiency=15.0,
                    dte_score=100.0,
                ),
                make_scored_row(
                    iv_adjusted_premium_per_day=0.05,
                    spread_score=100.0,
                    theta_efficiency=0.5,
                    dte_score=25.0,
                    delta_abs=0.50,
                ),
            ]
        )

    result = add_option_score(frame.copy())

    assert result.loc[0, "score_validation"] == "DISCREPANCY"
    assert result.loc[0, "final_score"] < result.loc[0, "option_score"]
    assert result.loc[1, "score_validation"] == "ALIGNED"


def test_add_option_score_returns_nan_when_required_inputs_are_missing(monkeypatch):
    """Option score should stay blank when required inputs are not available."""
    monkeypatch.setattr("opx.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame([make_scored_row(delta_abs=None)])

    result = add_option_score(frame.copy())

    assert result["option_score"].iloc[0] == pytest.approx(float("nan"), nan_ok=True)


def test_add_event_risk_flags_scores_earnings_and_dividend_tiers():
    """Event risk score should follow the defined point tiers and cap at 100."""
    frame = pd.DataFrame(
        [
            {"days_to_expiration": 10.0, "days_to_earnings": 3.0, "days_to_ex_div": 2.0},   # 60 + 40 = 100
            {"days_to_expiration": 10.0, "days_to_earnings": 8.0, "days_to_ex_div": 5.0},   # 30 + 20 = 50
            {"days_to_expiration": 20.0, "days_to_earnings": 15.0, "days_to_ex_div": 10.0}, # 0 + 0 = 0
            {"days_to_expiration": 10.0, "days_to_earnings": 3.0, "days_to_ex_div": float("nan")},  # 60 + 0 = 60
            {"days_to_expiration": 10.0, "days_to_earnings": float("nan"), "days_to_ex_div": 2.0},  # 0 + 40 = 40
            {"days_to_earnings": float("nan"), "days_to_ex_div": float("nan")},  # NaN
        ]
    )

    result = add_event_risk_flags(frame.copy())

    assert result.loc[0, "event_risk_score"] == pytest.approx(100.0)
    assert result.loc[1, "event_risk_score"] == pytest.approx(50.0)
    assert result.loc[2, "event_risk_score"] == pytest.approx(0.0)
    assert result.loc[3, "event_risk_score"] == pytest.approx(60.0)
    assert result.loc[4, "event_risk_score"] == pytest.approx(40.0)
    assert pd.isna(result.loc[5, "event_risk_score"])


def test_add_event_risk_flags_require_contract_to_span_the_event():
    """Rows should not be flagged for events that occur after the contract expires."""
    frame = pd.DataFrame(
        [
            {"days_to_expiration": 2.0, "days_to_earnings": 5.0, "days_to_ex_div": 4.0},
            {"days_to_expiration": 6.0, "days_to_earnings": 5.0, "days_to_ex_div": 4.0},
        ]
    )

    result = add_event_risk_flags(frame.copy())

    assert result.loc[0, "earnings_within_5d"] is None
    assert result.loc[0, "earnings_within_10d"] is None
    assert result.loc[0, "ex_div_within_3d"] is None
    assert pd.isna(result.loc[0, "event_risk_score"])

    assert result.loc[1, "earnings_within_5d"] is True
    assert result.loc[1, "earnings_within_10d"] is True
    assert result.loc[1, "ex_div_within_3d"] is False
    assert result.loc[1, "event_risk_score"] == pytest.approx(80.0)


def test_add_event_risk_flags_boolean_flags_use_object_dtype_not_float():
    """Proximity flags should be object dtype with True/False so they export as bool strings."""
    frame = pd.DataFrame(
        [
            {"days_to_expiration": 10.0, "days_to_earnings": 3.0, "days_to_ex_div": 2.0},
            {"days_to_expiration": 10.0, "days_to_earnings": 15.0, "days_to_ex_div": float("nan")},
            {"days_to_earnings": float("nan"), "days_to_ex_div": float("nan")},
        ]
    )

    result = add_event_risk_flags(frame.copy())

    assert result["earnings_within_5d"].dtype == object
    assert result["earnings_within_10d"].dtype == object
    assert result["ex_div_within_3d"].dtype == object
    assert result.loc[0, "earnings_within_5d"] is True
    assert result.loc[1, "earnings_within_5d"] is None
    assert result.loc[2, "earnings_within_5d"] is None


def test_add_event_risk_flags_handles_missing_columns_gracefully():
    """Event risk flags should still be added when source columns are absent."""
    frame = pd.DataFrame([{"strike": 100.0, "days_to_expiration": 14}])

    result = add_event_risk_flags(frame.copy())

    assert "earnings_within_5d" in result.columns
    assert "event_risk_score" in result.columns
    assert pd.isna(result.loc[0, "event_risk_score"])

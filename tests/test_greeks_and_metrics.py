"""Unit tests for Black-Scholes greeks and expected-move calculations."""
# pylint: disable=too-many-lines

import math

import pandas as pd
import pytest

from opx_chain.greeks import compute_greeks
from opx_chain.metrics import (
    add_derived_pricing_metrics,
    add_event_risk_flags,
    add_expected_move_by_expiration,
    add_option_score,
    add_quote_quality_metrics,
    add_screening_and_freshness_flags,
    classify_days_to_expiration_bucket,
)
from opx_chain.validate import validate_option_rows


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


def test_compute_greeks_does_not_substitute_missing_implied_volatility():
    """Missing or zero IV should not produce synthetic Black-Scholes outputs."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": None,
                "option_type": "call",
            },
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.0,
                "option_type": "put",
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    assert result[["delta", "probability_itm", "gamma", "vega", "theta"]].isna().all().all()
    assert result["has_valid_greeks"].tolist() == [False, False]


def test_compute_greeks_rejects_non_finite_underlying_price():
    """Non-finite spot values must not produce extreme derived Greeks."""
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

    result = compute_greeks(
        frame.copy(),
        underlying_price=float("inf"),
        risk_free_rate=0.045,
    )

    assert result[["delta", "probability_itm", "gamma", "vega", "theta"]].isna().all().all()
    assert result["has_valid_greeks"].tolist() == [False, False]


def test_compute_greeks_rejects_non_finite_strike_and_time():
    """Non-finite row inputs should not enter Black-Scholes calculations."""
    frame = pd.DataFrame(
        [
            {
                "strike": float("inf"),
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.25,
                "option_type": "call",
            },
            {
                "strike": 100,
                "time_to_expiration_years": float("inf"),
                "implied_volatility": 0.25,
                "option_type": "put",
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    assert result[["delta", "probability_itm", "gamma", "vega", "theta"]].isna().all().all()
    assert result["has_valid_greeks"].tolist() == [False, False]


def test_compute_greeks_marks_provider_greeks_valid_without_iv():
    """Provider-native Greeks remain valid even when derived Greeks cannot run."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": None,
                "option_type": "call",
                "delta": 0.25,
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    assert result.loc[0, "delta"] == 0.25
    assert pd.isna(result.loc[0, "probability_itm"])
    assert bool(result.loc[0, "has_valid_greeks"]) is True


def test_compute_greeks_does_not_preserve_non_finite_provider_greeks():
    """Provider-native greeks must be finite before they are trusted."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": None,
                "option_type": "call",
                "delta": float("inf"),
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    assert pd.isna(result.loc[0, "delta"])
    assert bool(result.loc[0, "has_valid_greeks"]) is False


def test_compute_greeks_does_not_preserve_boolean_provider_greeks():
    """Provider-native boolean risk values should fall back to derived Greeks."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100,
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.25,
                "option_type": "call",
                "delta": True,
                "probability_itm": False,
                "gamma": True,
                "vega": False,
                "theta": True,
            },
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110, risk_free_rate=0.045)

    for field in ("delta", "probability_itm", "gamma", "vega", "theta"):
        assert not isinstance(result.loc[0, field], (bool, type(pd.NA)))
        assert pd.notna(result.loc[0, field])
    assert result.loc[0, "delta"] != 1.0
    assert result.loc[0, "probability_itm"] != 0.0
    assert bool(result.loc[0, "has_valid_greeks"]) is True


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


def test_add_expected_move_by_expiration_rejects_non_finite_inputs():
    """Expected move should not be computed from infinite IV or spot values."""
    frame = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-04-17",
                "underlying_price": 100.0,
                "time_to_expiration_years": 0.25,
                "implied_volatility": float("inf"),
                "strike_distance_pct": 0.01,
            },
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-04-17",
                "underlying_price": float("inf"),
                "time_to_expiration_years": 0.25,
                "implied_volatility": 0.30,
                "strike_distance_pct": 0.01,
            },
        ]
    )

    result = add_expected_move_by_expiration(frame)

    assert result["expected_move"].isna().all()


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


def test_add_quote_quality_metrics_rejects_infinite_row_values():
    """Quote-quality booleans should require finite numeric inputs."""
    frame = pd.DataFrame(
        [
            {
                "strike": float("inf"),
                "bid": 1.00,
                "ask": 1.20,
                "implied_volatility": 0.30,
                "volume": 20,
                "open_interest": 100,
            },
            {
                "strike": 100.0,
                "bid": float("inf"),
                "ask": 2.00,
                "implied_volatility": 0.30,
                "volume": 20,
                "open_interest": 100,
            },
            {
                "strike": 100.0,
                "bid": 1.00,
                "ask": float("inf"),
                "implied_volatility": 0.30,
                "volume": 20,
                "open_interest": 100,
            },
            {
                "strike": 100.0,
                "bid": 1.00,
                "ask": 1.20,
                "implied_volatility": float("inf"),
                "volume": 20,
                "open_interest": 100,
            },
        ]
    )

    quoted = add_quote_quality_metrics(frame.copy(), underlying_price=100.0)

    assert quoted["has_valid_strike"].tolist() == [False, True, True, True]
    assert quoted["has_nonzero_bid"].tolist() == [True, False, True, True]
    assert quoted["has_nonzero_ask"].tolist() == [True, True, False, True]
    assert quoted["has_valid_iv"].tolist() == [True, True, True, False]
    assert quoted["has_valid_quote"].tolist() == [True, False, False, True]
    assert quoted.loc[[1, 2], ["mark_price_mid", "bid_ask_spread"]].isna().all().all()


def test_add_derived_pricing_metrics_uses_expected_fill_rule_by_spread_threshold(monkeypatch):
    """Expected fill should switch from midpoint to bid-plus-quarter-spread above 10%."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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


@pytest.mark.parametrize("underlying_price", [0.0, -1.0, float("inf")])
def test_add_derived_pricing_metrics_guards_otm_pct_divisor(
    monkeypatch,
    underlying_price,
):
    """Invalid spot prices should not produce spot-derived metrics."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame(
        [
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
            {
                "option_type": "put",
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

    quoted = add_quote_quality_metrics(frame.copy(), underlying_price=underlying_price)
    result = add_derived_pricing_metrics(quoted, underlying_price=underlying_price)

    spot_derived_columns = " ".join(
        [
            "strike_minus_spot strike_vs_spot_pct strike_distance_pct",
            "itm_amount otm_pct intrinsic_value",
            "extrinsic_value_bid extrinsic_value_mid extrinsic_value_ask",
            "estimated_margin_requirement",
            "return_on_margin return_on_margin_annualized",
        ]
    ).split()
    assert result[spot_derived_columns].isna().all().all()
    assert result["has_valid_underlying"].tolist() == [False, False]
    assert result["has_valid_greeks"].tolist() == [False, False]


def test_add_screening_and_freshness_flags_uses_prompt_spread_and_dte_tiers(monkeypatch):
    """Spread and DTE scores should follow the prompt's execution scoring tiers."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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


def test_add_screening_and_freshness_flags_vectorizes_days_bucket(monkeypatch):
    """DTE buckets should not call the scalar classifier once per row."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    monkeypatch.setattr(
        "opx_chain.metrics.classify_days_to_expiration_bucket",
        lambda _value: pytest.fail("scalar DTE classifier should not be called"),
    )
    fetched_at = pd.Timestamp("2026-03-20T16:00:00Z")
    base = {
        **make_scored_row(),
        "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
        "has_valid_quote": True,
        "has_nonzero_bid": True,
        "has_nonzero_ask": True,
        "has_valid_iv": True,
        "has_valid_greeks": True,
        "has_crossed_or_locked_market": False,
    }
    frame = pd.DataFrame([
        {**base, "days_to_expiration": dte}
        for dte in (10, 11, 18, 19, 26, 27)
    ])

    result = add_screening_and_freshness_flags(frame.copy(), fetched_at=fetched_at)

    assert result["days_bucket"].tolist() == [
        "Week_1",
        "Week_2",
        "Week_2",
        "Week_3",
        "Week_3",
        "Week_4",
    ]


def test_classify_days_to_expiration_bucket_uses_shared_boundaries():
    """Scalar DTE buckets should match the vectorized screening boundaries."""
    values = [None, float("inf"), 10, 11, 18, 19, 26, 27]

    assert [classify_days_to_expiration_bucket(value) for value in values] == [
        "UNKNOWN",
        "UNKNOWN",
        "Week_1",
        "Week_2",
        "Week_2",
        "Week_3",
        "Week_3",
        "Week_4",
    ]


def test_add_screening_and_freshness_flags_marks_non_finite_scores_missing(monkeypatch):
    """Scoring helpers should not turn non-finite inputs into usable defaults."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    fetched_at = pd.Timestamp("2026-03-20T16:00:00Z")
    row = {
        **make_scored_row(),
        "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
        "has_valid_quote": True,
        "has_nonzero_bid": True,
        "has_nonzero_ask": True,
        "has_valid_iv": True,
        "has_valid_greeks": True,
        "has_crossed_or_locked_market": False,
        "bid_ask_spread_pct_of_mid": float("inf"),
        "days_to_expiration": float("inf"),
        "delta_abs": float("inf"),
    }

    result = add_screening_and_freshness_flags(pd.DataFrame([row]), fetched_at=fetched_at)

    assert result.loc[0, "days_bucket"] == "UNKNOWN"
    assert pd.isna(result.loc[0, "spread_score"])
    assert pd.isna(result.loc[0, "dte_score"])
    assert pd.isna(result.loc[0, "option_score"])


def test_add_option_score_requires_finite_probability_itm(monkeypatch):
    """Non-finite risk-model probabilities should not produce ranking scores."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame([make_scored_row(probability_itm=float("inf"))])

    result = add_option_score(frame.copy())

    assert pd.isna(result.loc[0, "option_score"])
    assert pd.isna(result.loc[0, "final_score"])
    assert pd.isna(result.loc[0, "score_validation"])


def test_add_quote_quality_metrics_rejects_boolean_quotes():
    """Boolean bid/ask inputs should not crash or become valid quote prices."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100.0,
                "bid": True,
                "ask": False,
                "volume": 10,
                "open_interest": 20,
                "implied_volatility": 0.30,
            }
        ]
    )

    result = add_quote_quality_metrics(frame.copy(), underlying_price=101.0)

    assert pd.isna(result.loc[0, "bid"])
    assert pd.isna(result.loc[0, "ask"])
    assert bool(result.loc[0, "has_valid_quote"]) is False
    assert pd.isna(result.loc[0, "bid_ask_spread"])


def test_add_option_score_returns_bounded_value(monkeypatch):
    """Option score should stay within 0-100 and reward stronger inputs."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    frame = pd.DataFrame([make_scored_row(delta_abs=None)])

    result = add_option_score(frame.copy())

    assert result["option_score"].iloc[0] == pytest.approx(float("nan"), nan_ok=True)


def test_add_event_risk_flags_scores_earnings_and_dividend_tiers():
    """Event risk score should follow the defined point tiers and cap at 100."""
    frame = pd.DataFrame(
        [
            {
                "days_to_expiration": 10.0,
                "days_to_earnings": 3.0,
                "days_to_ex_div": 2.0,
            },  # 60 + 40 = 100
            {
                "days_to_expiration": 10.0,
                "days_to_earnings": 8.0,
                "days_to_ex_div": 5.0,
            },  # 30 + 20 = 50
            {
                "days_to_expiration": 20.0,
                "days_to_earnings": 15.0,
                "days_to_ex_div": 10.0,
            },  # 0 + 0 = 0
            {
                "days_to_expiration": 10.0,
                "days_to_earnings": 3.0,
                "days_to_ex_div": float("nan"),
            },  # 60 + 0 = 60
            {
                "days_to_expiration": 10.0,
                "days_to_earnings": float("nan"),
                "days_to_ex_div": 2.0,
            },  # 0 + 40 = 40
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


def test_add_event_risk_flags_treats_malformed_day_counts_as_missing():
    """Boolean and string event-day values should not score or raise."""
    frame = pd.DataFrame(
        [
            {"days_to_expiration": 30, "days_to_earnings": True, "days_to_ex_div": False},
            {"days_to_expiration": 30, "days_to_earnings": "true", "days_to_ex_div": "false"},
            {"days_to_expiration": 30, "days_to_earnings": float("inf"), "days_to_ex_div": 2.0},
        ]
    )

    result = add_event_risk_flags(frame.copy())

    assert pd.isna(result.loc[0, "event_risk_score"])
    assert pd.isna(result.loc[1, "event_risk_score"])
    assert result.loc[2, "event_risk_score"] == pytest.approx(40.0)
    assert result.loc[0, "earnings_within_5d"] is None
    assert result.loc[0, "ex_div_within_3d"] is None


def test_add_screening_and_freshness_flags_is_stale_quote_stays_nullable_boolean(monkeypatch):
    """is_stale_quote must be bool/None (object dtype) — not float64 from np.nan
    coercion — so downstream validators that accept bool or missing pass it.
    """
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
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
                "option_quote_time": pd.NaT,  # missing quote time → unknown staleness
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
                "option_quote_time": pd.Timestamp("2026-03-20T16:05:00Z"),
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
        ]
    )

    result = add_screening_and_freshness_flags(frame.copy(), fetched_at=fetched_at)

    assert result["is_stale_quote"].dtype == object
    assert result.loc[0, "is_stale_quote"] is False
    assert result.loc[1, "is_stale_quote"] is None
    assert result.loc[2, "is_stale_quote"] is True


def test_add_screening_and_freshness_flags_risk_model_uses_nullable_boolean(monkeypatch):
    """risk_model_inconsistent must not become 0.0/1.0 floats via np.nan coercion."""
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", make_score_config)
    fetched_at = pd.Timestamp("2026-03-20T16:00:00Z")
    frame = pd.DataFrame(
        [
            {
                "data_source": "stub",
                "underlying_symbol": "TEST",
                "contract_symbol": "TEST260501C00100000",
                "option_type": "call",
                "expiration_date": "2026-05-01",
                "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "underlying_price_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "days_to_expiration": 14,
                "strike_distance_pct": 0.02,
                "premium_per_day": 0.04,
                "iv_adjusted_premium_per_day": 0.04,
                "theta_efficiency": 8.0,
                "bid": 1.0,
                "ask": 1.1,
                "last_trade_price": 1.05,
                "strike": 100.0,
                "underlying_price": 102.0,
                "open_interest": 150,
                "volume": 20,
                "implied_volatility": 0.30,
                "delta_abs": 0.25,
                "probability_itm": 0.22,
                "has_valid_quote": True,
                "has_nonzero_bid": True,
                "has_nonzero_ask": True,
                "has_valid_iv": True,
                "has_valid_greeks": True,
                "has_crossed_or_locked_market": False,
                "bid_ask_spread_pct_of_mid": 0.08,
                "is_in_the_money": False,
            },
            {
                "data_source": "stub",
                "underlying_symbol": "TEST",
                "contract_symbol": "TEST260501C00105000",
                "option_type": "call",
                "expiration_date": "2026-05-01",
                "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "underlying_price_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "days_to_expiration": 14,
                "strike_distance_pct": 0.02,
                "premium_per_day": 0.04,
                "iv_adjusted_premium_per_day": 0.04,
                "theta_efficiency": 8.0,
                "bid": 1.0,
                "ask": 1.1,
                "last_trade_price": 1.05,
                "strike": 105.0,
                "underlying_price": 102.0,
                "open_interest": 150,
                "volume": 20,
                "implied_volatility": 0.30,
                "delta_abs": 0.70,
                "probability_itm": 0.40,
                "has_valid_quote": True,
                "has_nonzero_bid": True,
                "has_nonzero_ask": True,
                "has_valid_iv": True,
                "has_valid_greeks": True,
                "has_crossed_or_locked_market": False,
                "bid_ask_spread_pct_of_mid": 0.08,
                "is_in_the_money": False,
            },
            {
                "data_source": "stub",
                "underlying_symbol": "TEST",
                "contract_symbol": "TEST260501C00110000",
                "option_type": "call",
                "expiration_date": "2026-05-01",
                "option_quote_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "underlying_price_time": pd.Timestamp("2026-03-20T15:55:00Z"),
                "days_to_expiration": 14,
                "strike_distance_pct": 0.02,
                "premium_per_day": 0.04,
                "iv_adjusted_premium_per_day": 0.04,
                "theta_efficiency": 8.0,
                "bid": 1.0,
                "ask": 1.1,
                "last_trade_price": 1.05,
                "strike": 110.0,
                "underlying_price": 102.0,
                "open_interest": 150,
                "volume": 20,
                "implied_volatility": 0.30,
                "delta_abs": pd.NA,
                "probability_itm": 0.40,
                "has_valid_quote": True,
                "has_nonzero_bid": True,
                "has_nonzero_ask": True,
                "has_valid_iv": True,
                "has_valid_greeks": True,
                "has_crossed_or_locked_market": False,
                "bid_ask_spread_pct_of_mid": 0.08,
                "is_in_the_money": False,
            },
        ]
    )

    frame["risk_model_inconsistent"] = pd.Series([False, True, pd.NA], dtype="boolean")
    result = add_screening_and_freshness_flags(frame.copy(), fetched_at=fetched_at)

    assert str(result["risk_model_inconsistent"].dtype) == "boolean"
    assert bool(result.loc[0, "risk_model_inconsistent"]) is False
    assert bool(result.loc[1, "risk_model_inconsistent"]) is True
    assert pd.isna(result.loc[2, "risk_model_inconsistent"])

    findings = validate_option_rows(result)
    invalid_boolean_fields = {
        finding.field for finding in findings if finding.code == "invalid_boolean_field"
    }
    assert "risk_model_inconsistent" not in invalid_boolean_fields


@pytest.mark.parametrize("option_type", ["call", "put"])
@pytest.mark.parametrize("years,volatility", [(411 / 365, .5966), (.05, .2)])
def test_risk_consistency_keeps_self_consistent_model_with_distinct_probability(
    option_type, years, volatility,
):
    """Long-duration delta/probability divergence is not model inconsistency."""
    frame = pd.DataFrame([{"strike": 100., "time_to_expiration_years": years,
                           "implied_volatility": volatility, "option_type": option_type}])
    result = compute_greeks(frame, 65.76, .045)
    assert bool(result.loc[0, "risk_model_inconsistent"]) is False
    if years > 1:
        assert abs(abs(result.loc[0, "delta"]) - result.loc[0, "probability_itm"]) > .15


@pytest.mark.parametrize("native_delta,expected_flag", [
    (-.6109, False), (-.05, True), (.6109, True),
])
def test_risk_consistency_compares_native_signed_delta_without_replacing_it(
    native_delta, expected_flag,
):
    """Retain the actual INTC Greek and reject a material same-quantity mismatch."""
    frame = pd.DataFrame([{"strike": 100., "time_to_expiration_years": 411 / 365,
                           "implied_volatility": .5966, "option_type": "put",
                           "delta": native_delta}])
    result = compute_greeks(frame, 65.76, .045)
    assert result.loc[0, "delta"] == native_delta
    assert bool(result.loc[0, "risk_model_inconsistent"]) is expected_flag
    assert result.loc[0, "probability_itm"] == pytest.approx(.8155658550797064)


@pytest.mark.parametrize("iv,years,spot", [(0., 1., 65.76), (float("nan"), 1., 65.76),
                                         (.6, 0., 65.76), (.6, 1., 0.)])
def test_risk_consistency_stays_unknown_without_independent_model_inputs(iv, years, spot):
    """Native Greeks remain available without manufacturing a consistency pass."""
    frame = pd.DataFrame([{"strike": 100., "time_to_expiration_years": years,
                           "implied_volatility": iv, "option_type": "put", "delta": -.6109}])
    result = compute_greeks(frame, spot, .045)
    assert result.loc[0, "delta"] == -.6109
    assert pd.isna(result.loc[0, "risk_model_inconsistent"])
    assert str(result["risk_model_inconsistent"].dtype) == "boolean"

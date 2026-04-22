"""Unit tests for the cross-row enrichment metrics added in section 13."""

import numpy as np
import pandas as pd
from opx_chain.metrics import (
    add_iv_state_level,
    add_iv_state_term,
    add_listed_strike_increment,
    add_theta_efficiency_below_p25,
)


def _make_df(**columns):
    """Build a minimal DataFrame from keyword column arrays."""
    return pd.DataFrame(columns)


# ---------------------------------------------------------------------------
# add_iv_state_level
# ---------------------------------------------------------------------------

def _iv_level_frame(tickers, expirations, strikes, ivs, spot=100.0):
    """Build a test frame with the minimum columns needed by add_iv_state_level."""
    return pd.DataFrame({
        "underlying_symbol": tickers,
        "expiration_date": expirations,
        "strike": strikes,
        "implied_volatility": ivs,
        "strike_distance_pct": [abs(s - spot) / spot for s in strikes],
    })


def test_add_iv_state_level_classifies_high():
    """Representative IV at or above p70 should produce HIGH."""
    # 10 rows, ATM row (strike=100) at nearest expiration has IV=0.60 — highest
    tickers = ["TSLA"] * 10
    exps = ["2026-04-17"] * 10
    strikes = [80, 85, 90, 95, 100, 105, 110, 115, 120, 125]
    ivs = [0.20, 0.22, 0.24, 0.26, 0.60, 0.28, 0.30, 0.32, 0.34, 0.36]
    df = _iv_level_frame(tickers, exps, strikes, ivs)
    result = add_iv_state_level(df)
    assert (result["iv_state_level"] == "HIGH").all()


def test_add_iv_state_level_classifies_low():
    """Representative IV at or below p30 should produce LOW."""
    tickers = ["TSLA"] * 10
    exps = ["2026-04-17"] * 10
    strikes = [80, 85, 90, 95, 100, 105, 110, 115, 120, 125]
    # ATM row (strike=100) has lowest IV in distribution
    ivs = [0.50, 0.52, 0.54, 0.56, 0.10, 0.58, 0.60, 0.62, 0.64, 0.66]
    df = _iv_level_frame(tickers, exps, strikes, ivs)
    result = add_iv_state_level(df)
    assert (result["iv_state_level"] == "LOW").all()


def test_add_iv_state_level_classifies_neutral():
    """Representative IV between p30 and p70 should produce NEUTRAL."""
    tickers = ["TSLA"] * 10
    exps = ["2026-04-17"] * 10
    strikes = [80, 85, 90, 95, 100, 105, 110, 115, 120, 125]
    # ATM (strike=100) IV is near the median — well inside the middle band
    ivs = [0.20, 0.22, 0.24, 0.26, 0.30, 0.34, 0.38, 0.42, 0.46, 0.50]
    df = _iv_level_frame(tickers, exps, strikes, ivs)
    result = add_iv_state_level(df)
    assert (result["iv_state_level"] == "NEUTRAL").all()


def test_add_iv_state_level_unknown_when_fewer_than_5_rows():
    """Fewer than 5 rows for an underlying must produce UNKNOWN."""
    df = _iv_level_frame(["TSLA"] * 4, ["2026-04-17"] * 4, [95, 100, 105, 110], [0.3] * 4)
    result = add_iv_state_level(df)
    assert (result["iv_state_level"] == "UNKNOWN").all()


def test_add_iv_state_level_uses_nearest_expiration_for_atm():
    """ATM row must be selected from the nearest expiration, not the second."""
    # near exp: ATM row has IV=0.60 (HIGH), far exp has ATM IV=0.25 (LOW)
    tickers = ["TSLA"] * 10
    exps = ["2026-04-17"] * 5 + ["2026-05-16"] * 5
    strikes = [95, 97, 100, 103, 105] * 2
    # Near-exp ATM (strike=100) IV=0.60; far-exp ATM (strike=100) IV=0.25
    ivs = [0.25, 0.27, 0.60, 0.27, 0.25, 0.25, 0.27, 0.25, 0.27, 0.25]
    df = _iv_level_frame(tickers, exps, strikes, ivs)
    result = add_iv_state_level(df)
    # All rows classified from the near-expiration ATM, so HIGH
    assert (result["iv_state_level"] == "HIGH").all()


def test_add_iv_state_level_independent_per_underlying():
    """Different underlyings are classified independently."""
    tickers = ["AAPL"] * 6 + ["MSFT"] * 6
    exps = ["2026-04-17"] * 12
    strikes = [95, 97, 100, 103, 105, 107] * 2
    # AAPL ATM (strike=100) IV=0.60 → HIGH; MSFT ATM (strike=100) IV=0.10 → LOW
    ivs_aapl = [0.25, 0.27, 0.60, 0.27, 0.25, 0.23]
    ivs_msft = [0.50, 0.52, 0.10, 0.52, 0.50, 0.48]
    df = _iv_level_frame(tickers, exps, strikes, ivs_aapl + ivs_msft)
    result = add_iv_state_level(df)
    assert (result.loc[result["underlying_symbol"] == "AAPL", "iv_state_level"] == "HIGH").all()
    assert (result.loc[result["underlying_symbol"] == "MSFT", "iv_state_level"] == "LOW").all()


def test_add_iv_state_level_missing_required_column_returns_unknown():
    """Missing strike_distance_pct column should leave all rows UNKNOWN without crashing."""
    df = pd.DataFrame({
        "underlying_symbol": ["TSLA"] * 6,
        "expiration_date": ["2026-04-17"] * 6,
        "implied_volatility": [0.3] * 6,
        # strike_distance_pct absent
    })
    result = add_iv_state_level(df)
    assert (result["iv_state_level"] == "UNKNOWN").all()


# ---------------------------------------------------------------------------
# add_iv_state_term
# ---------------------------------------------------------------------------

def _iv_term_frame(tickers, expirations, ivs):
    """Build a minimal frame for add_iv_state_term tests."""
    return pd.DataFrame({
        "underlying_symbol": tickers,
        "expiration_date": expirations,
        "implied_volatility": ivs,
    })


def test_add_iv_state_term_rising():
    """Near-term IV at least 5% above far-term should produce RISING."""
    # near_iv=0.40, far_iv=0.30 → 0.40 >= 0.30*1.05=0.315 → RISING
    df = _iv_term_frame(
        ["TSLA"] * 4,
        ["2026-04-17", "2026-04-17", "2026-05-16", "2026-05-16"],
        [0.40, 0.40, 0.30, 0.30],
    )
    result = add_iv_state_term(df)
    assert (result["iv_state_term"] == "RISING").all()


def test_add_iv_state_term_falling():
    """Near-term IV at least 5% below far-term should produce FALLING."""
    # near_iv=0.25, far_iv=0.35 → 0.25 <= 0.35*0.95=0.3325 → FALLING
    df = _iv_term_frame(
        ["TSLA"] * 4,
        ["2026-04-17", "2026-04-17", "2026-05-16", "2026-05-16"],
        [0.25, 0.25, 0.35, 0.35],
    )
    result = add_iv_state_term(df)
    assert (result["iv_state_term"] == "FALLING").all()


def test_add_iv_state_term_flat():
    """Near-term IV within 5% of far-term should produce FLAT."""
    # near_iv=0.30, far_iv=0.31 → within ±5% band → FLAT
    df = _iv_term_frame(
        ["TSLA"] * 4,
        ["2026-04-17", "2026-04-17", "2026-05-16", "2026-05-16"],
        [0.30, 0.30, 0.31, 0.31],
    )
    result = add_iv_state_term(df)
    assert (result["iv_state_term"] == "FLAT").all()


def test_add_iv_state_term_unknown_when_single_expiration():
    """Single distinct expiration must produce UNKNOWN."""
    df = _iv_term_frame(["TSLA"] * 4, ["2026-04-17"] * 4, [0.30] * 4)
    result = add_iv_state_term(df)
    assert (result["iv_state_term"] == "UNKNOWN").all()


def test_add_iv_state_term_independent_per_underlying():
    """Different underlyings receive independent term classifications."""
    # AAPL: near=0.40, far=0.30 → RISING; MSFT: near=0.25, far=0.35 → FALLING
    tickers = ["AAPL"] * 4 + ["MSFT"] * 4
    exps = ["2026-04-17", "2026-04-17", "2026-05-16", "2026-05-16"] * 2
    ivs = [0.40, 0.40, 0.30, 0.30, 0.25, 0.25, 0.35, 0.35]
    df = _iv_term_frame(tickers, exps, ivs)
    result = add_iv_state_term(df)
    assert (result.loc[result["underlying_symbol"] == "AAPL", "iv_state_term"] == "RISING").all()
    assert (result.loc[result["underlying_symbol"] == "MSFT", "iv_state_term"] == "FALLING").all()


# ---------------------------------------------------------------------------
# add_listed_strike_increment
# ---------------------------------------------------------------------------

def _strike_frame(tickers, option_types, expirations, strikes):
    """Build a minimal frame for add_listed_strike_increment tests."""
    return pd.DataFrame({
        "underlying_symbol": tickers,
        "option_type": option_types,
        "expiration_date": expirations,
        "strike": strikes,
    })


def test_add_listed_strike_increment_basic():
    """Minimum adjacent difference across 3+ strikes should be broadcast."""
    # strikes 90, 95, 100, 105 → diffs are all 5 → increment = 5
    df = _strike_frame(
        ["TSLA"] * 4, ["call"] * 4, ["2026-04-17"] * 4, [90.0, 95.0, 100.0, 105.0]
    )
    result = add_listed_strike_increment(df)
    assert (result["listed_strike_increment"] == 5.0).all()


def test_add_listed_strike_increment_uses_minimum_diff():
    """When diffs are uneven the minimum positive difference is used."""
    # strikes 90, 95, 100, 102.5 → diffs 5, 5, 2.5 → increment = 2.5
    df = _strike_frame(
        ["TSLA"] * 4, ["call"] * 4, ["2026-04-17"] * 4, [90.0, 95.0, 100.0, 102.5]
    )
    result = add_listed_strike_increment(df)
    assert (result["listed_strike_increment"] == 2.5).all()


def test_add_listed_strike_increment_skips_expiration_with_fewer_than_3_rows():
    """First expiration with < 3 strikes should be skipped; second used instead."""
    tickers = ["TSLA"] * 6
    opt = ["call"] * 6
    exps = ["2026-04-17"] * 2 + ["2026-05-16"] * 4
    # near exp: only 2 strikes; far exp: 4 strikes with increment 5
    strikes = [95.0, 105.0, 90.0, 95.0, 100.0, 105.0]
    df = _strike_frame(tickers, opt, exps, strikes)
    result = add_listed_strike_increment(df)
    assert (result["listed_strike_increment"] == 5.0).all()


def test_add_listed_strike_increment_null_when_no_qualifying_expiration():
    """All rows should be NaN when no expiration has 3 or more distinct strikes."""
    df = _strike_frame(["TSLA"] * 2, ["call"] * 2, ["2026-04-17"] * 2, [95.0, 105.0])
    result = add_listed_strike_increment(df)
    assert result["listed_strike_increment"].isna().all()


def test_add_listed_strike_increment_independent_per_option_type():
    """Calls and puts receive their own increment values."""
    tickers = ["TSLA"] * 8
    opt = ["call"] * 4 + ["put"] * 4
    exps = ["2026-04-17"] * 8
    # calls: 90, 95, 100, 105 → 5; puts: 90, 92.5, 95, 97.5 → 2.5
    strikes = [90.0, 95.0, 100.0, 105.0, 90.0, 92.5, 95.0, 97.5]
    df = _strike_frame(tickers, opt, exps, strikes)
    result = add_listed_strike_increment(df)
    assert (result.loc[result["option_type"] == "call", "listed_strike_increment"] == 5.0).all()
    assert (result.loc[result["option_type"] == "put", "listed_strike_increment"] == 2.5).all()


# ---------------------------------------------------------------------------
# add_theta_efficiency_below_p25
# ---------------------------------------------------------------------------

def _theta_frame(tickers, option_types, theta_vals):
    """Build a minimal frame for add_theta_efficiency_below_p25 tests."""
    return pd.DataFrame({
        "underlying_symbol": tickers,
        "option_type": option_types,
        "theta_efficiency": theta_vals,
    })


def test_add_theta_efficiency_below_p25_flags_bottom_quartile():
    """Rows with theta_efficiency strictly below p25 should be True."""
    # 4 rows: values 1, 2, 3, 4 → p25=1.75 → only row with value 1 is below
    df = _theta_frame(["TSLA"] * 4, ["call"] * 4, [1.0, 2.0, 3.0, 4.0])
    result = add_theta_efficiency_below_p25(df)
    flags = result["theta_efficiency_below_p25"].tolist()
    assert bool(flags[0]) is True
    assert all(bool(f) is False for f in flags[1:])


def test_add_theta_efficiency_below_p25_all_equal_values():
    """When all values are equal no row is strictly below p25."""
    df = _theta_frame(["TSLA"] * 4, ["call"] * 4, [3.0, 3.0, 3.0, 3.0])
    result = add_theta_efficiency_below_p25(df)
    assert not result["theta_efficiency_below_p25"].any()


def test_add_theta_efficiency_below_p25_ignores_null_theta():
    """Rows with NaN theta_efficiency should remain NA, not affect percentile."""
    df = _theta_frame(["TSLA"] * 5, ["call"] * 5, [1.0, 2.0, np.nan, 3.0, 4.0])
    result = add_theta_efficiency_below_p25(df)
    assert pd.isna(result.loc[2, "theta_efficiency_below_p25"])
    # Non-null rows: 1, 2, 3, 4 → p25=1.75 → row 0 is True
    assert bool(result.loc[0, "theta_efficiency_below_p25"]) is True


def test_add_theta_efficiency_below_p25_independent_per_group():
    """Each (underlying, option_type) group has its own p25 threshold."""
    # TSLA calls: 1, 2, 3, 4 → p25=1.75; TSLA puts: 10, 20, 30, 40 → p25=17.5
    tickers = ["TSLA"] * 8
    opt = ["call"] * 4 + ["put"] * 4
    theta = [1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0]
    df = _theta_frame(tickers, opt, theta)
    result = add_theta_efficiency_below_p25(df)
    call_rows = result[result["option_type"] == "call"]
    put_rows = result[result["option_type"] == "put"]
    # Only first call row is below its group p25
    assert bool(call_rows.iloc[0]["theta_efficiency_below_p25"]) is True
    assert not call_rows.iloc[1:]["theta_efficiency_below_p25"].any()
    # Only first put row is below its group p25
    assert bool(put_rows.iloc[0]["theta_efficiency_below_p25"]) is True
    assert not put_rows.iloc[1:]["theta_efficiency_below_p25"].any()


def test_add_theta_efficiency_below_p25_missing_required_column():
    """Missing theta_efficiency column should not crash; all rows remain NA."""
    df = pd.DataFrame({"underlying_symbol": ["TSLA"], "option_type": ["call"]})
    result = add_theta_efficiency_below_p25(df)
    assert result["theta_efficiency_below_p25"].isna().all()

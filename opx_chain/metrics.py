"""Derived pricing, screening, and freshness metrics for option rows."""

import numpy as np
import pandas as pd

from opx_chain.config import get_runtime_config
from opx_chain.greeks import compute_greeks
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPES
from opx_chain.utils import finite_float, finite_numeric_series, is_finite_positive_number


DAYS_BUCKET_THRESHOLDS = (10.0, 18.0, 26.0)
DAYS_BUCKET_LABELS = ("Week_1", "Week_2", "Week_3", "Week_4")
UNKNOWN_DAYS_BUCKET = "UNKNOWN"


def classify_days_to_expiration_bucket(days_to_expiration):
    """Bucket expirations into coarse week ranges for quick filtering."""
    days = finite_float(days_to_expiration)
    if not np.isfinite(days):
        return UNKNOWN_DAYS_BUCKET
    for threshold, label in zip(DAYS_BUCKET_THRESHOLDS, DAYS_BUCKET_LABELS):
        if days <= threshold:
            return label
    return DAYS_BUCKET_LABELS[-1]


def _compute_days_bucket(days_to_expiration):
    """Vectorized version of classify_days_to_expiration_bucket."""
    days = _finite_numeric_series(days_to_expiration)
    raw = np.select(
        [days <= threshold for threshold in DAYS_BUCKET_THRESHOLDS],
        DAYS_BUCKET_LABELS[:-1],
        default=DAYS_BUCKET_LABELS[-1],
    )
    return np.where(days.notna(), raw, UNKNOWN_DAYS_BUCKET)


def _clip_zero_to_one(values):
    """Clamp numeric arrays to the inclusive [0, 1] score range."""
    return np.clip(values, 0.0, 1.0)


def _finite_numeric_series(values):
    """Return numeric values with non-finite entries masked as NaN."""
    return finite_numeric_series(values)


def _compute_spread_score(spread_pct):
    """Score execution quality from spread percent using prompt-aligned tiers."""
    spread_pct = _finite_numeric_series(spread_pct)
    raw = np.select(
        [
            spread_pct < 0.10,
            spread_pct <= 0.15,
            spread_pct <= 0.25,
        ],
        [
            100.0,
            85.0,
            np.maximum(0.0, 85.0 * (1 - ((spread_pct - 0.15) / 0.10))),
        ],
        default=0.0,
    )
    return np.where(spread_pct.notna(), raw, np.nan)


def _compute_dte_score(days_to_expiration):
    """Apply the prompt's tiered DTE preference."""
    days_to_expiration = _finite_numeric_series(days_to_expiration)
    raw = np.select(
        [
            days_to_expiration < 5,
            days_to_expiration < 7,
            days_to_expiration <= 21,
            days_to_expiration <= 35,
            days_to_expiration <= 45,
        ],
        [
            25.0,
            75.0,
            100.0,
            85.0,
            65.0,
        ],
        default=30.0,
    )
    return np.where(days_to_expiration.notna(), raw, np.nan)


def _compute_income_score(iv_adjusted_premium_per_day):
    """Score IV-adjusted premium-per-day with a floor and hard cap."""
    iv_adjusted_premium_per_day = _finite_numeric_series(iv_adjusted_premium_per_day)
    min_useful_premium_per_day = 0.01
    max_premium_per_day = 0.05
    raw = _clip_zero_to_one(
        (iv_adjusted_premium_per_day - min_useful_premium_per_day)
        / (max_premium_per_day - min_useful_premium_per_day)
    )
    return raw.where(iv_adjusted_premium_per_day.notna())


def _compute_theta_efficiency_score(theta_efficiency):
    """Normalize theta efficiency into a bounded score."""
    theta_efficiency = _finite_numeric_series(theta_efficiency)
    return _clip_zero_to_one(theta_efficiency / 15.0).where(theta_efficiency.notna())


def _series_finite_positive(series):
    """Return True only for finite numeric values greater than zero."""
    numeric = _finite_numeric_series(series)
    return numeric.notna() & np.isfinite(numeric) & (numeric > 0)


def _series_finite_nonnegative(series):
    """Return True only for finite numeric values greater than or equal to zero."""
    numeric = _finite_numeric_series(series)
    return numeric.notna() & np.isfinite(numeric) & (numeric >= 0)


def _compute_risk_level(df):
    """Classify row-level risk using delta as the score driver and ITM probability as validation."""
    delta_abs = _finite_numeric_series(df["delta_abs"])
    probability_itm = _finite_numeric_series(df["probability_itm"])
    return np.select(
        [
            (delta_abs < 0.30) & (probability_itm < 0.25),
            ((delta_abs >= 0.30) & (delta_abs <= 0.40))
            | ((probability_itm >= 0.25) & (probability_itm <= 0.35)),
            (delta_abs > 0.40) | (probability_itm > 0.35),
        ],
        ["LOW", "MODERATE", "HIGH"],
        default="UNKNOWN",
    )


def _compute_risk_score(delta_abs):
    """Use delta alone as the score-driving risk input."""
    delta_abs = _finite_numeric_series(delta_abs)
    raw = np.select(
        [
            delta_abs < 0.30,
            delta_abs <= 0.40,
        ],
        [
            1.0,
            0.75,
        ],
        default=0.35,
    )
    return np.where(delta_abs.notna(), raw, np.nan)


def _compute_score_validation(option_score, income_score, spread_score):
    """Assign row-level score validation labels from income and liquidity alignment."""
    return np.select(
        [
            (option_score >= 70.0) & ((income_score < 0.35) | (spread_score < 50.0)),
            (option_score < 50.0) & (income_score >= 0.60) & (spread_score >= 70.0),
        ],
        ["DISCREPANCY", "UNDERVALUED"],
        default="ALIGNED",
    )


def add_option_score(df):
    """Add a shared 0-100 option score built from income, liquidity, risk, and efficiency."""
    config = get_runtime_config()
    total_weight = (
        config.option_score_income_weight
        + config.option_score_liquidity_weight
        + config.option_score_risk_weight
        + config.option_score_efficiency_weight
    )
    if total_weight <= 0:
        df["option_score"] = np.nan
        df["score_validation"] = np.nan
        df["score_adjustment"] = np.nan
        df["final_score"] = np.nan
        return df

    numeric_inputs = {
        field: _finite_numeric_series(df[field])
        for field in (
            "premium_per_day",
            "bid",
            "ask",
            "open_interest",
            "volume",
            "delta_abs",
            "probability_itm",
            "days_to_expiration",
            "strike",
            "underlying_price",
            "iv_adjusted_premium_per_day",
            "spread_score",
            "dte_score",
            "theta_efficiency",
        )
    }
    income_score = _compute_income_score(numeric_inputs["iv_adjusted_premium_per_day"])
    spread_score_norm = _clip_zero_to_one(numeric_inputs["spread_score"] / 100.0)
    dte_score_norm = _clip_zero_to_one(numeric_inputs["dte_score"] / 100.0)
    risk_score = _compute_risk_score(numeric_inputs["delta_abs"])
    theta_efficiency_score = _compute_theta_efficiency_score(numeric_inputs["theta_efficiency"])
    efficiency_score = (dte_score_norm * 0.5) + (theta_efficiency_score * 0.5)

    weighted_score = (
        income_score * config.option_score_income_weight
        + spread_score_norm * config.option_score_liquidity_weight
        + risk_score * config.option_score_risk_weight
        + efficiency_score * config.option_score_efficiency_weight
    ) / total_weight

    required = (
        numeric_inputs["premium_per_day"].notna()
        & numeric_inputs["bid"].notna()
        & numeric_inputs["ask"].notna()
        & numeric_inputs["open_interest"].notna()
        & numeric_inputs["volume"].notna()
        & numeric_inputs["delta_abs"].notna()
        & numeric_inputs["probability_itm"].notna()
        & numeric_inputs["days_to_expiration"].notna()
        & numeric_inputs["strike"].notna()
        & numeric_inputs["underlying_price"].notna()
        & numeric_inputs["iv_adjusted_premium_per_day"].notna()
        & numeric_inputs["spread_score"].notna()
        & numeric_inputs["dte_score"].notna()
        & numeric_inputs["theta_efficiency"].notna()
        & df["option_type"].isin(OPTION_TYPES)
    )
    df["option_score"] = np.where(required, _clip_zero_to_one(weighted_score) * 100, np.nan)
    validation_values = _compute_score_validation(
        df["option_score"], income_score, df["spread_score"]
    )
    df["score_validation"] = pd.Series(validation_values, index=df.index, dtype="object")
    df.loc[~required, "score_validation"] = np.nan
    df["score_adjustment"] = np.select(
        [
            df["score_validation"] == "DISCREPANCY",
            df["score_validation"] == "UNDERVALUED",
        ],
        [-10.0, 5.0],
        default=0.0,
    )
    df["final_score"] = np.where(
        required,
        np.clip(df["option_score"] + df["score_adjustment"], 0.0, 100.0),
        np.nan,
    )
    return df


def add_quote_quality_metrics(df, underlying_price):
    """Add quote validation and basic liquidity quality fields."""
    for column in ("strike", "bid", "ask", "volume", "open_interest", "implied_volatility"):
        if column in df.columns:
            df[column] = _finite_numeric_series(df[column])
    df["has_valid_underlying"] = is_finite_positive_number(underlying_price)
    df["has_valid_strike"] = _series_finite_positive(df["strike"])
    df["bid_le_ask"] = df["bid"] <= df["ask"]
    df["has_nonzero_bid"] = _series_finite_positive(df["bid"])
    df["has_nonzero_ask"] = _series_finite_positive(df["ask"])
    has_valid_bid = _series_finite_nonnegative(df["bid"])
    has_valid_ask = _series_finite_nonnegative(df["ask"])
    df["has_crossed_or_locked_market"] = (
        df["bid"].notna() & df["ask"].notna() & (df["bid"] >= df["ask"])
    )
    df["has_valid_quote"] = (
        has_valid_bid
        & has_valid_ask
        & (df["bid"] >= 0)
        & (df["ask"] >= 0)
        & df["bid_le_ask"]
    )
    df["has_valid_iv"] = _series_finite_positive(df["implied_volatility"])

    df["mark_price_mid"] = np.where(df["has_valid_quote"], (df["bid"] + df["ask"]) / 2, np.nan)
    df["bid_ask_spread"] = np.where(df["has_valid_quote"], df["ask"] - df["bid"], np.nan)
    df["bid_ask_spread_pct_of_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["bid_ask_spread"] / df["mark_price_mid"],
        np.nan,
    )
    df["spread_to_strike_pct"] = np.where(
        df["has_valid_strike"],
        df["bid_ask_spread"] / df["strike"],
        np.nan,
    )
    df["spread_to_bid_pct"] = np.where(
        df["has_nonzero_bid"],
        df["bid_ask_spread"] / df["bid"],
        np.nan,
    )
    df["oi_to_volume_ratio"] = np.where(
        df["volume"] > 0,
        df["open_interest"] / df["volume"],
        np.nan,
    )

    return df


def add_derived_pricing_metrics(df, underlying_price):
    """Add premium, moneyness, break-even, and Black-Scholes-derived fields."""
    config = get_runtime_config()
    spot_price = finite_float(underlying_price)
    has_valid_spot = spot_price > 0
    df["strike_minus_spot"] = np.where(
        has_valid_spot,
        df["strike"] - spot_price,
        np.nan,
    )
    df["strike_vs_spot_pct"] = np.where(
        has_valid_spot,
        df["strike_minus_spot"] / spot_price,
        np.nan,
    )
    df["strike_distance_pct"] = np.abs(df["strike_vs_spot_pct"])

    call_itm_amount = np.where(
        has_valid_spot,
        np.maximum(spot_price - df["strike"], 0),
        np.nan,
    )
    put_itm_amount = np.where(
        has_valid_spot,
        np.maximum(df["strike"] - spot_price, 0),
        np.nan,
    )
    df["itm_amount"] = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        call_itm_amount,
        put_itm_amount,
    )
    df["otm_pct"] = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        np.maximum(df["strike_vs_spot_pct"], 0),
        np.maximum(-df["strike_vs_spot_pct"], 0),
    )

    df["intrinsic_value"] = df["itm_amount"]
    df["extrinsic_value_bid"] = df["bid"] - df["intrinsic_value"]
    df["extrinsic_value_mid"] = df["mark_price_mid"] - df["intrinsic_value"]
    df["extrinsic_value_ask"] = df["ask"] - df["intrinsic_value"]
    df["extrinsic_pct_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["extrinsic_value_mid"] / df["mark_price_mid"],
        np.nan,
    )
    df["has_negative_extrinsic_mid"] = df["extrinsic_value_mid"] < 0

    df["premium_reference_price"] = (
        df["mark_price_mid"].fillna(df["bid"]).fillna(df["last_trade_price"])
    )
    df["premium_reference_method"] = np.select(
        [
            df["mark_price_mid"].notna(),
            df["bid"].notna(),
            df["last_trade_price"].notna(),
        ],
        ["mid", "bid", "last_trade_price"],
        default="unavailable",
    )

    df["premium_to_strike"] = np.where(
        df["strike"] > 0,
        df["premium_reference_price"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_bid"] = np.where(
        df["strike"] > 0,
        df["bid"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_annualized"] = np.where(
        df["time_to_expiration_years"] > 0,
        df["premium_to_strike"] / df["time_to_expiration_years"],
        np.nan,
    )
    df["expected_fill_price"] = np.where(
        df["bid_ask_spread_pct_of_mid"] <= 0.10,
        df["mark_price_mid"],
        df["bid"] + (0.25 * df["bid_ask_spread"]),
    )
    df["premium_per_day"] = np.where(
        df["expected_fill_price"].notna(),
        df["expected_fill_price"] / np.maximum(df["days_to_expiration"], 1),
        np.nan,
    )
    df["iv_adjusted_premium_per_day"] = np.where(
        df["implied_volatility"] > 0,
        df["premium_per_day"] * (df["implied_volatility"] / 0.30),
        np.nan,
    )
    otm_amount = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        np.where(has_valid_spot, np.maximum(df["strike"] - spot_price, 0), np.nan),
        np.where(has_valid_spot, np.maximum(spot_price - df["strike"], 0), np.nan),
    )
    margin_floor = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        np.nan if not has_valid_spot else 0.10 * spot_price,
        0.10 * df["strike"],
    )
    df["estimated_margin_requirement"] = df["premium_reference_price"] + np.maximum(
        0.20 * spot_price - otm_amount,
        margin_floor,
    )
    df["return_on_margin"] = np.where(
        df["estimated_margin_requirement"] > 0,
        df["premium_reference_price"] / df["estimated_margin_requirement"],
        np.nan,
    )
    df["return_on_margin_annualized"] = np.where(
        df["time_to_expiration_years"] > 0,
        df["return_on_margin"] / df["time_to_expiration_years"],
        np.nan,
    )

    df = compute_greeks(df, underlying_price, config.risk_free_rate)

    df["theta_to_premium_ratio"] = np.where(
        df["premium_reference_price"] > 0,
        np.abs(df["theta"]) / df["premium_reference_price"],
        np.nan,
    )
    df["theta_dollars_per_day"] = np.abs(df["theta"]) * 100
    call_capital_price = (
        df["last_trade_price"]
        .combine_first(df["expected_fill_price"])
        .combine_first(df["mark_price_mid"])
    )
    df["capital_required"] = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        call_capital_price * 100,
        df["strike"] * 100,
    )
    df["theta_efficiency"] = np.where(
        df["capital_required"] > 0,
        df["theta_dollars_per_day"] / (df["capital_required"] / 1000.0),
        np.nan,
    )
    df["vega_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["vega"] / df["days_to_expiration"],
        np.nan,
    )
    df["break_even_if_short"] = np.where(
        df["option_type"] == OPTION_TYPE_CALL,
        df["strike"] + df["premium_reference_price"],
        df["strike"] - df["premium_reference_price"],
    )

    return df


def add_event_risk_flags(df):
    """Add earnings/dividend proximity flags and a composite event risk score."""
    blank = pd.Series(np.nan, index=df.index)
    dte = (
        _finite_numeric_series(df["days_to_earnings"])
        if "days_to_earnings" in df.columns
        else blank
    )
    dtd = _finite_numeric_series(df["days_to_ex_div"]) if "days_to_ex_div" in df.columns else blank
    row_dte = (
        _finite_numeric_series(df["days_to_expiration"])
        if "days_to_expiration" in df.columns
        else blank
    )

    spans_earnings = dte.notna() & ((row_dte.isna()) | ((dte >= 0) & (dte <= row_dte)))
    spans_ex_div = dtd.notna() & ((row_dte.isna()) | ((dtd >= 0) & (dtd <= row_dte)))

    df["earnings_within_5d"] = np.where(spans_earnings, dte <= 5, None)
    df["earnings_within_10d"] = np.where(spans_earnings, dte <= 10, None)
    df["ex_div_within_3d"] = np.where(spans_ex_div, dtd <= 3, None)

    earnings_pts = np.where(
        spans_earnings,
        np.select([dte <= 5, dte <= 10], [60.0, 30.0], default=0.0),
        np.nan,
    )
    div_pts = np.where(
        spans_ex_div,
        np.select([dtd <= 3, dtd <= 7], [40.0, 20.0], default=0.0),
        np.nan,
    )
    has_either = spans_earnings | spans_ex_div
    e_contrib = np.where(spans_earnings, earnings_pts, 0.0)
    d_contrib = np.where(spans_ex_div, div_pts, 0.0)
    df["event_risk_score"] = np.where(
        has_either,
        np.minimum(e_contrib + d_contrib, 100.0),
        np.nan,
    )
    return df


def add_screening_and_freshness_flags(df, fetched_at):
    """Mark stale quotes and tradability flags used by the viewer and screens."""
    config = get_runtime_config()
    df["quote_age_seconds"] = (fetched_at - df["option_quote_time"]).dt.total_seconds()
    df["is_stale_quote"] = np.where(
        df["quote_age_seconds"].notna(),
        (df["quote_age_seconds"] < 0)
        | (df["quote_age_seconds"] > config.stale_quote_seconds),
        None,
    )
    df["days_bucket"] = _compute_days_bucket(df["days_to_expiration"])
    df["near_expiry_near_money_flag"] = (
        (df["days_to_expiration"] <= 14) & (df["strike_distance_pct"] <= 0.03)
    )
    df["spread_score"] = _compute_spread_score(df["bid_ask_spread_pct_of_mid"])
    df["dte_score"] = _compute_dte_score(df["days_to_expiration"])
    df["risk_level"] = _compute_risk_level(df)
    # Greek calculation owns the like-for-like model comparison. Standalone
    # freshness assessment cannot infer it from a probability proxy.
    if "risk_model_inconsistent" not in df:
        df["risk_model_inconsistent"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    else:
        df["risk_model_inconsistent"] = df["risk_model_inconsistent"].astype("boolean")
    df["is_wide_market"] = (
        df["bid_ask_spread_pct_of_mid"] > config.max_spread_pct_of_mid
    )
    bid_screen = True if config.min_bid is None else (df["bid"] >= config.min_bid)
    df["passes_primary_screen"] = (
        bid_screen
        & (df["bid_ask_spread_pct_of_mid"] <= config.max_spread_pct_of_mid)
        & (df["open_interest"] >= config.min_open_interest)
        & (df["volume"] >= config.min_volume)
    )
    df["quote_quality_score"] = (
        df["has_valid_quote"].astype(int)
        + df["has_nonzero_bid"].astype(int)
        + df["has_nonzero_ask"].astype(int)
        + df["has_valid_iv"].astype(int)
        + df["has_valid_greeks"].astype(int)
        + (~df["has_crossed_or_locked_market"]).astype(int)
        + df["is_stale_quote"].fillna(False).eq(False).astype(int)
    )
    df = add_option_score(df)
    df = add_event_risk_flags(df)

    return df


def add_expected_move_by_expiration(df):
    """Add one expected-move estimate per underlying and expiration."""
    df = df.copy()
    for column in [
        "expected_move",
        "expected_move_pct",
        "expected_move_lower_bound",
        "expected_move_upper_bound",
    ]:
        df[column] = np.nan

    valid = (
        _series_finite_positive(df["underlying_price"])
        & _series_finite_positive(df["time_to_expiration_years"])
        & _series_finite_positive(df["implied_volatility"])
        & _series_finite_nonnegative(df["strike_distance_pct"])
    )
    if not valid.any():
        return df

    keys = ["underlying_symbol", "expiration_date"]
    atm_candidates = df.loc[valid].copy()
    grouped_distance = atm_candidates.groupby(keys)["strike_distance_pct"]
    atm_candidates["min_strike_distance_pct"] = grouped_distance.transform("min")
    atm_candidates = atm_candidates[
        np.isclose(
            atm_candidates["strike_distance_pct"],
            atm_candidates["min_strike_distance_pct"],
            equal_nan=False,
        )
    ]

    per_expiration = (
        atm_candidates.groupby(keys, as_index=False)
        .agg(
            underlying_price=("underlying_price", "first"),
            time_to_expiration_years=("time_to_expiration_years", "first"),
            expected_move_iv=("implied_volatility", "mean"),
        )
    )
    per_expiration["expected_move"] = (
        per_expiration["underlying_price"]
        * per_expiration["expected_move_iv"]
        * np.sqrt(per_expiration["time_to_expiration_years"])
    )
    per_expiration["expected_move_pct"] = (
        per_expiration["expected_move"] / per_expiration["underlying_price"]
    )
    per_expiration["expected_move_lower_bound"] = (
        per_expiration["underlying_price"] - per_expiration["expected_move"]
    )
    per_expiration["expected_move_upper_bound"] = (
        per_expiration["underlying_price"] + per_expiration["expected_move"]
    )

    return df.merge(
        per_expiration[
            keys
            + [
                "expected_move",
                "expected_move_pct",
                "expected_move_lower_bound",
                "expected_move_upper_bound",
            ]
        ],
        on=keys,
        how="left",
        suffixes=("", "_computed"),
    ).assign(
        expected_move=lambda frame: frame["expected_move_computed"].combine_first(
            frame["expected_move"]
        ),
        expected_move_pct=lambda frame: frame["expected_move_pct_computed"].combine_first(
            frame["expected_move_pct"]
        ),
        expected_move_lower_bound=lambda frame: (
            frame["expected_move_lower_bound_computed"].combine_first(
                frame["expected_move_lower_bound"]
            )
        ),
        expected_move_upper_bound=lambda frame: (
            frame["expected_move_upper_bound_computed"].combine_first(
                frame["expected_move_upper_bound"]
            )
        ),
    ).drop(
        columns=[
            "expected_move_computed",
            "expected_move_pct_computed",
            "expected_move_lower_bound_computed",
            "expected_move_upper_bound_computed",
        ]
    )


def add_iv_state_level(df):
    """Classify IV level per underlying as LOW/NEUTRAL/HIGH/UNKNOWN; broadcast to all rows.

    Must run pre-filter so the full IV distribution is used for percentile ranking.
    """
    df = df.copy()
    df["iv_state_level"] = "UNKNOWN"
    required = {"underlying_symbol", "implied_volatility", "expiration_date", "strike_distance_pct"}
    if not required.issubset(df.columns):
        return df

    for _, group in df.groupby("underlying_symbol"):
        valid_iv = _series_finite_positive(group["implied_volatility"])
        iv_vals = group.loc[valid_iv, "implied_volatility"]
        if len(iv_vals) < 5:
            continue

        p30 = iv_vals.quantile(0.30)
        p70 = iv_vals.quantile(0.70)

        # Use ATM row at nearest expiration as the representative IV.
        rep_iv = None
        for exp in sorted(group["expiration_date"].unique()):
            exp_rows = group[group["expiration_date"] == exp]
            candidates = exp_rows[
                _series_finite_positive(exp_rows["implied_volatility"])
                & _series_finite_nonnegative(exp_rows["strike_distance_pct"])
            ]
            if candidates.empty:
                continue
            atm_idx = candidates["strike_distance_pct"].idxmin()
            rep_iv = candidates.loc[atm_idx, "implied_volatility"]
            break

        if rep_iv is None or pd.isna(rep_iv) or not np.isfinite(rep_iv):
            rep_iv = iv_vals.median()
        if pd.isna(rep_iv) or not np.isfinite(rep_iv):
            continue

        if rep_iv >= p70:
            level = "HIGH"
        elif rep_iv <= p30:
            level = "LOW"
        else:
            level = "NEUTRAL"

        df.loc[group.index, "iv_state_level"] = level

    return df


def add_iv_state_term(df):
    """Classify IV term structure per underlying as RISING/FALLING/FLAT/UNKNOWN; broadcast.

    Compares median IV at the nearest expiration to the next expiration.
    Must run pre-filter so far-dated rows are not dropped before the comparison.
    """
    df = df.copy()
    df["iv_state_term"] = "UNKNOWN"
    required = {"underlying_symbol", "implied_volatility", "expiration_date"}
    if not required.issubset(df.columns):
        return df

    for _, group in df.groupby("underlying_symbol"):
        valid_rows = group[_series_finite_positive(group["implied_volatility"])]
        if valid_rows.empty:
            continue

        by_exp = valid_rows.groupby("expiration_date")["implied_volatility"].median()
        exps = sorted(by_exp.index)
        if len(exps) < 2:
            continue

        near_iv = by_exp[exps[0]]
        far_iv = by_exp[exps[1]]
        if (
            pd.isna(near_iv)
            or pd.isna(far_iv)
            or not np.isfinite(near_iv)
            or not np.isfinite(far_iv)
            or far_iv <= 0
        ):
            continue

        if near_iv >= far_iv * 1.05:
            term = "RISING"
        elif near_iv <= far_iv * 0.95:
            term = "FALLING"
        else:
            term = "FLAT"

        df.loc[group.index, "iv_state_term"] = term

    return df


def add_listed_strike_increment(df):
    """Derive the minimum adjacent strike step per (underlying, option_type); broadcast.

    Uses the nearest expiration with at least 3 rows. Must run pre-filter so adjacent
    strikes outside the distance band are still present when the increment is computed.
    """
    df = df.copy()
    df["listed_strike_increment"] = np.nan
    required = {"underlying_symbol", "option_type", "expiration_date", "strike"}
    if not required.issubset(df.columns):
        return df

    for _, group in df.groupby(["underlying_symbol", "option_type"]):
        valid_rows = group[group["strike"].notna() & (group["strike"] > 0)]
        if valid_rows.empty:
            continue

        increment = None
        for exp in sorted(valid_rows["expiration_date"].unique()):
            exp_rows = valid_rows[valid_rows["expiration_date"] == exp]
            strikes = np.sort(exp_rows["strike"].unique())
            if len(strikes) < 3:
                continue
            pos_diffs = np.diff(strikes)
            pos_diffs = pos_diffs[pos_diffs > 0]
            if pos_diffs.size == 0:
                continue
            min_diff = float(np.min(pos_diffs))
            if min_diff > 0:
                increment = min_diff
                break

        if increment is not None:
            df.loc[group.index, "listed_strike_increment"] = increment

    return df


def add_theta_efficiency_below_p25(df):
    """Flag rows below the 25th percentile of theta_efficiency within (underlying, option_type).

    Must run post-filter so untradeable rows do not distort the percentile distribution.
    """
    df = df.copy()
    df["theta_efficiency_below_p25"] = pd.array([pd.NA] * len(df), dtype="boolean")
    required = {"underlying_symbol", "option_type", "theta_efficiency"}
    if not required.issubset(df.columns):
        return df

    for (_, _), group in df.groupby(["underlying_symbol", "option_type"]):
        valid_rows = group[group["theta_efficiency"].notna()]
        if valid_rows.empty:
            continue
        p25 = valid_rows["theta_efficiency"].quantile(0.25)
        df.loc[valid_rows.index, "theta_efficiency_below_p25"] = (
            valid_rows["theta_efficiency"] < p25
        )

    return df

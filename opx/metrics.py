"""Derived pricing, screening, and freshness metrics for option rows."""

import numpy as np

from opx.config import get_runtime_config
from opx.greeks import compute_greeks


def classify_days_to_expiration_bucket(days_to_expiration):
    """Bucket expirations into coarse week ranges for quick filtering."""
    if days_to_expiration <= 10:
        return "Week_1"
    if days_to_expiration <= 18:
        return "Week_2"
    if days_to_expiration <= 26:
        return "Week_3"
    return "Week_4"


def _clip_zero_to_one(values):
    """Clamp numeric arrays to the inclusive [0, 1] score range."""
    return np.clip(values, 0.0, 1.0)


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
        return df

    min_useful_premium_per_day = 0.01
    max_premium_per_day = 0.05
    income_score = _clip_zero_to_one(
        (df["premium_per_day"] - min_useful_premium_per_day)
        / (max_premium_per_day - min_useful_premium_per_day)
    )
    spread_score = _clip_zero_to_one(1 - (df["bid_ask_spread_pct_of_mid"] / 0.25))
    oi_score = _clip_zero_to_one(df["open_interest"] / 1000.0)
    volume_score = _clip_zero_to_one(df["volume"] / 100.0)
    liquidity_score = spread_score * 0.5 + oi_score * 0.3 + volume_score * 0.2

    target_delta = np.where(df["option_type"] == "call", 0.25, 0.20)
    risk_score = _clip_zero_to_one(1 - (np.abs(df["delta_abs"] - target_delta) / target_delta))

    dte_score = _clip_zero_to_one(1 - (df["days_to_expiration"] / 35.0))
    distance_score = _clip_zero_to_one(1 - (df["strike_distance_pct"] / 0.30))
    efficiency_score = dte_score * 0.5 + distance_score * 0.5

    weighted_score = (
        income_score * config.option_score_income_weight
        + liquidity_score * config.option_score_liquidity_weight
        + risk_score * config.option_score_risk_weight
        + efficiency_score * config.option_score_efficiency_weight
    ) / total_weight

    required = (
        df["premium_per_day"].notna()
        & df["bid"].notna()
        & df["ask"].notna()
        & df["open_interest"].notna()
        & df["volume"].notna()
        & df["delta_abs"].notna()
        & df["days_to_expiration"].notna()
        & df["strike"].notna()
        & df["underlying_price"].notna()
        & df["strike_distance_pct"].notna()
        & df["option_type"].isin(["call", "put"])
    )
    df["option_score"] = np.where(required, _clip_zero_to_one(weighted_score) * 100, np.nan)
    return df


def add_quote_quality_metrics(df, underlying_price):
    """Add quote validation and basic liquidity quality fields."""
    df["has_valid_underlying"] = underlying_price > 0
    df["has_valid_strike"] = df["strike"] > 0
    df["bid_le_ask"] = df["bid"] <= df["ask"]
    df["has_nonzero_bid"] = df["bid"] > 0
    df["has_nonzero_ask"] = df["ask"] > 0
    df["has_crossed_or_locked_market"] = (
        df["bid"].notna() & df["ask"].notna() & (df["bid"] >= df["ask"])
    )
    df["has_valid_quote"] = (
        df["bid"].notna()
        & df["ask"].notna()
        & (df["bid"] >= 0)
        & (df["ask"] >= 0)
        & df["bid_le_ask"]
    )
    df["has_valid_iv"] = df["implied_volatility"] > 0

    df["mark_price_mid"] = np.where(df["has_valid_quote"], (df["bid"] + df["ask"]) / 2, np.nan)
    df["bid_ask_spread"] = np.where(df["has_valid_quote"], df["ask"] - df["bid"], np.nan)
    df["bid_ask_spread_pct_of_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["bid_ask_spread"] / df["mark_price_mid"],
        np.nan,
    )
    df["spread_to_strike_pct"] = np.where(
        df["strike"] > 0,
        df["bid_ask_spread"] / df["strike"],
        np.nan,
    )
    df["spread_to_bid_pct"] = np.where(
        df["bid"] > 0,
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
    df["strike_minus_spot"] = df["strike"] - underlying_price
    df["strike_vs_spot_pct"] = np.where(
        underlying_price > 0,
        df["strike_minus_spot"] / underlying_price,
        np.nan,
    )
    df["strike_distance_pct"] = np.abs(df["strike_vs_spot_pct"])

    call_itm_amount = np.maximum(underlying_price - df["strike"], 0)
    put_itm_amount = np.maximum(df["strike"] - underlying_price, 0)
    df["itm_amount"] = np.where(df["option_type"] == "call", call_itm_amount, put_itm_amount)
    df["otm_pct"] = np.where(
        df["option_type"] == "call",
        np.maximum(df["strike"] - underlying_price, 0) / underlying_price,
        np.maximum(underlying_price - df["strike"], 0) / underlying_price,
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
    df["premium_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["premium_reference_price"] / df["days_to_expiration"],
        np.nan,
    )
    otm_amount = np.where(
        df["option_type"] == "call",
        np.maximum(df["strike"] - underlying_price, 0),
        np.maximum(underlying_price - df["strike"], 0),
    )
    margin_floor = np.where(
        df["option_type"] == "call",
        0.10 * underlying_price,
        0.10 * df["strike"],
    )
    df["estimated_margin_requirement"] = df["premium_reference_price"] + np.maximum(
        0.20 * underlying_price - otm_amount,
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
    df["vega_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["vega"] / df["days_to_expiration"],
        np.nan,
    )
    df["break_even_if_short"] = np.where(
        df["option_type"] == "call",
        df["strike"] + df["premium_reference_price"],
        df["strike"] - df["premium_reference_price"],
    )

    return df


def add_screening_and_freshness_flags(df, fetched_at):
    """Mark stale quotes and tradability flags used by the viewer and screens."""
    config = get_runtime_config()
    df["quote_age_seconds"] = (fetched_at - df["option_quote_time"]).dt.total_seconds()
    df["is_stale_quote"] = np.where(
        df["quote_age_seconds"].notna(),
        df["quote_age_seconds"] > config.stale_quote_seconds,
        None,
    )
    df["days_bucket"] = df["days_to_expiration"].apply(classify_days_to_expiration_bucket)
    df["near_expiry_near_money_flag"] = (
        (df["days_to_expiration"] <= 14) & (df["strike_distance_pct"] <= 0.03)
    )
    df["is_wide_market"] = (
        df["bid_ask_spread_pct_of_mid"] > config.max_spread_pct_of_mid
    )
    df["passes_primary_screen"] = (
        (df["bid"] >= config.min_bid)
        & (df["bid_ask_spread_pct_of_mid"] <= config.max_spread_pct_of_mid)
        & (df["open_interest"] > config.min_open_interest)
        & (df["volume"] > config.min_volume)
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
        df["underlying_price"].notna()
        & (df["underlying_price"] > 0)
        & df["time_to_expiration_years"].notna()
        & (df["time_to_expiration_years"] > 0)
        & df["implied_volatility"].notna()
        & (df["implied_volatility"] > 0)
        & df["strike_distance_pct"].notna()
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


def add_roll_yield_metrics(df):
    """Compare each expiry to the nearest earlier expiry at the same strike and side."""
    df = df.copy()
    group_keys = ["underlying_symbol", "option_type", "strike"]
    ordered = df.sort_values(group_keys + ["days_to_expiration", "expiration_date"]).copy()

    grouped = ordered.groupby(group_keys)
    ordered["roll_from_expiration_date"] = grouped["expiration_date"].shift(1)
    ordered["roll_from_days_to_expiration"] = grouped["days_to_expiration"].shift(1)
    ordered["roll_from_premium_reference_price"] = grouped["premium_reference_price"].shift(1)
    ordered["roll_days_added"] = (
        ordered["days_to_expiration"] - ordered["roll_from_days_to_expiration"]
    )
    ordered["roll_net_credit"] = (
        ordered["premium_reference_price"] - ordered["roll_from_premium_reference_price"]
    )
    ordered["roll_yield"] = np.where(
        ordered["roll_days_added"] > 0,
        ordered["roll_net_credit"] / ordered["roll_days_added"],
        np.nan,
    )

    ordered.loc[
        ordered["roll_from_premium_reference_price"].isna() | (ordered["roll_days_added"] <= 0),
        ["roll_days_added", "roll_net_credit", "roll_yield"],
    ] = np.nan

    return ordered.drop(columns=["roll_from_days_to_expiration"]).sort_index()

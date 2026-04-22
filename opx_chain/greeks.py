"""Black-Scholes greek and ITM-probability calculations for option rows."""

import numpy as np
import pandas as pd
from scipy.stats import norm


def _merge_provider_and_derived(existing, derived):
    """Keep provider-native values when present and fill gaps with derived ones."""
    return existing.where(existing.notna(), derived) if existing is not None else derived


def compute_greeks(  # pylint: disable=too-many-locals
    df,
    underlying_price,
    risk_free_rate,
):
    """Compute Black-Scholes Greeks and ITM probabilities for valid rows."""
    strike = df["strike"].to_numpy(dtype=float)
    time_to_expiration = df["time_to_expiration_years"].to_numpy(dtype=float)
    sigma = df["implied_volatility"].replace(0, np.nan).fillna(0.3).to_numpy(dtype=float)

    valid = (underlying_price > 0) & (strike > 0) & (time_to_expiration > 0) & (sigma > 0)

    d1 = np.full(len(df), np.nan)
    d2 = np.full(len(df), np.nan)

    d1[valid] = (
        np.log(underlying_price / strike[valid])
        + (risk_free_rate + 0.5 * sigma[valid] ** 2) * time_to_expiration[valid]
    ) / (sigma[valid] * np.sqrt(time_to_expiration[valid]))
    d2[valid] = d1[valid] - sigma[valid] * np.sqrt(time_to_expiration[valid])

    pdf_d1 = norm.pdf(d1)
    cdf_d1 = norm.cdf(d1)
    cdf_d2 = norm.cdf(d2)

    is_call = df["option_type"] == "call"
    is_put = ~is_call
    valid_calls = valid & is_call.to_numpy()
    valid_puts = valid & is_put.to_numpy()

    delta = np.full(len(df), np.nan)
    delta[valid_calls] = cdf_d1[valid_calls]
    delta[valid_puts] = cdf_d1[valid_puts] - 1

    probability_itm = np.full(len(df), np.nan)
    probability_itm[valid_calls] = cdf_d2[valid_calls]
    probability_itm[valid_puts] = norm.cdf(-d2[valid_puts])

    gamma = np.full(len(df), np.nan)
    gamma[valid] = (
        pdf_d1[valid]
        / (underlying_price * sigma[valid] * np.sqrt(time_to_expiration[valid]))
    )

    vega = np.full(len(df), np.nan)
    vega[valid] = underlying_price * pdf_d1[valid] * np.sqrt(time_to_expiration[valid]) / 100

    theta = np.full(len(df), np.nan)
    theta[valid_calls] = (
        -(underlying_price * pdf_d1[valid_calls] * sigma[valid_calls])
        / (2 * np.sqrt(time_to_expiration[valid_calls]))
        - risk_free_rate
        * strike[valid_calls]
        * np.exp(-risk_free_rate * time_to_expiration[valid_calls])
        * cdf_d2[valid_calls]
    )
    theta[valid_puts] = (
        -(underlying_price * pdf_d1[valid_puts] * sigma[valid_puts])
        / (2 * np.sqrt(time_to_expiration[valid_puts]))
        + risk_free_rate
        * strike[valid_puts]
        * np.exp(-risk_free_rate * time_to_expiration[valid_puts])
        * (1 - cdf_d2[valid_puts])
    )

    derived_delta = pd.Series(delta, index=df.index, dtype=float)
    derived_probability_itm = pd.Series(probability_itm, index=df.index, dtype=float)
    derived_gamma = pd.Series(gamma, index=df.index, dtype=float)
    derived_vega = pd.Series(vega, index=df.index, dtype=float)
    derived_theta = pd.Series(theta / 365, index=df.index, dtype=float)

    df["delta"] = _merge_provider_and_derived(df.get("delta"), derived_delta)
    df["probability_itm"] = _merge_provider_and_derived(
        df.get("probability_itm"),
        derived_probability_itm,
    )
    df["gamma"] = _merge_provider_and_derived(df.get("gamma"), derived_gamma)
    df["vega"] = _merge_provider_and_derived(df.get("vega"), derived_vega)
    df["theta"] = _merge_provider_and_derived(df.get("theta"), derived_theta)
    df["delta_abs"] = _merge_provider_and_derived(df.get("delta_abs"), df["delta"].abs())
    df["delta_safety_pct"] = _merge_provider_and_derived(
        df.get("delta_safety_pct"),
        (1 - df["delta_abs"]) * 100,
    )
    df["delta_itm_proxy"] = _merge_provider_and_derived(
        df.get("delta_itm_proxy"),
        np.where(is_call, df["delta"], df["delta_abs"]),
    )
    df["has_valid_greeks"] = valid | df["delta"].notna()

    return df

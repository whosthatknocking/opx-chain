"""Black-Scholes greek and ITM-probability calculations for option rows."""

import numpy as np
from scipy.stats import norm


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

    df["delta"] = delta
    df["delta_abs"] = np.abs(delta)
    df["probability_itm"] = probability_itm
    df["gamma"] = gamma
    df["vega"] = vega
    df["theta"] = theta / 365
    df["delta_itm_proxy"] = np.where(is_call, df["delta"], df["delta_abs"])
    df["has_valid_greeks"] = valid

    return df

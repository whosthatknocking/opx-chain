import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from calendar import monthrange
from scipy.stats import norm

# ── CONFIG ─────────────────────────────────────────────
TICKERS = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]
MIN_BID = 0.50
RISK_FREE_RATE = 0.045

# Build a rolling expiration cutoff three months ahead.
today = datetime.today().date()
year = today.year
month = today.month + 3
if month > 12:
    month -= 12
    year += 1
_, last_day = monthrange(year, month)
MAX_EXPIRATION = f"{year}-{month:02d}-{last_day:02d}"

print(f"Today: {today}")
print(f"Max expiration: {MAX_EXPIRATION}")

# ── VECTOR GREEKS ──────────────────────────────────────
def compute_greeks(df, S, r):
    # Replace zero IVs before vectorized math to avoid divide-by-zero blowups.
    K = df['strike'].values
    T = df['T_years'].values
    sigma = df['impliedVolatility'].replace(0, np.nan).fillna(0.3).values

    valid = (T > 0) & (sigma > 0)

    d1 = np.full(len(df), np.nan)
    d2 = np.full(len(df), np.nan)

    d1[valid] = (np.log(S / K[valid]) + (r + 0.5 * sigma[valid]**2) * T[valid]) / (sigma[valid] * np.sqrt(T[valid]))
    d2[valid] = d1[valid] - sigma[valid] * np.sqrt(T[valid])

    N_d1 = norm.cdf(d1)
    N_d2 = norm.cdf(d2)
    n_d1 = norm.pdf(d1)

    df['delta'] = np.where(df['type'] == 'call', N_d1, N_d1 - 1)
    df['gamma'] = np.nan
    df['vega'] = np.nan
    df.loc[valid, 'gamma'] = n_d1[valid] / (S * sigma[valid] * np.sqrt(T[valid]))
    df.loc[valid, 'vega'] = S * n_d1[valid] * np.sqrt(T[valid]) / 100

    theta = np.full(len(df), np.nan)
    call_mask = df['type'] == 'call'
    put_mask = ~call_mask

    valid_calls = valid & call_mask
    valid_puts = valid & put_mask

    theta[valid_calls] = (
        -(S * n_d1[valid_calls] * sigma[valid_calls]) / (2 * np.sqrt(T[valid_calls]))
        - r * K[valid_calls] * np.exp(-r * T[valid_calls]) * N_d2[valid_calls]
    )
    theta[valid_puts] = (
        -(S * n_d1[valid_puts] * sigma[valid_puts]) / (2 * np.sqrt(T[valid_puts]))
        + r * K[valid_puts] * np.exp(-r * T[valid_puts]) * (1 - N_d2[valid_puts])
    )

    df['theta'] = theta / 365

    # 🎯 Assignment probability (approx)
    # Delta is used here as a quick assignment-risk proxy for ranking trades.
    df['assignment_prob'] = np.where(
        df['type'] == 'call',
        df['delta'],
        abs(df['delta'])
    )

    return df

# ── LADDER ─────────────────────────────────────────────
def assign_bucket(dte):
    if dte <= 10: return "Week_1"
    elif dte <= 18: return "Week_2"
    elif dte <= 26: return "Week_3"
    else: return "Week_4"

def roll_signal(row):
    # Treat near-expiry, near-the-money contracts as roll candidates.
    return row['dte'] <= 14 and abs(row['moneyness']) <= 0.03

# ── FETCH DATA ─────────────────────────────────────────
def fetch_chain(ticker):
    try:
        stock = yf.Ticker(ticker)
        S = stock.fast_info.get("lastPrice")

        if not S:
            return pd.DataFrame()

        expirations = stock.options
        rows = []

        for exp in expirations:
            if exp > MAX_EXPIRATION:
                continue

            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            dte = (exp_date - today).days
            if dte <= 0:
                continue

            T = dte / 365
            chain = stock.option_chain(exp)

            for typ, df in [("call", chain.calls), ("put", chain.puts)]:
                df = df.copy()
                df["type"] = typ
                df["ticker"] = ticker
                df["exp"] = exp
                df["dte"] = dte
                df["T_years"] = T

                # Drop obviously bad quotes before deriving spread-based features.
                df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
                df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
                df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
                df["openInterest"] = pd.to_numeric(df["openInterest"], errors="coerce")
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
                df["mid"] = (df["bid"] + df["ask"]) / 2
                valid_mid = df["mid"] > 0
                df["moneyness"] = (df["strike"] - S) / S
                df["yield"] = df["bid"] / df["strike"]
                df["annual_yield"] = df["yield"] / df["T_years"]
                df["spread"] = df["ask"] - df["bid"]
                df["spread_pct"] = np.where(valid_mid, df["spread"] / df["mid"], np.nan)
                # Ladder labels make it easier to balance expirations across weeks.
                df["ladder"] = df["dte"].apply(assign_bucket)

                df = compute_greeks(df, S, RISK_FREE_RATE)
                df["roll_flag"] = df.apply(roll_signal, axis=1)

                rows.append(df)

        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    except Exception as e:
        print(f"{ticker} error: {e}")
        return pd.DataFrame()

# ── LOAD ALL ───────────────────────────────────────────
dfs = []
for t in TICKERS:
    print(f"Loading {t}")
    d = fetch_chain(t)
    if not d.empty:
        dfs.append(d)

if not dfs:
    print("No data fetched.")
    raise SystemExit(0)

df = pd.concat(dfs, ignore_index=True)

# ── FILTER ─────────────────────────────────────────────
# Screen for tradable contracts by requiring minimum premium, tighter spreads, and basic liquidity.
filtered = df[
    (df["bid"] >= MIN_BID) &
    (df["spread_pct"] < 0.25) &
    (df["openInterest"] > 100) &
    (df["volume"] > 10)
].copy()

# Save
filtered.to_csv("options_engine_output.csv", index=False)
print("\nSaved: options_engine_output.csv")

# Options Fetcher

High-level Python project for collecting option chain data from Yahoo Finance, enriching it with derived metrics, and exporting the result to a timestamped CSV for downstream tools.

## What It Does

The script fetches near-term option chains for a configured list of tickers, normalizes the raw vendor data, calculates quality and pricing metrics, computes Black-Scholes Greeks, and writes a CSV file that another tool can consume.

The output is designed to be data-focused rather than decision-focused. It does not decide whether to close, roll, or open positions. Instead, it produces a richer dataset that can support those decisions elsewhere.

## Features

- Fetches call and put chains for configured tickers
- Limits expirations to a rolling three-month window
- Normalizes vendor fields into a stable CSV schema
- Adds quote quality, freshness, liquidity, and pricing metrics
- Adds underlying volatility context with `VIX` and trailing historical volatility
- Adds expiration-level expected move estimates
- Adds roll-yield metrics across expirations at the same strike
- Adds return-on-margin metrics using a transparent margin proxy
- Computes Black-Scholes `delta`, true ITM probability, `gamma`, `vega`, and `theta`
- Exports a timestamped CSV file for each run

## Requirements

- Python 3.10+
- Internet access for Yahoo Finance data

Install dependencies from `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How To Run

Run the project from the repository root:

```bash
python3 options_fetcher.py
```

You can also run the main entrypoint directly:

```bash
python3 main.py
```

## Output

Each run writes a CSV file in the project root using a timestamped filename:

```text
options_engine_output_YYYYMMDD_HHMMSS.csv
```

## CSV Field Reference

The exported CSV contains both raw option data and derived fields. Some values may be blank when Yahoo Finance does not provide enough data or when a calculation is not valid for that row.

### Contract and Expiration Fields

- `underlying_symbol`: Stock ticker for the option contract. Use it to group rows by underlying.
- `contract_symbol`: Vendor contract identifier. Use it as the unique option contract key.
- `option_type`: `call` or `put`. Use it to separate upside and downside contracts.
- `expiration_date`: Contract expiration date. Use it to sort the chain by maturity.
- `days_to_expiration`: Calendar days until expiration. Use it for short-dated screening and decay analysis.
- `time_to_expiration_years`: `days_to_expiration` expressed in years. Use it as the time input for Black-Scholes calculations.
- `strike`: Strike price of the contract. Use it to measure moneyness and break-even.
- `contract_size`: Contract multiplier from the vendor, typically `REGULAR`. Use it to confirm contract sizing conventions.

### Underlying Snapshot Fields

- `underlying_price`: Current underlying stock price used in calculations. Use it as the reference price for moneyness and Greeks.
- `underlying_currency`: Currency of the underlying quote. Use it to interpret all monetary fields correctly.
- `underlying_market_state`: Market session state from Yahoo Finance. Use it to judge whether prices are regular-session or extended-hours.
- `underlying_day_change_pct`: Underlying percentage move versus previous close. Use it to add context to the option chain.
- `historical_volatility`: Annualized realized volatility computed from the underlying's trailing 30 daily log returns. Use it to compare recent realized movement against option-implied pricing.
- `vix_level`: Latest CBOE Volatility Index level fetched for the run. Use it as a market-wide volatility regime reference.
- `vix_quote_time`: Timestamp of the VIX snapshot. Use it to judge whether the volatility regime reference is fresh.
- `underlying_price_time`: Timestamp of the underlying quote snapshot. Use it to compare timing with the option quote.
- `underlying_price_age_seconds`: Age of the underlying quote at fetch time. Use it to detect stale stock prices.
- `is_stale_underlying_price`: Flag showing whether the underlying quote is older than the configured staleness threshold. Use it to down-rank stale rows.

### Raw Option Quote Fields

- `bid`: Current best bid. Use it as the conservative executable premium estimate for selling.
- `ask`: Current best ask. Use it as the conservative executable premium estimate for buying.
- `last_trade_price`: Last reported trade price. Use it as a fallback reference when bid and ask are weak or missing.
- `volume`: Current session option volume. Use it as a liquidity signal.
- `open_interest`: Open contracts outstanding. Use it to judge market participation and contract depth.
- `implied_volatility`: Vendor-supplied implied volatility. Use it as the volatility input for Greeks and relative richness checks.
- `change`: Absolute price change reported by the vendor. Use it to understand the contract's move during the session.
- `percent_change`: Percentage price change reported by the vendor. Use it for relative move comparisons.
- `option_quote_time`: Timestamp of the option quote or last trade update. Use it to measure quote freshness.
- `is_in_the_money`: Vendor in-the-money flag. Use it as a quick classification check against derived moneyness fields.

### Quote Quality and Liquidity Fields

- `mark_price_mid`: Midpoint of bid and ask when the quote is valid. Use it as the default fair reference premium.
- `premium_reference_price`: Preferred premium used by derived calculations. It falls back from mid to bid to last trade price.
- `premium_reference_method`: Which source supplied `premium_reference_price`. Use it to judge how reliable premium-based metrics are.
- `bid_ask_spread`: Absolute spread between ask and bid. Use it to measure trading friction.
- `bid_ask_spread_pct_of_mid`: Spread divided by midpoint. Use it to compare spread quality across cheap and expensive contracts.
- `spread_to_strike_pct`: Spread divided by strike. Use it to normalize friction relative to contract notional level.
- `spread_to_bid_pct`: Spread divided by bid. Use it to see how expensive the spread is relative to collectible premium.
- `oi_to_volume_ratio`: Open interest divided by volume. Use it to distinguish established positions from fresh trading activity.

### Moneyness and Value Fields

- `strike_minus_spot`: Strike minus underlying price. Use it to see whether the strike sits above or below spot.
- `strike_vs_spot_pct`: `strike_minus_spot` as a percentage of spot. Use it for normalized moneyness comparisons.
- `strike_distance_pct`: Absolute distance between strike and spot as a percentage. Use it to find near-the-money contracts.
- `itm_amount`: In-the-money amount in dollars. Use it to separate intrinsic value from time value.
- `otm_pct`: Out-of-the-money distance as a percentage of spot. Use it to find target cushion on short options.
- `intrinsic_value`: Immediate exercise value. Use it as the core in-the-money value component.
- `extrinsic_value_bid`: Time value based on bid price. Use it to estimate conservative sell-side extrinsic premium.
- `extrinsic_value_mid`: Time value based on midpoint. Use it as the main extrinsic premium measure.
- `extrinsic_value_ask`: Time value based on ask price. Use it to estimate buy-side time premium.
- `extrinsic_pct_mid`: Extrinsic value as a share of midpoint price. Use it to compare how much of the option price is time value.
- `has_negative_extrinsic_mid`: Flag showing midpoint is below intrinsic value. Use it to detect bad quotes or pricing anomalies.

### Premium and Return-Oriented Fields

- `premium_to_strike`: Reference premium divided by strike. Use it as a simple premium yield measure.
- `premium_to_strike_bid`: Bid divided by strike. Use it for a more conservative premium yield estimate.
- `premium_to_strike_annualized`: `premium_to_strike` annualized by time to expiration. Use it to compare contracts with different expiries.
- `premium_per_day`: Reference premium earned per day until expiration. Use it to compare short-dated income efficiency.
- `estimated_margin_requirement`: Reg-T style per-share margin proxy for a short option, using `premium + max(20% of spot - OTM amount, 10% floor)`. Use it as the denominator for ROM-style comparisons.
- `return_on_margin`: `premium_reference_price / estimated_margin_requirement`. Use it to compare premium collected relative to estimated capital at risk.
- `return_on_margin_annualized`: `return_on_margin` annualized by time to expiration. Use it to compare ROM across expirations.
- `break_even_if_short`: Price where a short option position breaks even at expiration. Use it to evaluate downside or upside buffer.
- `expected_move`: One-standard-deviation expected dollar move for that expiration, computed as `spot * ATM_IV * sqrt(time)`. Use it as the core expected-move estimate for the expiry.
- `expected_move_pct`: `expected_move` as a percentage of spot. Use it to compare expected move across underlyings.
- `expected_move_lower_bound`: Spot minus `expected_move`. Use it as the lower expected-move boundary into expiration.
- `expected_move_upper_bound`: Spot plus `expected_move`. Use it as the upper expected-move boundary into expiration.
- `roll_from_expiration_date`: The nearest earlier expiration used as the roll source for the same underlying, option type, and strike. Use it to see which expiry the roll comparison came from.
- `roll_days_added`: Extra calendar days gained by rolling from `roll_from_expiration_date` to the current expiration. Use it as the time-extension denominator.
- `roll_from_premium_reference_price`: Premium reference price on the earlier expiration used for the roll comparison. Use it to audit the roll credit calculation.
- `roll_net_credit`: Current premium reference price minus `roll_from_premium_reference_price`. Use it to measure the extra credit from extending time.
- `roll_yield`: `roll_net_credit / roll_days_added`. Use it to compare net credit earned per extra day of time extension.

### Greek Fields

- `delta`: Black-Scholes delta. Use it as an estimate of directional sensitivity and a rough probability proxy.
- `delta_abs`: Absolute value of delta. Use it when you only care about magnitude, not call-versus-put sign.
- `delta_itm_proxy`: Delta normalized so higher values mean more in-the-money for both calls and puts. Use it for side-agnostic moneyness ranking.
- `probability_itm`: Black-Scholes probability of finishing in the money, derived from `d2` rather than delta. Use it when you want the model-based ITM probability instead of the delta approximation.
- `gamma`: Black-Scholes gamma. Use it to measure how quickly delta changes as the stock moves.
- `vega`: Black-Scholes vega. Use it to measure sensitivity to implied volatility changes.
- `vega_per_day`: Vega divided by days to expiration. Use it to compare vol sensitivity across expiries on a per-day basis.
- `theta`: Black-Scholes daily theta. Use it to estimate daily time decay.
- `theta_to_premium_ratio`: Absolute theta divided by premium. Use it to compare time decay efficiency relative to premium collected or paid.

### Validation, Freshness, and Screening Fields

- `has_valid_underlying`: True when the underlying price is positive. Use it to reject rows with unusable stock data.
- `has_valid_strike`: True when strike is positive. Use it to reject malformed contracts.
- `has_valid_quote`: True when bid and ask exist, are non-negative, and bid is not above ask. Use it to filter bad quotes.
- `has_valid_iv`: True when implied volatility is positive. Use it to identify rows suitable for Greek calculations.
- `has_valid_greeks`: True when the inputs required for Black-Scholes are valid. Use it to filter out rows with unreliable Greeks.
- `bid_le_ask`: True when bid is less than or equal to ask. Use it as a basic market sanity check.
- `has_nonzero_bid`: True when bid is greater than zero. Use it to find contracts with actual sell-side value.
- `has_nonzero_ask`: True when ask is greater than zero. Use it to find contracts with an actionable offer.
- `has_crossed_or_locked_market`: True when bid is greater than or equal to ask. Use it to detect suspicious market states.
- `quote_age_seconds`: Age of the option quote at fetch time. Use it to avoid stale option prices.
- `is_stale_quote`: Flag showing whether the option quote exceeds the staleness threshold. Use it to filter delayed quotes.
- `is_wide_market`: True when spread percentage exceeds the configured limit. Use it to remove illiquid contracts.
- `days_bucket`: Expiration bucket from `Week_1` through `Week_4`. Use it for quick grouping of near-term maturities.
- `near_expiry_near_money_flag`: True when expiration is within 14 days and strike is within 3% of spot. Use it to highlight short-dated near-the-money contracts.
- `passes_primary_screen`: True when bid, spread, open interest, and volume all pass configured thresholds. Use it as the main tradability filter.
- `quote_quality_score`: Simple composite score built from quote validity, IV, Greeks, market structure, and freshness checks. Use it to rank rows by data quality.

### Run Metadata Fields

- `fetched_at`: Timestamp when the script fetched the data. Use it to tie all rows to a single collection run.
- `data_source`: Source name for the data, currently `yfinance`. Use it for lineage and auditability.
- `script_version`: Internal script version string. Use it to track which schema or logic version generated the file.
- `risk_free_rate_used`: Risk-free rate used in Greek calculations. Use it to reproduce the Black-Scholes outputs.
- `fetch_status`: Fetch outcome marker for the row. Use it to identify successful versus degraded loads.
- `fetch_error`: Error text when a fetch issue is recorded. Use it for troubleshooting upstream data problems.

## Project Structure

```text
.
├── main.py
├── options_fetcher.py
├── options_fetcher_app/
│   ├── config.py
│   ├── export.py
│   ├── fetch.py
│   ├── greeks.py
│   ├── metrics.py
│   ├── normalize.py
│   └── utils.py
└── requirements.txt
```

## Configuration

Core configuration lives in `options_fetcher_app/config.py`, including:

- ticker list
- minimum liquidity thresholds
- spread threshold
- risk-free rate used for Greeks
- historical-volatility lookback
- stale quote threshold

## Notes

- Data is sourced from Yahoo Finance through `yfinance`.
- Quote timing and completeness depend on the upstream source.
- The exported CSV is intended to be consumed by another tool, so the script favors schema clarity and enriched raw data over trade recommendations.

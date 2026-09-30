# Field Reference

Schema note: the export is pinned to the canonical column set documented here. Provider-specific scratch fields or debug fields should not expand the CSV schema unless the spec is updated deliberately.

The exported CSV contains both provider-supplied and app-derived fields. Some values may be blank when the active provider does not expose the required source data or when a calculation is not valid for that row.

Implementation-status note:

- `Blank` in the provider mapping tables means the current product intentionally leaves that canonical field unpopulated for that provider.
- A blank value is not necessarily an upstream data error. It can mean the provider endpoint is not used in the current fetch path, the provider does not expose a compatible field, or the app has not implemented that derivation yet.
- When a field is blank by current design, this reference now treats that as the expected output.

## Blank and Null Value Contract

A blank value in the exported CSV means the field was not available for that row.
This is the single contract for absent data across all field categories.

**In the CSV artifact**, blank means an empty cell. `pd.DataFrame.to_csv()` writes
all null types — `NaN`, `pd.NA`, `pd.NaT`, and `None` — as an empty string. A
consumer reading the CSV must treat empty cells as absent values, not as zero,
`false`, or the empty string.

**In the Parquet artifact** (when Parquet support is added), blank means a
native-typed null. The serializer preserves the in-memory null type for each column:

| Column kind | In-memory null | CSV representation | Parquet representation |
|---|---|---|---|
| Numeric (`float`) | `np.nan` | empty cell | IEEE 754 NaN or null |
| Whole number (`Int64`) | `pd.NA` | empty cell | typed null |
| Boolean | `pd.NA` | empty cell | typed null |
| Timestamp | `pd.NaT` | empty cell | typed null |
| String / categorical | `None` or `np.nan` | empty cell | null |

The Parquet serializer must not coerce nulls to sentinel values such as `-1`,
`0`, or `""`. Consumers reading Parquet get properly-typed null values rather
than empty strings.

**Consumer responsibility:** type inference is the consumer's responsibility.
A consumer must not assume an empty cell means zero for a numeric field or
`false` for a boolean field. When a field is absent, the consumer should skip
that field or apply its own default for the downstream calculation.

## Viewer-Only Dataset Metadata Cards

These cards appear in the local viewer's `Dataset` tab. They are derived from
the selected run's `positions.csv` sidecar when that sidecar exists. They are
not exported CSV columns and are not part of the option-chain schema. opx-chain
does not expose a rich positions browser; portfolio row browsing belongs in
opx-strategy.

- `Positions`: Parsed count of stock tickers and held option contracts from the selected run sidecar. When no sidecar exists, the card reports `not captured` instead of falling back to the mutable default positions file.
- `Position Fingerprint`: Short display form of the SHA-256 fingerprint over parsed stock tickers and held option contract keys. The card tooltip includes the full fingerprint. Cosmetic CSV rewrites do not change it; semantic position changes do.
- `Position Coverage`: Counts how many parsed stock tickers and exact held option contracts are present in the selected chain artifact. This is a coverage check only; it does not show portfolio rows, quantities, cost basis, P/L, or account-level data.
- `Integrity`: Storage-backed datasets show the effective integrity state and
  bounded fatal/warning counts. Raw filesystem artifacts are explicitly labeled
  `raw read; integrity unknown` rather than implied to be validated.
- `Dataset Facts`: Shows whether the neutral content-bound facts projection is
  available. The projection contains only sorted tickers, per-ticker minimum and
  maximum `underlying_price_time`, and sorted expiration dates; it contains no
  strategy or portfolio policy.

These metadata values are not CSV columns. Their versions and content hash bind
them to the exact serialized dataset bytes.

## Contract and Expiration Fields

- `underlying_symbol`: Stock ticker for the option contract. Use it to group rows by underlying.
- `contract_symbol`: Vendor contract identifier. Use it as the unique option contract key.
- `option_type`: `call` or `put`. Use it to separate upside and downside contracts.
- `expiration_date`: Contract expiration date. Use it to sort the chain by maturity.
- `days_to_expiration`: Whole calendar days until expiration. Use it for short-dated screening and decay analysis. Lower means faster decay and more event risk.
- `time_to_expiration_years`: `days_to_expiration` expressed in years. Use it as the time input for Black-Scholes calculations.
- `strike`: Strike price of the contract. Use it to measure moneyness and break-even.
- `contract_size`: Contract multiplier from the vendor. Yahoo often reports `REGULAR`, Massive maps the numeric `shares_per_contract` value from the snapshot payload, and Market Data currently defaults this field to `REGULAR` because the chain response does not expose a separate contract-size field. Use it to confirm contract sizing conventions.

## Underlying Snapshot Fields

- `underlying_price`: Current underlying stock price used in calculations. Use it as the reference price for moneyness and Greeks.
- `underlying_day_change_pct`: Underlying percentage move versus previous close. Use it to add context to the option chain. Large absolute moves mean the underlying is already having an outsized session. This can be blank for providers that do not expose a reliable underlying previous-close context in the active fetch path.
- `historical_volatility`: Annualized realized volatility computed from the underlying's trailing 30 daily log returns. Use it to compare recent realized movement against option-implied pricing. Lower is calmer; higher means the stock has been moving more. This is currently expected to be blank for `massive` and `marketdata`.
- `underlying_price_time`: UTC timestamp of the underlying quote snapshot. CSV export uses ISO format with fractional seconds preserved when provided by the upstream provider. Use it to compare timing with the option quote.
- `underlying_price_age_seconds`: Age of the underlying quote at fetch time. Use it to detect stale stock prices. Lower is better; high values mean the stock snapshot may be stale.
- `is_stale_underlying_price`: Flag showing whether the underlying quote is older than the configured staleness threshold. Use it to down-rank stale rows.

## Standalone Price Context Artifact Fields

These optional fields are computed from the local daily-OHLCV history store and
written to `price_context_*.json` / `price_context_latest.json` when price
context is enabled. They are not part of the canonical option-chain CSV schema.
Consumers that need row-level price context should join the artifact to option
rows by `ticker` / `underlying_symbol`. Missing, stale, or errored price history
leaves numeric levels blank and uses metadata fields to explain why.

- `support_1`: Nearest computed support-like level at or below spot, selected from recent range, moving-average, and volume-node proxy levels.
- `support_2`: Next lower computed support-like level when available.
- `resistance_1`: Nearest computed resistance-like level at or above spot, selected from recent range, moving-average, and volume-node proxy levels.
- `resistance_2`: Next higher computed resistance-like level when available.
- `20d_high`: Highest daily high in the trailing 20 trading bars.
- `20d_low`: Lowest daily low in the trailing 20 trading bars.
- `50dma`: Trailing 50-trading-day simple moving average of daily closes. Blank when fewer than 50 bars are available.
- `200dma`: Trailing 200-trading-day simple moving average of daily closes. Blank when fewer than 200 bars are available.
- `rsi_14`: Fourteen-period daily RSI derived from adjusted daily closes. Blank when fewer than 15 usable bars are available.
- `ema_20`: Twenty-period exponential moving average of daily closes. Blank when fewer than 20 usable bars are available.
- `ema_50`: Fifty-period exponential moving average of daily closes. Blank when fewer than 50 usable bars are available.
- `ema_cloud_state`: Deterministic EMA trend state: `BULLISH` when 20 EMA and latest close are above 50 EMA, `BEARISH` when 20 EMA and latest close are below 50 EMA, `TRANSITION` when the EMA/price relationship is mixed, and `UNKNOWN` when required EMA inputs are unavailable.
- `price_vs_ema50_pct`: Latest close distance from 50 EMA as a percent. Positive means price is above 50 EMA; negative means below.
- `vwap`: Trailing 20-trading-bar volume-weighted average price approximation using daily typical price and volume.
- `volume_profile_high_volume_node`: Daily-history proxy for the high-volume node, using the typical price of the highest-volume day in the trailing 60 trading bars.
- `gap_fill_level`: Most recent unfilled daily gap-fill level detected in the trailing 60 trading bars.
- `pre_earnings_move_pct`: Reserved optional event-move context. Currently blank in opx-chain's daily-OHLCV implementation.
- `price_context_as_of`: Latest daily candle date used for the context in `YYYY-MM-DD` format.
- `price_context_age_days`: Calendar age of `price_context_as_of` relative to runtime `today`.
- `price_context_source`: Provider that supplied the OHLCV history.
- `price_context_lookback_trading_days`: Number of usable trading bars in the calculation.
- `price_context_calculation_method`: Calculation label. Current value is `daily_ohlcv_v1`.
- `price_context_staleness_status`: `FRESH`, `STALE`, `MISSING`, or `ERROR`. These values are defined by the `PriceContextStatus` artifact contract.

## Raw Option Quote Fields

- `bid`: Current best bid. Use it as the conservative executable premium estimate for selling. Higher means more immediate sell-side premium, all else equal.
- `ask`: Current best ask. Use it as the conservative executable premium estimate for buying.
- `last_trade_price`: Last reported trade price for the option contract itself, not the underlying stock. Use it as a fallback reference when bid and ask are weak or missing.
- `volume`: Current session option volume. Use it as a liquidity signal. Higher usually means better trading activity and easier fills.
- `open_interest`: Open contracts outstanding. Use it to judge market participation and contract depth. Higher usually means deeper, more established trading interest.
- `implied_volatility`: Vendor-supplied implied volatility. Use it as the volatility input for Greeks and relative richness checks. Higher means richer option pricing, but usually also more underlying uncertainty.
- `iv_state_level`: Per-underlying IV level classification derived from the full unfiltered chain: `HIGH` when the representative ATM IV is at or above the 70th percentile of that underlying's IV distribution, `LOW` at or below the 30th percentile, `NEUTRAL` in between, and `UNKNOWN` when the underlying has fewer than 5 rows. Broadcast to all rows for the underlying. Use it to quickly identify whether the current market is pricing rich or cheap vol.
- `iv_state_term`: Per-underlying IV term structure classification derived from the full unfiltered chain. Compares median IV at the nearest expiration (`near_iv`) to median IV at the next expiration (`far_iv`): `RISING` when `near_iv >= far_iv * 1.05`, `FALLING` when `near_iv <= far_iv * 0.95`, `FLAT` otherwise, and `UNKNOWN` when fewer than two distinct expirations are available. Broadcast to all rows for the underlying. Use it to detect near-term vol spikes (e.g. earnings crush) or backwardation.
- `listed_strike_increment`: Per-(underlying, option_type) minimum adjacent strike spacing, derived from the nearest expiration that has at least three distinct strikes. Broadcast to all rows for the (underlying, option_type) pair. Blank when no qualifying expiration is found. Use it to understand the resolution of the strike ladder for a given underlying and to detect non-standard increments.
- `change`: Absolute price change reported by the vendor. Use it to understand the contract's move during the session. This is currently expected to be blank for `marketdata`.
- `percent_change`: Percentage price change reported by the vendor. Use it for relative move comparisons. This is currently expected to be blank for `marketdata`.
- `option_quote_time`: UTC timestamp of the option quote or last trade update. CSV export uses ISO format with fractional seconds preserved when provided by the upstream provider. Use it to measure quote freshness.
- `is_in_the_money`: In-the-money classification from the provider when available, or derived from spot versus strike when the provider snapshot omits a direct flag. Use it as a quick classification check against derived moneyness fields.

## Corporate Event Fields

These fields are fetched once per ticker and broadcast to every option row for that underlying. They capture upcoming earnings and dividend events that elevate option risk and can distort standard pricing signals.
For Market Data, numeric event dates are interpreted on the `America/New_York` market calendar before `days_to_*` values are calculated.

- `next_earnings_date`: Next upcoming earnings report date in `YYYY-MM-DD` format. Use it to identify when the underlying is most likely to make a large move. Blank when no future earnings date is available or the provider does not support event data.
- `next_earnings_date_is_estimated`: True when the active provider marks `next_earnings_date` as an estimate rather than a confirmed company-announced date. Use it to avoid treating provisional earnings-calendar data as settled fact. Blank when no future earnings date is available or the provider does not expose estimate status.
- `next_earnings_date_source`: Provider field or source family used for the selected earnings date. Use it to audit whether the date came from a confirmed provider event field or an estimate fallback.
- `next_earnings_date_confidence`: Provider-derived confidence for the selected earnings date: `confirmed`, `estimated`, or `unknown`. Use it to mark provisional event dates without recomputing event windows downstream.
- `next_earnings_canonical_event_key`: Exact canonical event identity retained from a dedicated Event Data snapshot, for example `MSFT|earnings|next`. Blank for legacy snapshots without canonical event records.
- `days_to_earnings`: Whole calendar days until `next_earnings_date`. Use it to filter or down-rank option positions that span an earnings announcement. Lower values mean more immediate event exposure.
- `earnings_within_5d`: True when an earnings report falls within the next 5 calendar days and before the contract expires. Use it as a hard exclusion filter for strategies that cannot tolerate binary earnings risk.
- `earnings_within_10d`: True when an earnings report falls within the next 10 calendar days and before the contract expires. Use it to flag positions that enter an earnings window before expiration.
- `next_ex_div_date`: Next upcoming ex-dividend date in `YYYY-MM-DD` format. Use it to detect contracts that will experience dividend-related price adjustments before expiration.
- `next_ex_div_date_source`: Provider field or source family used for the selected ex-dividend date.
- `next_ex_div_date_confidence`: Provider-derived confidence for the selected ex-dividend date: `confirmed`, `estimated`, or `unknown`.
- `next_ex_div_canonical_event_key`: Exact canonical event identity retained from a dedicated Event Data snapshot, for example `MSFT|ex_dividend|next`. Blank for legacy snapshots without canonical event records.
- `days_to_ex_div`: Whole calendar days until `next_ex_div_date`. Use it to find contracts at risk of early assignment on dividend-paying underlyings. Lower values mean the ex-dividend date is approaching.
- `ex_div_within_3d`: True when an ex-dividend date falls within the next 3 calendar days and before the contract expires. Use it as a short-dated warning flag for early-assignment or price-gap risk on dividend underlyings.
- `dividend_amount`: Per-share cash dividend amount associated with `next_ex_div_date`. Use it to assess the scale of the expected price adjustment at ex-date.
- `event_risk_score`: Composite 0–100 event risk score derived from earnings and dividend proximity for events that occur before expiration. Earnings within 5 days contributes 60 points; within 10 days contributes 30 points. Ex-dividend within 3 days contributes 40 points; within 7 days contributes 20 points. The total is capped at 100. Blank when neither earnings nor dividend data is available before expiration. Use it to rank and filter contracts by combined near-term catalyst exposure.

## Quote Quality and Liquidity Fields

- `mark_price_mid`: Midpoint of bid and ask when the quote is valid. Use it as the default fair reference premium.
- `expected_fill_price`: Prompt-aligned sell-side execution estimate. It uses midpoint when `bid_ask_spread_pct_of_mid <= 0.10`, otherwise `bid + 25%` of the spread.
- `premium_reference_price`: Preferred premium used by derived calculations. It falls back from mid to bid to last trade price.
- `premium_reference_method`: Which source supplied `premium_reference_price`. Use it to judge how reliable premium-based metrics are.
- `bid_ask_spread`: Absolute spread between ask and bid. Use it to measure trading friction. Lower is better.
- `bid_ask_spread_pct_of_mid`: Spread divided by midpoint. Use it to compare spread quality across cheap and expensive contracts. Lower is better; the default fetch configuration currently keeps rows at or below `0.25` and filters out rows above that level.
- `spread_to_strike_pct`: Spread divided by strike. Use it to normalize friction relative to contract notional level.
- `spread_to_bid_pct`: Spread divided by bid. Use it to see how expensive the spread is relative to collectible premium. Lower is better.
- `oi_to_volume_ratio`: Open interest divided by volume. Use it to distinguish established positions from fresh trading activity. Very high values can mean established positions but muted current trading.

## Moneyness and Value Fields

- `strike_minus_spot`: Strike minus underlying price. Use it to see whether the strike sits above or below spot.
- `strike_vs_spot_pct`: `strike_minus_spot` as a percentage of spot. Use it for normalized moneyness comparisons.
- `strike_distance_pct`: Absolute distance between strike and spot as a percentage. Use it to find near-the-money contracts. Lower means closer to at-the-money; higher means farther OTM or ITM.
- `itm_amount`: In-the-money amount in dollars. Use it to separate intrinsic value from time value.
- `otm_pct`: Out-of-the-money distance as a percentage of spot. Use it to find target cushion on short options. Higher means more cushion, but usually less premium.
- `intrinsic_value`: Immediate exercise value. Use it as the core in-the-money value component.
- `extrinsic_value_bid`: Time value based on bid price. Use it to estimate conservative sell-side extrinsic premium.
- `extrinsic_value_mid`: Time value based on midpoint. Use it as the main extrinsic premium measure.
- `extrinsic_value_ask`: Time value based on ask price. Use it to estimate buy-side time premium.
- `extrinsic_pct_mid`: Extrinsic value as a share of midpoint price. Use it to compare how much of the option price is time value. Higher means more of the price is time value rather than intrinsic value.
- `has_negative_extrinsic_mid`: Flag showing midpoint is below intrinsic value. Use it to detect bad quotes or pricing anomalies.

## Premium and Return-Oriented Fields

- `premium_to_strike`: Reference premium divided by strike. Use it as a simple premium yield measure.
- `premium_to_strike_bid`: Bid divided by strike. Use it for a more conservative premium yield estimate.
- `premium_to_strike_annualized`: `premium_to_strike` annualized by time to expiration. Use it to compare contracts with different expiries. Higher can be attractive, but very high values often come with more risk or weaker liquidity.
- `premium_per_day`: Expected-fill premium earned per day until expiration, computed as `expected_fill_price / max(days_to_expiration, 1)`. Use it to compare short-dated income efficiency under a simple execution assumption.
- `iv_adjusted_premium_per_day`: `premium_per_day * (implied_volatility / 0.30)`. Use it as the main income-quality input for scoring so richer IV is reflected in the premium/day signal.
- `estimated_margin_requirement`: Reg-T style per-share margin proxy for a short option, using `premium + max(20% of spot - OTM amount, 10% floor)`. Use it as the denominator for ROM-style comparisons. Lower means less capital tied up, but not necessarily less real risk.
- `return_on_margin`: `premium_reference_price / estimated_margin_requirement`. Use it to compare premium collected relative to estimated capital at risk. Higher is usually better if quote quality and downside risk are still acceptable.
- `return_on_margin_annualized`: `return_on_margin` annualized by time to expiration. Use it to compare ROM across expirations. Higher is stronger on paper, but extreme values deserve extra caution.
- `break_even_if_short`: Price where a short option position breaks even at expiration. Use it to evaluate downside or upside buffer.
- `expected_move`: One-standard-deviation expected dollar move for that expiration, computed as `spot * ATM_IV * sqrt(time)`. Use it as the core expected-move estimate for the expiry.
- `expected_move_pct`: `expected_move` as a percentage of spot. Use it to compare expected move across underlyings. Lower means a calmer implied move; higher means the market expects more movement.
- `expected_move_lower_bound`: Spot minus `expected_move`. Use it as the lower expected-move boundary into expiration.
- `expected_move_upper_bound`: Spot plus `expected_move`. Use it as the upper expected-move boundary into expiration.

## Greek Fields

- `delta`: Black-Scholes delta. Use it as an estimate of directional sensitivity and a rough probability proxy.
- `delta_abs`: Absolute value of delta. Use it when you only care about magnitude, not call-versus-put sign. Lower usually means farther OTM; higher means closer to or deeper ITM.
- `delta_safety_pct`: Use it as a simple delta-based safety gauge for short premium trades. Higher means lower absolute delta and more distance from an at-the-money or in-the-money risk profile; lower means the trade is carrying more directional exposure.
- `delta_itm_proxy`: Delta normalized so higher values mean more in-the-money for both calls and puts. Use it for side-agnostic moneyness ranking.
- `probability_itm`: Black-Scholes probability of finishing in the money, derived from `d2` rather than delta when positive implied volatility is available; provider-native values are preserved when supplied. Use it when you want the model-based ITM probability instead of the delta approximation. Lower generally means less assignment/exercise risk for short premium trades.
- `gamma`: Black-Scholes gamma. Use it to measure how quickly delta changes as the stock moves. Higher means position risk can change faster as spot moves.
- `vega`: Black-Scholes vega. Use it to measure sensitivity to implied volatility changes. Higher means the option is more sensitive to vol expansion or crush.
- `vega_per_day`: Vega divided by days to expiration. Use it to compare vol sensitivity across expiries on a per-day basis.
- `theta`: Black-Scholes daily theta. Use it to estimate daily time decay.
- `theta_dollars_per_day`: Absolute daily theta scaled to one contract as `abs(theta) * 100`. Use it to compare raw daily decay capture across rows. Multiply by contract count only when you want absolute position theta magnitude; use signed `theta * 100` with position direction when you need signed position theta.
- `theta_to_premium_ratio`: Absolute theta divided by premium. Use it to compare time decay efficiency relative to premium collected or paid. Higher means faster daily decay relative to the option price.
- `capital_required`: Simplified one-contract capital proxy. Calls use `last_trade_price * 100`; puts use `strike * 100`.
- `theta_efficiency`: `theta_dollars_per_day / (capital_required / 1000)`. Use it to compare daily theta generation per `$1,000` of row-level capital.
- `theta_efficiency_below_p25`: Boolean flag that is `True` when a row's `theta_efficiency` falls below the 25th percentile of all post-filter rows for the same (underlying, option_type) pair, `False` otherwise, and blank when `theta_efficiency` is unavailable. Use it to quickly exclude the lowest-efficiency rows in a scan without hard-coding an absolute threshold.

## Validation, Freshness, and Screening Fields

- `has_valid_underlying`: True when the underlying price is positive. Use it to reject rows with unusable stock data.
- `has_valid_strike`: True when strike is positive. Use it to reject malformed contracts.
- `has_valid_quote`: True when bid and ask exist, are non-negative, and bid is not above ask. Use it to filter bad quotes.
- `has_valid_iv`: True when implied volatility is positive. Use it to identify rows suitable for Greek calculations.
- `has_valid_greeks`: True when real positive-IV Black-Scholes inputs are valid or a provider-native Greek/risk value is present. Missing or zero implied volatility does not get substituted. Use it to filter out rows with unreliable Greeks.
- `bid_le_ask`: True when bid is less than or equal to ask. Use it as a basic market sanity check.
- `has_nonzero_bid`: True when bid is greater than zero. Use it to find contracts with actual sell-side value.
- `has_nonzero_ask`: True when ask is greater than zero. Use it to find contracts with an actionable offer.
- `has_crossed_or_locked_market`: True when bid is greater than or equal to ask. Use it to detect suspicious market states.
- `quote_age_seconds`: Post-acquisition assessment time minus option quote time, after any bounded refresh. Cached timestamps remain unchanged. Negative values identify quotes later than the assessment instant, not quotes received after fetch started.
- `is_stale_quote`: True for negative quote ages or ages above the configured staleness threshold; missing ages remain unknown. Use it to identify future-dated or delayed quotes.
- `is_wide_market`: True when spread percentage exceeds the configured limit. Use it to remove illiquid contracts. `True` is usually a bad sign for execution quality.
- `days_bucket`: Expiration bucket from `Week_1` through `Week_4`. Use it for quick grouping of near-term maturities.
- `near_expiry_near_money_flag`: True when expiration is within 14 days and strike is within 3% of spot. Use it to highlight short-dated near-the-money contracts.
- `passes_primary_screen`: True when bid, spread, open interest, and volume all pass configured thresholds. Use it as the main tradability filter. `True` is generally better for practical trading candidates.
- `spread_score`: Execution-quality score from the prompt spread tiers. Higher is better.
- `dte_score`: Execution-quality score from the prompt DTE tiers. Higher is better.
- `risk_level`: Prompt-aligned row risk label using delta as the score-driving risk input.
- `risk_model_inconsistent`: Flag showing delta and `probability_itm` disagree materially.
- `quote_quality_score`: Simple composite score built from quote validity, IV, Greeks, market structure, and freshness checks. Use it to rank rows by data quality. Higher is better.
- `option_score`: Shared 0-100 row score built from IV-adjusted premium/day, spread execution quality, DTE execution quality, delta-only risk, and theta efficiency. Use it to sort contracts by overall attractiveness within one run before score validation adjustments.
- `score_validation`: Row-level alignment label: `DISCREPANCY`, `UNDERVALUED`, or `ALIGNED`.
- `score_adjustment`: Numeric adjustment applied after score validation.
- `final_score`: Final row score after applying `score_adjustment` to `option_score` and clamping the result into `0-100`.

## Run Metadata Fields

- `data_source`: Source name for the active provider. Use it for lineage and auditability.
- `risk_free_rate_used`: Risk-free rate used in Greek calculations. Use it to reproduce the Black-Scholes outputs.

## Provider Mapping Matrix

Legend:

- `Direct`: copied from the provider payload with only the canonical column rename
- `Transformed`: mapped from provider fields with normalization, coercion, or fallback logic
- `Derived`: calculated in shared app code after normalization
- `Blank`: the current implementation intentionally leaves this canonical field unpopulated for that provider unless shared app code later derives it

### Contract and Expiration Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `underlying_symbol` | Transformed: request ticker, filled into canonical field during normalization | Transformed: `underlying_asset.ticker`, fallback request ticker | Transformed: `underlying` -> `underlying_symbol` |
| `contract_symbol` | Direct: `contractSymbol` -> `contract_symbol` | Transformed: `details.ticker`, strips `O:` prefix | Direct: `optionSymbol` |
| `option_type` | Transformed: side supplied by fetch loop (`call`/`put`) | Transformed: `details.contract_type` mapped to canonical `call`/`put` | Transformed: chain rows are split by `side`, and shared normalization fills canonical `call`/`put` |
| `expiration_date` | Transformed: expiration from fetch loop | Direct: `details.expiration_date` | Transformed: `expiration` timestamp normalized to `YYYY-MM-DD` |
| `days_to_expiration` | Derived: from expiration date vs runtime `today` | Derived: from expiration date vs runtime `today` | Derived: from expiration date vs runtime `today` |
| `time_to_expiration_years` | Derived: `days_to_expiration / 365` | Derived: `days_to_expiration / 365` | Derived: `days_to_expiration / 365` |
| `strike` | Direct: `strike` | Direct: `details.strike_price` | Direct: `strike` |
| `contract_size` | Transformed: `contractSize` -> `contract_size` | Transformed: `details.shares_per_contract`, fallback `REGULAR` | Blank/Defaulted: chain payload does not expose contract size, so app fills `REGULAR` |

### Underlying Snapshot Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `underlying_price` | Transformed: `fast_info.lastPrice`, fallback `info.regularMarketPrice` / `info.previousClose` | Transformed: `underlying_asset.price`, fallback `underlying_asset.value` | Transformed: `stocks/quotes/{symbol}/ last`, fallback latest chain row with usable `underlyingPrice` |
| `underlying_day_change_pct` | Derived from provider values: `(last_price - previous_close) / previous_close` | Derived from provider values: `(underlying_price - day.previous_close) / previous_close` | Direct: `changepct` from `stocks/quotes/{symbol}/`, matched to the same stock quote that supplies `underlying_price_time` |
| `historical_volatility` | Derived from provider history: trailing daily log returns | Blank: not yet implemented for this provider | Blank: not yet implemented for this provider |
| `underlying_price_time` | Transformed: `info.regularMarketTime` normalized to UTC timestamp | Transformed: `underlying_asset.last_updated`, fallback day/trade/quote timestamps | Transformed: `stocks/quotes/{symbol}/ updated`, fallback latest chain row `updated`, normalized to UTC |
| `underlying_price_age_seconds` | Derived: fetch time minus `underlying_price_time` | Derived: fetch time minus `underlying_price_time` | Derived: fetch time minus `underlying_price_time` |
| `is_stale_underlying_price` | Derived: age compared to `stale_quote_seconds` | Derived: age compared to `stale_quote_seconds` | Derived: age compared to `stale_quote_seconds` |

### Standalone Price Context Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `support_1` | Derived in the standalone artifact from adjusted daily OHLCV history when `[price_context].enable = true` | Blank: daily-history adapter not implemented for this provider | Derived in the standalone artifact from daily split-adjusted stock candles when `[price_context].enable = true` |
| `support_2` | Derived from adjusted daily OHLCV history when available | Blank: daily-history adapter not implemented for this provider | Derived from daily split-adjusted stock candles when available |
| `resistance_1` | Derived in the standalone artifact from adjusted daily OHLCV history when `[price_context].enable = true` | Blank: daily-history adapter not implemented for this provider | Derived in the standalone artifact from daily split-adjusted stock candles when `[price_context].enable = true` |
| `resistance_2` | Derived from adjusted daily OHLCV history when available | Blank: daily-history adapter not implemented for this provider | Derived from daily split-adjusted stock candles when available |
| `20d_high` | Derived: trailing 20-bar daily high | Blank: daily-history adapter not implemented for this provider | Derived: trailing 20-bar daily high |
| `20d_low` | Derived: trailing 20-bar daily low | Blank: daily-history adapter not implemented for this provider | Derived: trailing 20-bar daily low |
| `50dma` | Derived: trailing 50-bar close average | Blank: daily-history adapter not implemented for this provider | Derived: trailing 50-bar close average |
| `200dma` | Derived: trailing 200-bar close average | Blank: daily-history adapter not implemented for this provider | Derived: trailing 200-bar close average |
| `vwap` | Derived: trailing 20-bar daily VWAP approximation | Blank: daily-history adapter not implemented for this provider | Derived: trailing 20-bar daily VWAP approximation |
| `volume_profile_high_volume_node` | Derived: typical price of highest-volume day in trailing 60 bars | Blank: daily-history adapter not implemented for this provider | Derived: typical price of highest-volume day in trailing 60 bars |
| `gap_fill_level` | Derived: latest unfilled daily gap level in trailing 60 bars | Blank: daily-history adapter not implemented for this provider | Derived: latest unfilled daily gap level in trailing 60 bars |
| `pre_earnings_move_pct` | Blank: not derived in current daily-OHLCV implementation | Blank: not derived in current daily-OHLCV implementation | Blank: not derived in current daily-OHLCV implementation |
| `price_context_*` metadata | Derived from price-context fetch/cache status | Blank/Metadata-only when unsupported | Derived from price-context fetch/cache status |

### Raw Quote and Activity Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `bid` | Direct: `bid` | Transformed: `last_quote.bid`, fallback `last_quote.bid_price` | Direct: `bid` |
| `ask` | Direct: `ask` | Transformed: `last_quote.ask`, fallback `last_quote.ask_price` | Direct: `ask` |
| `last_trade_price` | Transformed: `lastPrice` -> `last_trade_price` | Transformed: `last_trade.price`, fallback `day.close` | Transformed: `last` -> `last_trade_price` (option contract last, not `underlyingPrice`) |
| `volume` | Direct: `volume` | Direct: `day.volume` | Direct: `volume` |
| `open_interest` | Transformed: `openInterest` -> `open_interest` | Direct: `open_interest` | Transformed: `openInterest` -> `open_interest` |
| `implied_volatility` | Transformed: `impliedVolatility` -> `implied_volatility` | Direct/Transformed: top-level `implied_volatility`, coerced numeric | Direct/Transformed: `iv` -> `implied_volatility` |
| `iv_state_level` | Derived: pre-filter IV level classification per underlying | Derived: pre-filter IV level classification per underlying | Derived: pre-filter IV level classification per underlying |
| `iv_state_term` | Derived: pre-filter IV term structure classification per underlying | Derived: pre-filter IV term structure classification per underlying | Derived: pre-filter IV term structure classification per underlying |
| `listed_strike_increment` | Derived: minimum adjacent strike spacing per (underlying, option_type) | Derived: minimum adjacent strike spacing per (underlying, option_type) | Derived: minimum adjacent strike spacing per (underlying, option_type) |
| `change` | Direct: `change` | Direct: `day.change` | Blank: expected in the current implementation because the Market Data fetch path does not populate option-contract change |
| `percent_change` | Transformed: `percentChange` -> `percent_change` | Direct: `day.change_percent` | Blank: expected in the current implementation because the Market Data fetch path does not populate option-contract percent change |
| `option_quote_time` | Transformed: `lastTradeDate` -> UTC timestamp | Transformed: `last_quote.last_updated`, fallback `last_trade.sip_timestamp` / `day.last_updated` | Transformed: `updated` -> `option_quote_time` |
| `is_in_the_money` | Transformed: `inTheMoney` -> `is_in_the_money` | Derived from provider values: underlying spot vs strike | Transformed: `inTheMoney` -> `is_in_the_money` |

### Quote Quality and Liquidity Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `mark_price_mid` | Derived: midpoint of canonical bid/ask | Derived: midpoint of canonical bid/ask | Derived: midpoint of canonical bid/ask |
| `premium_reference_price` | Derived: midpoint, fallback bid, fallback last trade | Derived: midpoint, fallback bid, fallback last trade | Derived: midpoint, fallback bid, fallback last trade |
| `premium_reference_method` | Derived: source used for `premium_reference_price` | Derived: source used for `premium_reference_price` | Derived: source used for `premium_reference_price` |
| `bid_ask_spread` | Derived: `ask - bid` when quote is valid | Derived: `ask - bid` when quote is valid | Derived: `ask - bid` when quote is valid |
| `bid_ask_spread_pct_of_mid` | Derived: spread divided by midpoint | Derived: spread divided by midpoint | Derived: spread divided by midpoint |
| `spread_to_strike_pct` | Derived: spread divided by strike | Derived: spread divided by strike | Derived: spread divided by strike |
| `spread_to_bid_pct` | Derived: spread divided by bid | Derived: spread divided by bid | Derived: spread divided by bid |
| `oi_to_volume_ratio` | Derived: open interest divided by volume | Derived: open interest divided by volume | Derived: open interest divided by volume |

### Moneyness and Value Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `strike_minus_spot` | Derived: strike minus underlying price | Derived: strike minus underlying price | Derived: strike minus underlying price |
| `strike_vs_spot_pct` | Derived: `strike_minus_spot / underlying_price` | Derived: `strike_minus_spot / underlying_price` | Derived: `strike_minus_spot / underlying_price` |
| `strike_distance_pct` | Derived: absolute strike-vs-spot percent | Derived: absolute strike-vs-spot percent | Derived: absolute strike-vs-spot percent |
| `itm_amount` | Derived from option type, strike, and spot | Derived from option type, strike, and spot | Derived from option type, strike, and spot |
| `otm_pct` | Derived from option type, strike, and spot | Derived from option type, strike, and spot | Derived from option type, strike, and spot |
| `intrinsic_value` | Derived: equals `itm_amount` | Derived: equals `itm_amount` | Derived: shared calculations treat intrinsic as strike-vs-spot based even though provider `intrinsicValue` may exist |
| `extrinsic_value_bid` | Derived: `bid - intrinsic_value` | Derived: `bid - intrinsic_value` | Derived: `bid - intrinsic_value` |
| `extrinsic_value_mid` | Derived: `mark_price_mid - intrinsic_value` | Derived: `mark_price_mid - intrinsic_value` | Derived: `mark_price_mid - intrinsic_value` |
| `extrinsic_value_ask` | Derived: `ask - intrinsic_value` | Derived: `ask - intrinsic_value` | Derived: `ask - intrinsic_value` |
| `extrinsic_pct_mid` | Derived: extrinsic mid divided by midpoint | Derived: extrinsic mid divided by midpoint | Derived: extrinsic mid divided by midpoint |
| `has_negative_extrinsic_mid` | Derived: midpoint below intrinsic value flag | Derived: midpoint below intrinsic value flag | Derived: midpoint below intrinsic value flag |

### Premium and Return-Oriented Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `premium_to_strike` | Derived: premium reference divided by strike | Derived: premium reference divided by strike | Derived: premium reference divided by strike |
| `premium_to_strike_bid` | Derived: bid divided by strike | Derived: bid divided by strike | Derived: bid divided by strike |
| `premium_to_strike_annualized` | Derived: premium-to-strike annualized by expiry | Derived: premium-to-strike annualized by expiry | Derived: premium-to-strike annualized by expiry |
| `premium_per_day` | Derived: expected fill divided by `max(days_to_expiration, 1)` | Derived: expected fill divided by `max(days_to_expiration, 1)` | Derived: expected fill divided by `max(days_to_expiration, 1)` |
| `iv_adjusted_premium_per_day` | Derived: `premium_per_day * (implied_volatility / 0.30)` | Derived: `premium_per_day * (implied_volatility / 0.30)` | Derived: `premium_per_day * (implied_volatility / 0.30)` |
| `estimated_margin_requirement` | Derived: shared margin proxy formula | Derived: shared margin proxy formula | Derived: shared margin proxy formula |
| `return_on_margin` | Derived: premium reference divided by margin proxy | Derived: premium reference divided by margin proxy | Derived: premium reference divided by margin proxy |
| `return_on_margin_annualized` | Derived: return on margin annualized by expiry | Derived: return on margin annualized by expiry | Derived: return on margin annualized by expiry |
| `break_even_if_short` | Derived from strike, side, and premium reference | Derived from strike, side, and premium reference | Derived from strike, side, and premium reference |
| `expected_move` | Derived: expiry-level ATM-IV move estimate | Derived: expiry-level ATM-IV move estimate | Derived: expiry-level ATM-IV move estimate |
| `expected_move_pct` | Derived: expected move divided by spot | Derived: expected move divided by spot | Derived: expected move divided by spot |
| `expected_move_lower_bound` | Derived: spot minus expected move | Derived: spot minus expected move | Derived: spot minus expected move |
| `expected_move_upper_bound` | Derived: spot plus expected move | Derived: spot plus expected move | Derived: spot plus expected move |

### Greek Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `delta` | Derived: Black-Scholes in shared app code | Transformed or Derived: provider `greeks.delta` preserved, app fills gaps | Direct/Transformed: provider `delta` preserved, app fills gaps |
| `delta_abs` | Derived: absolute value of delta | Derived: absolute value of delta | Derived: absolute value of delta |
| `delta_safety_pct` | Derived: `(1 - abs(delta)) * 100` | Derived: `(1 - abs(delta)) * 100` | Derived: `(1 - abs(delta)) * 100` |
| `delta_itm_proxy` | Derived: side-normalized delta | Derived: side-normalized delta | Derived: side-normalized delta |
| `probability_itm` | Derived: Black-Scholes `d2` probability when positive IV is present | Provider value preserved, otherwise derived from Black-Scholes `d2` when positive IV is present | Provider value preserved, otherwise derived from Black-Scholes `d2` when positive IV is present |
| `gamma` | Derived: Black-Scholes in shared app code | Transformed or Derived: provider `greeks.gamma` preserved, app fills gaps | Direct/Transformed: provider `gamma` preserved, app fills gaps |
| `vega` | Derived: Black-Scholes in shared app code | Transformed or Derived: provider `greeks.vega` preserved, app fills gaps | Direct/Transformed: provider `vega` preserved, app fills gaps |
| `vega_per_day` | Derived: vega divided by days to expiry | Derived: vega divided by days to expiry | Derived: vega divided by days to expiry |
| `theta` | Derived: Black-Scholes daily theta | Transformed or Derived: provider `greeks.theta` preserved, app fills gaps | Direct/Transformed: provider `theta` preserved, app fills gaps |
| `theta_dollars_per_day` | Derived: `abs(theta) * 100` | Derived: `abs(theta) * 100` | Derived: `abs(theta) * 100` |
| `theta_to_premium_ratio` | Derived: absolute theta divided by premium reference | Derived: absolute theta divided by premium reference | Derived: absolute theta divided by premium reference |
| `capital_required` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` |
| `theta_efficiency` | Derived: theta dollars per day per `$1,000` of capital required | Derived: theta dollars per day per `$1,000` of capital required | Derived: theta dollars per day per `$1,000` of capital required |
| `theta_efficiency_below_p25` | Derived: post-filter p25 flag per (underlying, option_type) | Derived: post-filter p25 flag per (underlying, option_type) | Derived: post-filter p25 flag per (underlying, option_type) |

### Validation, Freshness, and Screening Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `has_valid_underlying` | Derived: underlying price positive | Derived: underlying price positive | Derived: underlying price positive |
| `has_valid_strike` | Derived: strike positive | Derived: strike positive | Derived: strike positive |
| `has_valid_quote` | Derived: bid/ask present, non-negative, and ordered | Derived: bid/ask present, non-negative, and ordered | Derived: bid/ask present, non-negative, and ordered |
| `has_valid_iv` | Derived: implied volatility positive | Derived: implied volatility positive | Derived: implied volatility positive |
| `has_valid_greeks` | Derived: valid positive-IV Black-Scholes inputs or provider Greek present | Derived: valid positive-IV Black-Scholes inputs or provider Greek present | Derived: valid positive-IV Black-Scholes inputs or provider Greek present |
| `bid_le_ask` | Derived: `bid <= ask` | Derived: `bid <= ask` | Derived: `bid <= ask` |
| `has_nonzero_bid` | Derived: bid greater than zero | Derived: bid greater than zero | Derived: bid greater than zero |
| `has_nonzero_ask` | Derived: ask greater than zero | Derived: ask greater than zero | Derived: ask greater than zero |
| `has_crossed_or_locked_market` | Derived: bid greater than or equal to ask | Derived: bid greater than or equal to ask | Derived: bid greater than or equal to ask |
| `is_wide_market` | Derived: spread percent exceeds configured threshold | Derived: spread percent exceeds configured threshold | Derived: spread percent exceeds configured threshold |
| `quote_age_seconds` | Derived: fetch time minus option quote time | Derived: fetch time minus option quote time | Derived: fetch time minus option quote time |
| `is_stale_quote` | Derived: quote age exceeds `stale_quote_seconds` | Derived: quote age exceeds `stale_quote_seconds` | Derived: quote age exceeds `stale_quote_seconds` |
| `days_bucket` | Derived: calendar bucket from days to expiry | Derived: calendar bucket from days to expiry | Derived: calendar bucket from days to expiry |
| `near_expiry_near_money_flag` | Derived: expiry and moneyness flag | Derived: expiry and moneyness flag | Derived: expiry and moneyness flag |
| `passes_primary_screen` | Derived: bid, spread, OI, and volume thresholds | Derived: bid, spread, OI, and volume thresholds | Derived: bid, spread, OI, and volume thresholds |
| `spread_score` | Derived: prompt spread execution score | Derived: prompt spread execution score | Derived: prompt spread execution score |
| `dte_score` | Derived: prompt DTE execution score | Derived: prompt DTE execution score | Derived: prompt DTE execution score |
| `risk_level` | Derived: delta-led risk classification with ITM-probability validation | Derived: delta-led risk classification with ITM-probability validation | Derived: delta-led risk classification with ITM-probability validation |
| `risk_model_inconsistent` | Derived: flag for material disagreement between delta and `probability_itm` | Derived: flag for material disagreement between delta and `probability_itm` | Derived: flag for material disagreement between delta and `probability_itm` |
| `quote_quality_score` | Derived: shared composite quality score | Derived: shared composite quality score | Derived: shared composite quality score |
| `option_score` | Derived: shared row score from IV-adjusted income, spread execution, delta-only risk, and efficiency | Derived: shared row score from IV-adjusted income, spread execution, delta-only risk, and efficiency | Derived: shared row score from IV-adjusted income, spread execution, delta-only risk, and efficiency |
| `score_validation` | Derived: row-level alignment label for score review | Derived: row-level alignment label for score review | Derived: row-level alignment label for score review |
| `score_adjustment` | Derived: numeric post-score adjustment | Derived: numeric post-score adjustment | Derived: numeric post-score adjustment |
| `final_score` | Derived: `option_score + score_adjustment`, clamped to `0-100` | Derived: `option_score + score_adjustment`, clamped to `0-100` | Derived: `option_score + score_adjustment`, clamped to `0-100` |

### Corporate Event Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `next_earnings_date` | Transformed: best-effort minimum upcoming date from Yahoo `info` earnings timestamps and `calendar` earnings values | Blank: expected because event fetching is not implemented for this provider | Transformed: selected from upcoming `stocks/earnings/{symbol}/` `reportDate`; Market Data `date` is the fiscal period end and is not used as the event date. Rows whose `reportedEPS` is already populated are excluded |
| `next_earnings_date_is_estimated` | Transformed/Blank: `info.isEarningsDateEstimate` when Yahoo exposes it for the chosen future earnings date, otherwise blank | Blank: expected because event fetching is not implemented for this provider | Derived: `True` when the selected date came from Market Data `reportDate`; blank when no future earnings date is available |
| `next_earnings_date_source` | Derived: `yfinance` when Yahoo supplies the selected future earnings date | Blank: expected because event fetching is not implemented for this provider | Derived: `marketdata.reportDate` for upcoming Market Data earnings events |
| `next_earnings_date_confidence` | Derived: `estimated`, `confirmed`, or `unknown` from Yahoo's estimate flag when a date is present | Blank: expected because event fetching is not implemented for this provider | Derived: `estimated` for upcoming Market Data `reportDate` values |
| `days_to_earnings` | Derived: `next_earnings_date` minus runtime `today` when Yahoo provides a future date | Blank: expected because event fetching is not implemented for this provider | Derived: `next_earnings_date` minus runtime `today` |
| `earnings_within_5d` | Derived: `days_to_earnings <= 5` and event occurs before expiration when Yahoo provides a future earnings date | Blank: expected because event fetching is not implemented for this provider | Derived: `days_to_earnings <= 5` and event occurs before expiration |
| `earnings_within_10d` | Derived: `days_to_earnings <= 10` and event occurs before expiration when Yahoo provides a future earnings date | Blank: expected because event fetching is not implemented for this provider | Derived: `days_to_earnings <= 10` and event occurs before expiration |
| `next_ex_div_date` | Transformed: best-effort minimum upcoming date from Yahoo `info.exDividendDate`, `calendar`, and future `dividends` index values | Blank: expected because event fetching is not implemented for this provider | Transformed: minimum upcoming `exDate` from `stocks/dividends/{symbol}/` via direct HTTP request |
| `next_ex_div_date_source` | Derived: `yfinance` when Yahoo supplies the selected future ex-dividend date | Blank: expected because event fetching is not implemented for this provider | Derived: `marketdata.exDate` when Market Data supplies an upcoming dividend row |
| `next_ex_div_date_confidence` | Derived: `confirmed` when Yahoo supplies a future ex-dividend date | Blank: expected because event fetching is not implemented for this provider | Derived: `confirmed` when Market Data supplies an upcoming `exDate` |
| `days_to_ex_div` | Derived: `next_ex_div_date` minus runtime `today` when Yahoo provides a future date | Blank: expected because event fetching is not implemented for this provider | Derived: `next_ex_div_date` minus runtime `today` |
| `ex_div_within_3d` | Derived: `days_to_ex_div <= 3` and event occurs before expiration when Yahoo provides a future ex-dividend date | Blank: expected because event fetching is not implemented for this provider | Derived: `days_to_ex_div <= 3` and event occurs before expiration |
| `dividend_amount` | Transformed/Blank: dividend amount from the future Yahoo `dividends` entry that matches `next_ex_div_date`, blank when Yahoo only exposes the date | Blank: expected because event fetching is not implemented for this provider | Transformed: `amount` corresponding to `next_ex_div_date` from dividend payload |
| `event_risk_score` | Derived: composite score from earnings and dividend proximity when Yahoo supplies future event dates before expiration | Blank: expected because event fetching is not implemented for this provider | Derived: composite score from earnings and dividend proximity for events before expiration |

### Run Metadata Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `data_source` | Derived/Constant: provider name `yfinance` | Derived/Constant: provider name `massive` | Derived/Constant: provider name `marketdata` |
| `risk_free_rate_used` | Derived/Constant: runtime config value | Derived/Constant: runtime config value | Derived/Constant: runtime config value |

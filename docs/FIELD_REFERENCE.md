# Field Reference

Schema note: the export is pinned to the canonical column set documented here. Provider-specific scratch fields or debug fields should not expand the CSV schema unless the spec is updated deliberately.

The exported CSV contains both provider-supplied and app-derived fields. Some values may be blank when the active provider does not expose the required source data or when a calculation is not valid for that row.

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
- `historical_volatility`: Annualized realized volatility computed from the underlying's trailing 30 daily log returns. Use it to compare recent realized movement against option-implied pricing. Lower is calmer; higher means the stock has been moving more.
- `underlying_price_time`: Timestamp of the underlying quote snapshot. Use it to compare timing with the option quote.
- `underlying_price_age_seconds`: Age of the underlying quote at fetch time. Use it to detect stale stock prices. Lower is better; high values mean the stock snapshot may be stale.
- `is_stale_underlying_price`: Flag showing whether the underlying quote is older than the configured staleness threshold. Use it to down-rank stale rows.

## Raw Option Quote Fields

- `bid`: Current best bid. Use it as the conservative executable premium estimate for selling. Higher means more immediate sell-side premium, all else equal.
- `ask`: Current best ask. Use it as the conservative executable premium estimate for buying.
- `last_trade_price`: Last reported trade price for the option contract itself, not the underlying stock. Use it as a fallback reference when bid and ask are weak or missing.
- `volume`: Current session option volume. Use it as a liquidity signal. Higher usually means better trading activity and easier fills.
- `open_interest`: Open contracts outstanding. Use it to judge market participation and contract depth. Higher usually means deeper, more established trading interest.
- `implied_volatility`: Vendor-supplied implied volatility. Use it as the volatility input for Greeks and relative richness checks. Higher means richer option pricing, but usually also more underlying uncertainty.
- `change`: Absolute price change reported by the vendor. Use it to understand the contract's move during the session. Some providers leave this blank in the single-call chain flow.
- `percent_change`: Percentage price change reported by the vendor. Use it for relative move comparisons. Some providers leave this blank in the single-call chain flow.
- `option_quote_time`: Timestamp of the option quote or last trade update. Use it to measure quote freshness.
- `is_in_the_money`: In-the-money classification from the provider when available, or derived from spot versus strike when the provider snapshot omits a direct flag. Use it as a quick classification check against derived moneyness fields.

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
- `delta_itm_proxy`: Delta normalized so higher values mean more in-the-money for both calls and puts. Use it for side-agnostic moneyness ranking.
- `probability_itm`: Black-Scholes probability of finishing in the money, derived from `d2` rather than delta. Use it when you want the model-based ITM probability instead of the delta approximation. Lower generally means less assignment/exercise risk for short premium trades.
- `gamma`: Black-Scholes gamma. Use it to measure how quickly delta changes as the stock moves. Higher means position risk can change faster as spot moves.
- `vega`: Black-Scholes vega. Use it to measure sensitivity to implied volatility changes. Higher means the option is more sensitive to vol expansion or crush.
- `vega_per_day`: Vega divided by days to expiration. Use it to compare vol sensitivity across expiries on a per-day basis.
- `theta`: Black-Scholes daily theta. Use it to estimate daily time decay.
- `theta_dollars_per_day`: Absolute daily theta scaled to one contract as `abs(theta) * 100`. Use it to compare raw daily decay capture across rows. Multiply by contract count only when you want absolute position theta magnitude; use signed `theta * 100` with position direction when you need signed position theta.
- `theta_to_premium_ratio`: Absolute theta divided by premium. Use it to compare time decay efficiency relative to premium collected or paid. Higher means faster daily decay relative to the option price.
- `capital_required`: Simplified one-contract capital proxy. Calls use `last_trade_price * 100`; puts use `strike * 100`.
- `theta_efficiency`: `theta_dollars_per_day / (capital_required / 1000)`. Use it to compare daily theta generation per `$1,000` of row-level capital.

## Validation, Freshness, and Screening Fields

- `has_valid_underlying`: True when the underlying price is positive. Use it to reject rows with unusable stock data.
- `has_valid_strike`: True when strike is positive. Use it to reject malformed contracts.
- `has_valid_quote`: True when bid and ask exist, are non-negative, and bid is not above ask. Use it to filter bad quotes.
- `has_valid_iv`: True when implied volatility is positive. Use it to identify rows suitable for Greek calculations.
- `has_valid_greeks`: True when the inputs required for Black-Scholes are valid. Use it to filter out rows with unreliable Greeks.
- `bid_le_ask`: True when bid is less than or equal to ask. Use it as a basic market sanity check.
- `has_nonzero_bid`: True when bid is greater than zero. Use it to find contracts with actual sell-side value.
- `has_nonzero_ask`: True when ask is greater than zero. Use it to find contracts with an actionable offer.
- `has_crossed_or_locked_market`: True when bid is greater than or equal to ask. Use it to detect suspicious market states.
- `quote_age_seconds`: Age of the option quote at fetch time. Use it to avoid stale option prices. Lower is better; high values mean the option quote may be stale.
- `is_stale_quote`: Flag showing whether the option quote exceeds the staleness threshold. Use it to filter delayed quotes.
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
- `Blank`: not currently provided by that provider for this canonical field unless shared app code later derives it

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
| `underlying_price` | Transformed: `fast_info.lastPrice`, fallback `info.regularMarketPrice` / `info.previousClose` | Transformed: `underlying_asset.price`, fallback `underlying_asset.value` | Transformed: first chain `underlyingPrice` |
| `underlying_day_change_pct` | Derived from provider values: `(last_price - previous_close) / previous_close` | Derived from provider values: `(underlying_price - day.previous_close) / previous_close` | Blank: one-call chain payload does not expose a reliable underlying day-change field |
| `historical_volatility` | Derived from provider history: trailing daily log returns | Blank: not currently supplied or derived for Massive | Blank: not currently supplied or derived for Market Data |
| `underlying_price_time` | Transformed: `info.regularMarketTime` normalized to UTC timestamp | Transformed: `underlying_asset.last_updated`, fallback day/trade/quote timestamps | Transformed: first chain `updated` normalized to UTC as the best-available underlying timestamp |
| `underlying_price_age_seconds` | Derived: fetch time minus `underlying_price_time` | Derived: fetch time minus `underlying_price_time` | Derived: fetch time minus `underlying_price_time` |
| `is_stale_underlying_price` | Derived: age compared to `stale_quote_seconds` | Derived: age compared to `stale_quote_seconds` | Derived: age compared to `stale_quote_seconds` |

### Raw Quote and Activity Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `bid` | Direct: `bid` | Transformed: `last_quote.bid`, fallback `last_quote.bid_price` | Direct: `bid` |
| `ask` | Direct: `ask` | Transformed: `last_quote.ask`, fallback `last_quote.ask_price` | Direct: `ask` |
| `last_trade_price` | Transformed: `lastPrice` -> `last_trade_price` | Transformed: `last_trade.price`, fallback `day.close` | Transformed: `last` -> `last_trade_price` (option contract last, not `underlyingPrice`) |
| `volume` | Direct: `volume` | Direct: `day.volume` | Direct: `volume` |
| `open_interest` | Transformed: `openInterest` -> `open_interest` | Direct: `open_interest` | Transformed: `openInterest` -> `open_interest` |
| `implied_volatility` | Transformed: `impliedVolatility` -> `implied_volatility` | Direct/Transformed: top-level `implied_volatility`, coerced numeric | Direct/Transformed: `iv` -> `implied_volatility` |
| `change` | Direct: `change` | Direct: `day.change` | Blank: current options-chain payload does not expose contract change |
| `percent_change` | Transformed: `percentChange` -> `percent_change` | Direct: `day.change_percent` | Blank: current options-chain payload does not expose contract percent change |
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
| `delta_itm_proxy` | Derived: side-normalized delta | Derived: side-normalized delta | Derived: side-normalized delta |
| `probability_itm` | Derived: Black-Scholes `d2` probability | Derived: Black-Scholes `d2` probability unless provider value is present later | Derived: Black-Scholes `d2` probability |
| `gamma` | Derived: Black-Scholes in shared app code | Transformed or Derived: provider `greeks.gamma` preserved, app fills gaps | Direct/Transformed: provider `gamma` preserved, app fills gaps |
| `vega` | Derived: Black-Scholes in shared app code | Transformed or Derived: provider `greeks.vega` preserved, app fills gaps | Direct/Transformed: provider `vega` preserved, app fills gaps |
| `vega_per_day` | Derived: vega divided by days to expiry | Derived: vega divided by days to expiry | Derived: vega divided by days to expiry |
| `theta` | Derived: Black-Scholes daily theta | Transformed or Derived: provider `greeks.theta` preserved, app fills gaps | Direct/Transformed: provider `theta` preserved, app fills gaps |
| `theta_dollars_per_day` | Derived: `abs(theta) * 100` | Derived: `abs(theta) * 100` | Derived: `abs(theta) * 100` |
| `theta_to_premium_ratio` | Derived: absolute theta divided by premium reference | Derived: absolute theta divided by premium reference | Derived: absolute theta divided by premium reference |
| `capital_required` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` | Derived: calls use `last_trade_price * 100`, puts use `strike * 100` |
| `theta_efficiency` | Derived: theta dollars per day per `$1,000` of capital required | Derived: theta dollars per day per `$1,000` of capital required | Derived: theta dollars per day per `$1,000` of capital required |

### Validation, Freshness, and Screening Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `has_valid_underlying` | Derived: underlying price positive | Derived: underlying price positive | Derived: underlying price positive |
| `has_valid_strike` | Derived: strike positive | Derived: strike positive | Derived: strike positive |
| `has_valid_quote` | Derived: bid/ask present, non-negative, and ordered | Derived: bid/ask present, non-negative, and ordered | Derived: bid/ask present, non-negative, and ordered |
| `has_valid_iv` | Derived: implied volatility positive | Derived: implied volatility positive | Derived: implied volatility positive |
| `has_valid_greeks` | Derived: valid Black-Scholes inputs or provider greek present | Derived: valid Black-Scholes inputs or provider greek present | Derived: valid Black-Scholes inputs or provider greek present |
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

### Run Metadata Mapping

| Field | yfinance | massive | marketdata |
| --- | --- | --- | --- |
| `data_source` | Derived/Constant: provider name `yfinance` | Derived/Constant: provider name `massive` | Derived/Constant: provider name `marketdata` |
| `risk_free_rate_used` | Derived/Constant: runtime config value | Derived/Constant: runtime config value | Derived/Constant: runtime config value |

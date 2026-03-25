# opx User Guide

`opx` downloads near-term option chains, enriches them with pricing and screening metrics, writes a timestamped CSV, and serves a local browser UI for inspection.

## Overview

- Fetches call and put chains for configured tickers
- Filters out zero-bid and wide-spread contracts before export
- Limits strikes to a configurable band around spot
- Computes Greeks, expected move, ROM-style metrics, option scoring, and volatility context
- Writes a timestamped CSV plus an append-only run log
- Includes a local browser for exploring the output interactively

The output is designed to be data-focused rather than decision-focused. It does not decide whether to close, roll, or open positions. Instead, it produces a richer dataset that can support those decisions elsewhere.

Warning: Yahoo Finance quote timestamps can lag, and the collected option or underlying data may be stale. Sparse or empty option-chain results are especially common near the regular market open because Yahoo data is delayed and cached, option markets may not have fully formed yet, the `yfinance` API is scraping-based and can be unreliable, and immediate post-open liquidity is often thin. Always check the freshness fields in the CSV or browser before relying on the output for trading decisions.

Warning: Massive options support for this project requires a Massive account with an options plan that exposes the option snapshot data, a usable underlying price, and quote access when you expect `bid` and `ask` to be populated. `Options Basic` does not expose the required access, `Options Starter` is the entry point for delayed options data, and lower tiers may still leave this app with trades but no quote fields. In practice, `bid` and `ask` access may require Massive's highest-cost quote-enabled options plan. Confirm your plan includes the quote and underlying-price coverage you expect before treating the output as current market data.

Warning: Market Data support requires a Market Data account and API token. The provider uses the official `marketdata-sdk-py` client and currently pulls one full options chain per ticker fetch sequence. Market Data's Free Forever tier is 24 hours delayed for both stock and options data, so this provider is not suitable for current-session option monitoring unless your plan includes fresher access.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install
python fetcher.py
python viewer.py
```

Then open `http://127.0.0.1:8000` in your browser.

## Running

Fetch data from the repository root:

```bash
python fetcher.py
```

Run the local viewer:

```bash
python viewer.py
```

The viewer includes:

- a sortable table for the exported CSV
- hover descriptions on column headers pulled from this guide
- a file selector for available CSV exports
- a `Reference` tab that shows the CSV field documentation
- an `Overview` tab for per-ticker snapshot metrics and opportunity highlights
- a `Chain View` tab for per-ticker/per-expiration chart inspection of chain structure, premium, theta efficiency, and screening/liquidity summaries
- a dark/light mode toggle
- header filters, including numeric min/max filtering for numeric columns
- dataset-level header cards for shared run metrics such as premium reference method
- interactive chart marks in `Chain View` that show hover tooltips and open the existing row-detail modal on click

## Output

Each run writes a CSV file to the `output/` directory using a timestamped filename:

```text
output/options_engine_output_YYYYMMDD_HHMMSS.csv
```

`output/` is the standard export directory used by both the fetcher and the viewer.

Operational details that are not row-specific are written to:

```text
logs/opx_runs.log
```

The run log records:

- per-expiration raw row counts returned by the active provider before app-side filtering
- per-ticker raw contract totals and kept-row totals
- provider-library error messages routed into the same log file when available
- final CSV row count and file size after export

If `debug_dump_provider_payload = true`, raw provider payload JSON files are also written under `debug_dump_dir`. Massive dumps are written per API response page with filenames such as `massive_TSLA_snapshot_chain_page_001_...json`, while yfinance dumps use labels such as `yfinance_TSLA_option_chain_2026-04-17_...json`.

## Configuration

Runtime configuration lives in `~/.config/opx/config.toml`. If the file is absent, the app falls back to built-in defaults and uses `yfinance` as the active provider.

If individual config values are missing, malformed, or out of range, the loader applies built-in defaults for those fields and the fetcher prints the resolved values plus any fallback warnings at startup.

Example config:

```toml
[settings]
tickers = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]
data_provider = "yfinance"

# Shared filtering
filters_min_bid = 0.50
filters_min_open_interest = 100
filters_min_volume = 10
filters_max_spread_pct_of_mid = 0.25
filters_max_strike_distance_pct = 0.30
filters_enable = true

# Shared analytics and freshness
risk_free_rate = 0.045
hv_lookback_days = 30
trading_days_per_year = 252
stale_quote_seconds = 21600
max_expiration_weeks = 26

# Shared scoring
option_score_income_weight = 0.30
option_score_liquidity_weight = 0.30
option_score_risk_weight = 0.25
option_score_efficiency_weight = 0.15

# Shared diagnostics
enable_validation = true
debug_dump_provider_payload = false
debug_dump_dir = "debug"

[providers.massive]
api_key = "replace-me"
snapshot_page_limit = 250
request_interval_seconds = 12.0

[providers.marketdata]
api_token = "replace-me"
mode = "delayed"
max_retries = 3
request_interval_seconds = 0.0
```

### Shared Settings

These settings apply regardless of which provider is active.

#### Shared Runtime Defaults

- `TICKERS = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]`: list of underlyings to fetch.
- `data_provider = "yfinance"`: provider implementation used by the fetch pipeline.

#### Shared Filtering Defaults

- `FILTERS_MIN_BID = 0.50`: excludes very low-premium contracts, in addition to the hard `bid == 0` filter.
- `FILTERS_MIN_OPEN_INTEREST = 100`: baseline open-interest threshold used by the screening metrics.
- `FILTERS_MIN_VOLUME = 10`: baseline daily volume threshold used by the screening metrics.
- `FILTERS_MAX_SPREAD_PCT_OF_MID = 0.25`: excludes contracts with spreads wider than 25% of midpoint.
- `FILTERS_MAX_STRIKE_DISTANCE_PCT = 0.30`: keeps only strikes within +/-30% of the latest underlying price.

#### Shared Analytics and Freshness Defaults

- `RISK_FREE_RATE = 0.045`: risk-free rate used in Black-Scholes calculations.
- `HV_LOOKBACK_DAYS = 30`: lookback window for historical volatility.
- `TRADING_DAYS_PER_YEAR = 252`: annualization factor for volatility.
- `STALE_QUOTE_SECONDS = 21600`: staleness threshold for option and underlying quotes.
- `MAX_EXPIRATION_WEEKS = 26`: caps expirations to roughly the next six months by default. Set it to any positive week count you want, or `0` to disable the expiration cap entirely.

#### Shared Scoring Defaults

- `OPTION_SCORE_INCOME_WEIGHT = 0.30`: weight on premium-per-day in the shared `option_score`.
- `OPTION_SCORE_LIQUIDITY_WEIGHT = 0.30`: weight on spread, open interest, and volume in the shared `option_score`.
- `OPTION_SCORE_RISK_WEIGHT = 0.25`: weight on the side-aware delta target in the shared `option_score`.
- `OPTION_SCORE_EFFICIENCY_WEIGHT = 0.15`: weight on days-to-expiration and strike-distance efficiency in the shared `option_score`.

#### Shared Diagnostics Defaults

- `FILTERS_ENABLE = true`: applies the zero-bid, strike-band, and wide-spread row filters after download. Set it to `false` when you want the raw downloaded rows to remain in the exported dataset while still computing metrics and quality flags.
- `ENABLE_VALIDATION = true`: runs shared row-level validation before post-download filtering and file-level validation before export. Set it to `false` when you want to skip validation findings and validation summary output entirely.
- `DEBUG_DUMP_PROVIDER_PAYLOAD = false`: when `true`, dump raw provider payloads to JSON before normalization so missing fields can be inspected directly.
- `DEBUG_DUMP_DIR = "debug"`: directory used for raw provider payload dumps. Dump filenames are prefixed with the provider name.

### Provider Settings

These settings are only used by the matching provider.

#### Massive Settings

- `[providers.massive].api_key`: Massive API key used only when `data_provider = "massive"`.
- `providers.massive.snapshot_page_limit = 250`: per-request Massive snapshot page size used for the option-chain endpoint. Values above `250` are clamped because the Massive snapshot endpoint rejects larger limits.
- `providers.massive.request_interval_seconds = 12.0`: minimum delay between Massive HTTP requests. This default is conservative for delayed-plan usage.

#### Market Data Settings

- `providers.marketdata.api_token`: Market Data API token used only when `data_provider = "marketdata"`.
- `providers.marketdata.mode`: optional Market Data SDK mode. Valid values are `live`, `cached`, and `delayed`. If omitted, the SDK uses its default behavior for your account and plan. Mode support and effective recency depend on the plan you are paying for; the Free Forever tier remains 24 hours delayed.
- `providers.marketdata.max_retries = 3`: retry count for Market Data rate-limit responses (`429`). The provider uses exponential backoff and honors `Retry-After` when the upstream response supplies it.
- `providers.marketdata.request_interval_seconds = 0.0`: optional minimum spacing between Market Data HTTP requests. Leave it at `0.0` unless you want extra pacing for low-credit or low-throughput plans.

### Common Configuration Tasks

- Change `tickers` when you want a different watchlist.
- Switch `data_provider` when you want to use a different market-data implementation.
- Tighten or loosen the `filters_*` threshold values when you want a narrower or broader tradability filter.
- Set `filters_enable = false` when you want to keep rows that would normally be removed by the shared post-download filters.
- Set `enable_validation = false` when you want to skip shared row/file validation and suppress validation summaries.
- Turn on `debug_dump_provider_payload = true` when you need to inspect the raw provider payload and confirm whether fields such as `last_quote`, `underlying_asset`, or Yahoo chain columns were present before normalization.
- Change `max_expiration_weeks` when you want a shorter or longer expiration window, or set it to `0` to disable the max-expiration cutoff.
- Change the shared analytics or freshness settings only if you want different modeling assumptions.
- Change the `option_score_*_weight` values when you want to tune the shared score without changing code. The weights must stay non-negative and their total must stay positive or the loader falls back to defaults.

### Provider-Specific Configuration Tasks

- Add `[providers.massive].api_key` only when you select `massive`.
- Raise or lower `snapshot_page_limit` and `request_interval_seconds` to match your Massive plan and tolerance for throttling.

- Add `[providers.marketdata].api_token` only when you select `marketdata`.
- Set `[providers.marketdata].mode` when you want to force the Market Data SDK to use `live`, `cached`, or `delayed` mode instead of the provider default. Keep in mind that account entitlements still control whether fresher data is actually available.
- Raise or lower `[providers.marketdata].max_retries` when you want a different tolerance for rate-limit retries.
- Set `[providers.marketdata].request_interval_seconds` above `0.0` only when you want additional client-side pacing on top of the provider's normal serial request flow.

## Scoring

`option_score` is a shared derived field in the `0-100` range. It is intended for relative ranking within one run, not as an absolute trading recommendation.

Current scoring logic:

- Expected fill: when `bid_ask_spread_pct_of_mid <= 10%`, scoring assumes fill at midpoint; otherwise it uses `bid + 25%` of the spread
- Income: `premium_per_day` is now derived from `expected_fill_price / max(days_to_expiration, 1)`, then adjusted by implied volatility using a `0.30` IV baseline to form `iv_adjusted_premium_per_day`
- Income scoring: penalizes `iv_adjusted_premium_per_day < 0.01` as near useless, then linearly rewards it from `0.01` up to the `0.05` cap
- Execution: `spread_score` uses prompt tiers with `<10% => 100`, `10-15% => 85`, `15-25%` decaying linearly to `0`, and `>25% => 0`
- DTE: `dte_score` uses prompt tiers with `7-21 => 100`, `5-6 => 75`, `22-35 => 85`, `36-45 => 65`, `<5 => 25`, and `>45 => 30`
- Risk: delta is the only score-driving risk input; `probability_itm` is used only to validate whether the risk model looks inconsistent
- Final score: `score_validation` flags `DISCREPANCY`, `UNDERVALUED`, or `ALIGNED`, and `final_score` applies the corresponding adjustment on top of `option_score`

Default top-level weights:

- `option_score_income_weight = 0.30`
- `option_score_liquidity_weight = 0.30`
- `option_score_risk_weight = 0.25`
- `option_score_efficiency_weight = 0.15`

The four `option_score_*_weight` settings control how much each component contributes to the final score. All weights must be non-negative, and their total must stay positive or the loader falls back to the built-in defaults shown above.

## Runtime Behavior

- The fetcher prints the config path it read, whether the file exists, and the full set of resolved runtime values it will apply.
- Secret values are redacted in that output. For example, the Massive API key and Market Data token are shown as `set` or `not set`, never in plaintext.
- When a config value is invalid and a code default is used instead, the fetcher prints a `Config fallbacks:` block so the override is visible.
- When validation is enabled, the fetcher prints a validation summary after combining ticker frames and before writing the CSV.
- During each ticker fetch, the fetcher prints provider progress, expiration counts, raw provider row counts, normalized-versus-kept row counts, and final kept rows so empty runs can be traced to a specific stage.
- The fetcher exits with status `0` after a successful CSV write, `1` when the run finishes with `No data fetched.`, and `130` when interrupted with `Ctrl+C`.

## Field Reference

The full CSV field reference lives in [FIELD_REFERENCE.md](FIELD_REFERENCE.md).

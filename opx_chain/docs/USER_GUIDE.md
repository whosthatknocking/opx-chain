# opx User Guide

`opx` downloads near-term option chains, enriches them with pricing and screening metrics, writes a timestamped CSV, and serves the local Options Screener UI for inspection.

## Overview

- Fetches call and put chains for configured tickers
- Filters out missing/zero-bid and wide-spread contracts before export
- Limits strikes to a configurable band around spot
- Computes Greeks, expected move, ROM-style metrics, option scoring, and volatility context
- Writes a timestamped CSV plus an append-only run log
- Includes the local Options Screener for exploring the output interactively

The output is designed to be data-focused rather than decision-focused. It does not decide whether to close, roll, or open positions. Instead, it produces a richer dataset that can support those decisions elsewhere.

Warning: Yahoo Finance quote timestamps can lag, and the collected option or underlying data may be stale. Sparse or empty option-chain results are especially common near the regular market open because Yahoo data is delayed and cached, option markets may not have fully formed yet, the `yfinance` API is scraping-based and can be unreliable, and immediate post-open liquidity is often thin. Always check the freshness fields in the CSV or browser before relying on the output for trading decisions.

Warning: Massive options support for this project requires a Massive account with an options plan that exposes the option snapshot data, a usable underlying price, and quote access when you expect `bid` and `ask` to be populated. `Options Basic` does not expose the required access, `Options Starter` is the entry point for delayed options data, and lower tiers may still leave this app with trades but no quote fields. In practice, `bid` and `ask` access may require Massive's highest-cost quote-enabled options plan. Confirm your plan includes the quote and underlying-price coverage you expect before treating the output as current market data.

Warning: Market Data support requires a Market Data account and API token. The provider uses the official `marketdata-sdk-py` client and currently pulls one full options chain plus a stock-quote snapshot per ticker fetch sequence, with additional event requests for earnings and dividends. Market Data's Free Forever tier is 24 hours delayed for both stock and options data, so this provider is not suitable for current-session option monitoring unless your plan includes fresher access.

## Quick Start

```
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/opx-chain"
cp config/example.toml "${XDG_CONFIG_HOME:-$HOME/.config}/opx-chain/config.toml"
opx-fetch
opx-view
```

For local development tools, install the optional extras instead:

```
python -m pip install -e ".[dev]"
python -m playwright install
```

Then open `http://127.0.0.1:8000` in your browser.

## Running

Fetch data with `opx-fetch`.

You can force the shared post-download filter toggle for a single run without editing `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`):

```bash
opx-fetch --disable-filters
opx-fetch --enable-filters
```

These flags override `settings.filters_enable` only for that process. If neither flag is passed, the fetcher uses the configured `filters_enable` value.

You can also override the positions-file path for one run:

```bash
opx-fetch --positions /path/to/runs/<run_id>/positions.csv
```

When `--positions` is omitted, the fetcher still defaults to `$XDG_DATA_HOME/opx-chain/positions.csv` (default `~/.local/share/opx-chain/positions.csv`). If the override path does not exist, the run continues without position-aware ticker expansion or filter bypass. If the file exists but cannot be parsed, the fetcher prints a warning and then continues with the same graceful fallback behavior.

To verify configuration, positions parsing, and storage reachability without provider API calls or output writes, run:

```bash
opx-fetch --dry-run
```

Dry runs print the tickers that would be fetched, the resolved positions source, and the storage backend check. If parquet output is configured, they also verify that `pyarrow` is installed. They do not create run logs, datasets, sidecars, or provider requests.

Check that every option position in the default positions file appears in the latest output dataset:

```bash
opx-check
```

The output line shows the path and when the file was fetched:

```
Output:    ~/.local/share/opx-chain/runs/<run-id>/output/<uuid>.csv  (fetched 2026-04-22 07:07)
```

Pass `--positions` or `--output` to override the defaults:

```bash
opx-check --positions ~/my-positions.csv --output /path/to/artifact.csv
```

`opx-check` exits with code `0` when all positions are found and `1` when any are missing, so it can be used in scripts.
When storage is enabled, the command uses only a complete published dataset and
revalidates its exact stored bytes before checking coverage. A missing,
tampered, schema-incompatible, legacy-unknown, or invalid dataset stops the
check with a clear integrity error. CSV and parquet datasets follow the same
validated path. Supplying `--output` selects an explicit inspection artifact
and validates that artifact before use.
For found contracts, it also prints `passes_primary_screen=true|false`; when the value is `false`, the output prints a short indented `failed_filters:` list with the exact failed filter names such as `filters_max_spread_pct_of_mid` or `filters_min_open_interest`.

Use `--freshness` when you want `opx-check` to recompute current option-chain age from the saved quote timestamps in the CSV:

```bash
opx-check --freshness
```

That summary is intentionally different from the stored `quote_age_seconds`, `underlying_price_age_seconds`, `is_stale_quote`, and `is_stale_underlying_price` fields. Those CSV columns describe freshness at fetch time. `opx-check --freshness` compares `option_quote_time` and `underlying_price_time` to the current clock when you run the command, so an old file will show `stale_now_rows` and `stale_underlyings_now` even if the fetch-time stale flags were `false` when the file was created.

Fetch-time ages use one assessment instant after all ticker quotes and any
bounded refresh have been received. Logs retain `fetch_started_at` separately
from `freshness_assessed_at`. Cached quote timestamps are unchanged; truly
future-dated quotes still have negative ages and are flagged stale.

Run the local viewer:

```
opx-view
```

Open the viewer and launch the page in your default browser:

```bash
opx-view --open
```

The viewer binds to `settings.viewer_host` and `settings.viewer_port` from `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`) by default. `OPX_VIEWER_HOST` and `OPX_VIEWER_PORT` still override those values when you need a one-off launch target.

The viewer includes:

- a sortable table for the selected dataset artifact
- lightweight positions counts, parsed-position fingerprint, and chain coverage metadata on the `Dataset` tab when the selected dataset has a run-level positions snapshot
- shareable tab URLs using `?tab=table`, `?tab=summary`, `?tab=chain`, or `?tab=readme`
- hover descriptions on column headers pulled from this guide
- a file selector for available dataset exports
- a `Reference` tab that shows the CSV field documentation
- an `Overview` tab for per-ticker snapshot metrics plus `Most Profitable`, `Moderate Risk`, `High Conviction Call`, and `High Conviction Put` highlights
- a `Chain View` tab for per-ticker/per-expiration chart inspection of chain structure, premium, theta efficiency, and screening/liquidity summaries
- a dark/light mode toggle
- header filters, including numeric min/max filtering for numeric columns
- dataset-level header cards for shared run metrics such as premium reference method
- interactive chart marks in `Chain View` that show hover tooltips and open the existing row-detail modal on click

## Output

`$XDG_DATA_HOME/opx-chain/` (defaulting to `~/.local/share/opx-chain/`) is the
standard data directory. Setting `[storage] dir = "/path/to/dir"` makes `opx-fetch`
use that directory for its lock, timestamped CSV side writes, `_latest` copy, and
storage-managed run artifacts. Relative `[storage] dir` values resolve under
`$XDG_DATA_HOME/opx-chain/`, not the shell's current working directory.

When storage is enabled (`[storage] enable = true`), each run gets its own
subdirectory under `runs/`:

```text
~/.local/share/opx-chain/runs/<run-id>/run.json           # run metadata
~/.local/share/opx-chain/runs/<run-id>/output/<uuid>.csv       # dataset_format = "csv" (default)
~/.local/share/opx-chain/runs/<run-id>/output/<uuid>.parquet   # dataset_format = "parquet"
```

When `also_write_csv = true` (the default), a timestamped CSV is also written
inside the run's output directory alongside a `_latest` copy:

```text
~/.local/share/opx-chain/runs/<run-id>/output/options_engine_output_YYYYMMDD_HHMMSS.csv
~/.local/share/opx-chain/runs/options_engine_output_latest.csv
```

When storage is disabled, the timestamped CSV and latest copy land directly in `runs/`:

```text
~/.local/share/opx-chain/runs/options_engine_output_YYYYMMDD_HHMMSS.csv
~/.local/share/opx-chain/runs/options_engine_output_latest.csv
```

All three tools (`opx-fetch`, `opx-check`, `opx-view`) discover artifacts through the
storage backend automatically — no `--data-dir` flag is needed.

Operational details that are not row-specific are written to:

```text
~/.local/state/opx-chain/logs/opx_runs.log
```

The run log records:

- the storage `run_id` for storage-backed runs, so run-log lines correlate with
  run metadata and dataset artifacts without timestamp cross-reference
- per-expiration raw row counts returned by the active provider before app-side filtering
- per-ticker raw contract totals and kept-row totals
- provider-library error messages routed into the same log file when available
- final CSV row count and file size after export

If `debug_dump_provider_payload = true`, raw provider payload JSON files are also written under `debug_dump_dir`. Relative `debug_dump_dir` values resolve under `$XDG_DATA_HOME/opx-chain/`; the default `debug_dump_dir = "debug"` becomes `~/.local/share/opx-chain/debug/`. Massive dumps are written per API response page with filenames such as `massive_TSLA_snapshot_chain_page_001_...json`, while yfinance dumps use labels such as `yfinance_TSLA_option_chain_2026-04-17_...json`.

## Configuration

Runtime configuration lives in `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`). If the file is absent, the app falls back to built-in defaults and uses `yfinance` as the active provider.

If individual config values are missing, malformed, or out of range, the loader applies built-in defaults for those fields and the fetcher prints the resolved values plus any fallback warnings at startup.

When `data_provider` is set to a paid provider, required credentials are fail-fast by default. `data_provider = "massive"` requires `[providers.massive].api_key`, and `data_provider = "marketdata"` requires `providers.marketdata.api_token`. Set `auto_fallback_to_yfinance = true` only when you intentionally want missing paid-provider credentials to fall back to `yfinance` instead of stopping the run.

Start from the tracked example at [`config/example.toml`](config/example.toml):

```
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/opx-chain"
cp config/example.toml "${XDG_CONFIG_HOME:-$HOME/.config}/opx-chain/config.toml"
```

Then update provider placeholders in `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`) for the provider you actually use.

### Shared Settings

These settings apply regardless of which provider is active.
Invalid numeric values fall back to the defaults with config warnings. Values that
represent minimums must be non-negative except `filters_min_bid`, which must be
positive when set; percentage caps, lookback windows, and annualization days must
be positive. Non-finite TOML floats such as `inf`, `-inf`, and `nan` are invalid.

#### Shared Runtime Defaults

- `tickers = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]`: list of underlyings to fetch.
- `data_provider = "yfinance"`: provider implementation used by the fetch pipeline.
- `auto_fallback_to_yfinance = false`: when `false`, selecting `massive` without `providers.massive.api_key` or `marketdata` without `providers.marketdata.api_token` raises a clear config error. Set to `true` only for an intentional missing-credential fallback to `yfinance`.

#### Shared Filtering Defaults

- `filters_min_bid = disabled`: bid minimum screen is disabled by default (previously `0.50`). Set it to a positive value to mark contracts below that premium threshold as `passes_primary_screen=false`; it does not remove those rows from the exported dataset.
- `filters_min_open_interest = 100`: baseline open-interest threshold used by the screening metrics.
- `filters_min_volume = 10`: baseline daily volume threshold used by the screening metrics.
- `filters_max_spread_pct_of_mid = 0.25`: excludes contracts with spreads wider than 25% of midpoint.
- `filters_max_strike_distance_pct = 0.35`: keeps only strikes within +/-35% of the latest underlying price.

#### Shared Analytics and Freshness Defaults

- `risk_free_rate = 0.045`: risk-free rate used in Black-Scholes calculations.
- `hv_lookback_days = 30`: lookback window for historical volatility.
- `trading_days_per_year = 252`: annualization factor for volatility.
- `stale_quote_seconds = 10800`: staleness threshold for option and underlying quotes.
- `max_expiration_weeks = 34`: caps expirations to roughly the next eight months by default. Set it to any positive week count you want, or set it to `0` to disable the expiration cap entirely. Do not use `null`; TOML has no null literal.

#### Optional Price Context Defaults

- `price_context.enable = false`: standalone daily-OHLCV support/resistance artifact generation is disabled by default.
- `price_context.lookback_days = 260`: desired usable daily trading-bar window used for moving averages, 20-day boundaries, VWAP, RSI/EMA technical context, and volume-node proxy. Providers whose history API accepts calendar spans, such as yfinance, request a buffered calendar period so the retained store can still satisfy the trading-bar lookback after weekends and holidays.
- `price_context.max_age_days = 7`: maximum age for the latest daily candle. Stale history exports blank numeric price-context fields plus `price_context_staleness_status = STALE`.
- `storage.price_context_ttl = 86400`: minimum age before retrying provider reconciliation for daily price-history bars, separate from faster-moving option-chain and quote caches.

Use `opx-fetch --enable-price-context` to write the artifact alongside a normal option-chain run, or
`opx-fetch --price-context-only` to reconcile the local price-history store and
write only the price-context JSON artifact without exporting option chains.
Use `opx-price-history-backfill` when you want to populate or refresh the
provider-scoped daily-bar store without writing a price-context artifact. For
example, the weekend volatility-advisory backfill can run both supported
providers over the same ticker set:

```bash
opx-price-history-backfill --providers marketdata,yfinance --tickers TSLA,NVDA,GOOGL --refresh
```

The command writes rows independently by provider, ticker, and trading date in
`price-history.db`. Use `--dry-run` to inspect current local coverage without
provider API calls or writes.

Successful storage-backed option-chain fetches automatically ingest the just-
written dataset into `iv-history.db` after the run is finalized. Use
`opx-iv-history-backfill` when you want to replay older retained datasets,
repair missing syncs, refresh existing syncs, or inspect coverage manually.
The retained-dataset replay path does not call provider APIs or create a new
fetch run. Non-refresh replay skips prior successful syncs with stored IV rows
and retries prior failed or empty sync attempts:

```bash
opx-iv-history-backfill --providers marketdata,yfinance --tickers TSLA,NVDA,GOOGL --lookback-days 365
```

The command writes daily aggregate IV rows independently by provider, ticker,
observation date, option type, DTE bucket, and delta bucket in `iv-history.db`.
Use `--dry-run` to inspect matching retained datasets and aggregate row counts
without writing the store.

When retained datasets do not provide enough observation dates, MarketData can
seed the same store from historical option-chain snapshots. This mode is
explicit because it may consume provider API requests. Run a dry-run first to
see the provider/ticker/date request plan:

```bash
opx-iv-history-backfill --providers marketdata --tickers TSLA,NVDA,GOOGL --fetch-historical --sessions 25 --dry-run
```

Then omit `--dry-run` to write aggregate rows to `iv-history.db`. Historical
fetch mode writes only the IV-history store; it does not create retained chain
datasets or pipeline run records. V1 supports MarketData historical chains.
When MarketData historical snapshots omit vendor IV and delta, the backfill
derives them from the historical quote price, strike, expiration, and underlying
price before storing aggregate IV observations.
Existing yfinance IV history can still coexist in the same store when replayed
from retained yfinance datasets.

If the command or a downstream Analytics view reports that `iv-history.db` is
malformed, first run retained-only recovery planning:

```bash
opx-iv-history-backfill --recover-corrupt --providers marketdata --dry-run
```

The output explicitly reports zero provider requests and the candidate row
count. If rows are available, repeat without `--dry-run`. The command builds and
validates a separate database, preserves the malformed file as
`iv-history.corrupt-<UTC timestamp>.db`, and atomically installs the rebuilt
store. It refuses to replace the active file when retained data yields no rows,
the candidate fails validation, or SQLite sidecars indicate an active writer.
Do not add `--fetch-historical`: corrupt-store recovery intentionally uses only
already-retained datasets.

#### Shared Viewer Defaults

- `viewer_host = "127.0.0.1"`: default bind host used by `opx-view`.
- `viewer_port = 8000`: default bind port used by `opx-view`.

#### Shared Scoring Defaults

- `option_score_income_weight = 0.30`: weight on premium-per-day in the shared `option_score`.
- `option_score_liquidity_weight = 0.30`: weight on spread, open interest, and volume in the shared `option_score`.
- `option_score_risk_weight = 0.25`: weight on the side-aware delta target in the shared `option_score`.
- `option_score_efficiency_weight = 0.15`: weight on days-to-expiration and strike-distance efficiency in the shared `option_score`.

#### Shared Diagnostics Defaults

- `filters_enable = true`: applies the missing/zero-bid, strike-band, and wide-spread row filters after download. Set it to `false` when you want the raw downloaded rows to remain in the exported dataset while still computing metrics and quality flags.
- `enable_validation = true`: runs shared row-level validation before post-download filtering and file-level validation before export, including rejection of non-finite numeric values such as `Infinity`. Set it to `false` when you want to skip validation findings and validation summary output entirely.
- `debug_dump_provider_payload = false`: when `true`, dump raw provider payloads to JSON before normalization so missing fields can be inspected directly.
- `debug_dump_dir = "debug"`: directory used for raw provider payload dumps. Relative values are resolved under `$XDG_DATA_HOME/opx-chain/`, so the default becomes `~/.local/share/opx-chain/debug/`. Dump filenames are prefixed with the provider name.

### Provider Settings

These settings are only used by the matching provider.

#### yfinance Settings

- `providers.yfinance.request_interval_seconds = 0.0`: optional minimum spacing between Yahoo Finance property/method calls. Leave it at `0.0` for current behavior, or raise it when Yahoo starts throttling unauthenticated traffic.
- `providers.yfinance.max_retries = 0`: retry count for transient yfinance/Yahoo call failures. The default preserves current behavior; raise it when you prefer a slower but more tolerant default-provider path.
- `providers.yfinance.backoff_seconds = 1.0`: base exponential-backoff delay used between yfinance retries.

#### Massive Settings

- `[providers.massive].api_key`: Massive API key used only when `data_provider = "massive"`.
- `providers.massive.snapshot_page_limit = 250`: per-request Massive snapshot page size used for the option-chain endpoint. Values above `250` are clamped because the Massive snapshot endpoint rejects larger limits.
- `providers.massive.request_interval_seconds = 12.0`: minimum delay between Massive HTTP requests. This default is conservative for delayed-plan usage.
- `providers.massive.max_retries = 3`: retry count for Massive snapshot-chain requests.
- `providers.massive.backoff_seconds = 1.0`: base exponential-backoff delay used between Massive snapshot retries.

#### Market Data Settings

- `providers.marketdata.api_token`: Market Data API token used only when `data_provider = "marketdata"`.
- `providers.marketdata.mode`: optional Market Data SDK mode. Valid values are `live`, `cached`, and `delayed`. If omitted, the SDK uses its default behavior for your account and plan. Mode support and effective recency depend on the plan you are paying for; the Free Forever tier remains 24 hours delayed.
- `providers.marketdata.max_retries = 3`: retry count for transient Market Data failures (`429`, `408`, `5xx`, and request exceptions). The provider uses exponential backoff and honors `Retry-After` when the upstream response supplies it. Expected no-data event responses, such as missing dividend data, are treated as blank fields instead of retried.
- `providers.marketdata.request_interval_seconds = 0.0`: optional minimum spacing between Market Data HTTP requests. Leave it at `0.0` unless you want extra pacing for low-credit or low-throughput plans.
- `providers.marketdata.backoff_seconds = 1.0`: base exponential-backoff delay used between Market Data retries when `Retry-After` is absent.

### Portfolio Positions (`$XDG_DATA_HOME/opx-chain/positions.csv`)

At the start of every run, `opx-fetch` reads `$XDG_DATA_HOME/opx-chain/positions.csv` (default `~/.local/share/opx-chain/positions.csv`, Fidelity export format) to drive two behaviors:

- **Position ticker expansion** _(always active)_: all stock tickers and option-underlying tickers found in the file are added to the effective fetch list for the run, even if they are not listed in `settings.tickers`. Today's expiration is also kept for any ticker with stock or option exposure so that options expiring on the current date are available for position matching.
- **Option filter bypass** _(active only when filters are enabled)_: an exact held option contract and every fetched contract with the same ticker, option type, and strike from that held expiration forward bypass all post-download quality filters. This finite held-strike corridor lets downstream consumers compare forward maintenance contracts even when the strike is far from spot. These rows are included regardless of bid, spread, or strike-distance settings; `max_expiration_weeks` still bounds what the provider fetches. Earlier expirations and other strikes/types retain the ordinary filters. When `filters_enable = false` or `--disable-filters` is used, all rows are already kept unconditionally so the bypass has no effect.

The file is re-read on every run. If the file does not exist, the run continues with normal behavior. If the file exists but cannot be parsed, the run prints a warning and continues without position-aware behavior.

The opx-chain viewer does not browse full portfolio rows. When a selected dataset has a run-level `positions.csv` sidecar, the `Dataset` tab surfaces only parsed stock/option counts, the parsed-position fingerprint, and whether the selected chain artifact covers those parsed holdings. Rich positions browsing and portfolio-specific workflows belong in opx-strategy.

If you need a one-off override, pass `opx-fetch --positions /path/to/positions.csv`. That changes only the file path used for that process; it does not change `filters_enable`, and it does not affect later runs.

The default positions path is user-local and outside version control — place your own export there without risk of committing personal data. The expected format is a UTF-8 or UTF-8-with-BOM standard Fidelity brokerage export with a required `Symbol` column. Stock rows use a plain ticker in the Symbol column; option rows use Fidelity's leading-dash option shorthand in the Symbol column (`-TICKERYYMMDDCSTRIKE` or `-TICKERYYMMDDPSTRIKE`). Do not provide full OCC padded-strike symbols such as `-AAPL261016C00230000`; the positions parser expects Fidelity's plain decimal strike format such as `-AAPL261016C230`.

```
Account Number,Account Name,Symbol,Description,...,Type
XXXXXXXXX,SAMPLE ACCOUNT,AAPL,APPLE INC,,,,,,,,,,,,Margin,
XXXXXXXXX,SAMPLE ACCOUNT,MSFT,MICROSOFT CORP,,,,,,,,,,,,Margin,
XXXXXXXXX,SAMPLE ACCOUNT, -AAPL261016C230,AAPL OCT 16 2026 $230 CALL,,,,,,,,,,,,Margin,
XXXXXXXXX,SAMPLE ACCOUNT, -MSFT260918P380,MSFT SEP 18 2026 $380 PUT,,,,,,,,,,,,Margin,
```

### Common Configuration Tasks

- Change `tickers` when you want a different watchlist.
- Switch `data_provider` when you want to use a different market-data implementation.
- Tighten or loosen the `filters_*` threshold values when you want a narrower or broader tradability filter.
- Set `filters_enable = false` when you want to keep rows that would normally be removed by the shared post-download filters.
- Use `opx-fetch --disable-filters` or `opx-fetch --enable-filters` when you want a one-off override without changing config.
- Set `enable_validation = false` when you want to skip shared row/file validation and suppress validation summaries.
- Turn on `debug_dump_provider_payload = true` when you need to inspect the raw provider payload and confirm whether fields such as `last_quote`, `underlying_asset`, or Yahoo chain columns were present before normalization.
- Change `max_expiration_weeks` when you want a shorter or longer expiration window, or set it to `0` to disable the max-expiration cutoff entirely.
- Change `viewer_host` or `viewer_port` when you want the local viewer to bind to a different interface or port by default.
- Change the shared analytics or freshness settings only if you want different modeling assumptions.
- Change the `option_score_*_weight` values when you want to tune the shared score without changing code. The weights must stay non-negative and their total must stay positive or the loader falls back to defaults.

### Provider-Specific Configuration Tasks

- Set `[providers.yfinance].request_interval_seconds` above `0.0` when Yahoo throttles the default provider path or when you want to slow large multi-expiration runs.
- Raise `[providers.yfinance].max_retries` and adjust `[providers.yfinance].backoff_seconds` when you want transient Yahoo failures to retry instead of failing or returning blank best-effort metadata immediately.

- Add `[providers.massive].api_key` only when you select `massive`.
- Raise or lower `snapshot_page_limit`, `request_interval_seconds`, `max_retries`, and `backoff_seconds` to match your Massive plan and tolerance for throttling.

- Add `[providers.marketdata].api_token` only when you select `marketdata`.
- Set `[providers.marketdata].mode` when you want to force the Market Data SDK to use `live`, `cached`, or `delayed` mode instead of the provider default. Keep in mind that account entitlements still control whether fresher data is actually available.
- Raise or lower `[providers.marketdata].max_retries` and `[providers.marketdata].backoff_seconds` when you want a different tolerance for transient Market Data retries.
- Set `[providers.marketdata].request_interval_seconds` above `0.0` only when you want additional client-side pacing on top of the provider's normal serial request flow.

## Filtering

The shared filters are a first-pass dataset gate that runs before ranking and overview heuristics.

Objective:

- The filtering layer exists to trim the downloaded chain into a more tradable, reviewable dataset.
- Its job is to remove contracts that are clearly too stale, too illiquid, too far from spot, or too weakly quoted to be useful for practical short-premium screening.
- It is not intended to define the strategy by itself; it is an executability and relevance gate ahead of scoring.

How to interpret it:

- Tightening the filters makes the dataset narrower and more execution-focused, usually at the cost of excluding speculative or thinly traded contracts.
- Loosening the filters broadens coverage, but it also increases the chance that high-ranking rows are driven by weak quotes, sparse volume, or far-from-spot strikes.
- `filters_enable = false` is mainly useful when you want to inspect the raw normalized rows while still computing the same metrics and quality flags. `opx-fetch --disable-filters` is the one-run equivalent.

## Scoring

`option_score` is a shared derived field in the `0-100` range. It is intended for relative ranking within one run, not as an absolute trading recommendation.

Objective:

- The score exists to rank contracts by overall short-premium attractiveness after normalization, so the top of the dataset favors contracts that look richer, cleaner, and more executable rather than merely highest premium.
- It is meant to compress several competing concerns into one sortable signal: income quality, liquidity and spread quality, practical time-to-expiration, and side-aware risk posture.
- It is not meant to predict market direction, replace discretionary thesis work, or tell you whether a contract is universally "good" outside the current run and current filter set.

How to interpret it:

- Higher `option_score` means the row looks stronger on a combined basis, not just on one metric like raw ROM or premium.
- A lower score does not necessarily mean the contract is unusable; it usually means some combination of spread quality, strike positioning, DTE profile, or risk efficiency is weaker than the higher-ranked alternatives in the same export.
- `delta_safety_pct` is exported as a simple inverse-delta companion to `delta_abs`. Higher values mean lower absolute delta and typically more distance from an at-the-money or in-the-money risk profile.
- `final_score` is the better summary value when you want the score after the row-level validation adjustment has been applied.

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

## Event Risk

The fetcher fetches upcoming corporate event data once per ticker and broadcasts it to every option row for that underlying. This gives every exported row a consistent view of near-term catalyst exposure regardless of expiration or strike.

What is fetched:

- Next upcoming earnings report date, whether that date is estimated, its provider source/confidence metadata, and how many days away it is
- Next upcoming ex-dividend date, provider source/confidence metadata, how many days away it is, and the associated per-share dividend amount

Derived flags:

- `next_earnings_date_is_estimated`: True when the provider documents the next earnings date as estimated rather than confirmed
- `earnings_within_5d`: True when an earnings report falls within 5 calendar days and before the contract expires
- `earnings_within_10d`: True when an earnings report falls within 10 calendar days and before the contract expires
- `ex_div_within_3d`: True when an ex-dividend date falls within 3 calendar days and before the contract expires
- `event_risk_score`: Composite 0–100 score combining earnings and dividend proximity for events that occur before expiration. Earnings within 5 days contributes 60 points, within 10 days contributes 30 points. Ex-dividend within 3 days adds 40 points, within 7 days adds 20 points. The total is capped at 100.

Provider availability:

- `marketdata`: earnings and dividend event data are fetched for every ticker
  - `next_earnings_date` uses Market Data `reportDate` for upcoming earnings events; Market Data `date` is the fiscal period end and is not used as the event date
  - `next_earnings_date_is_estimated`, `next_earnings_date_source`, and `next_earnings_date_confidence` preserve that upcoming Market Data `reportDate` values are estimates until the company has reported
  - `next_ex_div_date_source` and `next_ex_div_date_confidence` preserve that dividend dates came from Market Data `exDate`
  - day counts for expirations, earnings, and ex-dividend dates use the `America/New_York` market calendar so the CSV stays aligned with Market Data's documented date semantics
  - non-quota event endpoint failures leave the affected event fields blank and log `marketdata_event_degraded`; authentication and quota/rate-limit failures still abort so operator-facing provider-limit state is not hidden
- `yfinance`: event fields are populated on a best-effort basis from Yahoo metadata (`info`, `calendar`, and `dividends`) when future dates are available, with source/confidence metadata when a date is selected; blanks remain expected when Yahoo does not expose usable future event data
- `massive`: all event fields are blank for this provider

How to use it:

- Use `earnings_within_5d` or `earnings_within_10d` as a hard filter when screening contracts that must not span an earnings announcement
- Use `ex_div_within_3d` as a warning for short call positions on dividend-paying underlyings where early assignment risk is elevated near the ex-date
- Use `event_risk_score` as a secondary ranking signal to down-weight otherwise attractive contracts that carry near-term binary event exposure

## Price Context

Price context is optional and provider-backed. When enabled, `opx-chain`
reconciles a durable daily-OHLCV history store, computes deterministic flat
fields from the stored bars, and writes a standalone versioned JSON artifact. It
does not change the canonical option-chain CSV schema. Missing, stale, or
provider-failed history never fails the option-chain run; the numeric context
fields remain blank and `price_context_staleness_status` explains the state.

The local history store lives at `price-history.db` under the opx-chain data
directory, or under `[storage].dir` when that base directory is configured. New
tickers fetch the configured lookback. Existing tickers reuse stored historical
bars and fetch only required backfill or recent tail data after
`storage.price_context_ttl` expires.

Durable implied-volatility history for volatility advisory features lives in
`iv-history.db` under the same base directory. Successful storage-backed
option-chain fetches automatically populate it from the just-written dataset;
`opx-iv-history-backfill` can also replay retained option-chain datasets or
explicit MarketData historical option-chain fetches when retained datasets are
too sparse. The historical-fetch path writes only aggregate IV rows and may
consume provider API requests. MarketData historical snapshots may omit vendor
IV and delta, so opx-chain derives missing values from the historical option
quote before aggregation. The store keeps provider-scoped daily aggregate IV
observations so downstream consumers can compute ticker-wide and DTE-bucket IV
percentiles without relying on a current-chain proxy.

Current fields include `support_1`, `support_2`, `resistance_1`,
`resistance_2`, `20d_high`, `20d_low`, `50dma`, `200dma`, `vwap`,
`volume_profile_high_volume_node`, `gap_fill_level`, and
`pre_earnings_move_pct`, `rsi_14`, `ema_20`, `ema_50`,
`ema_cloud_state`, and `price_vs_ema50_pct`. Metadata fields include
`price_context_as_of`, `price_context_age_days`, `price_context_source`,
`price_context_lookback_trading_days`, `price_context_calculation_method`, and
`price_context_staleness_status`.

The RSI/EMA fields are deterministic market-data features only. opx-chain does
not assign strategy meaning to them; downstream consumers decide whether to
display, tag, rank, or ignore them.

Provider availability:

- `marketdata`: uses `stocks.candles(..., resolution="D", adjust_splits=true)`.
- `yfinance`: uses adjusted daily `Ticker.history(...)`.
- `massive`: currently returns blank price-context defaults.

The latest artifact is written to `price_context_latest.json` under the runs
directory, with timestamped copies named `price_context_YYYYMMDD_HHMMSS.json`.
Consumers that need row-level context should join artifact records to option rows
by `ticker` / `underlying_symbol`.

## Runtime Behavior

- The fetcher prints the config path it read, whether the file exists, and the full set of resolved runtime values it will apply.
- When `--enable-filters`, `--disable-filters`, `--enable-price-context`,
  `--disable-price-context`, or `--price-context-only` is passed, the fetcher
  prints a `CLI override:` line before the resolved config so the one-run
  override is explicit.
- Secret values are redacted in that output. For example, the Massive API key and Market Data token are shown as `set` or `not set`, never in plaintext.
- When a config value is invalid and a code default is used instead, the fetcher prints a `Config fallbacks:` block so the override is visible.
- When validation is enabled, the fetcher prints a validation summary after combining ticker frames and before writing the CSV.
- Always-on option-chain integrity checks run before lossy provider coercion,
  before filtering, at export serialization, and on storage-backed semantic
  reads. Fatal failures stop publication and include bounded finding codes and
  samples; they do not depend on `settings.enable_validation`.
- Storage-managed datasets remain hidden while a run is `running`, `failed`, or
  `interrupted`. A dataset is listed only after the owning run reaches
  `complete` with valid integrity metadata and available neutral dataset facts.
- During each ticker fetch, the fetcher prints provider progress, expiration counts, raw provider row counts, normalized-versus-kept row counts, and final kept rows so empty runs can be traced to a specific stage.
- The fetcher exits with status `0` after a successful CSV write, `1` when the run finishes with `No data fetched.`, and `130` when interrupted with `Ctrl+C`.

## Field Reference

The full CSV field reference lives in [FIELD_REFERENCE.md](FIELD_REFERENCE.md).

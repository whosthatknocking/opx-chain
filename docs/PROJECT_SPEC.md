# Overall Project Specification: opx-chain

## 1. Overview

`opx-chain` is a single-provider options data fetcher and local viewer.

The project downloads option-chain data for configured tickers, normalizes that data into a stable canonical CSV schema, computes shared analytics, writes timestamped exports, and serves the local Options Screener UI for inspection.

Current supported providers:

- `yfinance`
- `massive` (`Massive / Polygon.io`)
- `marketdata` (`Market Data`)

Core product rules:

- exactly one provider is active for a run
- output files must not mix providers
- the canonical CSV schema is the main product contract
- provider-specific data should map into existing canonical fields where semantics match
- `opx-view` remains unchanged as the viewer CLI entrypoint name

Current minimum supported runtime:

- Python `3.10+`

## 2. Current Product Scope

The project currently supports:

- config-driven provider selection
- config-driven runtime thresholds and fetch behavior
- a user-supplied portfolio positions input file at `$XDG_DATA_HOME/opx-chain/positions.csv` for ticker expansion and position-aware filter bypass
- provider-backed fetches through `yfinance` or `massive`
- provider-backed fetches through `marketdata`
- canonical CSV export with shared derived metrics
- provider-aware field reference documentation
- a local viewer for exported dataset artifacts (`opx-view`); `--data-dir DIR` scans
  an arbitrary directory for `.csv` and `.parquet` files
- provider debug payload dumps for raw-response inspection
- opt-in storage layer (`[storage] enable = true`) with filesystem and SQLite
  backends, parquet artifact format, configurable dataset retention, and a
  provider response cache; see `docs/STORAGE_SPEC.md`
- per-ticker corporate event data (earnings dates and ex-dividend dates) fetched from the active provider when supported, then broadcast to all option rows as event risk flags and a composite `event_risk_score`
- `marketdata` fetches event data via `stocks/earnings/{symbol}/` (SDK) and `stocks/dividends/{symbol}/` (direct HTTP), including an explicit earnings-date estimate flag
- `yfinance` fetches best-effort event data from Yahoo metadata (`info`, `calendar`, and `dividends`) when future dates are available

The project does not currently aim to:

- merge rows from multiple providers in one run
- auto-fallback between providers during a fetch
- expose provider-specific scratch fields in the CSV by default
- act as a live trading terminal
- implement a strategy engine, portfolio action engine, or trade-decision workflow
- implement a secret-management system beyond local user config
- own downstream strategy policy or absorb package-local helper behavior from
  `opx-strategy`; only documented market-data, storage, export, dataset,
  option-chain, and positions-parsing contracts belong in `opx-chain`

## 3. Naming and Packaging

The project name and repository name are `opx-chain`; the Python package path is `opx_chain`.

Implemented naming rules:

- the Python package path is `opx_chain`
- documentation and user-facing commands use `opx-chain`
- `opx-view` remains the unchanged viewer CLI entrypoint name
- the viewer implementation lives in `opx_chain.viewer`
- no temporary compatibility layer for the old package name remains in the repo

## 4. Runtime Model

### 4.1 Single Active Provider

At runtime there is exactly one active provider.

Allowed provider keys:

- `yfinance`
- `massive`
- `marketdata`

Behavior:

- all fetches for a run use the configured provider only
- all exported rows carry `data_source`
- viewer dataset metadata surfaces the provider when the dataset is single-provider
- shared logs use provider-neutral wording such as `raw_provider_rows`

### 4.2 Config Source

The single source of truth for runtime settings is:

- `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`)
- `config/example.toml` is the tracked template users copy into that path

The config loader is responsible for:

- provider selection
- ticker selection
- filter thresholds and enable/disable behavior
- analytics, freshness, and expiration-window settings
- viewer bind host and port
- option-score weight tuning
- validation enable/disable behavior
- Massive credentials
- Market Data credentials
- debug-dump settings
- Massive request pacing and page-size settings
- using the default portfolio positions input path at `$XDG_DATA_HOME/opx-chain/positions.csv`

Defaults:

- if the config file is missing, the app uses built-in defaults
- the default provider is `yfinance`
- shared filter config uses `settings.filters_*` keys
- the objective of shared filtering is to gate the dataset for baseline tradability and relevance before ranking, not to act as a standalone trade thesis
- current built-in filter defaults include:
  - `filters_max_spread_pct_of_mid = 0.25`
  - `filters_max_strike_distance_pct = 0.35`
  - `filters_min_bid = disabled` (set to a positive value to include bid threshold in `passes_primary_screen`; rows are not removed solely by this threshold)
  - `filters_min_open_interest = 100`
  - `filters_min_volume = 10`
  - `filters_enable = true`
- current built-in freshness default is `stale_quote_seconds = 10800`
- malformed or unsupported config values fall back to code defaults
- startup output prints the resolved config values actually applied
- secrets are redacted in startup output
- config fallback warnings are printed when defaults are applied

### 4.3 Secrets

Provider credentials are local-only configuration.

Current credential model:

- `yfinance` requires no secret
- `massive` reads `[providers.massive].api_key` from `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`)
- `marketdata` reads `[providers.marketdata].api_token` from `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`)

Current Market Data request controls:

- optional `[providers.marketdata].mode`
- valid values: `live`, `cached`, `delayed`
- if omitted, the SDK default behavior is used
- optional `[providers.marketdata].max_retries`
- default `3`
- used for transient Market Data HTTP retry handling with exponential backoff
- optional `[providers.marketdata].request_interval_seconds`
- default `0.0`
- adds client-side spacing between Market Data HTTP requests when needed
- optional `[providers.marketdata].backoff_seconds`
- default `1.0`
- controls the base exponential-backoff delay for Market Data transient retries when `Retry-After` is absent

Current yfinance request controls:

- optional `[providers.yfinance].request_interval_seconds`
- default `0.0`
- adds client-side spacing between Yahoo Finance property/method calls when needed
- optional `[providers.yfinance].max_retries`
- default `0`
- controls retry attempts around transient Yahoo/yfinance call failures
- optional `[providers.yfinance].backoff_seconds`
- default `1.0`
- controls the base exponential-backoff delay between yfinance retries

Current Massive request controls:

- optional `[providers.massive].snapshot_page_limit`
- default `250`, clamped to the provider endpoint maximum
- optional `[providers.massive].request_interval_seconds`
- default `12.0`
- adds client-side spacing between Massive HTTP requests
- optional `[providers.massive].max_retries`
- default `3`
- used for snapshot-chain retry handling with exponential backoff
- optional `[providers.massive].backoff_seconds`
- default `1.0`
- controls the base delay for Massive retry backoff

Rules:

- secrets must never be stored in tracked repo files
- secrets must never be printed in full
- secrets must never be copied into docs or logs

Current missing-key behavior:

- if `massive` is selected but `[providers.massive].api_key` is absent, runtime config falls back to `yfinance` and records a clear warning
- if `marketdata` is selected but `[providers.marketdata].api_token` is absent, runtime config falls back to `yfinance` and records a clear warning
- if `massive` is selected with invalid credentials, the Massive provider fails clearly when used
- if `marketdata` is selected with invalid credentials, the Market Data provider fails clearly when used

## 5. Provider Implementations

### 5.1 Shared Provider Contract

Providers implement `opx_chain.providers.base.DataProvider` and map vendor payloads into the canonical schema.

Canonical runtime interface:

```python
class DataProvider(ABC):
    def prepare_ticker_fetch(self, ticker: str) -> None: ...
    @property
    def external_logger_names(self) -> tuple[str, ...]: ...
    def debug_dump_payload(self, ticker: str, label: str, payload) -> Path | None: ...
    def load_ticker_events(self, ticker: str) -> dict: ...
    def load_price_history(self, ticker: str, *, lookback_days: int) -> pd.DataFrame: ...
    def load_historical_option_chain_frame(
        self,
        ticker: str,
        *,
        observation_date: date,
    ) -> pd.DataFrame: ...
    def load_underlying_snapshot(self, ticker: str) -> dict: ...
    def list_option_expirations(self, ticker: str) -> list[str]: ...
    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames: ...
    def normalize_option_frame(
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame: ...
```

Shared contract rules:

- preserve the canonical CSV schema unless there is a documented reason to change it
- use provider-native values directly when semantics match canonical fields
- transform provider fields when normalization is needed
- derive fields in app code only when provider values are absent or unsuitable
- leave fields blank rather than map misleading vendor values into canonical columns

### 5.2 YFinance Provider

Current characteristics:

- no provider account required
- supports underlying snapshot, expiration discovery, and option-chain fetches
- supports best-effort earnings-date and ex-dividend-date enrichment from Yahoo metadata
- uses app-derived analytics for many canonical fields
- may return stale, delayed, or sparse data, especially near the market open

### 5.3 Massive Provider

Current characteristics:

- backed by the official `massive` client library
- requires account onboarding and API key setup
- uses `RESTClient.list_snapshot_options_chain(...)` as the per-ticker collection path
- derives underlying details, expiration discovery, and contract rows from the returned snapshot payload

Implemented Massive behavior:

- request page size is configurable and clamped to the endpoint maximum of `250`
- request spacing is configurable through `providers.massive.request_interval_seconds`
- retry count is configurable through `providers.massive.max_retries`
- retry exponential-backoff base is configurable through `providers.massive.backoff_seconds`
- request caller header identifies the app as `opx-chain/<version>`
- fetch progress prints per-page API status and row-count progress
- raw per-response payload dumps can be written under `$XDG_DATA_HOME/opx-chain/debug/`

Field-mapping rules already implemented for Massive include:

- `underlying_asset.ticker -> underlying_symbol`
- `details.ticker -> contract_symbol`, stripping the `O:` prefix
- `underlying_asset.price`, fallback `underlying_asset.value` -> `underlying_price`
- `last_quote.bid/ask` -> canonical `bid` / `ask`
- top-level `implied_volatility` -> canonical `implied_volatility`
- provider greeks populate canonical greek columns when semantics match
- `is_in_the_money` is derived from spot versus strike because the snapshot model does not expose a direct canonical flag

### 5.4 Market Data Provider

Current characteristics:

- backed by the official `marketdata-sdk-py` client library
- requires account onboarding and API token setup
- uses one full `options.chain(..., expiration="all")` request per ticker fetch sequence
- derives expirations and per-expiration option frames from the cached full-chain payload
- plan access affects data recency; Market Data Free Forever is 24 hours delayed for both stocks and options

Implemented Market Data behavior:

- uses the official SDK client rather than ad hoc raw HTTP calls
- suppresses the SDK startup rate-limit probe so provider initialization does not spend an extra API call
- supports optional SDK request mode selection through `[providers.marketdata].mode`
- retries `429`, `408`, `5xx`, and transient request exceptions with configurable exponential backoff, and honors `Retry-After` when present
- treats expected event `no_data` responses, such as absent dividend data, as blank event fields rather than retryable failures
- optional client-side request spacing is available through `[providers.marketdata].request_interval_seconds`
- request caller header identifies the app as `opx-chain/<version>`
- fetch progress prints per-request API status and row-count progress
- raw response payload dumps can be written under `$XDG_DATA_HOME/opx-chain/debug/`

Field-mapping rules already implemented for Market Data include:

- `optionSymbol -> contract_symbol`
- `underlying -> underlying_symbol`
- `stocks/quotes/{symbol}/ last -> underlying_price`, with `updated` and `changepct` from the same quote row supplying `underlying_price_time` and `underlying_day_change_pct`
- optional daily price context uses `stocks.candles/{resolution}/{symbol}/` via the official SDK with `resolution="D"`, `countback=price_context.lookback_days`, and split adjustment enabled; yfinance uses a buffered calendar-day request to satisfy the same trading-bar lookback contract; the artifact also derives deterministic RSI/EMA technical fields while leaving all strategy interpretation to downstream consumers
- `last -> last_trade_price` for the option contract itself; `underlyingPrice` is not used for `last_trade_price`
- `updated -> option_quote_time` for option rows; if stock quotes are unavailable, the latest chain row with a usable `underlyingPrice` is used as a fallback for `underlying_price` and `underlying_price_time`
- `bid`, `ask`, `last`, `openInterest`, `volume`, `iv`, and greeks map directly into canonical fields
- `stocks/earnings/{symbol}/ reportDate` supplies the projected earnings event date for upcoming Market Data rows; `date` is the fiscal period end for the report and is not used as the event date. Rows whose `reportedEPS` is already populated are treated as historical and excluded from upcoming-event selection even if a stale future estimate remains
- `stocks/dividends/{symbol}/ exDate` supplies the next ex-dividend date and associated amount; event source/confidence metadata is preserved for both earnings and dividends
- runtime `today` and numeric Market Data event dates are interpreted on the `America/New_York` market calendar so expiration and catalyst day-count fields do not drift on non-Eastern hosts
- process runtime config may be cached within one market-calendar date, but long-running processes must refresh the cached config when the `America/New_York` date changes so DTE, event day counts, and expiration cutoffs do not freeze overnight

### 5.5 Optional Price Context

Price context is an optional standalone signal below the strategy engine. It can
run alongside an option-chain fetch or independently through
`opx-fetch --price-context-only`. It uses the same active provider as the option
chain run and reconciles daily OHLCV bars into the durable local
`price-history.db` store before deriving the JSON artifact. New tickers backfill
the configured lookback; existing tickers fetch only required backfill or recent
tail data after the price-context TTL expires. It does not alter the canonical
option-chain CSV schema; consumers join the versioned JSON artifact by ticker
when they need row-level price context.

`opx-price-history-backfill` provides the operational backfill path for
volatility-advisory feature readiness. It reconciles the same `price-history.db`
daily-bar store without writing option-chain datasets or price-context
artifacts. The store is keyed by provider, ticker, and trading date, so
`marketdata` and `yfinance` backfills can coexist for the same ticker set
without overwriting each other. `--dry-run` reports current local coverage
without provider API calls, and `--refresh` bypasses the price-history sync TTL
when the operator intentionally wants a weekend refresh.

Historical-IV coverage is maintained automatically for successful
storage-backed option-chain fetches: after a run and dataset are finalized,
opx-chain ingests the just-written dataset into `iv-history.db` without
changing the fetch result if the IV-history sync fails. `opx-iv-history-backfill`
provides the corresponding manual replay, repair, and seeding path. It replays
retained option-chain datasets from opx-chain storage and writes daily
aggregate IV observations into `iv-history.db`. The retained-dataset replay
path does not call provider APIs, create new option-chain datasets, or write
storage run records.
Rows are keyed by provider, ticker, observation date, option type, DTE bucket,
and delta bucket, which lets downstream volatility-advisory consumers compute
historical IV percentiles from durable local history instead of from the current
chain snapshot only.

Current provider behavior:

- `marketdata`: fetches daily split-adjusted stock candles from the official SDK.
- `yfinance`: fetches adjusted daily `Ticker.history(...)`.
- `massive`: leaves price context blank until a Massive daily-history adapter is added.

Stale or missing price history is non-fatal. The price-context artifact keeps
numeric fields blank and surfaces `price_context_staleness_status` as `MISSING`,
`STALE`, or `ERROR` so downstream consumers can warn operators without
pretending stale levels are usable.

## 6. Output Contract

### 6.1 Canonical CSV

The canonical CSV schema is the primary product contract.

Requirements:

- exported rows include `data_source`
- shared export stays pinned to the canonical column set
- the CSV always emits the full canonical column set in canonical order, even when some fields are blank for the active provider
- unexpected provider-specific scratch fields are dropped from export
- provider-specific branches should not create different CSV shapes
- shared derived fields such as `quote_quality_score` and `option_score` must be computed consistently across providers
- viewer-facing summaries should consume the same exported derived fields rather than separate provider-specific ranking logic

Canonical field sources may be:

- direct provider values
- transformed provider values
- app-derived values
- blank when the provider does not expose the required source data or the current implementation intentionally leaves that field unpopulated for the active provider

Provider-specific field availability and mapping behavior are documented in:

- `docs/FIELD_REFERENCE.md`

### 6.1.1 Option-chain integrity and publication

Option-chain integrity is a provider-neutral data contract, separate from the
older advisory validation summary. Fatal integrity failures stop the affected
fetch path; they are never downgraded to ordinary per-ticker provider errors or
silently coerced to nulls.

The same shared contract is enforced at four material boundaries:

- raw provider response, before lossy canonical coercion
- complete canonical ticker frame, before filtering can remove bad rows
- combined export frame and exact serialized bytes
- storage-backed semantic read, including content-hash and schema checks

Required contract identity, required numeric/time fields, finite nonnegative
market values, `bid <= ask`, unique contract symbols, unique canonical contract
keys, and symbol/column identity agreement are fatal invariants for published data. Findings use
bounded provider-neutral codes and samples. Provider names and upstream error
text may be recorded as provenance, but they do not alter the validation rules.

Acquisition has one bounded quote-only exception: after checking every row for
structural corruption, finite nonnegative crossed quotes (including positive
bid/zero ask) receive one provider refresh per affected expiration, bypassing
the local chain cache. A provider may internally request the whole ticker.
If that refresh raises a recognized network/timeout transport error, quarantine
the unresolved original quotes and retain other validated rows. Authentication,
quota, malformed responses and normalization errors are not transport waivers.
Evidence includes nullable `refresh_error` with only the exception class, never
upstream exception text or credential-bearing URLs.
Unresolved quotes are quarantined, not corrected, and never reach enrichment,
filter exemptions for held contracts, or published datasets. At most ten rows
and ten percent of the ticker frame (with a one-row floor) can qualify.
The denominator remains the original full ticker frame when validating a
refreshed expiration subset. Widespread corruption and all other fatal findings
still stop acquisition.
An empty remaining frame is not publishable. Null/malformed required quotes
remain fatal until their provider semantics have a separately approved contract.

Ticker results use `ok_with_warnings` with JSON evidence in `error_summary`:
schema version, provider, ticker, check time, refresh count, quarantine count,
and original/refreshed quote values and contract identities. The run log also
retains this evidence. Repaired quotes retain a warning even when no rows are
ultimately removed. These are complete fetches with quality warnings, not clean
coverage; downstream consumers must expose the warning and suppress actions
requiring any excluded held-contract price.

With storage enabled, `write_dataset` only stages a checked artifact. Its exact
run can finalize `complete` only with `integrity_status=valid` and
`dataset_facts_status=available`. Completed dataset metadata remains
discoverable for history and diagnostics even if it later evaluates invalid or
unknown; metadata lookup alone never authorizes semantic use. Semantic readers
must call `load_validated_option_chain_dataset(dataset_id)` and use the returned
exact handle, frame, summary, and facts together. Strategy thresholds and
portfolio meaning remain consumer responsibilities.

### 6.2 Logging and Progress Output

Runtime output is provider-neutral and intended to make fetch progress visible.

Current behavior includes:

- startup output for resolved config state
- optional shared validation summary before export
- provider name shown in shared run logging
- ticker- and expiration-level progress
- raw provider row counts
- kept-row counts after normalization and filtering
- Massive per-page API status and cumulative result counts
- Market Data per-request API status and result counts
- final row count and output-path reporting
- viewer summary highlights that use shared derived scores from the export

### 6.3 Shared Scoring

The product includes a shared provider-agnostic `option_score` field.

Requirements:

- `option_score` is a canonical derived field in the `0-100` range
- its objective is to rank contracts by combined short-premium attractiveness rather than by raw premium or ROM alone
- it is computed from shared normalized fields only, not provider-specific scratch fields
- the current score combines IV-adjusted income quality, spread execution quality, DTE execution quality, delta-only risk, and theta efficiency
- `premium_per_day` is derived from a prompt-aligned `expected_fill_price`
- `probability_itm` is validation-only and should not directly drive row ranking
- it should help users surface richer, cleaner, and more inspectable option-chain rows without pretending to be a trade recommendation system
- score weights are configurable through runtime config so tuning does not require code changes
- the configured weights must remain non-negative and sum to a positive total; otherwise defaults are used
- score output is visible both in the exported CSV and in the local viewer
- row-level score validation produces `score_validation`, `score_adjustment`, and `final_score`
- current viewer summary heuristics rank only rows passing `passes_primary_screen` when that field exists and prefer `final_score` as the score-aware tie-breaker

### 6.4 Exit Status

Current CLI exit behavior:

- `0` when a CSV is written successfully
- `1` when the run completes but no data is fetched
- `130` when interrupted with `Ctrl+C`

## 7. Operational Safeguards

### 7.1 Single-Run Lock

The fetcher uses a lock file to prevent concurrent runs.

Current behavior:

- a run acquires `<data-dir>/fetcher.lock`, where `<data-dir>` is
  `[storage].dir` when configured and otherwise `$XDG_DATA_HOME/opx-chain`;
  relative `[storage].dir` values resolve under `$XDG_DATA_HOME/opx-chain`
- a second run exits clearly if the lock is already held
- the lock file is removed when the run finishes or is interrupted

### 7.2 Debug Payload Dumps

Debug dumping is shared across providers.

Current behavior:

- controlled by config flags
- raw payload files are written under `$XDG_DATA_HOME/opx-chain/debug/`
- dump filenames are prefixed with provider name
- Massive dumps are written per HTTP response page and include page numbers
- yfinance dumps cover underlying snapshots, expiration lists, and option-chain payloads

### 7.3 Portfolio Positions Input

The fetcher can consume a user-supplied portfolio positions file.

Current behavior:

- the default input path is `$XDG_DATA_HOME/opx-chain/positions.csv`
- `opx-fetch` accepts an optional `--positions <path>` CLI override for one run
- the file is treated as user-managed input, not generated output
- stock tickers and option-underlying tickers found in the file are added to the effective fetch list for the run
- today's expiration is kept for any ticker with stock or option exposure in the positions file
- matching option contracts and same-ticker/type/strike contracts from each
  held expiration forward bypass post-download quality filters when filters are
  enabled; the configured provider expiration window still bounds this
  position-related corridor
- if the file is absent or cannot be parsed, the run continues without position-aware behavior
- the resolved positions path is logged at startup for run auditability
- `--positions` and `--enable-filters` / `--disable-filters` are independent; supplying a positions path does not change the filter toggle

### 7.4 Shared Validation

Shared validation is configurable and provider-agnostic.

Current behavior:

- controlled by `settings.enable_validation`
- row-level validation runs after normalization/enrichment and before post-download filtering
- row-level numeric validation rejects missing, non-numeric, and non-finite values
  (`NaN`, `Infinity`, `-Infinity`) in canonical numeric fields
- file-level validation runs on the combined frame before export
- validation findings use `warning` and `error` severities
- validation errors do not stop the run or block CSV export
- when validation is enabled, the run prints a validation summary before the CSV write

This configurable validation surface is advisory and remains useful for broad
quality reporting. It does not waive or replace the always-on option-chain
integrity contract in §6.1.1.

### 7.5 Read-Time Freshness Check

This requirement applies to downstream consumers of the exported CSV — it is not enforced by `opx-chain` itself. `opx-chain` publishes freshness fields in the export; what consumers do with them is governed by `docs/EXTERNAL_INTERFACE_SPEC.md` §6.

Any consumer of the exported CSV should revalidate data freshness at read time. The `is_stale_underlying_price` and `quote_age_seconds` fields are computed relative to the moment `opx-chain` ran, not relative to when the file is read. A CSV that was fresh at generation can be arbitrarily old by the time it is consumed.

Required behavior on import:

- Compute age of the dataset by comparing `underlying_price_time` (the oldest value across all rows) against the current wall-clock time at import.
- Emit a **warning** when the oldest underlying price is more than 3 hours old; proceed but flag affected tickers.
- Emit a **hard block** (abort the read or import with a clear error) when the oldest underlying price is more than 24 hours old; stale-by-a-day data produces unreliable Greeks, skewed screening metrics, and misleading inspection results.
- Report per-ticker staleness when tickers differ materially in age (e.g. one ticker's underlying was captured a day earlier than the rest).
- The hard-block threshold should be configurable; the 24-hour default is conservative enough to catch overnight-stale files while allowing intraday re-runs.

What to report on a freshness failure:

- which tickers are affected
- the timestamp of their oldest `underlying_price_time`
- the computed age in hours and minutes
- a clear message distinguishing warning (proceed with caution) from block (run aborted)

## 8. Documentation and Viewer

### 8.1 Documentation Layout

The documentation is split by audience.

Current structure:

- `AGENTS.md`: project-specific AI-agent guidance and source-of-truth ordering
- `README.md`: short landing page
- `docs/USER_GUIDE.md`: user-facing setup, running, config, and behavior
- `docs/FIELD_REFERENCE.md`: canonical field descriptions and provider mapping matrix
- `docs/STORAGE_SPEC.md`: storage backend, provider cache, artifacts, and retention contract
- `docs/METADATA_SPEC.md`: persisted run, dataset, ticker, validation, and artifact metadata semantics
- `docs/EXTERNAL_INTERFACE_SPEC.md`: stable public API surface for downstream tools
- `docs/DEVELOPMENT.md`: contributor/development reference
- `docs/DESIGN_SPEC.md`: viewer UI direction and visual standards

### 8.2 Provider-Aware Documentation

The documentation must make provider differences explicit.

Current documentation coverage includes:

- provider onboarding requirements
- provider-specific plan caveats
- generated versus transformed versus derived field behavior
- provider mapping matrix by canonical field
- viewer reference content sourced from the same field-reference document

### 8.3 Viewer Behavior

The viewer is a local inspection tool for exported datasets.

Current viewer behavior includes:

- dataset summary cards
- active provider surfaced through dataset metadata when constant across the file
- lightweight positions counts, parsed-position fingerprint, and chain coverage metadata on the `Dataset` tab when the selected dataset has a run-level `positions.csv` sidecar
- a `Reference` tab backed by the field-reference document
- a `Chain View` tab that derives per-ticker/per-expiration visualizations directly from the selected dataset rows
- sortable/filterable table view
- summary highlights restricted to primary-screen rows when available
- opportunity cards for `Most Profitable`, `Moderate Risk`, `High Conviction Call`, and `High Conviction Put` that surface `final_score`, `option_score`, `risk_level`, `spread_score`, `dte_score`, and `theta_efficiency`
- chain charts for delta-vs-strike/moneyness, premium-vs-spread, theta-efficiency-vs-delta, and a risk/liquidity summary
- interactive chart hover tooltips and click-through from chart marks into the existing row-detail modal

## 9. Validation Status

The current repository state is validated by automated tests and lint checks.

Current validation coverage includes:

- config loading and fallback behavior
- provider factory selection and unsupported-provider handling
- unchanged `opx-view` entrypoint behavior
- schema-preserving export behavior
- shared fetch logging behavior
- Massive normalization and field mapping
- Market Data normalization and field mapping
- Massive auth failure handling
- Market Data auth failure handling
- Massive retry and request-spacing behavior
- per-page Massive debug dump behavior
- Market Data shared fetch-path behavior
- viewer helper behavior tied to the field-reference docs
- event risk flag computation and boolean dtype consistency
- `append_ticker_event_fields` day-count broadcast behavior
- Market Data `load_ticker_events` earnings and dividend parsing
- base provider `load_ticker_events` blank-default behavior

Current verification state:

- automated test suite passes
- tracked Python files pass `pylint`

## 10. Implementation History

The following project work has already landed.

### 10.1 Config Migration and Rename

Completed:

- migrated runtime settings into `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`)
- renamed the project/package to `opx-chain` / `opx_chain`
- kept `opx-view` unchanged
- isolated credential access behind the config layer

### 10.2 Provider Contract Cleanup

Completed:

- introduced a shared provider registry/factory
- removed Yahoo-specific wording from shared fetch paths
- pinned export behavior to the canonical schema

### 10.3 Massive Provider Support

Completed:

- added the Massive provider module
- used the official Massive client
- implemented snapshot-chain fetch flow
- mapped Massive fields into the canonical schema
- preserved provider-native greeks when appropriate

### 10.4 Market Data Provider Support

Completed:

- added the Market Data provider module
- used the official Market Data SDK
- implemented one-chain-per-ticker fetch flow
- mapped Market Data fields into the canonical schema
- preserved provider-native greeks when appropriate

### 10.5 Documentation and Viewer Alignment

Completed:

- split user and development docs
- moved field reference into its own document
- added provider mapping matrix
- aligned viewer reference content with the field-reference document

### 10.6 Position-Aware Filtering

Completed:

- added support for a user-supplied portfolio positions file at `$XDG_DATA_HOME/opx-chain/positions.csv`
- added stock ticker expansion from held positions
- added option-filter bypass for held contracts
- added the `opx-check` CLI for position coverage against the latest output

### 10.7 Validation and Runtime UX

Completed:

- expanded automated tests
- added clear exit-status behavior
- added graceful interrupt handling
- added fetch progress output
- added raw provider debug dumps
- added single-run locking

### 10.8 Corporate Event Risk Fields

Completed:

- added `load_ticker_events` base class default returning blank event data for all providers
- implemented `load_ticker_events` in the Market Data provider using `stocks/earnings/{symbol}/` (SDK) and `stocks/dividends/{symbol}/` (direct HTTP)
- added `append_ticker_event_fields` to `fetch.py` to broadcast per-ticker event data to all option rows
- added `add_event_risk_flags` to `metrics.py` computing proximity flags and a composite `event_risk_score`
- added 9 new canonical event fields to the export column order
- updated `FIELD_REFERENCE.md` with field descriptions and provider mapping
- updated viewer summary tab and opportunity cards to surface event risk data

### 10.9 Strategy Engine Offload Fields

Completed:

- reshaped per-ticker loop in `fetch.py` to accumulate all normalized rows before filtering, enabling pre-filter cross-row enrichment
- implemented `add_iv_state_level` (pre-filter, per underlying, p30/p70 ATM IV classification)
- implemented `add_iv_state_term` (pre-filter, per underlying, near/far expiration median IV comparison)
- implemented `add_listed_strike_increment` (pre-filter, per (underlying, option_type), minimum adjacent strike spacing)
- implemented `add_theta_efficiency_below_p25` (post-filter, per (underlying, option_type), p25 percentile flag)
- added all 4 fields to the canonical export column order
- updated `FIELD_REFERENCE.md` with field descriptions and provider mapping

## 11. Current Change Rules

Future changes should preserve these project rules:

- keep the canonical CSV schema stable by default
- prefer provider mapping over schema expansion
- keep provider selection single-source and config-driven
- keep provider behavior explicit in documentation
- keep `opx-view` unchanged as the viewer CLI entrypoint name
- avoid temporary compatibility shims for completed rename work

## 12. Current Completion Status

As of the current repository state:

- provider model: complete for `yfinance`, `massive`, and `marketdata`
- config migration: complete
- rename to `opx-chain` / `opx_chain`: complete
- documentation split and provider-aware field reference: complete
- viewer/provider metadata alignment: complete
- validation coverage for shipped behavior: complete

There are no open milestone sections remaining in this specification. Future work, if any, should be added as new scoped proposals rather than re-opening the completed migration plan.

---

## 13. Strategy Engine Offload Fields (Landed)

The following computations were specified as future improvements and have been
implemented and landed in the pipeline. All four are computed within a single fetch run.
Each entry notes whether it runs before or after the pre-filter step.

### 13.1 IV State Level

**Field:** `iv_state_level` — per underlying; values: `LOW`, `NEUTRAL`, `HIGH`, `UNKNOWN`

**Computation:**
1. For each underlying, collect all `implied_volatility` values across its rows.
2. If fewer than 5 rows exist for the underlying, set `UNKNOWN` for all its rows.
3. Compute p30 and p70 of the IV distribution within that underlying.
4. Classify the underlying's representative IV (ATM row at nearest expiration, or median
   of all rows if ATM is ambiguous):
   - `HIGH` if representative IV ≥ p70
   - `LOW` if representative IV ≤ p30
   - `NEUTRAL` otherwise
5. Broadcast the single classified level to all rows for that underlying.

**Filter timing:** Pre-filter. Must run on the full unfiltered chain so the IV
distribution is not skewed by dropping wide-spread or low-volume rows before ranking.

---

### 13.2 IV State Term

**Field:** `iv_state_term` — per underlying; values: `RISING`, `FALLING`, `FLAT`, `UNKNOWN`

**Computation:**
1. For each underlying, group rows by expiration and compute the median
   `implied_volatility` per expiration.
2. Identify the nearest expiration (`near_exp`) and the next available expiration
   (`far_exp`). If fewer than 2 distinct expirations exist, set `UNKNOWN`.
3. `near_iv` = median IV at `near_exp`; `far_iv` = median IV at `far_exp`.
4. Classify:
   - `RISING` if `near_iv ≥ far_iv × 1.05` (near-term IV at least 5% above longer DTE)
   - `FALLING` if `near_iv ≤ far_iv × 0.95`
   - `FLAT` otherwise
5. Broadcast the classified term to all rows for that underlying.

**Filter timing:** Pre-filter. Needs the full term structure before the expiration
ceiling filter drops far-dated rows that are part of the comparison.

---

### 13.3 Listed Strike Increment

**Field:** `listed_strike_increment` — per underlying per option type; float

**Computation:**
1. For each (underlying, option_type) pair, find the nearest expiration that has ≥ 3
   rows. If none qualify, use the next nearest with ≥ 3 rows.
2. Sort those rows by strike ascending.
3. Compute all positive differences between adjacent strikes; take the minimum.
4. Broadcast the result to all rows for that (underlying, option_type).

**Filter timing:** Pre-filter. The full strike ladder is required before the strike
distance filter drops rows. Adjacent strikes needed for the increment calculation may
themselves fall outside the ±35% distance threshold.

---

### 13.4 Theta Efficiency Percentile Flag

**Field:** `theta_efficiency_below_p25` — boolean; per underlying per option type

**Computation:**
1. For each (underlying, option_type) group, collect `theta_efficiency` values from
   surviving rows.
2. Compute the 25th percentile (p25) of that group.
3. Set `True` for rows where `theta_efficiency < p25`; `False` otherwise.
4. Can be used by external consumers to label relatively weak rows within one
   (underlying, option_type) group, but that downstream interpretation is out of
   scope for `opx-chain` itself.

**Filter timing:** Post-filter. The percentile should reflect only tradeable rows so
untradeable contracts (zero bid, wide spread) do not distort the distribution.

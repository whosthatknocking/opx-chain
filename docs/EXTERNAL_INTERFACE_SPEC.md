# External Interface Specification

This document specifies the stable external interface that `opx-chain` exposes to
downstream consumers. It covers the CLI invocation contract, the Python package
interface, and the schema versioning contract.

`opx-chain` does not own any downstream system and has no dependency on them. This
document describes what `opx-chain` commits to stabilizing so that consumers can
integrate without coupling to internal implementation details.

---

## Boundary and Duplication Policy

`opx-chain` owns public contracts for option-chain market data, canonical
exports, storage/dataset discovery, provider-owned metadata, and positions
parsing. Downstream packages may depend on those contracts only through the
public surfaces documented here.

Generic helpers are not external contracts. Similar JSON helpers, timestamp
formatting, timestamp-age guards, XDG/path helpers, logging helpers, CLI/test
guards, display formatting, and error wording may remain package-local in
`opx-chain` and downstream packages. If one package has a concrete bug in those
areas, fix that package without creating a shared abstraction.

Promote behavior into this external interface only when downstream consumers
must rely on the same `opx-chain` domain semantics. Good candidates are storage
dataset handles and schema metadata, option-chain reuse and freshness fields,
provider/fetcher defaults that define exported rows, and option or ticker
parsing when it governs chain/positions boundary behavior. Strategy-layer
policy remains outside this interface.

## 1. Scope

Three integration points are in scope:

1. **CLI invocation** — a downstream orchestrator can invoke `opx-fetch` as a
   subprocess to trigger a fresh chain fetch
2. **Programmatic fetch** — a downstream consumer running in the same process can call
   `opx_chain.fetcher.run_fetch()` to trigger a fetch without spawning a subprocess
3. **Storage interface** — a downstream consumer can import `opx_chain` as a Python
   package and use `StorageBackend` to discover and read the latest chain dataset

Everything else — internal storage layout, provider adapters, scoring weights,
normalization logic — is internal to `opx-chain` and may change without notice.

---

## 2. CLI Invocation Contract

### 2.1 `opx-fetch`

`opx-fetch` is the entry point for triggering a fresh option-chain fetch.

A downstream orchestrator invokes it as a subprocess:

```
opx-fetch [--positions <path>] [--dry-run] [--enable-price-context | --disable-price-context] [--price-context-only]
```

The orchestrator must:
- wait for the process to exit before querying storage for the new dataset
- treat any non-zero exit code as a fetch failure
- not parse stdout or stderr for structured data; those streams are for logging only

**`--positions <path>` (optional)**

Overrides the default positions file path (`$XDG_DATA_HOME/opx-chain/positions.csv`,
default `~/.local/share/opx-chain/positions.csv`). When provided,
`opx-fetch` uses this file to determine which option contracts must survive hard
filters regardless of screening criteria. Each exact held contract and fetched
contract with the same ticker, option type, and strike from the held expiration
forward is retained; the configured provider expiration window remains the
outer bound. When the positions file is absent, behaviour is unchanged.

A downstream orchestrator that manages a per-run positions file passes the
run-specific path here:

```
opx-fetch --positions /path/to/runs/<run_id>/positions.csv
```

See `docs/PROJECT_SPEC.md` §7.3 for the full behaviour specification.

**`--dry-run` (optional)**

Validates the resolved config, parses the positions file, and checks storage
backend reachability without provider API calls or output writes. If parquet
storage is configured, the dry run also verifies that the optional `pyarrow`
dependency is installed before any provider call can be made. A dry run exits
`0` when those preflight checks pass. It is safe for operator diagnostics; a
downstream orchestrator should not treat it as producing a new dataset.

**`--enable-price-context` / `--disable-price-context` (optional)**

Overrides optional daily-OHLCV price-context capture for this run. These flags
apply to a normal option-chain fetch: the command still writes the option-chain
dataset when the fetch succeeds, and the price-context artifact is auxiliary.

**`--price-context-only` (optional)**

Refreshes only the daily-OHLCV price-context artifact and skips option-chain
export. A successful price-context-only run exits `0` after writing the
standalone versioned JSON artifact under the runs directory, but it does not
create a new option-chain dataset or storage run record. Downstream consumers
must not poll storage for a new dataset after this mode.

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Command completed successfully. For a normal option-chain fetch, at least one dataset was written to storage. For `--dry-run`, no dataset or artifact is written. For `--price-context-only`, only the standalone price-context artifact is written. |
| non-zero | Fetch failed or was interrupted; no new dataset should be assumed |

### 2.2 `opx-price-history-backfill`

`opx-price-history-backfill` is the stable operational command for refreshing
the local daily OHLCV store used by price context and volatility-advisory
features. It does not write an option-chain dataset, storage run record, or
price-context artifact. It only reconciles `price-history.db`.

Supported provider behavior is intentionally explicit:

```
opx-price-history-backfill --providers marketdata,yfinance --tickers TSLA,NVDA --refresh
```

**`--providers <provider[,provider...]>` (optional)**

Comma-separated provider list. Supported values are `marketdata` and
`yfinance`. When absent, the configured provider is used if it supports
price-history backfill.

**`--tickers <ticker[,ticker...]>` (optional)**

Comma-separated ticker list. When absent, `settings.tickers` from the resolved
config is used.

**`--lookback-days <n>` (optional)**

Daily-bar lookback to reconcile. When absent, `price_context.lookback_days` is
used.

**`--refresh` (optional)**

Bypasses the price-history sync TTL so the command attempts provider
reconciliation immediately. Without it, already-fresh provider/ticker/lookback
syncs may be served from local coverage.

**`--dry-run` (optional)**

Reports current provider/ticker coverage from local storage without provider
API calls or writes.

Rows are stored independently in `price-history.db` by
`(provider, ticker, trading_date)`, so `marketdata` and `yfinance` coverage for
the same ticker can coexist without overwriting each other. The durable
price-history store validates direct provider, ticker, date, lookback,
sync-status, row-count, and fetch-timestamp inputs at the public boundary and
raises `ValueError` for malformed direct calls.

### 2.3 `opx-iv-history-backfill`

`opx-iv-history-backfill` is the stable operational command for building
durable implied-volatility percentile history. By default it replays
option-chain datasets already retained by opx-chain storage and does not call
provider APIs, write a new option-chain dataset, or create a storage run record.
When explicitly invoked with `--fetch-historical`, it fetches historical
provider option-chain snapshots and writes only aggregate rows to
`iv-history.db`.

Successful storage-backed `opx-fetch` runs automatically invoke the retained-
dataset replay path for the just-written dataset after the run and dataset are
finalized. That automatic sync is non-fatal: an IV-history write failure is
logged and reported, but the option-chain fetch remains complete. The CLI
remains the manual replay, repair, and historical-seeding surface.

The default retained-dataset replay path does not call provider APIs.
Without `--refresh`, retained-dataset replay skips only prior successful syncs
that stored usable IV rows and still have matching observations in
`iv-history.db`; prior failed, empty, or metadata-only attempts are retried so
the command can repair transient read, ingestion, or restore gaps.

```
opx-iv-history-backfill --providers marketdata --tickers TSLA,NVDA --lookback-days 365
```

**`--providers <provider[,provider...]>` (optional)**

Comma-separated provider list. When absent, the configured provider is used.
Rows are stored independently by provider so retained `marketdata` and
`yfinance` datasets can coexist.

**`--tickers <ticker[,ticker...]>` (optional)**

Comma-separated ticker filter. When absent, `settings.tickers` from the resolved
config is used. If no configured tickers exist, all rows in matching datasets
are eligible.

**`--lookback-days <n>` (optional)**

Dataset `created_at` lookback window to inspect. Defaults to `365`.

**`--limit <n>` (optional)**

Maximum retained datasets to inspect per provider. Defaults to `200`.

**`--dataset-id <id>` (optional)**

Specific retained dataset to ingest. Can be repeated or comma-separated.
Cannot be combined with `--fetch-historical`. Explicit dataset ids still remain
inside the requested provider scope; a dataset retained for a different provider
is rejected instead of being ingested under the wrong provider summary.

**`--fetch-historical` (optional)**

Fetches historical option-chain snapshots from supported providers and writes
the derived aggregate IV rows directly to `iv-history.db`. This path does not
write retained option-chain datasets or storage run records. It may consume
provider API requests, so operators should run `--dry-run` first. V1 supports
`marketdata`; retained-dataset replay remains the path for existing `yfinance`
datasets.

```bash
opx-iv-history-backfill --providers marketdata --tickers TSLA,NVDA --fetch-historical --sessions 25 --dry-run
```

**`--sessions <n>` (optional)**

Historical business sessions to fetch when `--fetch-historical` is set.
Defaults to `20`.

**`--end-date <YYYY-MM-DD>` (optional)**

Last historical observation date for `--fetch-historical`. Defaults to the
configured market date.

**`--refresh` (optional)**

Reingests datasets even when an `iv_history_syncs` record already exists. In
historical-fetch mode, refetches provider/ticker/date rows even when local
coverage already exists.

**`--recover-corrupt` (optional)**

Checks `iv-history.db` read-only before constructing a store. When the database
is missing or fails SQLite integrity/schema validation, this mode builds a
fresh temporary database exclusively from retained option-chain datasets. A
successful write-mode recovery validates the rebuilt database, renames the
original to `iv-history.corrupt-<UTC timestamp>.db`, and atomically installs the
replacement. An empty replay, an invalid replacement, an active SQLite sidecar,
or an install failure leaves or restores the original database in place.

Recovery never calls a provider and cannot be combined with
`--fetch-historical`. Use `--dry-run` to prove that retained datasets can
produce rows without changing or opening the active database for writes. The
summary pins this boundary as `provider requests: 0`:

```bash
opx-iv-history-backfill --recover-corrupt --providers marketdata --dry-run
```

After reviewing the candidate row count, omit `--dry-run` to quarantine and
replace the unusable store. Write mode uses the shared fetch/backfill lock,
which is owned by the public recovery function as well as the CLI path.

**`--dry-run` (optional)**

For retained-dataset replay, reads matching datasets and reports derived
aggregate row counts without writing `iv-history.db`. For `--fetch-historical`,
reports the provider/ticker/date request plan and estimated provider-request
count without calling provider APIs or writing the store.

Rows are stored independently in `iv-history.db` by
`(provider, ticker, observation_date, option_type, dte_bucket, delta_bucket)`.
The public feature helpers consume ticker-wide and DTE-bucket aggregate rows to
compute one-year IV percentiles without re-reading raw option-chain artifacts.
Programmatic callers must pass real booleans for `refresh`, `dry_run`, and
`fetch_historical`, and positive integers for `lookback_days`, `limit`, and
`sessions`; CLI parsing performs this typing before invoking the runner. Ticker
filters use the same letters/dots/up-to-ten-character symbol policy as parsed
portfolio symbols.

### 2.4 No other `opx-fetch` CLI arguments are part of the external interface

`--enable-filters` and `--disable-filters` are internal operational flags, not part
of the stable downstream interface. A downstream orchestrator should not set them.

---

## 3. Python Package Interface

**Prerequisite:** the Python package interface is only available when storage is
enabled in the `opx-chain` config (`[storage] enable = true`). When storage is
disabled (the default), `opx_chain.storage` modules are importable but
`get_storage_backend()` returns `None` and `list_datasets` is not meaningful.
A downstream consumer must ensure the `opx-chain` instance it connects to has storage
enabled before using this interface.

A downstream consumer may import `opx_chain` as a Python dependency to query the storage
layer without shelling out or scanning the filesystem directly.

### 3.1 Public surface

The stable public surface is:

```python
from opx_chain.fetcher import fetch_ticker_option_chain, run_fetch
from opx_chain.backup_inventory import (
    BackupDependencyRecord,
    BackupInventory,
    build_backup_inventory,
)
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.models import DatasetHandle, DatasetRecord, RunRecord
from opx_chain.storage.factory import get_storage_backend
from opx_chain.utils import read_dataset_file
from opx_chain.integrity import (
    OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
    OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
    OptionChainDataIntegrityError,
    OptionChainDatasetFacts,
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityBoundary,
    OptionChainIntegrityCode,
    OptionChainIntegrityFinding,
    OptionChainIntegritySeverity,
    OptionChainIntegrityStatus,
    OptionChainIntegritySummary,
    OptionChainRowScope,
    OptionChainRowScopeIntegrityError,
    OptionChainRowScopeStatus,
    OptionChainSchemaCompatibilityError,
    OptionChainTickerTimeBounds,
    ValidatedOptionChainDataset,
    canonical_option_contract_key,
    evaluate_option_chain_dataset_facts_status,
    evaluate_option_chain_integrity_status,
    evaluate_option_chain_row_scope_status,
)
from opx_chain.positions import (
    OptionPositionKey,
    PositionSet,
    load_positions,
    positions_fingerprint,
)
from opx_chain.price_context import (
    PRICE_CONTEXT_RECORD_FIELDS,
    PRICE_CONTEXT_SCHEMA_VERSION,
    PriceContextStatus,
    blank_price_context,
)
from opx_chain.analyst_forecast import (
    ANALYST_FORECAST_SCHEMA_VERSION,
    fetch_analyst_forecasts,
)
from opx_chain.iv_history import (
    IVHistoryIntegrity,
    IVHistoryStore,
    build_iv_observation_frame,
    check_iv_history_integrity,
    get_iv_history_db_path,
    get_iv_history_store,
)
from opx_chain.iv_history_backfill import (
    IVHistoryRecoveryBusyError,
    IVHistoryRecoveryResult,
    recover_iv_history_store,
)
from opx_chain.event_data import (
    EVENT_DATA_FETCH_MODES,
    EVENT_DATA_FRESHNESS_POLICY,
    EVENT_DATA_PROVIDER_CHOICES,
    EVENT_DATA_SCHEMA_VERSION,
    EVENT_DATA_SUPPORTED_PROVIDERS,
    clear_event_columns,
    normalize_event_data_fetch_mode,
    normalize_event_data_provider,
    overlay_event_snapshot,
    run_event_fetch,
    summarize_latest_event_data,
)
from opx_chain.price_history import (
    PRICE_HISTORY_INTEGRITY_ERROR,
    PRICE_HISTORY_INTEGRITY_MISSING,
    PRICE_HISTORY_INTEGRITY_OK,
    PriceHistoryIntegrity,
    check_price_history_integrity,
    get_price_history_db_path,
    get_price_history_store,
)
from opx_chain.volatility_features import (
    DTE_BUCKETS,
    MIN_IV_HISTORY_OBSERVATIONS,
    SOURCE_ERROR,
    SOURCE_INSUFFICIENT_HISTORY,
    SOURCE_MISSING,
    SOURCE_PARTIAL,
    SOURCE_READY,
    SOURCE_STALE,
    VOLATILITY_FEATURE_METHOD,
    VOLATILITY_FEATURE_SCHEMA_VERSION,
    build_iv_features,
    build_price_volatility_features,
    build_ticker_volatility_features,
    dte_bucket,
    load_price_volatility_features,
)
from opx_chain.option_types import (
    OPTION_TYPE_CALL,
    OPTION_TYPE_CALL_LABEL,
    OPTION_TYPE_PUT,
    OPTION_TYPE_PUT_LABEL,
    OPTION_TYPES,
    normalize_option_type,
    option_type_label,
)
from opx_chain.paths import get_data_dir, get_runs_dir
from opx_chain.config import (
    DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS,
    US_MARKET_TIMEZONE,
    get_runtime_config,
    get_runtime_config_override,
    set_runtime_config_override,
)
from opx_chain import SCHEMA_VERSION
```

`read_dataset_file` is the only stable public import from `opx_chain.utils`;
other helpers in that module remain internal.

`load_positions`, `positions_fingerprint`, `PositionSet`, and
`OptionPositionKey` are the stable positions parsing surface for downstream
consumers that need the same stock ticker expansion, held-option contract keys,
and parsed-position fingerprint as `opx-fetch`.

`PRICE_CONTEXT_SCHEMA_VERSION`, `PRICE_CONTEXT_RECORD_FIELDS`,
`PriceContextStatus`, and `blank_price_context` are the stable price-context
artifact vocabulary for downstream consumers that join optional daily-OHLCV
levels to option-chain rows.

`ANALYST_FORECAST_SCHEMA_VERSION` and `fetch_analyst_forecasts` are the stable
analyst-forecast advisory surface for downstream consumers that need normalized
12-month analyst target/rating facts without depending on yfinance/Yahoo field
names. The payload is context-only market-data metadata; downstream strategy
packages own run lifecycle, artifact reuse, rendering, validation, and any
operator-facing advisory policy.

`get_price_history_store` is the stable durable daily-OHLCV history-store
factory for downstream consumers that need to pass provider-scoped price
history into public volatility feature builders. Consumers should still prefer
feature-builder inputs over direct table queries.

`get_price_history_db_path` exposes the configured durable price-history path.
`check_price_history_integrity` is the producer-owned, read-only backup and
recovery validation boundary: it applies SQLite integrity checks, required-table
checks, and the exact producer schema-version check, returning a
`PriceHistoryIntegrity` with `OK`, `ERROR`, or `MISSING` status. Downstream
consumers must use this API instead of inferring compatibility from
`PRAGMA quick_check` alone.

`VOLATILITY_FEATURE_SCHEMA_VERSION`, `VOLATILITY_FEATURE_METHOD`,
`build_price_volatility_features`, `load_price_volatility_features`,
`build_iv_features`, `build_ticker_volatility_features`, `dte_bucket`,
`DTE_BUCKETS`, and `MIN_IV_HISTORY_OBSERVATIONS` are the stable volatility
feature surface for downstream advisory consumers. These helpers and constants
expose stored daily-price realized-volatility features, current-chain IV
context, optional IV-history percentiles, DTE-bucket vocabulary, and minimum
history thresholds without requiring consumers to inspect `price-history.db` or
option-chain internals directly. `SOURCE_READY`, `SOURCE_PARTIAL`,
`SOURCE_INSUFFICIENT_HISTORY`, `SOURCE_STALE`, `SOURCE_MISSING`, and
`SOURCE_ERROR` are the stable source-health vocabulary emitted by these feature
builders.

`IVHistoryStore`, `build_iv_observation_frame`, and `get_iv_history_store` are
the stable durable-IV history surface for downstream consumers that need to
populate or inspect provider-scoped historical IV percentiles.
`get_iv_history_db_path`, `check_iv_history_integrity`, and
`IVHistoryIntegrity` expose the producer-owned path and read-only
`OK`/`ERROR`/`MISSING` health result without constructing the writable store.
`recover_iv_history_store` and `IVHistoryRecoveryResult` are the stable
retained-data-only rebuild surface. Write-mode calls acquire and release the
shared fetch/backfill writer lock inside the public function and raise
`IVHistoryRecoveryBusyError` before candidate construction when another writer
is active; callers cannot bypass locking by invoking the Python API instead of
the CLI. Dry-run calls do not acquire the writer lock because they never change
the active store.
Consumers should prefer
`build_ticker_volatility_features(..., iv_history_store=...)` instead of
querying `iv-history.db` directly. Store read/write helpers validate provider,
ticker, date, positive-window, sync-status, and row-count inputs at the public
boundary and raise `ValueError` for malformed direct calls rather than returning
backend-specific empty windows or raw SQLite errors.

`build_backup_inventory`, `BackupInventory`, and `BackupDependencyRecord` are
the stable backup-inventory surface for downstream applications that need to
archive OPX Chain execution dependencies without scanning private storage
layout. The inventory owns current dependency discovery for durable
price-history, durable IV-history, standalone price-context artifacts, Event
Data snapshot artifacts, and consumer-provided retained chain dataset locations
under the OPX Chain runs root. Event Data snapshot discovery includes the
producer-owned latest alias `event_snapshot_latest.json` plus retained
`event_snapshots/*.json` artifacts under the OPX Chain data root. Downstream
consumers own archive assembly and restore policy; they should not hard-code
OPX Chain dependency filenames, Event Data snapshot filenames, or runs-root
glob patterns.
When callers omit explicit roots, the inventory resolves the active runtime
`[storage].dir` and uses that storage base for both `BackupInventory.data_dir`
and `BackupInventory.runs_dir`. When `data_dir` is supplied without `runs_dir`,
`runs_dir` is derived as `<data_dir>/runs`; callers that pass one custom storage
base do not need to pass both roots to avoid split-root inventories. Storage-root
resolution is independent from provider credential validation, so missing fetch
tokens do not block local dependency discovery.
Dependency discovery is fail-closed for symlinked storage roots and symlinked
dependency parent directories. Unsafe roots still appear in the returned
`BackupInventory` metadata for diagnostics, but no dependency records are
reported from paths reached through those symlinks.
For standalone price-context artifacts, `BackupDependencyRecord.freshness_status`
summarizes every `records[].price_context_staleness_status` value; mixed
artifacts use `MIXED:<sorted statuses>` such as `MIXED:ERROR,FRESH,STALE`
rather than reporting only the first record.

`EVENT_DATA_SCHEMA_VERSION`, `EVENT_DATA_SUPPORTED_PROVIDERS`,
`EVENT_DATA_PROVIDER_CHOICES`, `EVENT_DATA_FETCH_MODES`,
`EVENT_DATA_FRESHNESS_POLICY`, `normalize_event_data_provider`,
`normalize_event_data_fetch_mode`, `run_event_fetch`,
`summarize_latest_event_data`, `overlay_event_snapshot`, and
`clear_event_columns` are the stable Event Data snapshot and overlay surface.
Downstream consumers may use them to configure provider/fetch-mode selection,
inspect retained source health, and apply a dedicated Event Data snapshot onto
an already-fetched option chain without depending on provider-native fields.

`OPTION_TYPE_CALL`, `OPTION_TYPE_PUT`, `OPTION_TYPE_CALL_LABEL`,
`OPTION_TYPE_PUT_LABEL`, `OPTION_TYPES`, `normalize_option_type`, and
`option_type_label` are the stable option-type normalization vocabulary for
downstream consumers that need to compare option rows, positions, and generated
candidate identifiers using the same canonical `call` / `put` values and
uppercase display/storage labels as `opx-chain`.

`fetch_ticker_option_chain` is the stable single-ticker option-chain fetch
helper for downstream consumers that need a narrow provider-owned rescue path
without creating a full fetch run. Prefer `run_fetch` for normal dataset
collection; use `fetch_ticker_option_chain` only when the caller already owns
run lifecycle, storage, and any operator-facing policy.

`get_data_dir`, `get_runs_dir`, `DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS`,
`US_MARKET_TIMEZONE`, `get_runtime_config`, `get_runtime_config_override`, and
`set_runtime_config_override` are stable runtime-environment helpers for
locating `opx-chain` data and run artifacts, applying the same US-market
calendar boundary as the fetcher, and temporarily binding an in-process fetch
to a specific runtime configuration. Downstream callers that set a runtime
override must restore the previous override in a `finally` block.

All other names within `opx_chain.fetcher`, `opx_chain.normalize`, `opx_chain.provider`,
and other internal modules are not part of the stable interface and may change across
releases.

### 3.2 Triggering a fresh fetch programmatically

```python
from opx_chain.fetcher import TickerFetchProgress, run_fetch

run_fetch(positions_path=Path("/path/to/runs/<run_id>/positions.csv"))
run_fetch(tickers=("TSLA", "NVDA"))
run_fetch(
    positions_path=Path.home() / ".local" / "share" / "opx-chain" / "positions.csv",
    tickers=("AAPL",),
    max_expiration_weeks=34,
    stale_quote_seconds=86_400,
    data_provider="marketdata",
    enable_filters=False,
)
run_fetch(dry_run=True)
run_fetch(price_context_only=True)
run_fetch(skip_events=True)

def report_progress(progress: TickerFetchProgress) -> None:
    print(progress.ticker, progress.current, progress.total, progress.status)

run_fetch(tickers=("TSLA", "NVDA"), progress_callback=report_progress)
```

`run_fetch()` is the in-process equivalent of invoking `opx-fetch` as a
subprocess. For normal option-chain fetches, it acquires the same exclusive
lock, runs the full fetch pipeline, and writes the result to storage. The
caller blocks until the fetch completes. Dry runs do not acquire the fetcher
lock and are not a lock-availability or concurrency preflight.

**`enable_filters` (optional `bool`)** — overrides generic post-download
filtering for this run only. `None` preserves configuration. `False` retains
all normalized rows that pass enrichment and validation, matching the CLI
`--disable-filters` path. The resolved value participates in the run config
fingerprint and is recorded in dataset row-scope metadata.

**`positions_path` (optional `Path`)** — overrides the default positions file, identical
in semantics to the `--positions` CLI flag. When absent, the configured default is used.

**`tickers` (optional `tuple[str, ...]`)** — overrides the ticker list from config for
this run only. The override replaces `settings.tickers` entirely; the positions file
can still add additional tickers via stock-ticker expansion. When absent, the configured
`settings.tickers` is used unchanged.

**`max_expiration_weeks` (optional `int`)** — overrides the configured maximum
expiration window for this run only. `0` disables the max-expiration filter for the
fetch. When absent, the configured `settings.max_expiration_weeks` is used unchanged.

**`stale_quote_seconds` (optional `int`)** — overrides the configured stale quote
threshold for this run only. Downstream callers use this to align fetch freshness with
their own run policy without editing opx-chain config. When absent, the configured
`settings.stale_quote_seconds` is used unchanged.

**`data_provider` (optional `str`)** — overrides `settings.data_provider` for this
run only. Supported values are `marketdata`, `massive`, and `yfinance`. Downstream
callers use this to select a provider per experimental or production run without
mutating the opx-chain config file. When absent, the configured provider is used
unchanged.

**`dry_run` (optional `bool`)** — when `True`, validates config loading,
positions parsing, and storage reachability without making provider API calls,
acquiring the fetcher lock, or writing run artifacts. This is the in-process
equivalent of `opx-fetch --dry-run`.

**`price_context_only` (optional `bool`)** — when `True`, reconciles only the
optional daily-OHLCV price-history store, writes the derived price-context
artifact, and skips option-chain export. This also enables price-context fetching
for the run, regardless of the config default.
The result is written as a standalone versioned JSON artifact under the runs
directory and does not change the option-chain dataset schema.

**`skip_events` (optional `bool`)** — when `True`, skips provider corporate-event
fetches during option-chain export and leaves event-derived columns blank. This
is for downstream orchestrators that apply a separate, authoritative Event Data
snapshot after chain acquisition. The default is `False`, preserving normal
`opx-fetch` behavior. Programmatic callers must pass a literal boolean; string
or integer boolean-like values are rejected before provider work.

**`progress_callback` (optional callable)** — receives immutable
`TickerFetchProgress(ticker, current, total, status)` events immediately before
and after each ticker fetch. `status` is `starting` or `completed`; `current`
counts completed tickers, so the starting event for the first ticker is `0`.
Callback exceptions are logged and ignored because progress telemetry must not
abort or invalidate a provider fetch. Dry-run and price-context-only modes do
not emit ticker progress.

**Errors:**

| Condition | Raised |
|---|---|
| Another fetch is already active (lock held) | `RuntimeError` |
| Fetch produces no data | `RuntimeError` |
| Provider or storage failure | provider-specific exception |

After a normal option-chain `run_fetch()` returns without error, the dataset is
available via `get_storage_backend()` exactly as it would be after a successful
normal `opx-fetch` subprocess exit. `dry_run=True` writes no result, and
`price_context_only=True` writes only the standalone price-context artifact
under the runs directory instead of creating a storage dataset or run record.

### 3.3 Obtaining a backend instance

```python
backend: StorageBackend = get_storage_backend()
```

`get_storage_backend()` returns the configured backend (filesystem or SQLite) based
on the `opx-chain` config. No arguments are required. The consumer must not construct a
backend directly.

### 3.4 Discovering the latest dataset

```python
records: list[DatasetRecord] = backend.list_datasets(limit=1)
```

Returns the most recent completed-run dataset metadata. Optional
`integrity_status` and `dataset_facts_status` filters let callers select
reusable candidates or retain invalid/unknown history for diagnostics. Staged,
failed, and interrupted runs are excluded. An unfiltered result is not proof of
semantic validity; §3.7 is the only semantic read boundary.

The consumer should validate:
- the list is non-empty (no datasets available → cannot proceed)
- `records[0].schema_version == SCHEMA_VERSION` (schema drift → must re-fetch or
  update the consumer to handle the new schema before proceeding)

### 3.5 Obtaining a dataset handle

```python
handle: DatasetHandle = backend.get_dataset(dataset_id)
```

Returns a `DatasetHandle` for the given completed-run `dataset_id`. This is an
identity/provenance lookup, not a semantic read, and it can describe a dataset
whose effective integrity later became invalid or unknown. Semantic consumers
use §3.7.

### 3.6 Retrieving a run record

```python
run: RunRecord = backend.get_run(run_id)
```

Returns the `RunRecord` for the given `run_id`. Raises `KeyError` when the run
does not exist. Downstream consumers use this to retrieve
`RunRecord.tickers` — the effective fetch universe for that run — and
`RunRecord.positions_fingerprint` — the SHA-256 of the canonical parsed
positions payload active when the chain was fetched — for cross-checking against
the consumer's own positions fingerprint. Cosmetic positions CSV rewrites, such
as line-ending or column-order changes, do not change this fingerprint when the
parsed stock and option positions are unchanged.

`status="running"` is transient. If a previous fetch process terminates without
running cleanup, the next real fetch that acquires the fetcher lock marks stale
`running` records as `interrupted` before creating a new run. Dry runs do not
perform this recovery because they are read-only diagnostics.

`run_id` is available on `DatasetRecord.run_id` (returned by `list_datasets`).

```python
records = backend.list_datasets(limit=1)
run = backend.get_run(records[0].run_id)
assert "TSLA" in run.tickers
assert run.positions_fingerprint == pipeline_positions_fingerprint
```

### 3.7 Reading the chain artifact

```python
validated: ValidatedOptionChainDataset = (
    backend.load_validated_option_chain_dataset(handle.dataset_id)
)
df = validated.frame
```

`load_validated_option_chain_dataset` is the required semantic read boundary.
It verifies publication state, supported schema/validator versions, exact
content hash, declared format, row/schema integrity, and content-bound dataset
facts before returning a deep-copied frame with the exact handle and summaries.
It raises `OptionChainDataIntegrityError` for invalid/tampered bytes and
`OptionChainSchemaCompatibilityError` for unsupported row schemas.

`read_dataset_file` remains a stable raw inspection/export helper. It dispatches
on CSV/parquet and normalizes format-sensitive dtypes, but it does not establish
storage publication state or bind bytes to metadata. Do not use it as a
replacement for the validated loader in calculations, retained-history replay,
position checks, or downstream decision systems.

### 3.8 Parsing positions consistently

```python
from opx_chain.positions import (
    OptionPositionKey,
    PositionSet,
    load_positions,
    positions_fingerprint,
)

positions: PositionSet = load_positions(Path("/path/to/positions.csv"))
held_contracts: frozenset[OptionPositionKey] = positions.option_keys
fingerprint: str = positions_fingerprint(Path("/path/to/positions.csv"), positions)
```

`load_positions()` parses the same Fidelity positions CSV format used by
`opx-fetch`. The file must be UTF-8 or UTF-8-with-BOM and include a `Symbol`
column. Missing files return an empty `PositionSet` silently; existing files
that cannot be parsed print a warning to stderr before returning an empty
`PositionSet`. It returns a `PositionSet` with:

- `stock_tickers`: stock symbols parsed from held stock rows
- `option_keys`: held option contracts as `OptionPositionKey` values with `ticker`,
  `expiration_date`, `option_type`, and `strike`
- `tickers`: the union of stock symbols and option-underlying symbols; this expands
  the effective fetch universe and controls same-day expiration retention

Missing, malformed, or unsupported files return an empty `PositionSet` instead of
raising, matching the fetch pipeline's graceful fallback behavior.

---

## 4. `DatasetHandle` Contract

`DatasetHandle` is the stable reference returned by `get_dataset`. The following
fields are part of the external interface contract:

```python
@dataclass
class DatasetHandle:
    dataset_id: str       # stable identifier for this dataset
    run_id: str           # fetch run that produced this dataset
    provider: str         # provider that produced this dataset
    location: str         # absolute or relative path to the artifact file
    schema_version: int   # matches SCHEMA_VERSION at write time
    script_version: str   # opx-chain package version that wrote the dataset
    row_count: int        # total rows in the artifact
    format: str           # "csv" | "parquet"
    content_hash: str     # SHA-256 of artifact bytes; use for integrity checks
    created_at: datetime  # UTC timestamp when the dataset was written
    integrity_status: OptionChainIntegrityStatus
    integrity_schema_version: int | None
    integrity_validator_version: int | None
    integrity_checked_at: datetime | None
    integrity_content_hash: str | None
    integrity_summary: OptionChainIntegritySummary | None
    dataset_facts_status: OptionChainDatasetFactsStatus
    dataset_facts: OptionChainDatasetFacts | None
    row_scope_status: OptionChainRowScopeStatus
    row_scope: OptionChainRowScope | None
```

**Change from STORAGE_SPEC §6:** `run_id`, `provider`, `content_hash`,
`created_at`, and `script_version` are exposed on `DatasetHandle`. Downstream
consumers need these for dataset provenance, direct provider lookup by
`dataset_id`, chain integrity verification, freshness checks, and
producer-version provenance without having to fetch the full `DatasetRecord` or
scan a paginated dataset listing.

`location` is an absolute path when the filesystem backend is active. Raw
inspection/export consumers must not construct or infer artifact paths
independently; semantic consumers call the validated loader instead of opening
`location`.

Effective status is evaluated from the versioned metadata rather than trusting
the stored status label alone. `valid` requires matching supported versions,
matching content hashes, and a valid summary. `available` facts require their
supported schema and the exact dataset content hash. Missing or stale metadata
evaluates to `unknown`.

`row_scope_status=available` proves the provider-neutral acquisition/filter
scope stored with the same dataset publication. Its strict version-1 object
records whether generic filters ran, the configured expiration horizon (`0`
means unbounded), and conserved normalized/kept/filtered/per-ticker totals.
Legacy records without this object remain `unknown`/null. Current malformed or
contradictory scope raises `OptionChainRowScopeIntegrityError` at the validated
load boundary.

### 4.1 Integrity summary and dataset facts

`OptionChainIntegritySummary` contains `schema_version`, `validator_version`,
`status`, `checked_at`, exact dataset/provider/hash provenance, aggregate row
and finding counts, `counts_by_code`, and at most five bounded samples (at most
two per code). Finding codes and boundaries are stable provider-neutral enums.

`OptionChainDatasetFacts` is deliberately neutral and contains only its schema
version, exact content hash, sorted ticker universe, sorted per-ticker minimum
and maximum underlying-price timestamps, and sorted expiration dates. It does
not express freshness thresholds, candidate eligibility, DTE policy, ranking,
or portfolio decisions.

`canonical_option_contract_key(...)` returns the shared normalized identity tuple used
for duplicate detection and downstream joins. Consumers must not redefine
strike rounding or option-type normalization locally.

---

## 5. Schema Version Contract

### 5.1 `SCHEMA_VERSION` constant

```python
# opx_chain/__init__.py
SCHEMA_VERSION: int = 2   # incremented on every breaking schema change
```

This integer is the join key between the chain artifact and the consumer's field
expectations. It is written into every `DatasetRecord` and `DatasetHandle` at write
time.

### 5.2 Breaking vs non-breaking changes

| Change type | Version bump required |
|---|---|
| Column removed | Yes |
| Column renamed | Yes |
| Column order changed | Yes |
| Column added (appended) | Yes — downstream must handle unknown columns gracefully, but version still bumps |
| Value format change (e.g., date string format) | Yes |
| Internal scoring weight change | No |
| New provider added | No |

### 5.3 Consumer responsibility

A consumer that detects `schema_version != SCHEMA_VERSION` must not read the
artifact. It should surface a clear error: `chain schema version mismatch:
expected {expected}, got {actual}`. The operator must either re-fetch with the
current `opx-chain` version or update the consumer to support the new schema.

Backward compatibility across schema versions is not guaranteed.

### 5.4 `PRICE_CONTEXT_SCHEMA_VERSION` constant

Optional price context is a separate artifact contract, not part of the
option-chain CSV schema.

```python
# opx_chain.price_context
PRICE_CONTEXT_SCHEMA_VERSION = 2
PriceContextStatus.FRESH.value == "FRESH"
```

The latest standalone artifact is written as `price_context_latest.json` under
the runs directory, with timestamped copies named
`price_context_YYYYMMDD_HHMMSS.json`.
It is derived from the local daily-bar history store (`price-history.db`) after
incremental reconciliation with the active provider; old stored bars are reused
and only missing/backfill/tail history is fetched.

```json
{
  "artifact_type": "price_context",
  "schema_version": 2,
  "provider": "marketdata",
  "fetched_at": "2026-05-06T20:00:00Z",
  "tickers": ["TSLA"],
  "records": [
    {
      "ticker": "TSLA",
      "support_1": 100.0,
      "rsi_14": 52.3,
      "ema_20": 105.4,
      "ema_50": 101.8,
      "ema_cloud_state": "BULLISH",
      "price_vs_ema50_pct": 3.1,
      "price_context_as_of": "2026-05-06",
      "price_context_staleness_status": "FRESH"
    }
  ]
}
```

Consumers join `records[].ticker` to option-chain `underlying_symbol` when they
need row-level price context.

`records[].price_context_staleness_status` uses the stable
`PriceContextStatus` vocabulary: `FRESH`, `STALE`, `MISSING`, and `ERROR`.
Backup inventory metadata aggregates those per-record values across the whole
artifact, returning the single status when all records agree and
`MIXED:<sorted statuses>` when records differ.
Schema version 2 adds deterministic technical indicator fields to each record:
`rsi_14`, `ema_20`, `ema_50`, `ema_cloud_state`, and
`price_vs_ema50_pct`. `ema_cloud_state` is one of `BULLISH`, `BEARISH`,
`TRANSITION`, or `UNKNOWN`. These are market-data features only; downstream
strategy packages decide whether they are rendered, tagged, ranked, or ignored.

### 5.5 Event Data Snapshots

Event Data snapshots are a separate programmatic contract, not part of the
option-chain CSV schema. Downstream consumers call `opx_chain.event_data`
helpers directly:

```python
from opx_chain.event_data import (
    normalize_event_data_fetch_mode,
    normalize_event_data_provider,
    run_event_fetch,
    summarize_latest_event_data,
)

result = run_event_fetch(
    enabled=True,
    provider="yfinance",
    chain_provider="marketdata",
    fetch_mode="auto",
    trading_date=date(2026, 6, 4),
    tickers=("GOOGL", "MSFT"),
    ticker_universe_source="new_run_portfolio_and_ticker_intents",
)
```

Every snapshot payload is JSON-safe and includes ticker coverage provenance:

```json
{
  "artifact_type": "event_data_snapshot",
  "schema_version": 1,
  "status": "ready",
  "provider": "yfinance",
  "resolved_provider": "yfinance",
  "trading_date": "2026-06-04",
  "freshness_policy": "trading_day",
  "fresh_through_trading_date": "2026-06-04",
  "ticker_universe_source": "new_run_portfolio_and_ticker_intents",
  "tickers_requested": ["GOOGL", "MSFT"],
  "tickers_succeeded": ["GOOGL"],
  "tickers_failed": [],
  "tickers_no_known_event": ["MSFT"],
  "records": [],
  "canonical_events": []
}
```

`overlay_event_snapshot(...)` broadcasts the selected snapshot records onto
each matching option-chain row. In addition to standardized date/source fields,
the overlay preserves the exact snapshot-owned
`next_earnings_canonical_event_key` and
`next_ex_div_canonical_event_key`. Downstream consumers must retain these keys
as provenance; they must not reconstruct them from dates. Legacy snapshots
without canonical records leave the fields blank.

Disabled Event Data returns `status="disabled"` without resolving provider-
specific requirements such as `same_as_chain`, without requiring a chain
provider, and without making provider calls. Disabled payloads set `provider`
and `resolved_provider` to `null` while still recording the requested ticker
universe and `ticker_universe_source`. Programmatic callers must pass a literal
boolean for `enabled`; string or integer boolean-like values are rejected before
provider resolution.

`summarize_latest_event_data(...)` is read-only source health. Same-trading-day
usable snapshots return `freshness_label="CURRENT_TRADING_DAY"` and
`auto_would_reuse=true` only when the retained snapshot has the same selected
`provider`, the same resolved provider, the requested trading date, and usable
per-ticker rows for every requested ticker. Usable per-ticker rows are
`provider_status="ready"` or `provider_status="no_known_event"`; rows marked
`provider_error` or `invalid_payload` do not count as covered ticker data even
when the ticker appears in `tickers_requested`. When multiple retained snapshots
qualify for a lookup, the newest parsed payload `fetched_at` wins; filesystem
mtime is only a fallback for payloads with missing or malformed timestamps.

Same-day retained snapshots with `status="missing"` or with failed required
ticker rows remain visible in source health, but they set
`auto_would_reuse=false` and `provider_api_call_expected=true` so Auto fetches
again. Older retained usable snapshots are surfaced as `status="stale"` /
`freshness_label="STALE"` with snapshot id, provider, fetched timestamp,
snapshot trading date, freshness-through date, `snapshot_age_days`,
`ticker_universe_source`, and `provider_api_call_expected=true`; they are not
reused by Auto fetch. Retained snapshots for a different selected or resolved
provider are surfaced as `status="provider_mismatch"` /
`freshness_label="PROVIDER_MISMATCH"` with retained-provider provenance and are
not reused by Auto. Malformed, provider-error, invalid-payload, disabled,
unsupported, or wrong-artifact retained snapshots are skipped for Auto reuse.

Explicit `trading_date` values must be `datetime.date` instances, not
`datetime.datetime`; explicit `now` values must be timezone-aware datetimes.
MarketData event endpoint quota/authentication failures and unexpected endpoint
errors surface through Event Data as provider failures rather than provider-
confirmed no-known-event rows. Expected no-data dividend responses still return
blank dividend fields.

### 5.6 `ANALYST_FORECAST_SCHEMA_VERSION` constant

Analyst forecast facts are a separate programmatic contract, not part of the
option-chain CSV schema. They are fetched on demand by downstream consumers:

```python
from datetime import date, datetime, timezone

from opx_chain.analyst_forecast import fetch_analyst_forecasts

payload = fetch_analyst_forecasts(
    ["GOOGL", "MSFT"],
    provider="yfinance",
    fetched_at=datetime(2026, 6, 4, 14, 34, tzinfo=timezone.utc),
    trading_date=date(2026, 6, 4),
)
```

The `tickers` argument must be a list, tuple, or set of ticker strings. Non-string
members are rejected before provider lookup.

The returned payload is JSON-safe and provider-neutral:

```json
{
  "schema_type": "analyst_forecast",
  "schema_version": 1,
  "provider": "yfinance",
  "source_quality": "research_fallback",
  "generated_at": "2026-06-04T14:34:00Z",
  "trading_date": "2026-06-04",
  "status": "ok",
  "warnings": [],
  "errors": [],
  "forecasts": [
    {
      "ticker": "GOOGL",
      "status": "ok",
      "as_of": "2026-06-04",
      "as_of_source": "fetched_at_fallback",
      "horizon_months": 12,
      "currency": "USD",
      "target_low": 340.0,
      "target_mean": 433.47,
      "target_median": 430.0,
      "target_high": 550.0,
      "analyst_count": null,
      "consensus_rating": "buy",
      "recommendation_count": 47,
      "rating_counts": {
        "strong_buy": 12,
        "buy": 26,
        "hold": 8,
        "sell": 1,
        "strong_sell": 0
      },
      "warnings": []
    }
  ]
}
```

Supported provider ids are explicit. Version 1 supports only `yfinance`.
`tickers` must be a list, tuple, or set of ticker strings; scalar strings such
as `"NVDA"` are rejected instead of being interpreted as character tickers.
Unsupported provider ids, invalid tickers, naive `fetched_at` datetimes, or
`datetime.datetime` values passed as `trading_date` raise `ValueError` before
provider calls. Provider errors are row-scoped when possible so one failed
ticker does not prevent usable rows for other tickers.

The yfinance implementation uses structured price-target and recommendation
summary fields only. It does not scrape consumer HTML pages, does not expose raw
Yahoo field names through this public payload, and does not synthesize
`analyst_count` from recommendation distributions.

### 5.7 `VOLATILITY_FEATURE_SCHEMA_VERSION` constant

Volatility advisory feature snapshots are a separate programmatic contract, not
part of the option-chain CSV schema. They are built on demand from the local
daily-price history store and the current option-chain frame:

```python
from opx_chain.price_history import get_price_history_store
from opx_chain.volatility_features import build_ticker_volatility_features

snapshot = build_ticker_volatility_features(
    ticker="TSLA",
    chain=chain_df,
    price_history_store=get_price_history_store(config),
    provider="marketdata",
    as_of=date(2026, 5, 22),
)
```

The snapshot is JSON-safe:

```json
{
  "schema_version": 1,
  "method": "vrp_lite_features_v1",
  "ticker": "TSLA",
  "provider": "marketdata",
  "source_status": "PARTIAL",
  "price": {
    "method": "close_to_close_rv_v1",
    "newest_completed_session": "2026-05-22",
    "price_history_lookback_sessions": 260,
    "rv_3d": 0.0182,
    "rv_3d_percentile_1y": 62.5
  },
  "iv": {
    "method": "current_chain_with_optional_history_v1",
    "representative_iv": 0.345,
    "iv_percentile_1y": null,
    "iv_source_method": "current_chain_proxy",
    "dte_buckets": {
      "8_14": {
        "representative_iv": 0.345,
        "iv_percentile": null,
        "current_observation_count": 42,
        "history_observation_count": 0
      }
    }
  }
}
```

`price` uses close-to-close log-return realized volatility over 3, 5, and
10-trading-day windows and percentile ranks those values against the loaded
history window. Values are decimal volatility over the window, not annualized
figures. `iv` uses current-chain representative IV plus optional IV-history
percentiles. When no durable IV history is supplied, historical percentile
fields remain `null` and `iv_source_method` is `current_chain_proxy`; consumers
must not treat current cross-section rank as a historical IV percentile.
Direct helper calls enforce ticker scope at the public boundary: price-history
frames with ticker/provider identity columns are filtered to the requested
symbol and provider, option-chain frames must carry a ticker identity column,
and unscoped IV-history frames are ignored for ticker-specific percentiles.
Volatility helper identity and window arguments are validated before store or
calculation work starts: tickers must be non-empty stock symbols using the same
letters/dots/up-to-ten-character policy as parsed portfolio symbols, required
providers must be non-empty strings, `as_of` must be a `date`, `datetime`, or
`YYYY-MM-DD` string, lookback and minimum-history windows must be positive
integers, and stale-day windows must be non-negative integers. When IV-history
frames carry `observation_date` or `date`, minimum-history readiness is based on
distinct observation dates, not duplicate rows for the same date.

`source_status` uses the stable source-health constants `SOURCE_READY`,
`SOURCE_PARTIAL`, `SOURCE_INSUFFICIENT_HISTORY`, `SOURCE_STALE`,
`SOURCE_MISSING`, and `SOURCE_ERROR`, whose values are `READY`, `PARTIAL`,
`INSUFFICIENT_HISTORY`, `STALE`, `MISSING`, and `ERROR`. Strategy-layer policy
decides whether these features are advisory, affect ranking, or are ignored;
`opx-chain` only supplies data facts and readiness metadata.

---

## 6. Staleness Contract

A downstream consumer is responsible for determining whether the latest dataset
is fresh enough for its purposes. `opx-chain` does not enforce freshness on behalf of
consumers.

The consumer should use `DatasetHandle.created_at` as the dataset-level timestamp.
For per-ticker freshness, the chain artifact includes `underlying_price_time` per
row. Optional price context is a separate JSON artifact with its own
`PRICE_CONTEXT_SCHEMA_VERSION`, `price_context_as_of`, `price_context_age_days`,
and `price_context_staleness_status`; consumers should apply a separate
freshness policy to those slower-moving daily-OHLCV fields rather than treating
them like intraday option quotes.

`opx-chain` does not expose a staleness API. The consumer decides what "fresh enough"
means and blocks its own pipeline when the threshold is exceeded.

---

## 7. Implemented Interface Notes

The following notes record the implemented behavior behind this interface. They
are present-tense contract notes, not pending TODOs.

### 7.1 `SCHEMA_VERSION` public constant

`SCHEMA_VERSION` lives in `opx_chain/__init__.py` and is the canonical public
constant for option-chain artifact schema compatibility. Export and storage code
read this constant rather than defining a second schema version value.
`DatasetRecord.schema_version` and `DatasetHandle.schema_version` carry the
writer's schema value for downstream compatibility checks.

### 7.2 `DatasetHandle` provenance and integrity fields

`DatasetHandle` includes `run_id`, `provider`, `content_hash`, `created_at`,
and `script_version` in addition to artifact location, format, row count, and
schema version. `get_dataset()` populates those fields from the persisted
`DatasetRecord` so consumers do not need to scan paginated dataset listings for
basic provenance and integrity checks. It also carries effective integrity and
dataset-facts status plus the versioned, content-bound projections described in
§4.1. Semantic consumers still call
`load_validated_option_chain_dataset(dataset_id)`; handle metadata alone does
not replace exact-byte read validation.

### 7.3 Add `--positions` argument to `opx-fetch`

Implemented. Behaviour is specified in `docs/PROJECT_SPEC.md` §7.3.

### 7.4 `get_storage_backend()` public factory function

Implemented. `opx_chain.storage.factory.get_storage_backend()` returns a
`StorageBackend` instance configured from the `opx-chain` config, or `None` when
storage is disabled. Enabled backends are memoized within the process by the
storage-affecting config values, so repeated calls return the same backend
instance until storage config changes or the cache is cleared.

Direct `get_storage_backend(config=...)` callers are validated at the same
public boundary as loaded runtime config for storage-affecting scalars:
false-like `storage_enabled` values disable storage, malformed enablement raises
`ConfigError`, `storage_backend` must be `filesystem` or `sqlite`, and
`storage_max_runs_retained` must be a nonnegative non-boolean integer.
`storage_dataset_format` uses the same `storage.dataset_format` boundary as
loaded config and must be `csv` or `parquet`.

### 7.5 `also_write_csv` config option

When `[storage] also_write_csv = false` (default `true`), `opx-fetch` skips
writing the timestamped
`<data-dir>/runs/options_engine_output_<ts>.csv` file. `<data-dir>` is
`[storage].dir` when configured and otherwise `$XDG_DATA_HOME/opx-chain`. Only the
storage-managed artifact is written. Downstream orchestrators that read the
timestamped filename pattern must either keep `also_write_csv = true` or switch to
reading through `get_storage_backend().list_datasets()`.

### 7.6 Provider cache public boundary

Implemented. `opx_chain.storage.cache.get_provider_cache()` returns `NullCache`
for `cache_backend = "none"` and `FilesystemCache` for
`cache_backend = "filesystem"`. Direct `config=` callers are validated against
the same cache backend and cache directory contract as loaded runtime config.
Relative cache directories resolve under `$XDG_CACHE_HOME/opx-chain/`.

Provider-cache method calls also share a stable boundary across disabled and
filesystem-backed cache implementations: keys must be nonblank strings, payloads
must be `bytes`, and TTLs must be positive non-boolean integers. Fetch
orchestration treats malformed cached JSON objects, malformed reserved pandas
timestamp markers, wrong-typed chain pickle payloads, and unpicklable chain
payloads as corrupt cache entries and invalidates them before refetching.

### 7.7 `opx-view --data-dir` and `--csv`

`opx-view` accepts a `--data-dir DIR` argument that overrides all dataset
discovery — it scans `DIR` for `.csv` and `.parquet` files ordered by
modification time. The `--csv` flag skips the storage backend and reads
timestamped CSV exports directly from the output directory. The default
behavior queries the storage backend first, falling back to the timestamped
CSV glob when no storage records exist.

### 7.8 `get_run()` on `StorageBackend`

`StorageBackend.get_run(run_id: str) -> RunRecord` is part of the formal storage
protocol and is implemented by the filesystem, SQLite, and memory backends.
Downstream consumers may call it through the typed interface to retrieve
run-level provenance such as effective tickers and positions fingerprint.

---

## 8. What Does Not Change

- CSV output format and column order (governed by `SCHEMA_VERSION`)
- output directory layout
- provider selection, scoring, and strategy-neutral normalization semantics
- `StorageBackend` write interface — consumers are read-only; they never call
  `create_run`, `write_dataset`, or any write method
- `opx-chain` config file format

---

## 9. Relationship to STORAGE_SPEC

This document and `docs/STORAGE_SPEC.md` are complementary:

- `STORAGE_SPEC.md` specifies the full internal storage architecture, all backends,
  the implementation order, and the testing strategy
- this document specifies the external-facing subset of that architecture that
  downstream consumers may depend on

When STORAGE_SPEC changes affect the public surface (e.g., a new field on
`DatasetHandle`), this document must be updated in the same commit.
# Quote-quality acquisition warnings

`StorageBackend.get_ticker_results(run_id)` can return `status="ok_with_warnings"`.
This means usable validated rows exist but acquisition refreshed or quarantined
isolated crossed quotes. `error_summary` contains JSON with `schema_version=1`,
`ticker`, `provider`, `checked_at`, `refresh_attempts`, `quarantined_count`, and
`affected_quotes` (original/refreshed bid/ask, quote time, identity and outcome).
Do not classify the whole ticker as unavailable. Surface the warning on both
fresh and reused datasets, and treat omitted held contracts as unavailable for
price-dependent actions. Published rows still pass strict integrity validation.
Row-scope normalized/filtered counts describe validated rows entering ordinary
post-download screening; quarantine counts are separate in the quality evidence.
Consumers must not claim complete candidate coverage from row-scope counts alone
when a ticker carries quality warnings.

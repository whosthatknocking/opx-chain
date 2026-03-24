# Overall Project Specification: opx

## 1. Overview

`opx` is a single-provider options data fetcher and local viewer.

The project downloads option-chain data for configured tickers, normalizes that data into a stable canonical CSV schema, computes shared analytics, writes timestamped exports, and serves a local browser UI for inspection.

Current supported providers:

- `yfinance`
- `massive` (`Massive / Polygon.io`)

Core product rules:

- exactly one provider is active for a run
- output files must not mix providers
- the canonical CSV schema is the main product contract
- provider-specific data should map into existing canonical fields where semantics match
- `viewer.py` remains unchanged as the top-level entrypoint name

## 2. Current Product Scope

The project currently supports:

- config-driven provider selection
- config-driven runtime thresholds and fetch behavior
- provider-backed fetches through `yfinance` or `massive`
- canonical CSV export with shared derived metrics
- provider-aware field reference documentation
- a local viewer for exported CSV files
- provider debug payload dumps for raw-response inspection

The project does not currently aim to:

- merge rows from multiple providers in one run
- auto-fallback between providers during a fetch
- expose provider-specific scratch fields in the CSV by default
- act as a live trading terminal
- implement a secret-management system beyond local user config

## 3. Naming and Packaging

The project name, repository name, and package path are `opx`.

Implemented naming rules:

- the Python package path is `opx`
- documentation and user-facing commands use `opx`
- `viewer.py` remains the unchanged entrypoint name
- no temporary compatibility layer for the old package name remains in the repo

## 4. Runtime Model

### 4.1 Single Active Provider

At runtime there is exactly one active provider.

Allowed provider keys:

- `yfinance`
- `massive`

Behavior:

- all fetches for a run use the configured provider only
- all exported rows carry `data_source`
- viewer dataset metadata surfaces the provider when the dataset is single-provider
- shared logs use provider-neutral wording such as `raw_provider_rows`

### 4.2 Config Source

The single source of truth for runtime settings is:

- `~/.config/opx/config.toml`

The config loader is responsible for:

- provider selection
- ticker selection
- thresholds and model settings
- Massive credentials
- debug-dump settings
- Massive request pacing and page-size settings

Defaults:

- if the config file is missing, the app uses built-in defaults
- the default provider is `yfinance`
- malformed or unsupported config values fall back to code defaults
- startup output prints the resolved config values actually applied
- secrets are redacted in startup output
- config fallback warnings are printed when defaults are applied

### 4.3 Secrets

Provider credentials are local-only configuration.

Current credential model:

- `yfinance` requires no secret
- `massive` reads `[providers.massive].api_key` from `~/.config/opx/config.toml`

Rules:

- secrets must never be stored in tracked repo files
- secrets must never be printed in full
- secrets must never be copied into docs or logs

Current missing-key behavior:

- if `massive` is selected but `[providers.massive].api_key` is absent, runtime config falls back to `yfinance` and records a clear warning
- if `massive` is selected with invalid credentials, the Massive provider fails clearly when used

## 5. Provider Implementations

### 5.1 Shared Provider Contract

Providers implement a shared interface and map vendor payloads into the canonical schema.

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
- provider retry handling uses exponential backoff with 3 retries
- request caller header identifies the app as `opx/<version>`
- fetch progress prints per-page API status and row-count progress
- raw per-response payload dumps can be written to `debug/`

Field-mapping rules already implemented for Massive include:

- `underlying_asset.ticker -> underlying_symbol`
- `details.ticker -> contract_symbol`, stripping the `O:` prefix
- `underlying_asset.price`, fallback `underlying_asset.value` -> `underlying_price`
- `last_quote.bid/ask` -> canonical `bid` / `ask`
- top-level `implied_volatility` -> canonical `implied_volatility`
- provider greeks populate canonical greek columns when semantics match
- `is_in_the_money` is derived from spot versus strike because the snapshot model does not expose a direct canonical flag

## 6. Output Contract

### 6.1 Canonical CSV

The canonical CSV schema is the primary product contract.

Requirements:

- exported rows include `data_source`
- shared export stays pinned to the canonical column set
- unexpected provider-specific scratch fields are dropped from export
- provider-specific branches should not create different CSV shapes

Canonical field sources may be:

- direct provider values
- transformed provider values
- app-derived values
- blank when the provider does not expose the required source data

Provider-specific field availability and mapping behavior are documented in:

- `docs/FIELD_REFERENCE.md`

### 6.2 Logging and Progress Output

Runtime output is provider-neutral and intended to make fetch progress visible.

Current behavior includes:

- startup output for resolved config state
- provider name shown in shared run logging
- ticker- and expiration-level progress
- raw provider row counts
- kept-row counts after normalization and filtering
- Massive per-page API status and cumulative result counts
- final row count and output-path reporting

### 6.3 Exit Status

Current CLI exit behavior:

- `0` when a CSV is written successfully
- `1` when the run completes but no data is fetched
- `130` when interrupted with `Ctrl+C`

## 7. Operational Safeguards

### 7.1 Single-Run Lock

The fetcher uses a lock file to prevent concurrent runs.

Current behavior:

- a run acquires `logs/fetcher.lock`
- a second run exits clearly if the lock is already held
- the lock file is removed when the run finishes or is interrupted

### 7.2 Debug Payload Dumps

Debug dumping is shared across providers.

Current behavior:

- controlled by config flags
- raw payload files are written under `debug/`
- dump filenames are prefixed with provider name
- Massive dumps are written per HTTP response page and include page numbers
- yfinance dumps cover underlying snapshots, expiration lists, and option-chain payloads

## 8. Documentation and Viewer

### 8.1 Documentation Layout

The documentation is split by audience.

Current structure:

- `README.md`: short landing page
- `docs/USER_GUIDE.md`: user-facing setup, running, config, and behavior
- `docs/DEVELOPMENT.md`: contributor/development reference
- `docs/FIELD_REFERENCE.md`: canonical field descriptions and provider mapping matrix

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
- a `Reference` tab backed by the field-reference document
- sortable/filterable table view
- summary highlights for exported contracts

## 9. Validation Status

The current repository state is validated by automated tests and lint checks.

Current validation coverage includes:

- config loading and fallback behavior
- provider factory selection and unsupported-provider handling
- unchanged `viewer.py` entrypoint behavior
- schema-preserving export behavior
- shared fetch logging behavior
- Massive normalization and field mapping
- Massive auth failure handling
- Massive retry and request-spacing behavior
- per-page Massive debug dump behavior
- viewer helper behavior tied to the field-reference docs

Current verification state:

- automated test suite passes
- tracked Python files pass `pylint`

## 10. Implementation History

The following project work has already landed.

### 10.1 Config Migration and Rename

Completed:

- migrated runtime settings into `~/.config/opx/config.toml`
- renamed the project/package to `opx`
- kept `viewer.py` unchanged
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

### 10.4 Documentation and Viewer Alignment

Completed:

- split user and development docs
- moved field reference into its own document
- added provider mapping matrix
- aligned viewer reference content with the field-reference document

### 10.5 Validation and Runtime UX

Completed:

- expanded automated tests
- added clear exit-status behavior
- added graceful interrupt handling
- added fetch progress output
- added raw provider debug dumps
- added single-run locking

## 11. Current Change Rules

Future changes should preserve these project rules:

- keep the canonical CSV schema stable by default
- prefer provider mapping over schema expansion
- keep provider selection single-source and config-driven
- keep provider behavior explicit in documentation
- keep `viewer.py` unchanged as the entrypoint name
- avoid temporary compatibility shims for completed rename work

## 12. Current Completion Status

As of the current repository state:

- provider model: complete for `yfinance` and `massive`
- config migration: complete
- rename to `opx`: complete
- documentation split and provider-aware field reference: complete
- viewer/provider metadata alignment: complete
- validation coverage for shipped behavior: complete

There are no open milestone sections remaining in this specification. Future work, if any, should be added as new scoped proposals rather than re-opening the completed migration plan.

# Development Guide

This guide is for people changing the codebase, adding providers, or working on project tooling.

## Project Structure

The repository is organized around a small set of package and documentation
areas. Keep this map at directory/responsibility granularity; file-level
inventories drift quickly and are covered by tests only for load-bearing docs
and modules.

```text
.
├── docs/                  # public specs, user guide, metadata/storage contracts
├── opx_chain/             # runtime package and CLI entry-point modules
│   ├── providers/         # provider adapters plus shared provider contract
│   ├── storage/           # storage protocol, backends, serializers, atomic IO
│   └── docs/              # packaged read-only docs served with the wheel
├── scripts/               # local quality, version, and screenshot helpers
├── tests/                 # pytest contract, behavior, and doc drift coverage
└── pyproject.toml         # package metadata, entry points, and tool config
```

Load-bearing source-of-truth documents:

- `docs/PROJECT_SPEC.md`: fetch pipeline, provider contract, and runtime design
- `docs/EXTERNAL_INTERFACE_SPEC.md`: stable downstream public interface
- `docs/STORAGE_SPEC.md`: storage protocol, backends, retention, and artifacts
- `docs/METADATA_SPEC.md`: record/table metadata contract
- `docs/FIELD_REFERENCE.md`: exported option-chain field definitions
- `docs/USER_GUIDE.md`: operator setup and usage

Load-bearing runtime modules:

- `opx_chain/fetcher.py`: storage-enabled fetch orchestration
- `opx_chain/fetch.py`: provider fetch/normalization flow
- `opx_chain/positions.py`: Fidelity positions parsing and fingerprinting
- `opx_chain/price_context.py`: daily-OHLCV price-context artifact
- `opx_chain/price_history.py`: durable daily-OHLCV history store
- `opx_chain/iv_history.py`: durable aggregate implied-volatility history store
- `opx_chain/volatility_features.py`: downstream volatility feature builders
- `opx_chain/storage/`: storage backends, atomic writes, and artifact models
- `opx_chain/viewer.py`: local browser UI and HTTP API

Load-bearing local tooling:

- `scripts/run_local_quality_checks.sh`: commit-hook quality gate wrapper
- `scripts/run_local_coverage.sh`: local coverage report helper
- `scripts/check_version.py`: package/tag version consistency check

Runtime-generated files live under the XDG base directories rather than the repository root:

- `$XDG_CONFIG_HOME/opx-chain/` for `config.toml`
- `$XDG_DATA_HOME/opx-chain/` for `runs/`, `debug/`, `positions.csv`,
  `price-history.db`, `iv-history.db`, and `fetcher.lock`
- `[storage].dir` overrides the data directory for fetcher locks, CSV side writes,
  and storage-managed run artifacts when configured.
- `$XDG_STATE_HOME/opx-chain/` for run logs
- `$XDG_CACHE_HOME/opx-chain/cache/` for provider cache files when `cache_backend = "filesystem"`

## Provider Contract

The runtime uses exactly one active provider per run. The selected provider is recorded in `data_source`, and shared code paths keep the exported CSV pinned to the canonical column set documented in the user guide.

Runtime interface:

- `DataProvider.prepare_ticker_fetch`
- `DataProvider.external_logger_names`
- `DataProvider.debug_dump_payload`
- `DataProvider.load_ticker_events`
- `DataProvider.load_price_history`
- `DataProvider.load_historical_option_chain_frame`
- `DataProvider.load_underlying_snapshot`
- `DataProvider.list_option_expirations`
- `DataProvider.load_option_chain`
- `DataProvider.normalize_option_frame`

Provider rules:

- provider-native values should populate canonical columns when the semantics match
- derived app values should be used only when the provider does not supply the canonical field or cannot be mapped safely
- provider-specific scratch or debug fields should not expand the CSV schema implicitly
- mixed-provider rows should not appear in the same output file
- preserve raw provider values until the shared provider-response integrity
  validator has inspected them; do not coerce malformed present values into
  ordinary nulls before that boundary
- run the shared complete-frame integrity validator before post-download
  filtering so invalid rows cannot disappear into filter loss

## Option-chain integrity boundary

Public, provider-neutral contracts live in `opx_chain.integrity`. Validation
implementation details live in the package-private
`opx_chain._integrity_validation` module. Provider adapters may expose only the
minimal raw-value preservation needed by that shared validator; they must not
define provider-specific copies of integrity policy.

Every output path validates the exact serialized bytes that it will publish.
Storage backends stage those bytes and metadata during `write_dataset`, then
make the exact dataset id discoverable only when `finalize_run` completes.
Semantic storage consumers call
`load_validated_option_chain_dataset(dataset_id)` instead of reading
`DatasetHandle.location` directly. Direct reads are limited to raw inspection,
download/export surfaces, and explicit compatibility paths that perform their
own validation.

Integrity checks must remain independent of strategy concepts such as DTE
limits, portfolio exposure, recommendation eligibility, or candidate ranking.

## Local Style Contracts

Repeated helper logic should be centralized inside this package without
creating cross-package glue. In particular:

- Package loggers use `opx_chain.runlog.get_logger(...)` and
  `logger_name(...)`. Reusable module loggers should be named `_LOGGER`.
- Runtime UTC timestamp displays, compact artifact filename timestamps, and
  run-id fallback timestamps use `opx_chain.timestamps` helpers/constants
  rather than repeating `strftime(...)` format strings.
- Cross-package duplication with `opx-strategy` is not enough reason to add a
  shared abstraction. Promote an API only when it is part of the documented
  market-data, storage, dataset, option-chain, or positions-parsing contract.

## Development Setup

Install dependencies from `pyproject.toml`:

```
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

For the full local development setup, add the optional dev extras:

```
python -m pip install -e ".[dev]"
python -m playwright install
```

This installs all market-data client libraries used by the project, including the official `massive` client for Massive / Polygon access and the official `marketdata-sdk-py` client for Market Data access.

`playwright` is optional for the fetch/export pipeline itself, but required if you want automated browser screenshots or browser-driven UI checks.

For Massive / Polygon access, this project assumes you have an options-capable Massive account. The default `request_interval_seconds = 12.0` is intentionally conservative for delayed-plan usage, and you should adjust it together with `max_retries` and `backoff_seconds` in `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`) to match the actual rate limits and throughput your Massive options plan allows.

For Market Data access, this project assumes you have a Market Data account and API token configured under `[providers.marketdata].api_token`.
The Market Data provider now retries transient failures (`429`, `408`, `5xx`, and request exceptions) with exponential backoff, honors `Retry-After` when present, and exposes optional client-side pacing through `[providers.marketdata].request_interval_seconds`. Expected event no-data responses, such as absent dividend data, are classified as blank event fields rather than transient failures.

## External Dependencies

The runtime depends on a small set of external libraries and upstream market-data services. Keep the code and docs aligned with the official SDKs and API documentation rather than reverse-engineering payloads from old logs.

### Core Python Dependencies

- `pandas`: primary tabular container for provider frames, normalization, and CSV export
- `numpy`: numeric coercion, missing-value handling, and vectorized calculations
- `pytest`: test runner
- `coverage` and `pytest-cov`: local and CI-friendly coverage reporting
- `pylint`: static linting used in CI
- `playwright`: optional browser automation for viewer screenshots and UI checks

### Provider Libraries and Upstream APIs

- `yfinance`
  - Package: `yfinance`
  - Upstream surface used here: `Ticker.info`, `Ticker.fast_info`, `Ticker.options`, `Ticker.option_chain(...)`, and `Ticker.history(...)`
  - Project reference: https://github.com/ranaroussi/yfinance
  - Notes:
    - this is an unofficial Yahoo Finance wrapper
    - quote completeness and timestamp behavior can drift without a versioned API contract

- Massive / Polygon
  - Package: `massive`
  - Upstream surface used here: official `RESTClient` with `list_snapshot_options_chain(...)`
  - Primary API endpoint: `GET /v3/snapshot/options/{underlyingAsset}`
  - API reference: https://massive.com/docs/rest/options/snapshots/option-chain-snapshot
  - Client reference: https://polygon.readthedocs.io/en/latest/Library-Interface-Documentation.html
  - Notes:
    - the app uses the official client, not raw `urllib` calls
    - request pacing and page size are controlled through config because plan limits vary
    - current implementation derives underlying snapshot, expirations, and chain rows from this single snapshot-chain flow

- Market Data
  - Package: `marketdata-sdk-py`
  - Upstream surface used here: official `MarketDataClient.options.chain(...)`
  - SDK installation reference: https://www.marketdata.app/docs/sdk/py/installation/
  - SDK authentication reference: https://www.marketdata.app/docs/sdk/py/authentication/
  - Options chain reference: https://www.marketdata.app/docs/sdk/py/options/chain/
  - Notes:
    - the provider uses a single `options.chain(symbol, expiration="all", output_format=OutputFormat.INTERNAL, mode=...)` call per ticker
    - the SDK supports `mode`, which the app exposes through `[providers.marketdata].mode`
    - Market Data's Free Forever tier is 24 hours delayed for stocks and options, so tests and user docs should not describe that plan as near-real-time
    - the app adds its own transient retry/backoff handling and optional request spacing instead of assuming a fixed SDK-side rate-limit policy
    - the provider intentionally disables the SDK startup rate-limit probe to avoid spending an extra API call during initialization

## Provider Integration Notes

When changing a provider implementation, verify all three layers together:

- package contract
  - the installed SDK/wrapper shape and method signatures
- upstream API contract
  - endpoint fields, pagination behavior, timestamp semantics, auth expectations
- canonical mapping
  - how provider fields land in the exported schema described in [FIELD_REFERENCE.md](FIELD_REFERENCE.md)

Rules to keep the provider layer stable:

- prefer official SDKs when the provider offers one
- avoid adding extra per-ticker API calls when the active endpoint already carries the needed fields
- treat provider-side filtering carefully; shared app filtering should stay the main screening path unless there is a strong reason to narrow upstream payloads
- keep debug payload dumps representative of the exact provider response shape so mapping regressions can be audited later
- update both [FIELD_REFERENCE.md](FIELD_REFERENCE.md) and [PROJECT_SPEC.md](PROJECT_SPEC.md) when a provider mapping or dependency changes

## Debugging Config

The runtime exposes a small debugging config surface through `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`). Use [`../config/example.toml`](../config/example.toml) as the starting point for local config and then override only the debugging keys you need.

Objective:

- The debugging config exists to make provider and normalization issues inspectable without changing code or adding ad hoc print statements.
- Its main purpose is to answer questions like "did the provider actually send this field?" and "did the app drop or transform it later?"
- It should help you debug missing values, stale timestamps, quote-shape regressions, and provider mapping changes while keeping the canonical CSV schema clean.

Current debugging settings:

- `debug_dump_provider_payload = true|false`
  - when enabled, the app writes raw provider payloads to disk before normalization
  - use this when a canonical field is unexpectedly blank, a provider response shape appears to have changed, or a mapping bug is suspected
- `debug_dump_dir = "debug"` (resolved under `$XDG_DATA_HOME/opx-chain/`)
  - controls where raw provider payload files are written
  - use a custom path when you want to isolate one investigation from older dumps
- `enable_validation = true|false`
  - controls whether shared row-level and file-level validation runs
  - keep this enabled by default; disable it only when you need to inspect raw normalized output without validation noise

Example debugging override:

```toml
[settings]
enable_validation = true
debug_dump_provider_payload = true
debug_dump_dir = "debug/provider-check"
```

How to use it:

- turn on `debug_dump_provider_payload` before reproducing the issue
- run `opx-fetch`
- inspect the newest files under `debug_dump_dir` and compare them with the exported CSV fields; relative paths resolve under `$XDG_DATA_HOME/opx-chain/`
- turn the dump back off after the investigation so normal runs do not accumulate unnecessary payload files

## Shared Metrics and Viewer Scope

Shared metrics should stay provider-agnostic once data has been normalized into the canonical schema.

Current shared ranking fields include:

- `quote_quality_score`
- `option_score`

Rules:

- derive them from canonical columns only
- keep formulas and config knobs shared across providers
- expose meaningful user-tunable inputs through runtime config when that improves iteration without code changes
- keep the viewer aligned with those fields so exported rankings and summary-tab highlights reflect the same scoring logic

## Documentation Assets

Regenerate the viewer screenshot used in the user docs with:

```
python scripts/capture_viewer_screenshot.py
```

By default this saves a dark-mode full-page screenshot to:

```text
docs/images/viewer-option-chain.png
```

Optional flags:

```
python scripts/capture_viewer_screenshot.py --theme light
python scripts/capture_viewer_screenshot.py --output docs/images/viewer-custom.png
```

## Verification

Run the basic test suite with:

```
pytest
```

Run the linter with:

```
pylint $(git ls-files '*.py')
```

Run the dedicated local coverage workflow with:

```bash
./scripts/run_local_coverage.sh
```

That script:

- runs the full pytest suite under coverage using the repo's `pyproject.toml` coverage config
- measures the packaged modules plus the top-level compatibility entrypoint `main.py`
- prints a missing-lines terminal report sorted by lowest coverage first
- writes browser and machine-readable artifacts to `htmlcov/index.html`, `coverage.xml`, and `coverage.json`
- prints a final `--skip-covered` report so only files with remaining gaps stay in view

If you need a non-default interpreter, set `OPX_COVERAGE_PYTHON=/path/to/python` before running the script.

## Local Git Hooks

This repo includes a tracked pre-commit hook at `.githooks/pre-commit` that runs the same baseline quality checks before every commit:

```bash
pytest -q
pylint $(git ls-files '*.py')
```

Enable it in your local clone with:

```bash
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit scripts/run_local_quality_checks.sh
```

Once enabled, `git commit` will stop before creating the commit if either check fails.

Notes:

- the hook prefers `.venv/bin/python` when present and otherwise falls back to `python3`
- set `OPX_SKIP_PRE_COMMIT_CHECKS=1` only when you intentionally need to bypass the local hook for a one-off commit
- set `OPX_PRE_COMMIT_PYTHON=/path/to/python` if you need the hook to use a specific interpreter

## Notes

- Market data is routed through a configurable provider layer.
- Quote timing and completeness depend on the upstream source and plan access.
- The exported CSV is intended to be consumed by another tool, so the script favors schema clarity and enriched raw data over trade recommendations.
- The viewer is intended for inspection and triage, not as a live trading terminal.

## Versioning and Releases

The package version is defined in `pyproject.toml` and is the single source of truth for releases.

Rules:

- Use plain semver `X.Y.Z` in `pyproject.toml`.
- Runtime `SCRIPT_VERSION`, package `opx_chain.__version__`, provider user-agent strings, and release validation all derive from that package version.
- GitHub release tags must be named `vX.Y.Z` and must match the package version exactly.

GitHub automation:

- `.github/workflows/version.yml` validates version consistency on pull requests, pushes to `main`, and manual dispatches.
- The same workflow creates a GitHub Release automatically when a matching `vX.Y.Z` tag is pushed.
- Release artifacts include the built source distribution and wheel from `python -m build`.
- The release job runs `twine check dist/*`, installs the built wheel, and re-runs version validation before publishing the GitHub Release.

CI support window:

- `.github/workflows/ci.yml` runs tests on Python `3.10`, `3.11`, `3.12`, and `3.13`.
- Keep the CI matrix aligned with the supported runtime range declared in `pyproject.toml`.

Local validation:

```bash
python scripts/check_version.py
python scripts/check_version.py --tag v0.1.0
```
# Integrity Validation Performance

Timestamp validity checks memoize exact built-in strings within each validation
invocation (at most 1024 entries). Provider-response and canonical-frame checks
use independent caches; other scalar types keep the existing pandas parser.
Eviction affects only runtime. Validation findings, ordering, samples and all
publication/consumer integrity boundaries remain unchanged. No cross-dataset
cache or provider requests are introduced.

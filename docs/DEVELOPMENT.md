# Development Guide

This guide is for people changing the codebase, adding providers, or working on project tooling.

## Project Structure

```text
.
├── fetcher.py
├── viewer.py
├── docs/
│   ├── DEVELOPMENT.md
│   ├── USER_GUIDE.md
│   └── images/
│       └── viewer-option-chain.png
├── scripts/
│   └── capture_viewer_screenshot.py
├── opx/
│   ├── config.py
│   ├── export.py
│   ├── fetch.py
│   ├── greeks.py
│   ├── metrics.py
│   ├── normalize.py
│   ├── providers/
│   ├── runlog.py
│   ├── viewer.py
│   ├── viewer_static/
│   └── utils.py
├── main.py
├── logs/
├── debug/
├── outputs/
└── requirements.txt
```

## Provider Contract

The runtime uses exactly one active provider per run. The selected provider is recorded in `data_source`, and shared code paths keep the exported CSV pinned to the canonical column set documented in the user guide.

Provider rules:

- provider-native values should populate canonical columns when the semantics match
- derived app values should be used only when the provider does not supply the canonical field or cannot be mapped safely
- provider-specific scratch or debug fields should not expand the CSV schema implicitly
- mixed-provider rows should not appear in the same output file

## Development Setup

Install dependencies from `requirements.txt`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install
```

This installs all market-data client libraries used by the project, including the official `massive` client for Massive / Polygon access and the official `marketdata-sdk-py` client for Market Data access.

`playwright` is optional for the fetch/export pipeline itself, but required if you want automated browser screenshots or browser-driven UI checks.

For Massive / Polygon access, this project assumes you have an options-capable Massive account. The default `request_interval_seconds = 12.0` is intentionally conservative for delayed-plan usage, and you should adjust it in `~/.config/opx/config.toml` to match the actual rate limits and throughput your Massive options plan allows.

For Market Data access, this project assumes you have a Market Data account and API token configured under `[providers.marketdata].api_token`.
The Market Data provider now retries `429` rate-limit responses with exponential backoff, honors `Retry-After` when present, and exposes optional client-side pacing through `[providers.marketdata].request_interval_seconds`.

## External Dependencies

The runtime depends on a small set of external libraries and upstream market-data services. Keep the code and docs aligned with the official SDKs and API documentation rather than reverse-engineering payloads from old logs.

### Core Python Dependencies

- `pandas`: primary tabular container for provider frames, normalization, and CSV export
- `numpy`: numeric coercion, missing-value handling, and vectorized calculations
- `pytest`: test runner
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
    - the app adds its own `429` retry/backoff handling and optional request spacing instead of assuming a fixed SDK-side rate-limit policy
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
- update both [FIELD_REFERENCE.md](FIELD_REFERENCE.md) and [../PROJECT_SPEC.md](../PROJECT_SPEC.md) when a provider mapping or dependency changes

## Documentation Assets

Regenerate the viewer screenshot used in the user docs with:

```bash
python scripts/capture_viewer_screenshot.py
```

By default this saves a dark-mode full-page screenshot to:

```text
docs/images/viewer-option-chain.png
```

Optional flags:

```bash
python scripts/capture_viewer_screenshot.py --theme light
python scripts/capture_viewer_screenshot.py --output docs/images/viewer-custom.png
```

## Verification

Run the basic test suite with:

```bash
pytest
```

Run the linter with:

```bash
pylint $(git ls-files '*.py')
```

## Notes

- Market data is routed through a configurable provider layer.
- Quote timing and completeness depend on the upstream source and plan access.
- The exported CSV is intended to be consumed by another tool, so the script favors schema clarity and enriched raw data over trade recommendations.
- The viewer is intended for inspection and triage, not as a live trading terminal.

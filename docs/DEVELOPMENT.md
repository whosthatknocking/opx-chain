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

This installs both market-data client libraries used by the project, including the official `massive` client for Massive / Polygon access.

`playwright` is optional for the fetch/export pipeline itself, but required if you want automated browser screenshots or browser-driven UI checks.

For Massive / Polygon access, this project assumes you have an options-capable Massive account. The default `request_interval_seconds = 12.0` is intentionally conservative for delayed-plan usage, and you should adjust it in `~/.config/opx/config.toml` to match the actual rate limits and throughput your Massive options plan allows.

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

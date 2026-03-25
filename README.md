# opx

`opx` downloads near-term option chains, enriches them with pricing and screening metrics, writes a timestamped CSV, and serves a local browser UI for inspection.

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

![opx viewer](docs/images/viewer-option-chain.png)

## What You Get

- Fetches call and put chains for configured tickers
- Filters out zero-bid and wide-spread contracts before export
- Limits strikes to a configurable band around spot
- Computes Greeks, expected move, ROM-style metrics, configurable option scoring, and volatility context
- Writes a timestamped CSV plus an append-only run log
- Includes a local browser for exploring the output interactively, including dataset inspection, per-ticker overview cards, and chain visualizations with chart tooltips and click-through row details

Generated files are standardized under:

- `output/` for exported CSV snapshots
- `logs/` for run logs
- `debug/` for optional raw provider payload dumps

## Documentation

- User guide: [docs/USER_GUIDE.md](docs/USER_GUIDE.md)
- CSV field reference: [docs/FIELD_REFERENCE.md](docs/FIELD_REFERENCE.md)
- Development guide: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
- Project spec: [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md)
- Design spec: [docs/DESIGN_SPEC.md](docs/DESIGN_SPEC.md)

## Important Notes

- Yahoo Finance can be delayed, stale, or sparse, especially near the regular market open. Always check freshness fields before relying on the output.
- Massive support depends on your plan. Lower tiers can leave you with trades but no `bid` or `ask`, and quote access may require Massive's highest-cost quote-enabled options plan.
- Market Data plan access affects recency. The Free Forever tier is 24 hours delayed for both stock and options data, so treat that provider as end-of-day-plus data unless your plan includes fresher access.

## Requirements

- Python 3.10+
- Python dependencies installed from `requirements.txt`
- Internet access for provider data

Key dependencies:

- `yfinance` for the baseline Yahoo Finance provider
- `massive` for the official Massive / Polygon client library
- `marketdata-sdk-py` for the official Market Data client library
- `pandas`, `numpy`, and `scipy` for normalization and analytics
- `pytest` for the automated test suite
- `playwright` for browser-driven screenshot and UI checks

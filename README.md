# Options Fetcher

High-level Python project for collecting option chain data from Yahoo Finance, enriching it with derived metrics, and exporting the result to a timestamped CSV for downstream tools.

## What It Does

The script fetches near-term option chains for a configured list of tickers, normalizes the raw vendor data, calculates quality and pricing metrics, computes Black-Scholes Greeks, and writes a CSV file that another tool can consume.

The output is designed to be data-focused rather than decision-focused. It does not decide whether to close, roll, or open positions. Instead, it produces a richer dataset that can support those decisions elsewhere.

## Features

- Fetches call and put chains for configured tickers
- Limits expirations to a rolling three-month window
- Normalizes vendor fields into a stable CSV schema
- Adds quote quality, freshness, liquidity, and pricing metrics
- Computes Black-Scholes `delta`, `gamma`, `vega`, and `theta`
- Exports a timestamped CSV file for each run

## Requirements

- Python 3.10+
- Internet access for Yahoo Finance data

Install dependencies from `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How To Run

Run the project from the repository root:

```bash
python3 options_fetcher.py
```

You can also run the main entrypoint directly:

```bash
python3 main.py
```

## Output

Each run writes a CSV file in the project root using a timestamped filename:

```text
options_engine_output_YYYYMMDD_HHMMSS.csv
```

The CSV includes:

- raw option quote fields
- normalized option and underlying metadata
- quote freshness and quality flags
- liquidity and spread metrics
- intrinsic and extrinsic value metrics
- break-even and premium-based metrics
- Black-Scholes Greeks

## Project Structure

```text
.
├── main.py
├── options_fetcher.py
├── options_fetcher_app/
│   ├── config.py
│   ├── export.py
│   ├── fetch.py
│   ├── greeks.py
│   ├── metrics.py
│   ├── normalize.py
│   └── utils.py
└── requirements.txt
```

## Configuration

Core configuration lives in `options_fetcher_app/config.py`, including:

- ticker list
- minimum liquidity thresholds
- spread threshold
- risk-free rate used for Greeks
- stale quote threshold

## Notes

- Data is sourced from Yahoo Finance through `yfinance`.
- Quote timing and completeness depend on the upstream source.
- The exported CSV is intended to be consumed by another tool, so the script favors schema clarity and enriched raw data over trade recommendations.

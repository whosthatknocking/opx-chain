"""Pytest configuration ensuring the repository root is importable."""

from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest  # pylint: disable=wrong-import-position

from opx.config import RuntimeConfig, reset_runtime_config  # pylint: disable=wrong-import-position


def make_runtime_config(**overrides):
    """Build a standard runtime config for tests with optional overrides."""
    defaults = {
        "tickers": ("TEST",),
        "min_bid": 0.5,
        "min_open_interest": 100,
        "min_volume": 10,
        "max_spread_pct_of_mid": 0.25,
        "risk_free_rate": 0.045,
        "hv_lookback_days": 30,
        "trading_days_per_year": 252,
        "data_provider": "yfinance",
        "stale_quote_seconds": 900,
        "enable_post_download_filters": True,
        "debug_dump_provider_payload": False,
        "debug_dump_dir": Path("/tmp/opx-provider-debug"),
        "max_strike_distance_pct": 0.30,
        "max_expiration_weeks": 14,
        "max_expiration": "2026-06-30",
        "today": date(2026, 3, 20),
        "massive_api_key": None,
        "marketdata_api_token": None,
        "marketdata_mode": None,
        "marketdata_max_retries": 3,
        "marketdata_request_interval_seconds": 0.0,
        "massive_snapshot_page_limit": 250,
        "massive_request_interval_seconds": 12.0,
        "config_path": Path("/tmp/opx.toml"),
    }
    defaults.update(overrides)
    return RuntimeConfig(**defaults)


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Ensure tests do not share cached runtime config state."""
    reset_runtime_config()
    yield
    reset_runtime_config()

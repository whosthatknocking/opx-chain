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
        "option_score_income_weight": 0.30,
        "option_score_liquidity_weight": 0.30,
        "option_score_risk_weight": 0.25,
        "option_score_efficiency_weight": 0.15,
        "data_provider": "yfinance",
        "stale_quote_seconds": 21600,
        "enable_filters": True,
        "enable_validation": True,
        "debug_dump_provider_payload": False,
        "debug_dump_dir": Path("/tmp/opx-provider-debug"),
        "viewer_host": "127.0.0.1",
        "viewer_port": 8000,
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
        "storage_enabled": False,
        "storage_backend": "filesystem",
        "storage_max_runs_retained": 0,
        "storage_dataset_format": "csv",
        "storage_write_legacy_csv": True,
        "provider_cache_backend": "none",
        "provider_cache_dir": Path("cache"),
        "provider_snapshot_ttl": 300,
        "provider_chain_ttl": 300,
        "provider_events_ttl": 86400,
    }
    defaults.update(overrides)
    return RuntimeConfig(**defaults)


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Ensure tests do not share cached runtime config state."""
    reset_runtime_config()
    yield
    reset_runtime_config()

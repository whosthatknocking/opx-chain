"""Config-loader and provider-selection tests for Milestone 1."""

from pathlib import Path

import pytest

from opx.config import ConfigError, load_runtime_config, reset_runtime_config
from opx.providers import MassiveProvider, YFinanceProvider, get_data_provider


def test_load_runtime_config_uses_defaults_when_file_is_absent(tmp_path: Path):
    """Default config should keep yfinance usable without a user config file."""
    config = load_runtime_config(tmp_path / "missing.toml")

    assert config.data_provider == "yfinance"
    assert config.massive_api_key is None
    assert config.tickers
    assert config.config_path == tmp_path / "missing.toml"


def test_load_runtime_config_reads_user_config_file(tmp_path: Path):
    """Runtime settings should load from ~/.config/opx/config.toml format."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
tickers = ["spy", "qqq"]
data_provider = "yfinance"
min_bid = 1.25
max_expiration = "2026-07-31"

[providers.massive]
api_key = "secret"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.tickers == ("SPY", "QQQ")
    assert config.data_provider == "yfinance"
    assert config.min_bid == 1.25
    assert config.max_expiration == "2026-07-31"
    assert config.massive_api_key == "secret"


def test_load_runtime_config_requires_massive_key_only_when_selected(tmp_path: Path):
    """Massive key validation should trigger only for the Massive provider."""
    yfinance_config = tmp_path / "yfinance.toml"
    yfinance_config.write_text("[settings]\ndata_provider = 'yfinance'\n", encoding="utf-8")
    assert load_runtime_config(yfinance_config).data_provider == "yfinance"

    massive_config = tmp_path / "massive.toml"
    massive_config.write_text("[settings]\ndata_provider = 'massive'\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="Missing Massive API key"):
        load_runtime_config(massive_config)


def test_get_data_provider_returns_provider_from_runtime_config(monkeypatch, tmp_path: Path):
    """Provider factory should resolve yfinance and massive from config."""
    yfinance_config = tmp_path / "yfinance.toml"
    yfinance_config.write_text("[settings]\ndata_provider = 'yfinance'\n", encoding="utf-8")
    monkeypatch.setattr("opx.config.DEFAULT_CONFIG_PATH", yfinance_config)
    assert isinstance(get_data_provider(), YFinanceProvider)

    massive_config = tmp_path / "massive.toml"
    massive_config.write_text(
        "[settings]\ndata_provider = 'massive'\n\n[providers.massive]\napi_key = 'secret'\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("opx.config.DEFAULT_CONFIG_PATH", massive_config)

    reset_runtime_config()
    assert isinstance(get_data_provider(), MassiveProvider)


def test_load_runtime_config_rejects_unsupported_provider(tmp_path: Path):
    """Unsupported provider names should raise a clear error."""
    config_path = tmp_path / "bad.toml"
    config_path.write_text("[settings]\ndata_provider = 'invalid'\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="Unsupported provider 'invalid'"):
        load_runtime_config(config_path)

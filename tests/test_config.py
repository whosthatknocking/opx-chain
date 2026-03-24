"""Config-loader and provider-selection tests for Milestone 1."""

from pathlib import Path

from opx.config import describe_runtime_config, load_runtime_config, reset_runtime_config
from opx.providers import (
    PROVIDER_FACTORIES,
    MassiveProvider,
    YFinanceProvider,
    get_data_provider,
)


def test_load_runtime_config_uses_defaults_when_file_is_absent(tmp_path: Path):
    """Default config should keep yfinance usable without a user config file."""
    config = load_runtime_config(tmp_path / "missing.toml")

    assert config.data_provider == "yfinance"
    assert config.massive_api_key is None
    assert config.marketdata_api_token is None
    assert config.marketdata_max_retries == 3
    assert config.marketdata_request_interval_seconds == 0.0
    assert config.massive_snapshot_page_limit == 250
    assert config.massive_request_interval_seconds == 12.0
    assert config.debug_dump_provider_payload is False
    assert config.debug_dump_dir == Path("debug")
    assert config.enable_post_download_filters is True
    assert config.max_expiration_weeks == 26
    assert config.max_expiration is not None
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
enable_post_download_filters = false
debug_dump_provider_payload = true
debug_dump_dir = "logs/provider_payloads"
max_expiration_weeks = 8

[providers.massive]
api_key = "secret"
snapshot_page_limit = 250
request_interval_seconds = 1.5

[providers.marketdata]
api_token = "market-token"
mode = "delayed"
max_retries = 5
request_interval_seconds = 0.75
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.tickers == ("SPY", "QQQ")
    assert config.data_provider == "yfinance"
    assert config.min_bid == 1.25
    assert config.enable_post_download_filters is False
    assert config.debug_dump_provider_payload is True
    assert config.debug_dump_dir == Path("logs/provider_payloads")
    assert config.max_expiration_weeks == 8
    assert config.max_expiration is not None
    assert config.massive_api_key == "secret"
    assert config.marketdata_api_token == "market-token"
    assert config.marketdata_mode == "delayed"
    assert config.marketdata_max_retries == 5
    assert config.marketdata_request_interval_seconds == 0.75
    assert config.massive_snapshot_page_limit == 250
    assert config.massive_request_interval_seconds == 1.5


def test_load_runtime_config_requires_massive_key_only_when_selected(tmp_path: Path):
    """Missing Massive credentials should fall back to the default provider."""
    yfinance_config = tmp_path / "yfinance.toml"
    yfinance_config.write_text("[settings]\ndata_provider = 'yfinance'\n", encoding="utf-8")
    assert load_runtime_config(yfinance_config).data_provider == "yfinance"

    massive_config = tmp_path / "massive.toml"
    massive_config.write_text("[settings]\ndata_provider = 'massive'\n", encoding="utf-8")

    config = load_runtime_config(massive_config)
    assert config.data_provider == "yfinance"
    assert any("falling back to 'yfinance'" in warning for warning in config.config_warnings)


def test_load_runtime_config_requires_marketdata_token_only_when_selected(tmp_path: Path):
    """Missing Market Data credentials should fall back to the default provider."""
    marketdata_config = tmp_path / "marketdata.toml"
    marketdata_config.write_text(
        "[settings]\ndata_provider = 'marketdata'\n",
        encoding="utf-8",
    )

    config = load_runtime_config(marketdata_config)
    assert config.data_provider == "yfinance"
    assert any("providers.marketdata.api_token" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_marketdata_mode(tmp_path: Path):
    """Invalid Market Data mode values should fall back to the default."""
    config_path = tmp_path / "marketdata-mode.toml"
    config_path.write_text(
        """
[providers.marketdata]
mode = "fast"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)
    assert config.marketdata_mode is None
    assert any("providers.marketdata.mode" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_marketdata_tuning(tmp_path: Path):
    """Invalid Market Data rate-limit settings should fall back to defaults."""
    negative_retries = tmp_path / "marketdata-retries.toml"
    negative_retries.write_text(
        """
[providers.marketdata]
max_retries = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_retries)
    assert config.marketdata_max_retries == 3
    assert any("providers.marketdata.max_retries" in warning for warning in config.config_warnings)

    negative_interval = tmp_path / "marketdata-interval.toml"
    negative_interval.write_text(
        """
[providers.marketdata]
request_interval_seconds = -0.5
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_interval)
    assert config.marketdata_request_interval_seconds == 0.0
    assert any(
        "providers.marketdata.request_interval_seconds" in warning
        for warning in config.config_warnings
    )


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


def test_load_runtime_config_defaults_unsupported_provider(tmp_path: Path):
    """Unsupported provider names should fall back to the default provider."""
    config_path = tmp_path / "bad.toml"
    config_path.write_text("[settings]\ndata_provider = 'invalid'\n", encoding="utf-8")

    config = load_runtime_config(config_path)
    assert config.data_provider == "yfinance"
    assert any("settings.data_provider" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_massive_tuning(tmp_path: Path):
    """Invalid Massive request spacing and page-size settings should use defaults."""
    zero_limit = tmp_path / "zero-limit.toml"
    zero_limit.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
snapshot_page_limit = 0
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(zero_limit)
    assert config.massive_snapshot_page_limit == 250
    assert any("snapshot_page_limit" in warning for warning in config.config_warnings)

    too_large_limit = tmp_path / "too-large-limit.toml"
    too_large_limit.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
snapshot_page_limit = 1000
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(too_large_limit)
    assert config.massive_snapshot_page_limit == 250
    assert any("clamped to 250" in warning for warning in config.config_warnings)

    negative_interval = tmp_path / "negative-interval.toml"
    negative_interval.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
request_interval_seconds = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_interval)
    assert config.massive_request_interval_seconds == 12.0
    assert any("request_interval_seconds" in warning for warning in config.config_warnings)

    bad_debug_toggle = tmp_path / "bad-debug-toggle.toml"
    bad_debug_toggle.write_text(
        """
[settings]
data_provider = "massive"
debug_dump_provider_payload = "yes"
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(bad_debug_toggle)
    assert config.debug_dump_provider_payload is False
    assert any("debug_dump_provider_payload" in warning for warning in config.config_warnings)

    bad_debug_dir = tmp_path / "bad-debug-dir.toml"
    bad_debug_dir.write_text(
        """
[settings]
debug_dump_dir = 42
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(bad_debug_dir)
    assert config.debug_dump_dir == Path("debug")
    assert any("debug_dump_dir" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_filter_toggle(tmp_path: Path):
    """Invalid filter-toggle values should fall back to the default."""
    config_path = tmp_path / "bad-filter-toggle.toml"
    config_path.write_text(
        """
[settings]
enable_post_download_filters = "sometimes"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.enable_post_download_filters is True
    assert any("enable_post_download_filters" in warning for warning in config.config_warnings)


def test_load_runtime_config_supports_disabling_max_expiration(tmp_path: Path):
    """A zero-week max expiration should disable the expiration cap."""
    config_path = tmp_path / "no-expiration-cap.toml"
    config_path.write_text(
        """
[settings]
max_expiration_weeks = 0
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.max_expiration_weeks == 0
    assert config.max_expiration is None


def test_load_runtime_config_defaults_invalid_toml(tmp_path: Path):
    """Malformed config files should fall back to built-in defaults."""
    config_path = tmp_path / "broken.toml"
    config_path.write_text("[settings\n", encoding="utf-8")

    config = load_runtime_config(config_path)

    assert config.data_provider == "yfinance"
    assert config.tickers


def test_describe_runtime_config_masks_massive_key(tmp_path: Path):
    """Resolved config output should avoid printing secrets."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[providers.massive]
api_key = "secret"

[providers.marketdata]
api_token = "market-token"
""".strip(),
        encoding="utf-8",
    )

    lines = describe_runtime_config(load_runtime_config(config_path))

    assert any(line.endswith("set") for line in lines if "api_key" in line)
    assert all("secret" not in line for line in lines)
    assert all("market-token" not in line for line in lines)
    assert any("debug_dump_provider_payload" in line for line in lines)
    assert any("debug_dump_dir" in line for line in lines)
    assert any("providers.marketdata.api_token" in line for line in lines)


def test_provider_registry_exposes_supported_providers():
    """The shared factory registry should enumerate the supported provider set."""
    assert set(PROVIDER_FACTORIES) == {"yfinance", "massive", "marketdata"}

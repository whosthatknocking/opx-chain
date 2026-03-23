"""Runtime configuration loading for the options fetch pipeline."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib

SUPPORTED_PROVIDERS = frozenset({"yfinance", "massive"})
SCRIPT_VERSION = "2026-03-23.2"
DEFAULT_CONFIG_PATH = Path("~/.config/opx/config.toml").expanduser()


class ConfigError(ValueError):
    """Raised when user config is invalid for the requested runtime."""


@dataclass(frozen=True)
# pylint: disable=too-many-instance-attributes
class RuntimeConfig:
    """Resolved runtime settings used by the application."""

    tickers: tuple[str, ...]
    min_bid: float
    min_open_interest: int
    min_volume: int
    max_spread_pct_of_mid: float
    risk_free_rate: float
    hv_lookback_days: int
    trading_days_per_year: int
    data_provider: str
    stale_quote_seconds: int
    max_strike_distance_pct: float
    max_expiration: str
    today: date
    massive_api_key: str | None
    config_path: Path


def _default_max_expiration(today):
    year = today.year
    month = today.month + 4
    if month > 12:
        month -= 12
        year += 1
    _, last_day = monthrange(year, month)
    return f"{year}-{month:02d}-{last_day:02d}"


def _coerce_list(value, *, field_name):
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"Config field '{field_name}' must be a list of strings.")
    normalized = tuple(item.strip().upper() for item in value if item.strip())
    if not normalized:
        raise ConfigError(f"Config field '{field_name}' must not be empty.")
    return normalized


def _coerce_str(value, *, field_name):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Config field '{field_name}' must be a string.")
    normalized = value.strip()
    if not normalized:
        raise ConfigError(f"Config field '{field_name}' must not be blank.")
    return normalized


def _coerce_int(value, *, field_name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Config field '{field_name}' must be an integer.")
    return value


def _coerce_float(value, *, field_name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"Config field '{field_name}' must be numeric.")
    return float(value)


def _read_config_data(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    if not isinstance(data, dict):
        raise ConfigError(f"Config file '{config_path}' must contain a TOML table.")
    return data


def _value_or_default(value, default):
    return default if value is None else value


def load_runtime_config(config_path: Path | None = None) -> RuntimeConfig:
    """Load runtime config from the user config file, falling back to defaults."""
    resolved_path = (config_path or DEFAULT_CONFIG_PATH).expanduser()
    data = _read_config_data(resolved_path)
    settings = data.get("settings", {})
    providers = data.get("providers", {})
    if settings is None:
        settings = {}
    if providers is None:
        providers = {}
    if not isinstance(settings, dict):
        raise ConfigError("Config table 'settings' must be a TOML table.")
    if not isinstance(providers, dict):
        raise ConfigError("Config table 'providers' must be a TOML table.")
    massive_settings = providers.get("massive", {})
    if massive_settings is None:
        massive_settings = {}
    if not isinstance(massive_settings, dict):
        raise ConfigError("Config table 'providers.massive' must be a TOML table.")

    today = datetime.today().date()
    config = RuntimeConfig(
        tickers=_value_or_default(
            _coerce_list(settings.get("tickers"), field_name="settings.tickers"),
            ("TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"),
        ),
        min_bid=_value_or_default(
            _coerce_float(settings.get("min_bid"), field_name="settings.min_bid"),
            0.50,
        ),
        min_open_interest=_value_or_default(
            _coerce_int(
                settings.get("min_open_interest"),
                field_name="settings.min_open_interest",
            ),
            100,
        ),
        min_volume=_value_or_default(
            _coerce_int(settings.get("min_volume"), field_name="settings.min_volume"),
            10,
        ),
        max_spread_pct_of_mid=_value_or_default(
            _coerce_float(
                settings.get("max_spread_pct_of_mid"),
                field_name="settings.max_spread_pct_of_mid",
            ),
            0.25,
        ),
        risk_free_rate=_value_or_default(
            _coerce_float(
                settings.get("risk_free_rate"),
                field_name="settings.risk_free_rate",
            ),
            0.045,
        ),
        hv_lookback_days=_value_or_default(
            _coerce_int(
                settings.get("hv_lookback_days"),
                field_name="settings.hv_lookback_days",
            ),
            30,
        ),
        trading_days_per_year=_value_or_default(
            _coerce_int(
                settings.get("trading_days_per_year"),
                field_name="settings.trading_days_per_year",
            ),
            252,
        ),
        data_provider=_value_or_default(
            _coerce_str(
                settings.get("data_provider"),
                field_name="settings.data_provider",
            ),
            "yfinance",
        ),
        stale_quote_seconds=_value_or_default(
            _coerce_int(
                settings.get("stale_quote_seconds"),
                field_name="settings.stale_quote_seconds",
            ),
            15 * 60,
        ),
        max_strike_distance_pct=_value_or_default(
            _coerce_float(
                settings.get("max_strike_distance_pct"),
                field_name="settings.max_strike_distance_pct",
            ),
            0.30,
        ),
        max_expiration=_value_or_default(
            _coerce_str(
                settings.get("max_expiration"),
                field_name="settings.max_expiration",
            ),
            _default_max_expiration(today),
        ),
        today=today,
        massive_api_key=_coerce_str(
            massive_settings.get("api_key"),
            field_name="providers.massive.api_key",
        ),
        config_path=resolved_path,
    )
    validate_runtime_config(config)
    return config


def validate_runtime_config(config: RuntimeConfig) -> None:
    """Validate provider selection and required credentials."""
    if config.data_provider not in SUPPORTED_PROVIDERS:
        supported = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise ConfigError(
            f"Unsupported provider '{config.data_provider}'. Supported providers: {supported}."
        )
    if config.data_provider == "massive" and not config.massive_api_key:
        raise ConfigError(
            "Missing Massive API key in "
            f"'{config.config_path}'. Set [providers.massive] api_key when using "
            "data_provider = 'massive'."
        )


@lru_cache(maxsize=1)
def get_runtime_config() -> RuntimeConfig:
    """Return the cached runtime config for the current process."""
    return load_runtime_config()


def reset_runtime_config() -> None:
    """Clear the cached runtime config, primarily for tests."""
    get_runtime_config.cache_clear()


def get_provider_credentials(provider_name: str) -> dict[str, str]:
    """Return credentials for the selected provider without exposing config internals."""
    config = get_runtime_config()
    if provider_name == "massive" and config.massive_api_key:
        return {"api_key": config.massive_api_key}
    return {}

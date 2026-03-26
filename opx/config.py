"""Runtime configuration loading for the options fetch pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib

SUPPORTED_PROVIDERS = frozenset({"yfinance", "massive", "marketdata"})
SCRIPT_VERSION = "2026-03-26.1"
DEFAULT_CONFIG_PATH = Path("~/.config/opx/config.toml").expanduser()
DEFAULT_TICKERS = ("TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR")
DEFAULT_DATA_PROVIDER = "yfinance"
DEFAULT_MIN_BID = 0.50
DEFAULT_MIN_OPEN_INTEREST = 100
DEFAULT_MIN_VOLUME = 10
DEFAULT_MAX_SPREAD_PCT_OF_MID = 0.25
DEFAULT_RISK_FREE_RATE = 0.045
DEFAULT_HV_LOOKBACK_DAYS = 30
DEFAULT_TRADING_DAYS_PER_YEAR = 252
DEFAULT_STALE_QUOTE_SECONDS = 10800
DEFAULT_ENABLE_FILTERS = True
DEFAULT_ENABLE_VALIDATION = True
DEFAULT_OPTION_SCORE_INCOME_WEIGHT = 0.30
DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT = 0.30
DEFAULT_OPTION_SCORE_RISK_WEIGHT = 0.25
DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT = 0.15
DEFAULT_MAX_STRIKE_DISTANCE_PCT = 0.30
DEFAULT_MAX_EXPIRATION_WEEKS = 26
SUPPORTED_MARKETDATA_MODES = frozenset({"live", "cached", "delayed"})
DEFAULT_MARKETDATA_MAX_RETRIES = 3
DEFAULT_MARKETDATA_REQUEST_INTERVAL_SECONDS = 0.0
DEFAULT_VIEWER_HOST = "127.0.0.1"
DEFAULT_VIEWER_PORT = 8000
MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT = 250
DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT = MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT
DEFAULT_MASSIVE_REQUEST_INTERVAL_SECONDS = 12.0
DEFAULT_DEBUG_DUMP_PROVIDER_PAYLOAD = False
DEFAULT_DEBUG_DUMP_DIR = Path("debug")


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
    option_score_income_weight: float
    option_score_liquidity_weight: float
    option_score_risk_weight: float
    option_score_efficiency_weight: float
    data_provider: str
    stale_quote_seconds: int
    enable_filters: bool
    enable_validation: bool
    max_strike_distance_pct: float
    max_expiration_weeks: int
    max_expiration: str | None
    today: date
    massive_api_key: str | None
    marketdata_api_token: str | None
    marketdata_mode: str | None
    marketdata_max_retries: int
    marketdata_request_interval_seconds: float
    massive_snapshot_page_limit: int
    massive_request_interval_seconds: float
    debug_dump_provider_payload: bool
    debug_dump_dir: Path
    viewer_host: str
    viewer_port: int
    config_path: Path
    config_warnings: tuple[str, ...] = field(default_factory=tuple)


def _default_max_expiration(today, weeks):
    return (today + timedelta(weeks=weeks)).isoformat()


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


def _coerce_bool(value, *, field_name):
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ConfigError(f"Config field '{field_name}' must be true or false.")
    return value


def _coerce_float(value, *, field_name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"Config field '{field_name}' must be numeric.")
    return float(value)


def _coerce_path(value, *, field_name):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Config field '{field_name}' must be a string path.")
    normalized = value.strip()
    if not normalized:
        raise ConfigError(f"Config field '{field_name}' must not be blank.")
    return Path(normalized).expanduser()


def _read_config_data(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _value_or_default(value, default):
    return default if value is None else value


def _append_default_warning(warnings: list[str], field_name: str, default) -> None:
    warnings.append(f"{field_name}: using default {default!r}.")


def _resolve_config_value(  # pylint: disable=too-many-arguments
    raw_value,
    *,
    field_name,
    default,
    coercer,
    warnings,
    validator=None,
):
    try:
        value = _value_or_default(coercer(raw_value, field_name=field_name), default)
    except ConfigError:
        _append_default_warning(warnings, field_name, default)
        return default
    if validator is not None and not validator(value):
        _append_default_warning(warnings, field_name, default)
        return default
    return value


def _resolve_table(value, *, field_name, warnings):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    _append_default_warning(warnings, field_name, {})
    return {}


def _clamp_massive_snapshot_page_limit(value: int, warnings: list[str]) -> int:
    """Clamp Massive snapshot page size to the endpoint's documented maximum."""
    if value <= 0:
        _append_default_warning(warnings, "providers.massive.snapshot_page_limit", 250)
        return DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT
    if value > MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT:
        warnings.append(
            "providers.massive.snapshot_page_limit: clamped to 250 because "
            "/v3/snapshot/options/{underlyingAsset} rejects larger values."
        )
        return MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT
    return value


def load_runtime_config(config_path: Path | None = None) -> RuntimeConfig:  # pylint: disable=too-many-locals
    """Load runtime config from the user config file, falling back to defaults."""
    resolved_path = (config_path or DEFAULT_CONFIG_PATH).expanduser()
    warnings: list[str] = []
    data = _read_config_data(resolved_path)
    settings = _resolve_table(data.get("settings", {}), field_name="settings", warnings=warnings)
    providers = _resolve_table(data.get("providers", {}), field_name="providers", warnings=warnings)
    massive_settings = _resolve_table(
        providers.get("massive", {}),
        field_name="providers.massive",
        warnings=warnings,
    )
    marketdata_settings = _resolve_table(
        providers.get("marketdata", {}),
        field_name="providers.marketdata",
        warnings=warnings,
    )

    today = datetime.today().date()
    data_provider = _resolve_config_value(
        settings.get("data_provider"),
        field_name="settings.data_provider",
        default=DEFAULT_DATA_PROVIDER,
        coercer=_coerce_str,
        warnings=warnings,
        validator=lambda value: value in SUPPORTED_PROVIDERS,
    )
    massive_warnings = warnings if data_provider == "massive" else []
    marketdata_warnings = warnings if data_provider == "marketdata" else []
    massive_api_key = _resolve_config_value(
        massive_settings.get("api_key"),
        field_name="providers.massive.api_key",
        default=None,
        coercer=_coerce_str,
        warnings=massive_warnings,
    )
    marketdata_api_token = _resolve_config_value(
        marketdata_settings.get("api_token"),
        field_name="providers.marketdata.api_token",
        default=None,
        coercer=_coerce_str,
        warnings=marketdata_warnings,
    )
    marketdata_mode = _resolve_config_value(
        marketdata_settings.get("mode"),
        field_name="providers.marketdata.mode",
        default=None,
        coercer=_coerce_str,
        warnings=marketdata_warnings,
        validator=lambda value: value is None or value in SUPPORTED_MARKETDATA_MODES,
    )
    if data_provider == "massive" and not massive_api_key:
        warnings.append(
            "providers.massive.api_key: using default None and falling back to 'yfinance'."
        )
        data_provider = DEFAULT_DATA_PROVIDER
    if data_provider == "marketdata" and not marketdata_api_token:
        warnings.append(
            "providers.marketdata.api_token: using default None and falling back to 'yfinance'."
        )
        data_provider = DEFAULT_DATA_PROVIDER

    config = RuntimeConfig(
        tickers=_resolve_config_value(
            settings.get("tickers"),
            field_name="settings.tickers",
            default=DEFAULT_TICKERS,
            coercer=_coerce_list,
            warnings=warnings,
        ),
        min_bid=_resolve_config_value(
            settings.get("filters_min_bid"),
            field_name="settings.filters_min_bid",
            default=DEFAULT_MIN_BID,
            coercer=_coerce_float,
            warnings=warnings,
        ),
        min_open_interest=_resolve_config_value(
            settings.get("filters_min_open_interest"),
            field_name="settings.filters_min_open_interest",
            default=DEFAULT_MIN_OPEN_INTEREST,
            coercer=_coerce_int,
            warnings=warnings,
        ),
        min_volume=_resolve_config_value(
            settings.get("filters_min_volume"),
            field_name="settings.filters_min_volume",
            default=DEFAULT_MIN_VOLUME,
            coercer=_coerce_int,
            warnings=warnings,
        ),
        max_spread_pct_of_mid=_resolve_config_value(
            settings.get("filters_max_spread_pct_of_mid"),
            field_name="settings.filters_max_spread_pct_of_mid",
            default=DEFAULT_MAX_SPREAD_PCT_OF_MID,
            coercer=_coerce_float,
            warnings=warnings,
        ),
        risk_free_rate=_resolve_config_value(
            settings.get("risk_free_rate"),
            field_name="settings.risk_free_rate",
            default=DEFAULT_RISK_FREE_RATE,
            coercer=_coerce_float,
            warnings=warnings,
        ),
        hv_lookback_days=_resolve_config_value(
            settings.get("hv_lookback_days"),
            field_name="settings.hv_lookback_days",
            default=DEFAULT_HV_LOOKBACK_DAYS,
            coercer=_coerce_int,
            warnings=warnings,
        ),
        trading_days_per_year=_resolve_config_value(
            settings.get("trading_days_per_year"),
            field_name="settings.trading_days_per_year",
            default=DEFAULT_TRADING_DAYS_PER_YEAR,
            coercer=_coerce_int,
            warnings=warnings,
        ),
        option_score_income_weight=_resolve_config_value(
            settings.get("option_score_income_weight"),
            field_name="settings.option_score_income_weight",
            default=DEFAULT_OPTION_SCORE_INCOME_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
        ),
        option_score_liquidity_weight=_resolve_config_value(
            settings.get("option_score_liquidity_weight"),
            field_name="settings.option_score_liquidity_weight",
            default=DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
        ),
        option_score_risk_weight=_resolve_config_value(
            settings.get("option_score_risk_weight"),
            field_name="settings.option_score_risk_weight",
            default=DEFAULT_OPTION_SCORE_RISK_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
        ),
        option_score_efficiency_weight=_resolve_config_value(
            settings.get("option_score_efficiency_weight"),
            field_name="settings.option_score_efficiency_weight",
            default=DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
        ),
        data_provider=data_provider,
        stale_quote_seconds=_resolve_config_value(
            settings.get("stale_quote_seconds"),
            field_name="settings.stale_quote_seconds",
            default=DEFAULT_STALE_QUOTE_SECONDS,
            coercer=_coerce_int,
            warnings=warnings,
        ),
        enable_filters=_resolve_config_value(
            settings.get("filters_enable"),
            field_name="settings.filters_enable",
            default=DEFAULT_ENABLE_FILTERS,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        enable_validation=_resolve_config_value(
            settings.get("enable_validation"),
            field_name="settings.enable_validation",
            default=DEFAULT_ENABLE_VALIDATION,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        debug_dump_provider_payload=_resolve_config_value(
            settings.get("debug_dump_provider_payload"),
            field_name="settings.debug_dump_provider_payload",
            default=DEFAULT_DEBUG_DUMP_PROVIDER_PAYLOAD,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        debug_dump_dir=_resolve_config_value(
            settings.get("debug_dump_dir"),
            field_name="settings.debug_dump_dir",
            default=DEFAULT_DEBUG_DUMP_DIR,
            coercer=_coerce_path,
            warnings=warnings,
        ),
        viewer_host=_resolve_config_value(
            settings.get("viewer_host"),
            field_name="settings.viewer_host",
            default=DEFAULT_VIEWER_HOST,
            coercer=_coerce_str,
            warnings=warnings,
        ),
        viewer_port=_resolve_config_value(
            settings.get("viewer_port"),
            field_name="settings.viewer_port",
            default=DEFAULT_VIEWER_PORT,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: 1 <= value <= 65535,
        ),
        max_strike_distance_pct=_resolve_config_value(
            settings.get("filters_max_strike_distance_pct"),
            field_name="settings.filters_max_strike_distance_pct",
            default=DEFAULT_MAX_STRIKE_DISTANCE_PCT,
            coercer=_coerce_float,
            warnings=warnings,
        ),
        max_expiration_weeks=_resolve_config_value(
            settings.get("max_expiration_weeks"),
            field_name="settings.max_expiration_weeks",
            default=DEFAULT_MAX_EXPIRATION_WEEKS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 0,
        ),
        max_expiration=None,
        today=today,
        massive_api_key=massive_api_key,
        marketdata_api_token=marketdata_api_token,
        marketdata_mode=marketdata_mode,
        marketdata_max_retries=_resolve_config_value(
            marketdata_settings.get("max_retries"),
            field_name="providers.marketdata.max_retries",
            default=DEFAULT_MARKETDATA_MAX_RETRIES,
            coercer=_coerce_int,
            warnings=marketdata_warnings,
            validator=lambda value: value >= 0,
        ),
        marketdata_request_interval_seconds=_resolve_config_value(
            marketdata_settings.get("request_interval_seconds"),
            field_name="providers.marketdata.request_interval_seconds",
            default=DEFAULT_MARKETDATA_REQUEST_INTERVAL_SECONDS,
            coercer=_coerce_float,
            warnings=marketdata_warnings,
            validator=lambda value: value >= 0,
        ),
        massive_snapshot_page_limit=_clamp_massive_snapshot_page_limit(_resolve_config_value(
            massive_settings.get("snapshot_page_limit"),
            field_name="providers.massive.snapshot_page_limit",
            default=DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT,
            coercer=_coerce_int,
            warnings=massive_warnings,
        ), massive_warnings),
        massive_request_interval_seconds=_resolve_config_value(
            massive_settings.get("request_interval_seconds"),
            field_name="providers.massive.request_interval_seconds",
            default=DEFAULT_MASSIVE_REQUEST_INTERVAL_SECONDS,
            coercer=_coerce_float,
            warnings=massive_warnings,
            validator=lambda value: value >= 0,
        ),
        config_path=resolved_path,
        config_warnings=tuple(warnings),
    )
    object.__setattr__(
        config,
        "max_expiration",
        (
            None
            if config.max_expiration_weeks == 0
            else _default_max_expiration(today, config.max_expiration_weeks)
        ),
    )
    if (
        config.option_score_income_weight
        + config.option_score_liquidity_weight
        + config.option_score_risk_weight
        + config.option_score_efficiency_weight
        <= 0
    ):
        warnings.append(
            "settings.option_score_*_weight: total weight must be positive; using defaults."
        )
        object.__setattr__(config, "option_score_income_weight", DEFAULT_OPTION_SCORE_INCOME_WEIGHT)
        object.__setattr__(
            config,
            "option_score_liquidity_weight",
            DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT,
        )
        object.__setattr__(config, "option_score_risk_weight", DEFAULT_OPTION_SCORE_RISK_WEIGHT)
        object.__setattr__(
            config,
            "option_score_efficiency_weight",
            DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT,
        )
        object.__setattr__(config, "config_warnings", tuple(warnings))
    return config


def validate_runtime_config(config: RuntimeConfig) -> None:
    """Validate runtime config built programmatically outside the loader."""
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
    if config.data_provider == "marketdata" and not config.marketdata_api_token:
        raise ConfigError(
            "Missing Market Data API token in "
            f"'{config.config_path}'. Set [providers.marketdata] api_token when using "
            "data_provider = 'marketdata'."
        )
    if (
        config.marketdata_mode is not None
        and config.marketdata_mode not in SUPPORTED_MARKETDATA_MODES
    ):
        raise ConfigError(
            "Config field 'providers.marketdata.mode' must be one of: "
            f"{', '.join(sorted(SUPPORTED_MARKETDATA_MODES))}."
        )
    if config.marketdata_max_retries < 0:
        raise ConfigError(
            "Config field 'providers.marketdata.max_retries' must be non-negative."
        )
    if config.marketdata_request_interval_seconds < 0:
        raise ConfigError(
            "Config field 'providers.marketdata.request_interval_seconds' must be non-negative."
        )
    if (
        config.option_score_income_weight
        + config.option_score_liquidity_weight
        + config.option_score_risk_weight
        + config.option_score_efficiency_weight
        <= 0
    ):
        raise ConfigError("Option score weights must sum to a positive value.")
    if not 0 < config.massive_snapshot_page_limit <= MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT:
        raise ConfigError(
            "Config field 'providers.massive.snapshot_page_limit' must be between 1 and 250."
        )
    if config.massive_request_interval_seconds < 0:
        raise ConfigError(
            "Config field 'providers.massive.request_interval_seconds' must be non-negative."
        )
    if not str(config.debug_dump_dir).strip():
        raise ConfigError("Config field 'settings.debug_dump_dir' must not be blank.")
    if not config.viewer_host.strip():
        raise ConfigError("Config field 'settings.viewer_host' must not be blank.")
    if not 1 <= config.viewer_port <= 65535:
        raise ConfigError("Config field 'settings.viewer_port' must be between 1 and 65535.")


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
    if provider_name == "marketdata" and config.marketdata_api_token:
        return {"api_token": config.marketdata_api_token}
    return {}


def describe_runtime_config(config: RuntimeConfig) -> tuple[str, ...]:
    """Return human-readable lines describing the resolved runtime configuration."""
    masked_massive_key = "set" if config.massive_api_key else "not set"
    masked_marketdata_token = "set" if config.marketdata_api_token else "not set"
    return (
        f"Config path: {config.config_path}",
        f"Config file exists: {config.config_path.exists()}",
        f"Applied provider: {config.data_provider}",
        f"Applied tickers: {', '.join(config.tickers)}",
        f"Applied filters_min_bid: {config.min_bid}",
        f"Applied filters_min_open_interest: {config.min_open_interest}",
        f"Applied filters_min_volume: {config.min_volume}",
        f"Applied filters_max_spread_pct_of_mid: {config.max_spread_pct_of_mid}",
        f"Applied filters_max_strike_distance_pct: {config.max_strike_distance_pct}",
        f"Applied risk_free_rate: {config.risk_free_rate}",
        f"Applied hv_lookback_days: {config.hv_lookback_days}",
        f"Applied trading_days_per_year: {config.trading_days_per_year}",
        f"Applied option_score_income_weight: {config.option_score_income_weight}",
        f"Applied option_score_liquidity_weight: {config.option_score_liquidity_weight}",
        f"Applied option_score_risk_weight: {config.option_score_risk_weight}",
        f"Applied option_score_efficiency_weight: {config.option_score_efficiency_weight}",
        f"Applied stale_quote_seconds: {config.stale_quote_seconds}",
        f"Applied filters_enable: {config.enable_filters}",
        f"Applied enable_validation: {config.enable_validation}",
        f"Applied debug_dump_provider_payload: {config.debug_dump_provider_payload}",
        f"Applied debug_dump_dir: {config.debug_dump_dir}",
        f"Applied viewer_host: {config.viewer_host}",
        f"Applied viewer_port: {config.viewer_port}",
        f"Applied max_expiration_weeks: {config.max_expiration_weeks}",
        f"Applied max_expiration: {config.max_expiration or 'disabled'}",
        f"Applied providers.massive.api_key: {masked_massive_key}",
        f"Applied providers.marketdata.api_token: {masked_marketdata_token}",
        f"Applied providers.marketdata.mode: {config.marketdata_mode or 'default'}",
        f"Applied providers.marketdata.max_retries: {config.marketdata_max_retries}",
        (
            "Applied providers.marketdata.request_interval_seconds: "
            f"{config.marketdata_request_interval_seconds}"
        ),
        f"Applied providers.massive.snapshot_page_limit: {config.massive_snapshot_page_limit}",
        (
            "Applied providers.massive.request_interval_seconds: "
            f"{config.massive_request_interval_seconds}"
        ),
    )

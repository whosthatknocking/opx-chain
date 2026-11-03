"""Options chain data pipeline."""

from opx_chain.analyst_forecast import (
    ANALYST_FORECAST_SCHEMA_VERSION,
    fetch_analyst_forecasts,
)
from opx_chain.version import __version__
from opx_chain.benchmark_series import (
    BENCHMARK_SERIES_SCHEMA_VERSION,
    CBOE_VIX_URL,
    MARKETDATA_SPY_URL,
    ApprovedNetworkScope,
    BenchmarkSeriesError,
    DailyObservation,
    DailySeriesRequest,
    DailySeriesResult,
    DailySeriesSnapshot,
    RetainedFrom,
    SessionClose,
    acquire_daily_series,
)

SCHEMA_VERSION: int = 2

from opx_chain.integrity import (  # noqa: E402  # pylint: disable=wrong-import-position
    OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
    OptionChainRowScope,
    OptionChainRowScopeIntegrityError,
    OptionChainRowScopeStatus,
)

__all__ = [
    "__version__",
    "SCHEMA_VERSION",
    "ANALYST_FORECAST_SCHEMA_VERSION",
    "fetch_analyst_forecasts",
    "BENCHMARK_SERIES_SCHEMA_VERSION",
    "CBOE_VIX_URL",
    "MARKETDATA_SPY_URL",
    "ApprovedNetworkScope",
    "BenchmarkSeriesError",
    "DailyObservation",
    "DailySeriesRequest",
    "DailySeriesResult",
    "DailySeriesSnapshot",
    "RetainedFrom",
    "SessionClose",
    "acquire_daily_series",
    "OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION",
    "OptionChainRowScope",
    "OptionChainRowScopeIntegrityError",
    "OptionChainRowScopeStatus",
]

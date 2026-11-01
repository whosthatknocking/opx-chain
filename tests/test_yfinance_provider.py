"""YFinance provider tests covering snapshot normalization, events, and debug payload dumping."""

# pylint: disable=duplicate-code

from datetime import date
from pathlib import Path
import json

import pandas as pd
import pytest
from requests import exceptions as requests_exceptions

from conftest import make_runtime_config
from opx_chain.providers.base import ProviderQuotaError
from opx_chain.providers.yfinance import YFinanceProvider, compute_historical_volatility


class FakeChain:  # pylint: disable=too-few-public-methods
    """Minimal yfinance option-chain stand-in."""

    def __init__(self):
        self.calls = pd.DataFrame([{"contractSymbol": "TSLACALL", "bid": 1.0, "ask": 1.1}])
        self.puts = pd.DataFrame([{"contractSymbol": "TSLAPUT", "bid": 0.9, "ask": 1.0}])


class FakeTicker:  # pylint: disable=too-few-public-methods
    """Minimal yfinance ticker stand-in."""

    def __init__(self, ticker, **_kwargs):
        self.ticker = ticker
        self.fast_info = {"lastPrice": 101.5, "previousClose": 100.0}
        self.info = {
            "regularMarketTime": "2026-03-23T13:30:00Z",
            "regularMarketPrice": 101.5,
            "previousClose": 100.0,
        }
        self.options = ("2026-04-17",)

    def option_chain(self, expiration_date):
        """Return a minimal option-chain payload."""
        assert expiration_date == "2026-04-17"
        return FakeChain()

    def history(self, **_kwargs):
        """Return enough daily bars for HV calculation."""
        return pd.DataFrame({"Close": [100.0, 101.0, 99.5, 102.0] * 30})


def test_yfinance_provider_can_dump_raw_payloads(monkeypatch, tmp_path: Path, capsys):
    """Shared provider debug mode should dump raw yfinance payloads to JSON."""
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )
    monkeypatch.setattr("opx_chain.providers.base.get_runtime_config", lambda: make_runtime_config(
        debug_dump_provider_payload=True,
        debug_dump_dir=tmp_path,
    ))
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FakeTicker)

    provider = YFinanceProvider()
    provider.load_underlying_snapshot("TSLA")
    expirations = provider.list_option_expirations("TSLA")
    chain = provider.load_option_chain("TSLA", expirations[0])

    assert not chain.calls.empty
    assert not chain.puts.empty
    dumped_files = sorted(tmp_path.glob("yfinance_TSLA_*.json"))
    assert len(dumped_files) == 3
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in dumped_files]
    labels = {payload["label"] for payload in payloads}
    assert labels == {"underlying_snapshot", "expirations", "option_chain_2026-04-17"}
    assert "yfinance debug: dumped underlying_snapshot payload to" in capsys.readouterr().out


def test_yfinance_snapshot_preserves_zero_last_price_instead_of_falling_back(monkeypatch):
    """A real zero price should remain zero instead of being replaced by previous-close fallback."""
    class ZeroPriceTicker:  # pylint: disable=too-few-public-methods
        """Minimal ticker stub that reports a zero last price."""

        def __init__(self, _ticker, **_kwargs):
            self.fast_info = {"lastPrice": 0.0, "previousClose": 10.0}
            self.info = {
                "regularMarketTime": "2026-03-23T13:30:00Z",
                "regularMarketPrice": 11.0,
                "previousClose": 10.0,
            }

        def history(self, **_kwargs):
            """Return enough daily bars for HV calculation."""
            return pd.DataFrame({"Close": [100.0, 101.0, 99.5, 102.0] * 30})

    def fake_runtime_config():
        """Return a standard runtime config for the provider test."""
        return make_runtime_config()

    monkeypatch.setattr("opx_chain.providers.yfinance.get_runtime_config", fake_runtime_config)
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", ZeroPriceTicker)

    snapshot = YFinanceProvider().load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == 0.0


def test_yfinance_provider_load_ticker_events_parses_earnings_and_dividends(monkeypatch):
    """Yahoo event metadata should populate the canonical earnings and dividend fields."""
    class EventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub with future earnings and dividend metadata."""

        def __init__(self, ticker, **_kwargs):
            super().__init__(ticker)
            self.info.update(
                {
                    "earningsTimestampStart": 1777507200,  # 2026-04-29 UTC
                    "earningsTimestampEnd": 1777939200,    # 2026-05-04 UTC
                    "isEarningsDateEstimate": True,
                    "exDividendDate": 1776556800,          # 2026-04-18 UTC
                }
            )
            self.calendar = {
                "Earnings Date": [
                    pd.Timestamp("2026-04-29"),
                    pd.Timestamp("2026-05-04"),
                ],
                "Ex-Dividend Date": pd.Timestamp("2026-04-18"),
            }
            self.dividends = pd.Series(
                [0.88, 0.75],
                index=pd.to_datetime(["2026-04-18", "2026-07-18"]),
                dtype="float64",
            )

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 17)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", EventTicker)

    events = YFinanceProvider().load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-29"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "yfinance"
    assert events["next_earnings_date_confidence"] == "estimated"
    assert events["next_ex_div_date"] == "2026-04-18"
    assert events["next_ex_div_date_source"] == "yfinance"
    assert events["next_ex_div_date_confidence"] == "confirmed"
    assert events["dividend_amount"] == pytest.approx(0.88)


def test_yfinance_provider_prefers_info_ex_dividend_date_over_calendar(monkeypatch):
    """Yahoo calendar ex-dividend dates can lag/shift and must not override info."""
    class EventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub matching the GOOGL off-by-one dividend-date shape."""

        def __init__(self, ticker, **_kwargs):
            super().__init__(ticker)
            self.info.update(
                {
                    "exDividendDate": 1780876800,  # 2026-06-08 00:00 UTC, date-only
                }
            )
            self.calendar = {
                "Ex-Dividend Date": date(2026, 6, 7),
            }
            self.dividends = pd.Series(dtype="float64")

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 6, 4)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", EventTicker)

    events = YFinanceProvider().load_ticker_events("GOOGL")

    assert events["next_ex_div_date"] == "2026-06-08"
    assert events["next_ex_div_date_source"] == "yfinance"
    assert events["next_ex_div_date_confidence"] == "confirmed"
    assert pd.isna(events["dividend_amount"])


def test_yfinance_provider_uses_calendar_ex_dividend_when_stronger_sources_missing(
    monkeypatch,
):
    """Yahoo calendar remains a fallback when info and dividend series are blank."""
    class EventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub with only calendar dividend metadata."""

        def __init__(self, ticker, **_kwargs):
            super().__init__(ticker)
            self.calendar = {
                "Ex-Dividend Date": date(2026, 6, 8),
            }
            self.dividends = pd.Series(dtype="float64")

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 6, 4)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", EventTicker)

    events = YFinanceProvider().load_ticker_events("GOOGL")

    assert events["next_ex_div_date"] == "2026-06-08"
    assert events["next_ex_div_date_source"] == "yfinance"
    assert events["next_ex_div_date_confidence"] == "confirmed"
    assert pd.isna(events["dividend_amount"])


def test_yfinance_provider_rejects_boolean_dividend_amounts(monkeypatch):
    """Malformed Yahoo dividend booleans should not become one-dollar amounts."""
    class EventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker fixture with malformed boolean dividend data."""

        def __init__(self, ticker, **_kwargs):
            super().__init__(ticker)
            self.dividends = pd.Series(
                [True],
                index=pd.to_datetime(["2026-04-18"]),
                dtype="object",
            )

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 17)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", EventTicker)

    events = YFinanceProvider().load_ticker_events("TSLA")

    assert events["next_ex_div_date"] == "2026-04-18"
    assert pd.isna(events["dividend_amount"])


def test_yfinance_provider_load_price_history_uses_daily_adjusted_history(monkeypatch):
    """Price-context history should use the provider's paced Yahoo history call."""
    calls = []

    class PriceHistoryTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub that records history kwargs."""

        def history(self, **kwargs):
            calls.append(kwargs)
            return pd.DataFrame(
                {
                    "Open": [100.0],
                    "High": [101.0],
                    "Low": [99.0],
                    "Close": [100.5],
                    "Volume": [1000],
                },
                index=pd.to_datetime(["2026-05-01"]),
            )

    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", PriceHistoryTicker)

    history = YFinanceProvider().load_price_history("TSLA", lookback_days=260)

    assert not history.empty
    assert calls == [{"period": "378d", "interval": "1d", "auto_adjust": True}]


@pytest.mark.parametrize("lookback_days", [True, False, 0, -1, 1.5, "260", None])
def test_yfinance_provider_load_price_history_validates_lookback_before_yahoo(
    monkeypatch,
    lookback_days,
):
    """Direct provider calls should reject malformed lookbacks before Yahoo work."""
    ticker_calls = []

    class PriceHistoryTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub that fails the test if constructed before validation."""

        def __init__(self, ticker, **_kwargs):
            ticker_calls.append(ticker)
            super().__init__(ticker)

    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", PriceHistoryTicker)

    with pytest.raises(ValueError, match="lookback_days"):
        YFinanceProvider().load_price_history("TSLA", lookback_days=lookback_days)

    assert not ticker_calls


def test_yfinance_provider_load_ticker_events_returns_blanks_on_missing_data(monkeypatch):
    """Yahoo event loading should degrade to blank canonical fields when data is absent."""
    class BlankEventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub with no future event metadata."""

        def __init__(self, ticker, **_kwargs):
            super().__init__(ticker)
            self.info = {}
            self.calendar = None
            self.dividends = pd.Series(dtype="float64")

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 17)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", BlankEventTicker)

    events = YFinanceProvider().load_ticker_events("TSLA")

    assert events["next_earnings_date"] is None
    assert events["next_earnings_date_is_estimated"] is None
    assert events["next_earnings_date_source"] is None
    assert events["next_earnings_date_confidence"] is None
    assert events["next_ex_div_date"] is None
    assert events["next_ex_div_date_source"] is None
    assert events["next_ex_div_date_confidence"] is None
    assert pd.isna(events["dividend_amount"])


def test_yfinance_provider_respects_request_interval(monkeypatch):
    """Configured yfinance request spacing should pace provider calls."""
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(yfinance_request_interval_seconds=1.5),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FakeTicker)

    monotonic_values = iter([100.0, 100.2, 100.2])
    monkeypatch.setattr(
        "opx_chain.providers.base.time.monotonic",
        lambda: next(monotonic_values),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.time.sleep", sleep_calls.append)

    provider = YFinanceProvider()
    provider.list_option_expirations("TSLA")
    provider.list_option_expirations("TSLA")

    assert sleep_calls == [pytest.approx(1.3)]


def test_yfinance_provider_retries_configured_failures(monkeypatch, capsys):
    """Configured yfinance retries should retry transient Yahoo call failures."""
    attempts = {"count": 0}

    class FlakyTicker:  # pylint: disable=too-few-public-methods
        """Ticker stub that fails once before returning options."""

        def __init__(self, ticker, **_kwargs):
            self.ticker = ticker

        @property
        def options(self):
            """Raise once, then return the expected expiration list."""
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("rate limited")
            return ("2026-04-17",)

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            yfinance_max_retries=1,
            yfinance_backoff_seconds=0.25,
        ),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FlakyTicker)
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.yfinance.time.sleep", sleep_calls.append)

    expirations = YFinanceProvider().list_option_expirations("TSLA")

    assert expirations == ["2026-04-17"]
    assert sleep_calls == [pytest.approx(0.25)]
    assert "yfinance api: TSLA expirations retry_in=0.25s" in capsys.readouterr().out


def test_yfinance_provider_raises_quota_error_after_rate_limit_retries(monkeypatch):
    """Exhausted Yahoo rate limits should abort as ProviderQuotaError."""

    class LimitedTicker:  # pylint: disable=too-few-public-methods
        """Ticker stub that always raises a rate-limit error."""

        @property
        def options(self):
            """Simulate a terminal Yahoo rate limit."""
            raise RuntimeError("429 too many requests")

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            yfinance_max_retries=1,
            yfinance_backoff_seconds=0.25,
        ),
    )
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.yf.Ticker", lambda _ticker, **_kwargs: LimitedTicker()
    )
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.yfinance.time.sleep", sleep_calls.append)

    with pytest.raises(ProviderQuotaError, match="Yahoo Finance TSLA expirations failed"):
        YFinanceProvider().list_option_expirations("TSLA")

    assert sleep_calls == [pytest.approx(0.25)]


@pytest.mark.parametrize("error", [AttributeError("bad attribute"), ValueError("bad parse")])
def test_yfinance_provider_does_not_retry_non_transient_errors(monkeypatch, error):
    """Programmer bugs and permanent parse errors should fail without backoff."""

    class BrokenTicker:  # pylint: disable=too-few-public-methods
        """Ticker stub that raises a non-transient error."""

        @property
        def options(self):
            """Raise the configured non-transient exception."""
            raise error

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            yfinance_max_retries=2,
            yfinance_backoff_seconds=0.25,
        ),
    )
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.yf.Ticker", lambda _ticker, **_kwargs: BrokenTicker()
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.yfinance.time.sleep", sleep_calls.append)

    with pytest.raises(type(error), match=str(error)):
        YFinanceProvider().list_option_expirations("TSLA")

    assert not sleep_calls


def test_yfinance_safe_metadata_paths_propagate_quota_errors():
    """Best-effort Yahoo metadata wrappers should not swallow quota failures."""
    provider = YFinanceProvider()

    class LimitedTicker:  # pylint: disable=too-few-public-methods
        """Ticker stub whose metadata properties simulate terminal quota errors."""

        @property
        def info(self):
            """Simulate a typed quota error after retry classification."""
            raise ProviderQuotaError("info quota exhausted")

        @property
        def calendar(self):
            """Simulate a typed quota error after retry classification."""
            raise ProviderQuotaError("calendar quota exhausted")

        @property
        def dividends(self):
            """Simulate a typed quota error after retry classification."""
            raise ProviderQuotaError("dividends quota exhausted")

    stock = LimitedTicker()

    with pytest.raises(ProviderQuotaError, match="info quota exhausted"):
        provider._safe_info("TSLA", stock)  # pylint: disable=protected-access
    with pytest.raises(ProviderQuotaError, match="calendar quota exhausted"):
        provider._safe_calendar("TSLA", stock)  # pylint: disable=protected-access
    with pytest.raises(ProviderQuotaError, match="dividends quota exhausted"):
        provider._safe_dividends("TSLA", stock)  # pylint: disable=protected-access


def test_yfinance_safe_metadata_paths_default_invalid_payload_types():
    """Best-effort Yahoo metadata wrappers should reject unexpected payload shapes."""
    provider = YFinanceProvider()

    class WeirdTicker:  # pylint: disable=too-few-public-methods
        """Ticker stub whose metadata properties return mismatched types."""

        info = []
        calendar = []
        dividends = {}

    stock = WeirdTicker()

    assert provider._safe_info("TSLA", stock) == {}  # pylint: disable=protected-access
    assert provider._safe_calendar("TSLA", stock) is None  # pylint: disable=protected-access
    dividends = provider._safe_dividends("TSLA", stock)  # pylint: disable=protected-access
    assert isinstance(dividends, pd.Series)
    assert dividends.empty


def test_yfinance_historical_volatility_propagates_quota_errors(monkeypatch):
    """Historical-volatility fallback should not hide typed quota failures."""
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        make_runtime_config,
    )

    class Stock:  # pylint: disable=too-few-public-methods
        """Unused stock placeholder for injected history loader."""

    def load_history(**_kwargs):
        raise ProviderQuotaError("history quota exhausted")

    with pytest.raises(ProviderQuotaError, match="history quota exhausted"):
        compute_historical_volatility(Stock(), load_history=load_history)


def test_yfinance_fast_info_retry_log_names_action(monkeypatch, capsys):
    """Transient fast-info retries should include the action in the operator log label."""
    attempts = {"count": 0}

    class FlakyFastInfoTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub that fails the first fast_info lookup."""

        @property
        def fast_info(self):
            """Raise once, then return the underlying snapshot payload."""
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise requests_exceptions.ConnectionError("fast info unavailable")
            return {"lastPrice": 101.5, "previousClose": 100.0}

        @fast_info.setter
        def fast_info(self, _value):
            """Accept FakeTicker initialization assignment."""

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            yfinance_max_retries=1,
            yfinance_backoff_seconds=0.25,
        ),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FlakyFastInfoTicker)
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.yfinance.time.sleep", lambda _seconds: None)

    snapshot = YFinanceProvider().load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == pytest.approx(101.5)
    assert "yfinance api: TSLA fast_info retry_in=0.25s" in capsys.readouterr().out

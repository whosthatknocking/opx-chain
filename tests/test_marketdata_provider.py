"""Market Data provider tests covering SDK parsing and shared fetch behavior."""

# pylint: disable=too-many-lines

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import cast
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pytest
from marketdata.exceptions import BaseMarketdataException
from marketdata.output_types.options_chain import OptionsChain
from marketdata.sdk_error import MarketDataClientErrorResult

from conftest import make_runtime_config
from opx_chain import SCHEMA_VERSION, fetch
from opx_chain.config import set_runtime_config_override
from opx_chain.export import prepare_export_frame
from opx_chain.providers.base import DataProvider, ProviderAuthenticationError, ProviderQuotaError
from opx_chain.providers.marketdata import CALLER_USER_AGENT, MarketDataProvider
from opx_chain.runlog import logger_name
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary


def make_chain_result():
    """Build an SDK options-chain result for one underlying and expiration."""
    return OptionsChain(
        s="ok",
        optionSymbol=["TSLA260417C00100000", "TSLA260417P00095000"],
        underlying=["TSLA", "TSLA"],
        expiration=[1776403800, 1776403800],
        side=["call", "put"],
        strike=[100.0, 95.0],
        firstTraded=[1710709200, 1710709200],
        dte=[28, 28],
        updated=[1710942000, 1710942010],
        bid=[1.2, 0.8],
        bidSize=[10, 12],
        mid=[1.3, 0.9],
        ask=[1.4, 1.0],
        askSize=[14, 15],
        last=[1.31, 0.91],
        openInterest=[450, 300],
        volume=[120, 75],
        inTheMoney=[True, False],
        intrinsicValue=[2.5, 0.0],
        extrinsicValue=[0.0, 0.9],
        underlyingPrice=[102.5, 102.5],
        iv=[0.31, 0.29],
        delta=[0.42, -0.28],
        gamma=[0.07, 0.05],
        theta=[-0.11, -0.09],
        vega=[0.18, 0.16],
    )

class FakeResponse:  # pylint: disable=too-few-public-methods
    """Small httpx-like response used by the fake Market Data client."""

    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload)

    def json(self):
        """Return the canned JSON payload."""
        return self._payload


class FakeMarketDataClient:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Minimal fake of the official SDK client used by the provider tests."""

    def __init__(self, token=None, logger=None):
        self.token = token
        self.logger = logger
        self.headers = {}
        self.client = type("HTTPClient", (), {"headers": {}})()
        self.options = type("OptionsResource", (), {})()
        self._chain_result = make_chain_result()
        self.last_chain_kwargs = None
        self.options.chain = self._options_chain
        self._dividend_status_code = 200
        self._dividend_payload = {"s": "ok", "exDate": [], "amount": []}
        self._candles_payload = {
            "s": "ok",
            "t": [1777593600, 1777680000],
            "o": [100.0, 101.0],
            "h": [102.0, 103.0],
            "l": [99.0, 100.0],
            "c": [101.0, 102.0],
            "v": [1000, 1200],
        }
        self._quote_payload = {
            "s": "ok",
            "symbol": ["TSLA"],
            "last": [103.0],
            "changepct": [0.025],
            "updated": [1710942020],
        }
        self.request_urls = []
        self.last_candles_kwargs = None
        self.stocks = SimpleNamespace(
            earnings=self._stocks_earnings,
            candles=self._stocks_candles,
        )

    def _stocks_earnings(self, _symbol, **_kwargs):
        return type("StockEarnings", (), {"reportDate": [], "s": "ok"})()

    def _stocks_candles(self, _symbol, **kwargs):
        self.last_candles_kwargs = dict(kwargs)
        return self._candles_payload


    def _make_request(self, _method, url, *_args, **_kwargs):
        self.request_urls.append(url)
        if "stocks/dividends/" in url:
            return FakeResponse(self._dividend_status_code, self._dividend_payload)
        if "stocks/quotes/" in url:
            return FakeResponse(200, self._quote_payload)
        if "options/chain/" in url:
            expiration_values = []
            for value in self._chain_result.expiration:
                if hasattr(value, "timestamp"):
                    expiration_values.append(int(value.timestamp()))
                else:
                    expiration_values.append(value)
            updated_values = []
            for value in self._chain_result.updated:
                if hasattr(value, "timestamp"):
                    updated_values.append(int(value.timestamp()))
                else:
                    updated_values.append(value)
            payload = {
                "s": "ok",
                "optionSymbol": self._chain_result.optionSymbol,
                "underlying": self._chain_result.underlying,
                "expiration": expiration_values,
                "side": self._chain_result.side,
                "strike": self._chain_result.strike,
                "updated": updated_values,
                "bid": self._chain_result.bid,
                "ask": self._chain_result.ask,
                "last": self._chain_result.last,
                "openInterest": self._chain_result.openInterest,
                "volume": self._chain_result.volume,
                "inTheMoney": self._chain_result.inTheMoney,
                "underlyingPrice": self._chain_result.underlyingPrice,
                "iv": self._chain_result.iv,
                "delta": self._chain_result.delta,
                "gamma": self._chain_result.gamma,
                "theta": self._chain_result.theta,
                "vega": self._chain_result.vega,
            }
            return FakeResponse(200, payload)
        return FakeResponse(200, {"s": "ok"})

    def _options_chain(self, symbol, **_kwargs):
        self.last_chain_kwargs = dict(_kwargs)
        self._make_request("GET", f"options/chain/{symbol}/?expiration=all")
        return self._chain_result


def patch_marketdata_client(monkeypatch):
    """Route the provider through the local fake SDK client with a fake token."""
    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", FakeMarketDataClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )


def fake_client(provider: MarketDataProvider) -> FakeMarketDataClient:
    """Return the provider client with the concrete fake type for test-only inspection."""
    return cast(FakeMarketDataClient, provider._client())  # pylint: disable=protected-access


def test_marketdata_provider_uses_namespaced_sdk_logger(monkeypatch):
    """The SDK logger constructed by opx-chain should stay under the package namespace."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()
    client = fake_client(provider)

    sdk_logger_name = logger_name("providers.marketdata.sdk")
    assert provider.external_logger_names == (sdk_logger_name,)
    assert client.logger.name == sdk_logger_name


def test_marketdata_provider_builds_snapshot_and_option_chain(monkeypatch):
    """Market Data provider should derive expirations, chains, and underlying snapshot."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode=None),
    )
    provider = MarketDataProvider()

    snapshot = provider.load_underlying_snapshot("TSLA")
    expirations = provider.list_option_expirations("TSLA")
    chain = provider.load_option_chain("TSLA", expirations[0])
    normalized = provider.normalize_option_frame(
        df=chain.calls,
        underlying_price=snapshot["underlying_price"],
        expiration_date=expirations[0],
        option_type="call",
        ticker="TSLA",
    )

    assert snapshot["underlying_price"] == 103.0
    assert snapshot["underlying_day_change_pct"] == pytest.approx(0.025)
    assert str(snapshot["underlying_price_time"]) == "2024-03-20 13:40:20+00:00"
    assert expirations == ["2026-04-17"]
    assert len(chain.calls) == 1
    assert len(chain.puts) == 1
    assert chain.calls.iloc[0]["underlying"] == "TSLA"
    assert chain.calls.iloc[0]["bid"] == 1.2
    assert chain.calls.iloc[0]["ask"] == 1.4
    assert chain.calls.iloc[0]["delta"] == 0.42
    assert chain.calls.iloc[0]["optionSymbol"] == "TSLA260417C00100000"
    assert normalized.iloc[0]["contract_symbol"] == "TSLA260417C00100000"
    assert normalized.iloc[0]["underlying_symbol"] == "TSLA"
    assert normalized.iloc[0]["implied_volatility"] == 0.31
    assert normalized.iloc[0]["data_source"] == "marketdata"
    assert fake_client(provider).last_chain_kwargs["mode"] is None  # pylint: disable=no-member


def test_marketdata_provider_load_price_history_uses_daily_candles(monkeypatch):
    """Market Data price context should use stock daily candles with split adjustment."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="delayed"),
    )
    provider = MarketDataProvider()

    history = provider.load_price_history("TSLA", lookback_days=260)
    client = fake_client(provider)

    assert not history.empty
    assert history["c"].tolist() == [101.0, 102.0]
    assert client.last_candles_kwargs["resolution"] == "D"  # pylint: disable=no-member
    assert client.last_candles_kwargs["countback"] == 260  # pylint: disable=no-member
    assert client.last_candles_kwargs["adjust_splits"] is True  # pylint: disable=no-member
    assert client.last_candles_kwargs["mode"].value == "delayed"  # pylint: disable=no-member


@pytest.mark.parametrize("lookback_days", [True, False, 0, -1, 1.5, "260", None])
def test_marketdata_provider_load_price_history_validates_lookback_before_sdk(
    monkeypatch,
    lookback_days,
):
    """Direct provider calls should reject malformed lookbacks before paid SDK work."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()

    with pytest.raises(ValueError, match="lookback_days"):
        provider.load_price_history("TSLA", lookback_days=lookback_days)

    assert provider._client_cache is None  # pylint: disable=protected-access


def test_marketdata_provider_loads_historical_chain_for_iv_history(monkeypatch):
    """Historical IV seeding should pass a provider date and return canonical columns."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="delayed"),
    )
    provider = MarketDataProvider()

    frame = provider.load_historical_option_chain_frame(
        "TSLA",
        observation_date=date(2026, 4, 1),
    )
    client = fake_client(provider)

    assert client.last_chain_kwargs["date"] == "2026-04-01"  # pylint: disable=no-member
    assert client.last_chain_kwargs["expiration"] == "all"  # pylint: disable=no-member
    assert client.last_chain_kwargs["mode"].value == "delayed"  # pylint: disable=no-member
    assert frame["underlying_symbol"].tolist() == ["TSLA", "TSLA"]
    assert frame["option_type"].tolist() == ["call", "put"]
    assert frame["implied_volatility"].tolist() == [0.31, 0.29]
    assert frame["days_to_expiration"].tolist() == [16, 16]
    assert str(frame["option_quote_time"].iloc[0]) == "2024-03-20 13:40:00+00:00"


@pytest.mark.parametrize(
    "observation_date",
    [None, "2026-04-01", True, 1775001600, datetime(2026, 4, 1, 12, 30)],
)
def test_marketdata_provider_validates_historical_chain_observation_date_before_sdk(
    monkeypatch,
    observation_date,
):
    """Historical IV seeding should reject malformed dates before paid SDK work."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()

    with pytest.raises(ValueError, match="observation_date"):
        provider.load_historical_option_chain_frame(
            "TSLA",
            observation_date=observation_date,
        )

    assert provider._client_cache is None  # pylint: disable=protected-access


def test_marketdata_provider_derives_historical_iv_when_vendor_iv_missing(monkeypatch):
    """Historical IV seeding should derive IV from quotes when Market Data omits it."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="delayed", risk_free_rate=0.04),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)
    client._chain_result.iv = [None, None]  # pylint: disable=protected-access,no-member
    client._chain_result.delta = [None, None]  # pylint: disable=protected-access,no-member
    client._chain_result.bid = [4.0, 0.8]  # pylint: disable=protected-access,no-member
    client._chain_result.mid = [4.1, 0.9]  # pylint: disable=protected-access,no-member
    client._chain_result.ask = [4.2, 1.0]  # pylint: disable=protected-access,no-member
    client._chain_result.last = [4.1, 0.9]  # pylint: disable=protected-access,no-member

    frame = provider.load_historical_option_chain_frame(
        "TSLA",
        observation_date=date(2026, 4, 1),
    )

    assert frame["implied_volatility"].notna().all()
    assert frame["delta"].notna().all()
    assert frame["implied_volatility"].gt(0).all()
    assert frame.loc[frame["option_type"] == "call", "delta"].iloc[0] > 0
    assert frame.loc[frame["option_type"] == "put", "delta"].iloc[0] < 0


def test_marketdata_prepare_ticker_fetch_clears_ticker_caches(monkeypatch):
    """Market Data should not reuse in-process quote or chain caches across fetches."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()
    client = fake_client(provider)
    provider.prepare_ticker_fetch("TSLA")

    provider._fetch_stock_quote_snapshot("TSLA", None)  # pylint: disable=protected-access
    provider._fetch_stock_quote_snapshot("TSLA", None)  # pylint: disable=protected-access
    provider._chain_frame("TSLA", None)  # pylint: disable=protected-access
    provider._chain_frame("TSLA", None)  # pylint: disable=protected-access

    assert client.last_chain_kwargs is not None  # pylint: disable=no-member
    quote_urls = [url for url in client.request_urls if "stocks/quotes/" in url]  # pylint: disable=no-member
    chain_urls = [url for url in client.request_urls if "options/chain/" in url]  # pylint: disable=no-member
    assert len(quote_urls) == 1
    assert len(chain_urls) == 1

    provider.prepare_ticker_fetch("TSLA")

    provider._fetch_stock_quote_snapshot("TSLA", None)  # pylint: disable=protected-access
    provider._chain_frame("TSLA", None)  # pylint: disable=protected-access
    quote_urls = [url for url in client.request_urls if "stocks/quotes/" in url]  # pylint: disable=no-member
    chain_urls = [url for url in client.request_urls if "options/chain/" in url]  # pylint: disable=no-member
    assert len(quote_urls) == 2
    assert len(chain_urls) == 2


def test_marketdata_provider_snapshot_falls_back_to_latest_chain_row(monkeypatch):
    """Chain fallback should keep underlying price paired with the same row timestamp."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()
    client = fake_client(provider)
    client._quote_payload = {"s": "ok", "symbol": [], "last": [], "changepct": [], "updated": []}  # pylint: disable=protected-access,no-member
    client._chain_result.underlyingPrice = [101.0, 102.5]  # pylint: disable=protected-access,no-member
    client._chain_result.updated = [1710942000, 1710942010]  # pylint: disable=protected-access,no-member

    snapshot = provider.load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == 102.5
    assert pd.isna(snapshot["underlying_day_change_pct"])
    assert str(snapshot["underlying_price_time"]) == "2024-03-20 13:40:10+00:00"


def test_marketdata_provider_snapshot_uses_latest_stock_quote_row(monkeypatch):
    """Stock quote snapshots should keep last, changepct, and updated from one row."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()
    client = fake_client(provider)
    client._quote_payload = {  # pylint: disable=protected-access,no-member
        "s": "ok",
        "symbol": ["TSLA", "TSLA"],
        "last": [101.5, 104.0],
        "changepct": [0.01, 0.03],
        "updated": [1710942000, 1710942030],
    }

    snapshot = provider.load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == 104.0
    assert snapshot["underlying_day_change_pct"] == pytest.approx(0.03)
    assert str(snapshot["underlying_price_time"]) == "2024-03-20 13:40:30+00:00"


def test_marketdata_provider_normalizes_string_expiration_values(monkeypatch):
    """String-form expiration values should still normalize into canonical dates."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()
    client = fake_client(provider)
    client._chain_result.expiration = ["2026-04-17", "2026-04-17"]  # pylint: disable=protected-access,no-member

    expirations = provider.list_option_expirations("TSLA")

    assert expirations == ["2026-04-17"]


def test_marketdata_provider_client_sets_app_user_agent(monkeypatch):
    """The provider should override the SDK user agent with the app identity."""
    patch_marketdata_client(monkeypatch)
    provider = MarketDataProvider()

    client = provider._client()  # pylint: disable=protected-access

    assert client.headers["User-Agent"] == CALLER_USER_AGENT
    assert client.client.headers["User-Agent"] == CALLER_USER_AGENT


def test_marketdata_provider_client_rebuilds_after_token_override(monkeypatch):
    """Credential overrides should rebuild the cached Market Data client."""
    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", FakeMarketDataClient)
    provider = MarketDataProvider()

    set_runtime_config_override(make_runtime_config(marketdata_api_token="first-token"))
    first_client = fake_client(provider)
    set_runtime_config_override(make_runtime_config(marketdata_api_token="second-token"))
    second_client = fake_client(provider)

    assert first_client is not second_client
    assert first_client.token == "first-token"
    assert second_client.token == "second-token"


def test_marketdata_provider_passes_configured_mode(monkeypatch):
    """Configured Market Data mode should be forwarded to the SDK chain call."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="delayed"),
    )
    provider = MarketDataProvider()

    provider.list_option_expirations("TSLA")

    assert str(fake_client(provider).last_chain_kwargs["mode"].value) == "delayed"  # pylint: disable=no-member


def test_marketdata_ticker_cache_keys_include_configured_mode(monkeypatch):
    """In-process chain and quote caches should not cross Market Data modes."""
    patch_marketdata_client(monkeypatch)
    selected_mode = "delayed"
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode=selected_mode),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    provider.list_option_expirations("TSLA")
    selected_mode = "live"
    provider.list_option_expirations("TSLA")
    selected_mode = "delayed"
    provider.load_underlying_snapshot("TSLA")
    selected_mode = "live"
    provider.load_underlying_snapshot("TSLA")

    request_urls = getattr(client, "request_urls")
    chain_urls = [url for url in request_urls if "options/chain/" in url]
    quote_urls = [url for url in request_urls if "stocks/quotes/" in url]
    assert len(chain_urls) == 2
    assert quote_urls == [
        "stocks/quotes/TSLA/?mode=delayed",
        "stocks/quotes/TSLA/?mode=live",
    ]
    assert str(client.last_chain_kwargs["mode"].value) == "live"  # pylint: disable=no-member


def test_marketdata_provider_passes_configured_mode_to_raw_endpoints(monkeypatch):
    """Raw Market Data quote and dividend endpoints should honor configured mode."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="cached"),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    provider.load_underlying_snapshot("TSLA")
    provider.load_ticker_events("TSLA")

    request_urls = getattr(client, "request_urls")
    assert "stocks/quotes/TSLA/?mode=cached" in request_urls
    assert "stocks/dividends/TSLA/?mode=cached" in request_urls


def test_marketdata_provider_retries_rate_limits(monkeypatch):
    """429 responses should retry with backoff and eventually succeed."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    responses = iter(
        [
            FakeResponse(429, {"s": "error"}, headers={"Retry-After": "0.25"}),
            FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]}),
        ]
    )

    def fake_request(_method, _url, *_args, **_kwargs):
        return next(responses)

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [0.25]


@pytest.mark.parametrize("retry_after", ["inf", "-inf", "nan"])
def test_marketdata_provider_ignores_non_finite_retry_after(monkeypatch, retry_after):
    """Non-finite Retry-After values should fall back to configured backoff."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
            marketdata_backoff_seconds=0.5,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    responses = iter(
        [
            FakeResponse(429, {"s": "error"}, headers={"Retry-After": retry_after}),
            FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]}),
        ]
    )

    def fake_request(_method, _url, *_args, **_kwargs):
        return next(responses)

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [0.5]


def test_marketdata_decode_response_json_rejects_non_finite_literals():
    """Provider HTTP decode boundary should reject non-standard JSON constants."""
    response = FakeResponse(200, {"value": 1})
    response.text = '{"value": Infinity}'

    assert MarketDataProvider._decode_response_json(response) is None  # pylint: disable=protected-access


def test_marketdata_provider_retries_rate_limits_with_http_date(monkeypatch):
    """HTTP-date Retry-After headers should also determine retry delay."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
        ),
    )

    class FixedDateTime(datetime):
        """Freeze provider clock for HTTP-date Retry-After delay math."""

        @classmethod
        def now(cls, tz=None):
            fixed = datetime(2026, 5, 2, 12, 0, 0, tzinfo=timezone.utc)
            if tz is None:
                return fixed.replace(tzinfo=None)
            return fixed.astimezone(tz)

    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.marketdata.datetime", FixedDateTime)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    responses = iter(
        [
            FakeResponse(
                429,
                {"s": "error"},
                headers={"Retry-After": "Sat, 02 May 2026 12:00:02 GMT"},
            ),
            FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]}),
        ]
    )

    def fake_request(_method, _url, *_args, **_kwargs):
        return next(responses)

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [2.0]


def test_marketdata_provider_uses_configured_backoff_without_retry_after(monkeypatch):
    """429 responses without Retry-After should use the configured backoff base."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
            marketdata_backoff_seconds=0.5,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    responses = iter(
        [
            FakeResponse(429, {"s": "error"}),
            FakeResponse(429, {"s": "error"}),
            FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]}),
        ]
    )

    def fake_request(_method, _url, *_args, **_kwargs):
        return next(responses)

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [0.5, 1.0]


def test_marketdata_provider_retries_transient_server_errors(monkeypatch):
    """Transient 5xx responses should use the same retry budget as rate limits."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
            marketdata_backoff_seconds=0.25,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    responses = iter(
        [
            FakeResponse(503, {"s": "error"}),
            FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]}),
        ]
    )

    def fake_request(_method, _url, *_args, **_kwargs):
        return next(responses)

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [0.25]


def test_marketdata_provider_retries_network_exceptions(monkeypatch, capsys):
    """Transient request exceptions should retry before surfacing failures."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
            marketdata_backoff_seconds=0.25,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    attempts = []

    def fake_request(_method, _url, *_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise httpx.TimeoutException("temporary network failure")
        return FakeResponse(200, {"optionSymbol": ["TSLA260417C00100000"]})

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    response = wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert response.status_code == 200
    assert sleep_calls == [0.25]
    assert len(attempts) == 2
    assert "error_type=TimeoutException" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: AttributeError("programmer bug"), AttributeError),
        (lambda: json.JSONDecodeError("bad json", "", 0), json.JSONDecodeError),
    ],
)
def test_marketdata_provider_does_not_retry_non_transient_exceptions(
    monkeypatch,
    error_factory,
    error_type,
):
    """Non-transient exceptions should fail immediately without retry backoff."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
            marketdata_backoff_seconds=0.25,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    attempts = []

    def fake_request(_method, _url, *_args, **_kwargs):
        attempts.append(1)
        raise error_factory()

    wrapped = provider._wrap_logged_request(fake_request)  # pylint: disable=protected-access

    with pytest.raises(error_type):
        wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert not sleep_calls
    assert len(attempts) == 1


def test_marketdata_provider_respects_request_interval(monkeypatch):
    """Configured Market Data pacing should delay back-to-back HTTP requests."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_request_interval_seconds=1.5,
        ),
    )
    monotonic_values = iter([10.0, 10.2, 10.2, 12.0])
    monkeypatch.setattr(
        "opx_chain.providers.base.time.monotonic",
        lambda: next(monotonic_values),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.base.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    wrapped = provider._wrap_logged_request(  # pylint: disable=protected-access
        lambda _method, _url, *_args, **_kwargs: FakeResponse(200, {"optionSymbol": []})
    )

    wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")
    wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert sleep_calls == [pytest.approx(1.3)]


def test_marketdata_provider_invalid_credentials_fail_clearly(monkeypatch):
    """Authentication-like SDK errors should map to the provider auth exception."""

    class FailingClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _make_request(self, _method, url, *_args, **_kwargs):
            if "stocks/quotes/" in url:
                return FakeResponse(401, {"s": "error"})
            return super()._make_request(_method, url, *_args, **_kwargs)

        def _options_chain(self, _symbol, **_kwargs):
            return MarketDataClientErrorResult(BaseMarketdataException("Unauthorized token"))

    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", FailingClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderAuthenticationError):
        provider.load_underlying_snapshot("TSLA")


@pytest.mark.parametrize(
    ("message", "status_code"),
    [
        ("You've reached the daily request limit for your Market Data account.", None),
        ("temporarily unavailable", 429),
    ],
)
def test_marketdata_provider_sdk_quota_errors_raise_quota_error(
    monkeypatch,
    message,
    status_code,
):
    """SDK quota responses must raise ProviderQuotaError, not be swallowed."""

    class LimitedClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _options_chain(self, _symbol, **_kwargs):
            error = BaseMarketdataException(message)
            if status_code is not None:
                error.status_code = status_code
            return MarketDataClientErrorResult(error)

    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", LimitedClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderQuotaError, match=message):
        provider.load_option_chain("TSLA", "2026-06-20")


def test_marketdata_provider_stock_quote_quota_error_aborts(monkeypatch):
    """Raw stock quote quota responses should not fall back to the chain endpoint."""

    class LimitedQuoteClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _make_request(self, _method, url, *_args, **_kwargs):
            self.request_urls.append(url)
            if "stocks/quotes/" in url:
                return FakeResponse(
                    503,
                    {
                        "s": "error",
                        "errmsg": "too many requests; retry later",
                    },
                )
            return FakeMarketDataClient._make_request(self, _method, url, *_args, **_kwargs)

    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", LimitedQuoteClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_max_retries=0),
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderQuotaError):
        provider.load_underlying_snapshot("TSLA")

    assert fake_client(provider).request_urls == ["stocks/quotes/TSLA/"]  # pylint: disable=no-member


def test_marketdata_provider_event_earnings_quota_error_aborts(monkeypatch):
    """Earnings event quota responses should abort instead of becoming blanks."""

    class LimitedEarningsClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _stocks_earnings(self, _symbol, **_kwargs):
            return MarketDataClientErrorResult(
                BaseMarketdataException(
                    "You've reached the daily request limit for your Market Data account."
                )
            )

    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", LimitedEarningsClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderQuotaError):
        provider.load_ticker_events("TSLA")


def test_marketdata_provider_event_dividend_quota_error_aborts(monkeypatch):
    """Dividend event quota responses should abort instead of becoming blanks."""

    class LimitedDividendClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _make_request(self, _method, url, *_args, **_kwargs):
            if "stocks/dividends/" in url:
                return FakeResponse(
                    429,
                    {
                        "s": "error",
                        "errmsg": "You've reached the daily request limit.",
                    },
                )
            return super()._make_request(_method, url, *_args, **_kwargs)

    monkeypatch.setattr("opx_chain.providers.marketdata.OpxMarketDataClient", LimitedDividendClient)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_max_retries=0),
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderQuotaError):
        provider.load_ticker_events("TSLA")


def test_marketdata_provider_dividend_no_data_does_not_retry(monkeypatch):
    """Expected no-dividend responses should return blanks without burning retries."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            today=date(2026, 4, 16),
            marketdata_max_retries=3,
            marketdata_backoff_seconds=1.0,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx_chain.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    client = fake_client(provider)
    client._dividend_status_code = 404  # pylint: disable=protected-access
    client._dividend_payload = {"s": "no_data"}  # pylint: disable=protected-access

    events = provider.load_ticker_events("TSLA")

    assert events["next_ex_div_date"] is None
    assert pd.isna(events["dividend_amount"])
    assert not sleep_calls


def test_fetch_ticker_option_chain_runs_with_marketdata_selected(monkeypatch, tmp_path):
    """The shared fetch path should run successfully when Market Data is selected."""
    patch_marketdata_client(monkeypatch)
    config = make_runtime_config(
        data_provider="marketdata",
        marketdata_api_token="token",
        config_path=tmp_path / "config.toml",
        max_expiration_weeks=52,
        max_expiration="2027-03-31",
    )
    monkeypatch.setattr("opx_chain.fetch.get_runtime_config", lambda: config)
    monkeypatch.setattr("opx_chain.fetch.get_data_provider", MarketDataProvider)

    result = fetch.fetch_ticker_option_chain("TSLA")

    assert not result.empty
    assert set(result["data_source"]) == {"marketdata"}
    assert set(result["underlying_symbol"]) == {"TSLA"}
    assert "premium_reference_price" in result.columns
    assert result["delta"].notna().any()
    assert result["underlying_day_change_pct"].isna().all()
    assert result["underlying_price"].tolist() == pytest.approx([102.5, 102.5])
    assert (result["underlying_price_time"] == result["option_quote_time"]).all()
    assert result["historical_volatility"].isna().all()


def test_marketdata_provider_load_ticker_events_parses_earnings_and_dividends(monkeypatch):
    """load_ticker_events should parse upcoming earnings and dividend dates correctly."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 4, 16)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    market_tz = ZoneInfo("America/New_York")
    fiscal_period_end_ts = int(datetime(2026, 4, 29, tzinfo=market_tz).timestamp())
    earnings_estimate_ts = int(datetime(2026, 4, 30, tzinfo=market_tz).timestamp())
    past_report_ts = int(datetime(2026, 4, 1, tzinfo=market_tz).timestamp())
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings",
                (),
                {
                    "date": [past_report_ts, fiscal_period_end_ts],
                    "reportDate": [past_report_ts, earnings_estimate_ts],
                    "s": "ok",
                },
            )(),
        },
    )()

    ex_div_ts = int(datetime(2026, 4, 18, tzinfo=market_tz).timestamp())
    client._dividend_payload = {  # pylint: disable=protected-access
        "s": "ok",
        "exDate": [ex_div_ts],
        "amount": [0.88],
    }

    events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-30"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "marketdata.reportDate"
    assert events["next_earnings_date_confidence"] == "estimated"
    assert events["next_ex_div_date"] == "2026-04-18"
    assert events["next_ex_div_date_source"] == "marketdata.exDate"
    assert events["next_ex_div_date_confidence"] == "confirmed"
    assert events["dividend_amount"] == pytest.approx(0.88)


def test_marketdata_provider_rejects_boolean_dividend_amounts(monkeypatch):
    """Malformed MarketData dividend booleans should not become one-dollar amounts."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 4, 16)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)
    market_tz = ZoneInfo("America/New_York")
    ex_div_ts = int(datetime(2026, 4, 18, tzinfo=market_tz).timestamp())
    client._dividend_payload = {  # pylint: disable=protected-access
        "s": "ok",
        "exDate": [ex_div_ts],
        "amount": [True],
    }

    events = provider.load_ticker_events("TSLA")

    assert events["next_ex_div_date"] == "2026-04-18"
    assert pd.isna(events["dividend_amount"])


def test_marketdata_provider_ignores_fiscal_period_end_date_for_future_earnings(monkeypatch):
    """Market Data `date` is fiscal period end, not the upcoming report event."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 5, 19)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    market_tz = ZoneInfo("America/New_York")
    fiscal_period_end_ts = int(datetime(2026, 4, 30, tzinfo=market_tz).timestamp())
    projected_report_ts = int(datetime(2026, 5, 27, tzinfo=market_tz).timestamp())
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings",
                (),
                {
                    "date": [fiscal_period_end_ts],
                    "reportDate": [projected_report_ts],
                    "reportedEPS": [None],
                    "s": "ok",
                },
            )(),
        },
    )()

    events = provider.load_ticker_events("NVDA")

    assert events["next_earnings_date"] == "2026-05-27"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "marketdata.reportDate"
    assert events["next_earnings_date_confidence"] == "estimated"


def test_marketdata_provider_skips_stale_estimate_when_reported_eps_present(monkeypatch):
    """A future reportDate with reportedEPS populated reflects a stale pre-announcement estimate."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 4, 23)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    market_tz = ZoneInfo("America/New_York")
    stale_estimate_ts = int(datetime(2026, 4, 28, tzinfo=market_tz).timestamp())
    next_quarter_ts = int(datetime(2026, 7, 23, tzinfo=market_tz).timestamp())
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings",
                (),
                {
                    "reportDate": [stale_estimate_ts, next_quarter_ts],
                    "reportedEPS": [1.50, None],
                    "s": "ok",
                },
            )(),
        },
    )()

    events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-07-23"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "marketdata.reportDate"
    assert events["next_earnings_date_confidence"] == "estimated"


def test_marketdata_provider_parses_numeric_event_dates_in_market_timezone(monkeypatch):
    """Numeric dates should resolve using the provider's documented Eastern calendar."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 4, 29)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)
    utc_timestamp = int(datetime(2026, 4, 30, 0, 30, tzinfo=timezone.utc).timestamp())
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings", (), {"reportDate": [utc_timestamp], "s": "ok"}
            )(),
        },
    )()

    events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-29"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "marketdata.reportDate"
    assert events["next_earnings_date_confidence"] == "estimated"


def test_marketdata_provider_load_ticker_events_degrades_earnings_api_failure(
    monkeypatch,
    caplog,
):
    """Unexpected earnings API failures should not block dividend event data."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 16)),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)
    market_tz = ZoneInfo("America/New_York")
    ex_div_ts = int(datetime(2026, 4, 18, tzinfo=market_tz).timestamp())
    client._dividend_payload = {  # pylint: disable=protected-access
        "s": "ok",
        "exDate": [ex_div_ts],
        "amount": [0.88],
    }

    def exploding_earnings(_sym, **_kw):
        raise RuntimeError("API unreachable")

    client.stocks = type("StocksResource", (), {"earnings": exploding_earnings})()

    with caplog.at_level("WARNING", logger="opx_chain.providers.marketdata"):
        events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] is None
    assert events["next_earnings_date_is_estimated"] is None
    assert events["next_earnings_date_source"] is None
    assert events["next_earnings_date_confidence"] is None
    assert events["next_ex_div_date"] == "2026-04-18"
    assert events["next_ex_div_date_source"] == "marketdata.exDate"
    assert events["next_ex_div_date_confidence"] == "confirmed"
    assert events["dividend_amount"] == pytest.approx(0.88)
    assert "marketdata_event_degraded ticker=TSLA event=earnings" in caplog.text


def test_marketdata_provider_load_ticker_events_degrades_dividend_api_failure(
    monkeypatch,
    caplog,
):
    """Unexpected dividend API failures should not block earnings event data."""

    class ExplodingDividendClient(FakeMarketDataClient):  # pylint: disable=missing-class-docstring,too-few-public-methods
        def _make_request(self, _method, url, *_args, **_kwargs):
            if "stocks/dividends/" in url:
                raise RuntimeError("API unreachable")
            return super()._make_request(_method, url, *_args, **_kwargs)

    monkeypatch.setattr(
        "opx_chain.providers.marketdata.OpxMarketDataClient",
        ExplodingDividendClient,
    )
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    monkeypatch.setattr(
        "opx_chain.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 16)),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings",
                (),
                {"reportDate": ["2026-04-30"], "reportedEPS": [None], "s": "ok"},
            )(),
        },
    )()

    with caplog.at_level("WARNING", logger="opx_chain.providers.marketdata"):
        events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-30"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_earnings_date_source"] == "marketdata.reportDate"
    assert events["next_earnings_date_confidence"] == "estimated"
    assert events["next_ex_div_date"] is None
    assert events["next_ex_div_date_source"] is None
    assert events["next_ex_div_date_confidence"] is None
    assert pd.isna(events["dividend_amount"])
    assert "marketdata_event_degraded ticker=TSLA event=dividends" in caplog.text


def test_base_provider_load_ticker_events_returns_blank_defaults():
    """Base DataProvider default should return blank events for unsupported providers."""

    class MinimalProvider(DataProvider):  # pylint: disable=too-few-public-methods
        """Bare-minimum concrete provider to test the base class default."""

        name = "minimal"

        def load_underlying_snapshot(self, ticker):
            return {}

        def list_option_expirations(self, ticker):
            return []

        def load_option_chain(self, ticker, expiration_date):
            return None

        # pylint: disable-next=too-many-arguments,too-many-positional-arguments
        def normalize_option_frame(
            self, df, underlying_price, expiration_date, option_type, ticker
        ):
            return df

    provider = MinimalProvider()
    events = provider.load_ticker_events("AAPL")

    assert events["next_earnings_date"] is None
    assert events["next_earnings_date_is_estimated"] is None
    assert events["next_earnings_date_source"] is None
    assert events["next_earnings_date_confidence"] is None
    assert events["next_ex_div_date"] is None
    assert events["next_ex_div_date_source"] is None
    assert events["next_ex_div_date_confidence"] is None
    assert pd.isna(events["dividend_amount"])


def test_marketdata_to_storage_pipeline_trace(monkeypatch, tmp_path):  # pylint: disable=too-many-statements,too-many-locals
    """Trace specific field values from the MarketData SDK payload through to the storage artifact.

    Fixture data (from make_chain_result):
      Call  TSLA260417C00100000  strike=100  bid=1.2  ask=1.4  iv=0.31  delta=0.42  theta=-0.11
      Put   TSLA260417P00095000  strike=95   bid=0.8  ask=1.0  iv=0.29  delta=-0.28 theta=-0.09
    Stock quote: last=103.0, changepct=0.025
    Config today=2026-03-20 → expiration 2026-04-17 → 28 DTE
    """
    patch_marketdata_client(monkeypatch)
    today = date(2026, 3, 20)
    config = make_runtime_config(
        data_provider="marketdata",
        marketdata_api_token="token",
        today=today,
        max_expiration="2026-06-30",
        max_strike_distance_pct=0.30,
        max_spread_pct_of_mid=0.25,
        enable_filters=True,
        enable_validation=False,
    )

    def config_factory():
        return config

    monkeypatch.setattr("opx_chain.fetch.get_runtime_config", config_factory)
    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", config_factory)
    monkeypatch.setattr("opx_chain.metrics.get_runtime_config", config_factory)
    monkeypatch.setattr("opx_chain.providers.marketdata.get_runtime_config", config_factory)
    monkeypatch.setattr("opx_chain.fetch.get_data_provider", MarketDataProvider)

    # ── Stage 1: fetch ──────────────────────────────────────────────────────
    fetched = fetch.fetch_ticker_option_chain("TSLA")

    assert len(fetched) == 2, "both call and put must survive filters"
    assert set(fetched["option_type"]) == {"call", "put"}

    call_fetched = fetched[fetched["option_type"] == "call"].iloc[0]
    put_fetched = fetched[fetched["option_type"] == "put"].iloc[0]

    # Values that must survive unchanged from provider payload
    assert call_fetched["contract_symbol"] == "TSLA260417C00100000"
    assert call_fetched["bid"] == pytest.approx(1.2)
    assert call_fetched["ask"] == pytest.approx(1.4)
    assert call_fetched["implied_volatility"] == pytest.approx(0.31)
    assert call_fetched["delta"] == pytest.approx(0.42)   # kept via _merge_provider_and_derived
    assert call_fetched["theta"] == pytest.approx(-0.11)  # kept via _merge_provider_and_derived
    assert call_fetched["strike"] == pytest.approx(100.0)
    assert call_fetched["underlying_price"] == pytest.approx(102.5)
    assert pd.isna(call_fetched["underlying_day_change_pct"])
    assert call_fetched["underlying_price_time"] == call_fetched["option_quote_time"]
    assert call_fetched["days_to_expiration"] == 28
    assert call_fetched["data_source"] == "marketdata"

    # Values derived by the pipeline from provider inputs
    assert call_fetched["mark_price_mid"] == pytest.approx(1.30)
    assert call_fetched["bid_ask_spread"] == pytest.approx(0.20)
    assert call_fetched["bid_ask_spread_pct_of_mid"] == pytest.approx(0.20 / 1.30, rel=1e-4)
    assert call_fetched["strike_minus_spot"] == pytest.approx(100.0 - 102.5)
    assert call_fetched["theta_dollars_per_day"] == pytest.approx(abs(-0.11) * 100)

    assert put_fetched["contract_symbol"] == "TSLA260417P00095000"
    assert put_fetched["bid"] == pytest.approx(0.8)
    assert put_fetched["ask"] == pytest.approx(1.0)
    assert put_fetched["implied_volatility"] == pytest.approx(0.29)

    # ── Stage 2: export ─────────────────────────────────────────────────────
    export_df = prepare_export_frame([fetched])

    assert len(export_df) == 2
    assert "contract_symbol" in export_df.columns
    assert "bid" in export_df.columns

    # ── Stage 3: write to storage ───────────────────────────────────────────
    backend = FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )
    run_id = backend.create_run(RunContext(
        provider="marketdata",
        tickers=("TSLA",),
        config_fingerprint="trace_test",
        positions_fingerprint="",
    ))
    record = backend.write_dataset(
        run_id,
        DatasetWrite(data=export_df, provider="marketdata", schema_version=SCHEMA_VERSION),
    )
    backend.finalize_run(run_id, RunSummary(status="complete"))

    # ── Stage 4: verify DatasetRecord ───────────────────────────────────────
    assert record.row_count == 2
    assert record.schema_version == SCHEMA_VERSION
    assert record.provider == "marketdata"
    assert record.format == "csv"
    assert len(record.content_hash) == 64         # SHA-256 hex
    assert record.content_hash.isalnum()
    import os  # pylint: disable=import-outside-toplevel
    assert os.path.exists(record.location)

    # ── Stage 5: round-trip through the storage API ─────────────────────────
    records = backend.list_datasets(limit=1)
    assert len(records) == 1
    handle = backend.get_dataset(records[0].dataset_id)
    assert handle.row_count == 2
    assert handle.schema_version == SCHEMA_VERSION
    assert handle.content_hash == record.content_hash
    assert handle.format == "csv"

    run_record = backend.get_run(run_id)
    assert run_record.status == "complete"
    assert run_record.provider == "marketdata"
    assert run_record.dataset_id == record.dataset_id
    assert run_record.positions_fingerprint == ""

    # ── Stage 6: verify the artifact CSV preserves all traced values ─────────
    artifact_df = pd.read_csv(handle.location)
    assert len(artifact_df) == 2

    call_csv = artifact_df[artifact_df["option_type"] == "call"].iloc[0]
    put_csv = artifact_df[artifact_df["option_type"] == "put"].iloc[0]

    # Raw provider fields preserved through export to CSV
    assert call_csv["contract_symbol"] == "TSLA260417C00100000"
    assert call_csv["underlying_symbol"] == "TSLA"
    assert call_csv["expiration_date"] == "2026-04-17"
    assert call_csv["bid"] == pytest.approx(1.2)
    assert call_csv["ask"] == pytest.approx(1.4)
    assert call_csv["implied_volatility"] == pytest.approx(0.31)
    assert call_csv["delta"] == pytest.approx(0.42)
    assert call_csv["strike"] == pytest.approx(100.0)
    assert call_csv["underlying_price"] == pytest.approx(102.5)
    assert pd.isna(call_csv["underlying_day_change_pct"])
    assert call_csv["underlying_price_time"] == call_csv["option_quote_time"]
    assert int(call_csv["days_to_expiration"]) == 28
    assert call_csv["data_source"] == "marketdata"

    # Pipeline-derived fields preserved through export to CSV
    assert call_csv["mark_price_mid"] == pytest.approx(1.30)
    assert call_csv["bid_ask_spread"] == pytest.approx(0.20)
    assert call_csv["bid_ask_spread_pct_of_mid"] == pytest.approx(0.20 / 1.30, rel=1e-4)
    assert call_csv["strike_minus_spot"] == pytest.approx(-2.5)
    assert call_csv["theta_dollars_per_day"] == pytest.approx(11.0)  # abs(theta) * 100

    # Put side
    assert put_csv["contract_symbol"] == "TSLA260417P00095000"
    assert put_csv["bid"] == pytest.approx(0.8)
    assert put_csv["ask"] == pytest.approx(1.0)
    assert put_csv["underlying_price"] == pytest.approx(102.5)

"""Market Data provider tests covering SDK parsing and shared fetch behavior."""

from datetime import date, datetime, timezone
from typing import cast

import pandas as pd
import pytest
from marketdata.exceptions import BaseMarketdataException
from marketdata.output_types.options_chain import OptionsChain
from marketdata.sdk_error import MarketDataClientErrorResult

from conftest import make_runtime_config
from opx import fetch
from opx.providers.base import DataProvider, ProviderAuthenticationError
from opx.providers.marketdata import CALLER_USER_AGENT, MarketDataProvider


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
        self._dividend_payload = {"s": "ok", "exDate": [], "amount": []}
        self._quote_payload = {
            "s": "ok",
            "symbol": ["TSLA"],
            "last": [103.0],
            "changepct": [0.025],
            "updated": [1710942020],
        }
        self.stocks = type("StocksResource", (), {"earnings": self._stocks_earnings})()

    def _stocks_earnings(self, _symbol, **_kwargs):
        return type("StockEarnings", (), {"reportDate": [], "s": "ok"})()


    def _make_request(self, _method, url, *_args, **_kwargs):
        if "stocks/dividends/" in url:
            return FakeResponse(200, self._dividend_payload)
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
    monkeypatch.setattr("opx.providers.marketdata.OpxMarketDataClient", FakeMarketDataClient)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )


def fake_client(provider: MarketDataProvider) -> FakeMarketDataClient:
    """Return the provider client with the concrete fake type for test-only inspection."""
    return cast(FakeMarketDataClient, provider._client())  # pylint: disable=protected-access


def test_marketdata_provider_builds_snapshot_and_option_chain(monkeypatch):
    """Market Data provider should derive expirations, chains, and underlying snapshot."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
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


def test_marketdata_provider_passes_configured_mode(monkeypatch):
    """Configured Market Data mode should be forwarded to the SDK chain call."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(marketdata_mode="delayed"),
    )
    provider = MarketDataProvider()

    provider.list_option_expirations("TSLA")

    assert str(fake_client(provider).last_chain_kwargs["mode"].value) == "delayed"  # pylint: disable=no-member


def test_marketdata_provider_retries_rate_limits(monkeypatch):
    """429 responses should retry with backoff and eventually succeed."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_max_retries=2,
            marketdata_request_interval_seconds=0.0,
        ),
    )
    sleep_calls = []
    monkeypatch.setattr("opx.providers.marketdata.time.sleep", sleep_calls.append)
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


def test_marketdata_provider_respects_request_interval(monkeypatch):
    """Configured Market Data pacing should delay back-to-back HTTP requests."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(
            marketdata_request_interval_seconds=1.5,
        ),
    )
    monotonic_values = iter([10.0, 10.2, 10.2, 12.0])
    monkeypatch.setattr("opx.providers.marketdata.time.monotonic", lambda: next(monotonic_values))
    sleep_calls = []
    monkeypatch.setattr("opx.providers.marketdata.time.sleep", sleep_calls.append)
    provider = MarketDataProvider()
    wrapped = provider._wrap_logged_request(  # pylint: disable=protected-access
        lambda _method, _url, *_args, **_kwargs: FakeResponse(200, {"optionSymbol": []})
    )

    wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")
    wrapped("GET", "https://api.marketdata.app/v1/options/chain/TSLA/")

    assert sleep_calls == [pytest.approx(1.3)]


def test_marketdata_provider_invalid_credentials_fail_clearly(monkeypatch):
    """Authentication-like SDK errors should map to the provider auth exception."""

    class FailingClient(FakeMarketDataClient):  # pylint: disable=too-few-public-methods
        """Fake SDK client that turns chain requests into auth failures."""

        def _make_request(self, _method, url, *_args, **_kwargs):
            if "stocks/quotes/" in url:
                return FakeResponse(401, {"s": "error"})
            return super()._make_request(_method, url, *_args, **_kwargs)

        def _options_chain(self, _symbol, **_kwargs):
            return MarketDataClientErrorResult(BaseMarketdataException("Unauthorized token"))

    monkeypatch.setattr("opx.providers.marketdata.OpxMarketDataClient", FailingClient)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_provider_credentials",
        lambda provider_name: {"api_token": "token"} if provider_name == "marketdata" else {},
    )
    provider = MarketDataProvider()

    with pytest.raises(ProviderAuthenticationError):
        provider.load_underlying_snapshot("TSLA")


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
    monkeypatch.setattr("opx.fetch.get_runtime_config", lambda: config)
    monkeypatch.setattr("opx.fetch.get_data_provider", MarketDataProvider)

    result = fetch.fetch_ticker_option_chain("TSLA")

    assert not result.empty
    assert set(result["data_source"]) == {"marketdata"}
    assert set(result["underlying_symbol"]) == {"TSLA"}
    assert "premium_reference_price" in result.columns
    assert result["delta"].notna().any()


def test_marketdata_provider_load_ticker_events_parses_earnings_and_dividends(monkeypatch):
    """load_ticker_events should parse upcoming earnings and dividend dates correctly."""
    patch_marketdata_client(monkeypatch)
    today = date(2026, 4, 16)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=today),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    earnings_ts = int(datetime(2026, 4, 30, tzinfo=timezone.utc).timestamp())
    past_earnings_ts = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())
    client.stocks = type(
        "StocksResource",
        (),
        {
            "earnings": lambda _self, _sym, **_kw: type(
                "StockEarnings", (), {"reportDate": [past_earnings_ts, earnings_ts], "s": "ok"}
            )(),
        },
    )()

    ex_div_ts = int(datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp())
    client._dividend_payload = {  # pylint: disable=protected-access
        "s": "ok",
        "exDate": [ex_div_ts],
        "amount": [0.88],
    }

    events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-30"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_ex_div_date"] == "2026-04-18"
    assert events["dividend_amount"] == pytest.approx(0.88)


def test_marketdata_provider_load_ticker_events_returns_blanks_on_api_failure(monkeypatch):
    """load_ticker_events should return blank values when the API call raises."""
    patch_marketdata_client(monkeypatch)
    monkeypatch.setattr(
        "opx.providers.marketdata.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 16)),
    )
    provider = MarketDataProvider()
    client = fake_client(provider)

    def exploding_earnings(_sym, **_kw):
        raise RuntimeError("API unreachable")

    client.stocks = type("StocksResource", (), {"earnings": exploding_earnings})()

    events = provider.load_ticker_events("TSLA")

    assert events["next_earnings_date"] is None
    assert events["next_earnings_date_is_estimated"] is None
    assert events["next_ex_div_date"] is None
    assert pd.isna(events["dividend_amount"])


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
    assert events["next_ex_div_date"] is None
    assert pd.isna(events["dividend_amount"])

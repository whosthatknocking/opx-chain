"""Massive provider tests covering snapshot parsing and retry behavior."""

from pathlib import Path

import pandas as pd
import pytest
from massive.rest.models.snapshot import OptionContractSnapshot

from opx import fetch
from opx.greeks import compute_greeks
from opx.config import reset_runtime_config
from opx.providers.base import ProviderAuthenticationError
from opx.providers.massive import CALLER_USER_AGENT, DEFAULT_SNAPSHOT_PAGE_LIMIT, MassiveProvider


def make_snapshot_results():
    """Build a small Massive snapshot payload for one underlying and expiration."""
    return (
        {
            "details": {
                "ticker": "O:TSLA260417C00100000",
                "contract_type": "call",
                "expiration_date": "2026-04-17",
                "strike_price": 100.0,
                "shares_per_contract": "REGULAR",
            },
            "last_quote": {
                "bid": 1.2,
                "ask": 1.4,
                "last_updated": "2026-03-20T13:40:00Z",
            },
            "last_trade": {
                "price": 1.3,
                "sip_timestamp": "2026-03-20T13:40:02Z",
            },
            "day": {
                "change": 0.1,
                "change_percent": 0.02,
                "volume": 120,
                "close": 1.25,
            },
            "greeks": {
                "delta": 0.42,
                "gamma": 0.07,
                "theta": -0.11,
                "vega": 0.18,
            },
            "implied_volatility": 0.31,
            "open_interest": 450,
            "underlying_asset": {
                "ticker": "TSLA",
                "price": 102.5,
                "last_updated": "2026-03-20T13:39:59Z",
                "change_percent": 0.015,
            },
        },
        {
            "details": {
                "ticker": "O:TSLA260417P00095000",
                "contract_type": "put",
                "expiration_date": "2026-04-17",
                "strike_price": 95.0,
                "shares_per_contract": "REGULAR",
            },
            "last_quote": {
                "bid": 0.8,
                "ask": 1.0,
                "last_updated": "2026-03-20T13:40:10Z",
            },
            "last_trade": {
                "price": 0.9,
                "sip_timestamp": "2026-03-20T13:40:11Z",
            },
            "day": {
                "change": -0.03,
                "change_percent": -0.01,
                "volume": 75,
                "close": 0.92,
            },
            "greeks": {
                "delta": -0.28,
                "gamma": 0.05,
                "theta": -0.09,
                "vega": 0.16,
            },
            "implied_volatility": 0.29,
            "open_interest": 300,
            "underlying_asset": {
                "ticker": "TSLA",
                "price": 102.5,
                "last_updated": "2026-03-20T13:39:59Z",
                "change_percent": 0.015,
            },
        },
    )


def make_snapshot_model_results():
    """Build official-client snapshot model objects matching the endpoint schema."""
    raw_results = (
        {
            "details": {
                "ticker": "O:TSLA260417C00100000",
                "contract_type": "call",
                "expiration_date": "2026-04-17",
                "strike_price": 100.0,
                "shares_per_contract": 100,
            },
            "last_quote": {
                "bid": 1.2,
                "ask": 1.4,
                "last_updated": 1710942000000000000,
            },
            "last_trade": {
                "price": 1.3,
                "sip_timestamp": 1710942002000000000,
            },
            "day": {
                "change": 0.1,
                "change_percent": 0.02,
                "volume": 120,
                "close": 1.25,
                "last_updated": 1710942005000000000,
                "previous_close": 1.15,
            },
            "greeks": {
                "delta": 0.42,
                "gamma": 0.07,
                "theta": -0.11,
                "vega": 0.18,
            },
            "implied_volatility": 0.31,
            "open_interest": 450,
            "underlying_asset": {
                "ticker": "TSLA",
                "price": 102.5,
                "last_updated": 1710941999000000000,
            },
        },
        {
            "details": {
                "ticker": "O:TSLA260417P00095000",
                "contract_type": "put",
                "expiration_date": "2026-04-17",
                "strike_price": 95.0,
                "shares_per_contract": 100,
            },
            "last_quote": {
                "bid": 0.8,
                "ask": 1.0,
                "last_updated": 1710942010000000000,
            },
            "last_trade": {
                "price": 0.9,
                "sip_timestamp": 1710942011000000000,
            },
            "day": {
                "change": -0.03,
                "change_percent": -0.01,
                "volume": 75,
                "close": 0.92,
                "last_updated": 1710942012000000000,
                "previous_close": 0.95,
            },
            "greeks": {
                "delta": -0.28,
                "gamma": 0.05,
                "theta": -0.09,
                "vega": 0.16,
            },
            "implied_volatility": 0.29,
            "open_interest": 300,
            "underlying_asset": {
                "ticker": "TSLA",
                "price": 102.5,
                "last_updated": 1710941999000000000,
            },
        },
    )
    return tuple(OptionContractSnapshot.from_dict(item) for item in raw_results)


def test_massive_provider_builds_snapshot_and_option_chain(monkeypatch):
    """Massive provider should derive expirations, chains, and underlying snapshot."""
    monkeypatch.setattr(
        MassiveProvider,
        "_snapshot_results",
        lambda self, ticker: make_snapshot_results(),
    )
    provider = MassiveProvider()

    snapshot = provider.load_underlying_snapshot("TSLA")
    expirations = provider.list_option_expirations("TSLA")
    chain = provider.load_option_chain("TSLA", "2026-04-17")

    assert snapshot["underlying_price"] == 102.5
    assert snapshot["underlying_day_change_pct"] == 0.015
    assert str(snapshot["underlying_price_time"]) == "2026-03-20 13:39:59+00:00"
    assert expirations == ["2026-04-17"]
    assert len(chain.calls) == 1
    assert len(chain.puts) == 1
    assert chain.calls.iloc[0]["underlying_symbol"] == "TSLA"
    assert bool(chain.calls.iloc[0]["is_in_the_money"]) is True
    assert bool(chain.puts.iloc[0]["is_in_the_money"]) is False
    assert chain.calls.iloc[0]["delta"] == 0.42
    assert chain.puts.iloc[0]["contract_symbol"] == "TSLA260417P00095000"
    assert chain.puts.iloc[0]["contract_symbol"].startswith(chain.puts.iloc[0]["underlying_symbol"])


def test_massive_provider_parses_official_client_model_objects(monkeypatch):
    """Official client model instances should parse into the canonical row shape."""
    monkeypatch.setattr(
        MassiveProvider,
        "_snapshot_results",
        lambda self, ticker: make_snapshot_model_results(),
    )
    provider = MassiveProvider()

    snapshot = provider.load_underlying_snapshot("TSLA")
    chain = provider.load_option_chain("TSLA", "2026-04-17")
    normalized = provider.normalize_option_frame(
        df=chain.calls,
        underlying_price=102.5,
        expiration_date="2026-04-17",
        option_type="call",
        ticker="TSLA",
    )

    assert snapshot["underlying_price"] == 102.5
    assert str(snapshot["underlying_price_time"]) == "2024-03-20 13:39:59+00:00"
    assert normalized.iloc[0]["underlying_symbol"] == "TSLA"
    assert normalized.iloc[0]["contract_symbol"] == "TSLA260417C00100000"
    assert normalized.iloc[0]["contract_symbol"].startswith(normalized.iloc[0]["underlying_symbol"])
    assert bool(normalized.iloc[0]["is_in_the_money"]) is True
    assert chain.calls.iloc[0]["bid"] == 1.2
    assert chain.calls.iloc[0]["ask"] == 1.4
    assert str(normalized.iloc[0]["option_quote_time"]) == "2024-03-20 13:40:00+00:00"
    assert chain.calls.iloc[0]["open_interest"] == 450


def test_massive_provider_underlying_price_falls_back_to_value(monkeypatch):
    """Underlying price should fall back to `underlying_asset.value` when needed."""
    payload = list(make_snapshot_model_results())
    payload[0].underlying_asset.price = None
    payload[0].underlying_asset.value = 101.25
    payload[1].underlying_asset.price = None
    payload[1].underlying_asset.value = 101.25
    monkeypatch.setattr(
        MassiveProvider,
        "_snapshot_results",
        lambda self, ticker: tuple(payload),
    )
    provider = MassiveProvider()

    snapshot = provider.load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == 101.25


def test_massive_provider_logs_each_http_call_status(capsys):
    """Wrapped Massive HTTP calls should print status and response row counts."""
    provider = MassiveProvider()

    class Response:  # pylint: disable=too-few-public-methods
        """Minimal HTTP response stub."""

        status = 200
        data = (
            b'{"results":[{"ticker":"a"},{"ticker":"b"}],'
            b'"next_url":"https://api.example.test/next"}'
        )

    wrapped = provider._wrap_logged_request(  # pylint: disable=protected-access
        lambda method, url, *args, **kwargs: Response()
    )

    response = wrapped("GET", "https://api.example.test/v3/snapshot/options/TSLA")

    stdout = capsys.readouterr().out
    assert response.status == 200
    assert (
        "massive api: snapshot_chain status=200 page=1 results_count=2 "
        "results_total=2 has_next_page=true"
        in stdout
    )


def test_massive_provider_client_sets_app_user_agent(monkeypatch):
    """Massive requests should advertise the app name and version in User-Agent."""
    monkeypatch.setattr(
        "opx.providers.massive.get_provider_credentials",
        lambda _provider_name: {"api_key": "secret"},
    )
    provider = MassiveProvider()

    client = provider._client()  # pylint: disable=protected-access

    assert client.headers["User-Agent"] == CALLER_USER_AGENT
    assert client.client.headers["User-Agent"] == CALLER_USER_AGENT


def test_massive_provider_normalization_keeps_provider_greeks(monkeypatch):
    """Provider-native Massive greeks should survive normalization for later shared use."""
    monkeypatch.setattr(
        MassiveProvider,
        "_snapshot_results",
        lambda self, ticker: make_snapshot_results(),
    )
    provider = MassiveProvider()

    chain = provider.load_option_chain("TSLA", "2026-04-17")
    normalized = provider.normalize_option_frame(
        df=chain.calls,
        underlying_price=102.5,
        expiration_date="2026-04-17",
        option_type="call",
        ticker="TSLA",
    )

    assert normalized.loc[normalized.index[0], "data_source"] == "massive"
    assert normalized.loc[normalized.index[0], "delta"] == 0.42
    assert normalized.loc[normalized.index[0], "gamma"] == 0.07
    assert normalized.loc[normalized.index[0], "vega"] == 0.18


def test_massive_provider_retries_rate_limits(monkeypatch):
    """Rate-limited Massive requests should retry with exponential backoff."""
    provider = MassiveProvider()
    attempts = {"count": 0}
    seen_params = []
    sleeps = []

    def fake_list_snapshot_options_chain(_ticker, params=None):  # pylint: disable=unused-argument
        attempts["count"] += 1
        seen_params.append(params)
        if attempts["count"] < 3:
            raise RuntimeError("429 rate limited")
        return []

    class FakeClient:  # pylint: disable=too-few-public-methods
        """Minimal official-client stand-in for retry tests."""

        def list_snapshot_options_chain(self, ticker, params=None):
            """Simulate repeated snapshot requests."""
            return fake_list_snapshot_options_chain(ticker, params=params)

    fake_client = FakeClient()

    def fake_client_factory():
        """Return the fake Massive client."""
        return fake_client

    monkeypatch.setattr(provider, "_client", fake_client_factory)
    monkeypatch.setattr("opx.providers.massive.time.sleep", sleeps.append)

    payload = provider._fetch_snapshot_results("TSLA")  # pylint: disable=protected-access

    assert payload == ()
    assert attempts["count"] == 3
    assert seen_params == [{"limit": DEFAULT_SNAPSHOT_PAGE_LIMIT}] * 3
    assert sleeps == [1.0, 2.0]


def test_massive_provider_spaces_underlying_http_requests(monkeypatch):
    """Configured request spacing should apply between paginated client HTTP calls."""
    provider = MassiveProvider()
    # pylint: disable=protected-access
    provider._last_request_started_at = 100.0
    monotonic_values = iter([105.0, 112.0])
    sleeps = []
    wrapped_calls = []

    monkeypatch.setattr("opx.providers.massive.time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("opx.providers.massive.time.sleep", sleeps.append)
    monkeypatch.setattr(provider, "_request_interval_seconds", lambda: 12.0)

    wrapped = provider._wrap_rate_limited_get(  # pylint: disable=protected-access
        lambda *args, **kwargs: wrapped_calls.append((args, kwargs)) or "ok"
    )

    result = wrapped("/v3/snapshot/options/TSLA", params={"limit": 250})

    assert result == "ok"
    assert sleeps == [7.0]
    assert wrapped_calls == [(("/v3/snapshot/options/TSLA",), {"params": {"limit": 250}})]
    assert provider._last_request_started_at == 112.0


def test_compute_greeks_preserves_provider_values():
    """Shared greek derivation should not overwrite provider-native greeks when present."""
    frame = pd.DataFrame(
        [
            {
                "strike": 100.0,
                "time_to_expiration_years": 0.5,
                "implied_volatility": 0.25,
                "option_type": "call",
                "delta": 0.42,
                "gamma": 0.07,
                "theta": -0.11,
                "vega": 0.18,
            }
        ]
    )

    result = compute_greeks(frame.copy(), underlying_price=110.0, risk_free_rate=0.045)

    assert result.loc[0, "delta"] == 0.42
    assert result.loc[0, "gamma"] == 0.07
    assert result.loc[0, "theta"] == -0.11
    assert result.loc[0, "vega"] == 0.18


def test_massive_provider_invalid_credentials_fail_clearly(monkeypatch):
    """Authentication failures should surface as clear Massive credential errors."""
    provider = MassiveProvider()

    class FakeClient:  # pylint: disable=too-few-public-methods
        """Client stub that simulates an auth failure."""

        def list_snapshot_options_chain(self, ticker, params=None):  # pylint: disable=unused-argument
            """Raise an auth-like error."""
            raise RuntimeError("403 forbidden")

    fake_client = FakeClient()

    def fake_client_factory():
        """Return the fake auth-failure client."""
        return fake_client

    monkeypatch.setattr(provider, "_client", fake_client_factory)

    with pytest.raises(ProviderAuthenticationError, match="Massive authentication failed"):
        provider._fetch_snapshot_results("TSLA")  # pylint: disable=protected-access


def test_fetch_ticker_option_chain_runs_with_massive_selected(monkeypatch, tmp_path: Path):
    """The shared fetch path should execute end-to-end when Massive is selected in config."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
tickers = ["TSLA"]
data_provider = "massive"
max_expiration = "2026-06-30"

[providers.massive]
api_key = "secret"
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr("opx.config.DEFAULT_CONFIG_PATH", config_path)
    reset_runtime_config()
    monkeypatch.setattr(
        MassiveProvider,
        "_snapshot_results",
        lambda self, ticker: make_snapshot_results(),
    )

    result = fetch.fetch_ticker_option_chain("TSLA")

    assert not result.empty
    assert set(result["data_source"]) == {"massive"}
    assert "delta" in result.columns
    assert result["delta"].notna().all()


def test_fetch_ticker_option_chain_reraises_massive_auth_errors(monkeypatch):
    """Invalid Massive credentials should fail fast through the shared fetch path."""
    provider = MassiveProvider()

    def raise_auth_error(_ticker):
        """Raise a clear provider auth error."""
        raise ProviderAuthenticationError("Massive authentication failed.")

    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)
    monkeypatch.setattr(provider, "load_underlying_snapshot", raise_auth_error)

    with pytest.raises(ProviderAuthenticationError, match="Massive authentication failed"):
        fetch.fetch_ticker_option_chain("TSLA")

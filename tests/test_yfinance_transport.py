"""Yahoo transport uses corrected trust for origin and proxy without requests."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ssl
from threading import Barrier, Event

import certifi
from curl_cffi import Curl, CurlInfo, CurlOpt, ffi
import pytest
from test_yfinance_provider import FakeTicker

from opx_chain.providers import _yfinance_transport as transport
from opx_chain.providers.yfinance import YFinanceProvider


class BeforeNetwork(Exception):
    """Stop a prepared request before libcurl can perform network I/O."""


def test_corrected_transport_roots_and_normal_verification():
    """The provider session never selects the affected system-default bundle."""
    session = transport.yfinance_session()
    bundle = certifi.where()
    assert session.verify is True
    assert session.impersonate == "chrome"
    for option in (CurlOpt.CAINFO, CurlOpt.PROXY_CAINFO):
        assert session.curl_options[option] == bundle
        context = ssl.create_default_context(cafile=session.curl_options[option])
        roots = {hashlib.sha256(item).hexdigest()
                 for item in context.get_ca_certs(binary_form=True)}
        assert "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b" not in roots
        assert context.check_hostname is True
        assert context.verify_mode == ssl.CERT_REQUIRED


def test_actual_prepared_request_keeps_peer_and_proxy_verification(monkeypatch):
    """Effective native options use the fixed bundle before any external I/O."""
    options = {}
    original = Curl.setopt

    def setopt(handle, option, value):
        options[option] = value
        return original(handle, option, value)

    def stop(handle, *_args, **_kwargs):
        assert handle.getinfo(CurlInfo.CAPATH) == b""
        raise BeforeNetwork()

    monkeypatch.setattr(Curl, "setopt", setopt)
    monkeypatch.setattr(Curl, "perform", stop)
    with pytest.raises(BeforeNetwork):
        transport.yfinance_session().get("https://example.invalid/", verify=False)
    assert options[CurlOpt.CAPATH] is ffi.NULL
    assert options[CurlOpt.PROXY_CAPATH] is ffi.NULL
    assert options[CurlOpt.CAINFO] == certifi.where()
    assert options[CurlOpt.PROXY_CAINFO] == certifi.where()
    for option in (CurlOpt.SSL_VERIFYPEER, CurlOpt.PROXY_SSL_VERIFYPEER):
        assert options[option] == 1
    for option in (CurlOpt.SSL_VERIFYHOST, CurlOpt.PROXY_SSL_VERIFYHOST):
        assert options[option] == 2


def test_every_yahoo_provider_entry_uses_the_verified_session(monkeypatch):
    """Prices, chains, events, forecasts and history share the verified client."""
    class CompleteTicker(FakeTicker):
        """Include analyst endpoints in the provider-wide transport fixture."""

        def get_analyst_price_targets(self):
            """Return a minimal nonempty target response."""
            return {"current": 101.5}

        def get_recommendations_summary(self):
            """Return a minimal nonempty recommendation response."""
            return {"strongBuy": 1}

    calls = []
    expected = transport.yfinance_session()

    def ticker(symbol, *, session):
        assert session is expected
        calls.append(symbol)
        return CompleteTicker(symbol)

    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", ticker)
    provider = YFinanceProvider()
    provider.load_underlying_snapshot("TSLA")
    provider.load_ticker_events("TSLA")
    provider.load_analyst_forecast("TSLA")
    provider.load_price_history("TSLA", lookback_days=30)
    provider.list_option_expirations("TSLA")
    provider.load_option_chain("TSLA", "2026-04-17")
    assert calls == ["TSLA"] * 6


def test_concurrent_initialization_reuses_one_cookie_session(monkeypatch):
    """Parallel first requests cannot replace one another's cookie sessions."""
    gate = Barrier(2)
    entered, release = Event(), Event()
    created = []

    def create(**kwargs):
        created.append(kwargs)
        entered.set()
        assert release.wait(2)
        return object()

    def get():
        gate.wait(timeout=2)
        return transport.yfinance_session()

    monkeypatch.setattr(transport, "_SESSION", None)
    monkeypatch.setattr(transport.curl_requests, "Session", create)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(get) for _ in range(2)]
        assert entered.wait(2)
        release.set()
        clients = [item.result(timeout=2) for item in futures]
    assert len(created) == 1
    assert clients[0] is clients[1]

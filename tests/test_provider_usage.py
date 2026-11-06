"""Offline provider-credit normalization, rotation, transport and ordering tests."""
# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
from marketdata.client import MarketDataClient
import pytest

from opx_chain import provider_usage as usage
from opx_chain.providers.marketdata import OpxMarketDataClient


@pytest.fixture(autouse=True)
def credential(monkeypatch):
    token = {"api_token": "test-credential"}
    monkeypatch.setattr(usage, "get_provider_credentials", lambda _: token)
    usage._activate("")
    monkeypatch.setattr(usage, "_now", lambda: datetime(2026, 11, 6, tzinfo=timezone.utc))
    return token


def headers(remaining="200", reset="1794009600", **kwargs):
    result = {"X-Api-Ratelimit-Limit": "1000", "X-Api-Ratelimit-Remaining": remaining,
              "X-Api-Ratelimit-Reset": reset, "X-Api-Ratelimit-Consumed": "2"}
    result.update(kwargs)
    return result


def capture(values):
    usage.capture_response(usage.get_provider_usage("marketdata").generation, values)
    return usage.get_provider_usage("marketdata").observation


@pytest.mark.parametrize("bad", [True, "1.2", "NaN", "inf", "", "1e3", "999999999999999999999"])
def test_invalid_remaining_does_not_become_zero(bad):
    observation = capture(headers(bad))
    assert observation["credits_remaining"] is None
    assert observation["credit_limit"] == 1000
    assert observation["credits_consumed_by_request"] == 2


def test_ordering_partial_new_window_and_negative_balance():
    assert capture(headers("200"))["credits_remaining"] == 200
    assert capture(headers("300"))["credits_remaining"] == 200
    assert capture(headers("0"))["credits_remaining"] == 0
    assert capture(headers("-1"))["credits_remaining"] == -1
    assert capture(headers("bad"))["credits_remaining"] == -1
    assert capture({})["credits_remaining"] == -1
    assert capture(headers("900", "1794096000"))["credits_remaining"] == 900
    assert capture(headers("-10"))["credits_remaining"] == 900


def test_credential_rotation_drops_old_inflight(credential):
    old = usage.get_provider_usage("marketdata").generation
    capture(headers())
    credential["api_token"] = "new-credential"
    assert usage.get_provider_usage("marketdata").observation is None
    usage.capture_response(old, headers("0"))
    assert usage.get_provider_usage("marketdata").observation is None
    credential.clear()
    assert usage.get_provider_usage("marketdata").capability == "not_configured"


def test_get_is_network_free_and_returns_copies(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: pytest.fail("network"))
    state = usage.get_provider_usage("marketdata")
    assert state.observation is None
    capture(headers())
    state = usage.get_provider_usage("marketdata")
    state.observation["credits_remaining"] = 999
    assert usage.get_provider_usage("marketdata").observation["credits_remaining"] == 200
    assert usage.get_provider_usage("yfinance").capability == "not_supported"
    with pytest.raises(ValueError):
        usage.get_provider_usage("unknown")


@pytest.mark.parametrize("status,payload,error", [
    (200, {"email": "private", "optionsDataPermissions": "delayed"}, None),
    (203, None, None), (200, usage._INVALID_BODY, "INVALID_USAGE_RESPONSE"),
    (401, {"message": "secret"}, "AUTHENTICATION_FAILED"),
    (429, {"errmsg": "API credit limit"}, "CREDIT_EXHAUSTED"),
    (429, {"errmsg": "concurrent requests"}, "CONCURRENCY_LIMIT"),
    (429, {}, "PROVIDER_ERROR"), (500, {}, "PROVIDER_ERROR"),
])
def test_refresh_allowlists_and_keeps_error_headers(monkeypatch, status, payload, error):
    calls = []
    async def request(token, timeout):
        calls.append((token, timeout))
        return status, headers("199"), payload
    monkeypatch.setattr(usage, "_request_user", request)
    result = usage.refresh_provider_usage("marketdata")
    assert len(calls) == 1
    assert result.error_code == error
    assert result.state.observation["credits_remaining"] == 199
    assert "private" not in repr(result)
    assert "test-credential" not in repr(result)


def test_user_transport_exact_endpoint_and_no_retry(monkeypatch):
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(503, headers=headers(), json={"message": "secret"})
    client = httpx.AsyncClient
    monkeypatch.setattr(usage.httpx, "AsyncClient", lambda **kw: client(
        **kw, transport=httpx.MockTransport(respond)))
    result = usage.refresh_provider_usage("marketdata")
    assert result.error_code == "PROVIDER_ERROR"
    assert len(calls) == 1
    assert str(calls[0].url) == "https://api.marketdata.app/user/"


def test_timeout_preserves_old_observation(monkeypatch):
    before = capture(headers())
    async def request(*_):
        raise asyncio.TimeoutError
    monkeypatch.setattr(usage, "_request_user", request)
    result = usage.refresh_provider_usage("marketdata")
    assert result.error_code == "TIMEOUT"
    assert result.state.observation == before


def test_sdk_response_hook_observes_without_startup_probe(monkeypatch):
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(429, headers=headers("-2"), json={"errmsg": "limit"})
    monkeypatch.setattr(MarketDataClient, "_get_client", lambda _: httpx.Client(
        base_url="https://api.marketdata.app", transport=httpx.MockTransport(respond)))
    client = OpxMarketDataClient(token="test-credential")
    assert not calls
    with client.client:
        client.client.get("/v1/options/chain/TEST/")
    assert len(calls) == 1
    assert usage.get_provider_usage("marketdata").observation["credits_remaining"] == -2


def test_mismatched_client_token_never_populates_current_credential():
    hook = usage.response_observer("old-credential")
    hook(httpx.Response(200, headers=headers("0")))
    assert usage.get_provider_usage("marketdata").observation is None


@pytest.mark.parametrize("remaining,limit,reset,complete", [
    ("200", "0", "1794009600", True),
    ("200", "bad", "1794009600", True),
    ("2000", "1000", "1794009600", False),
    ("200", "1000", "9999999999999999", False),
    ("200", "1000", "NaN", False),
])
def test_partial_validity(remaining, limit, reset, complete):
    observation = capture(headers(remaining, reset, **{"X-Api-Ratelimit-Limit": limit}))
    assert usage._complete(observation) is complete


def test_total_deadline_includes_pacing(monkeypatch):
    monkeypatch.setattr(usage, "get_runtime_config", lambda: SimpleNamespace(
        marketdata_request_interval_seconds=1))
    monkeypatch.setattr(httpx, "AsyncClient",
                        lambda **_: pytest.fail("deadline exceeded before I/O"))
    assert usage.refresh_provider_usage("marketdata", timeout_seconds=0.01).error_code == "TIMEOUT"

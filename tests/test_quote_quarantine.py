"""Acquisition-only quote quarantine must not weaken stored integrity."""
# pylint: disable=missing-function-docstring

import json
from types import SimpleNamespace

import pandas as pd
import pytest
import httpx
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
from test_integrity import _frame

from opx_chain.integrity import OptionChainDataIntegrityError
from opx_chain.providers.base import ProviderAuthenticationError, ProviderQuotaError
from opx_chain.quote_quarantine import quarantine_unusable_quotes


def frames():
    """Return one valid and one unusable, structurally identified contract."""
    return pd.concat([
        _frame(),
        _frame(contract_symbol="SYNTH260821C00110000", strike=110, bid=.01, ask=0),
    ], ignore_index=True)


class Provider:
    """Count bounded refreshes without contacting an upstream service."""

    name = "synthetic-provider"

    def __init__(self, replacement):
        self.replacement = replacement
        self.calls = 0

    def load_option_chain(self, ticker, expiration):
        assert (ticker, expiration) == ("SYNTH", "2026-08-21")
        self.calls += 1
        return SimpleNamespace(calls=self.replacement, puts=pd.DataFrame())

    def normalize_option_frame(self, **kwargs):
        return kwargs["df"]


def run(frame, replacement=None):
    """Exercise the public acquisition helper with deterministic quotes."""
    provider = Provider(frame if replacement is None else replacement)
    result, report = quarantine_unusable_quotes(
        frame, provider=provider, ticker="SYNTH", underlying_price=105,
    )
    return result, json.loads(report) if report else None, provider.calls


def test_valid_quotes_do_not_refresh():
    result, report, calls = run(_frame())
    assert len(result) == 1 and report is None and calls == 0


@pytest.mark.parametrize("ask", [0, .005])
def test_quote_only_failure_is_quarantined_after_one_refresh(ask):
    frame = frames()
    frame.loc[1, "ask"] = ask
    result, report, calls = run(frame)
    assert len(result) == 1 and calls == 1
    assert report["quarantined_count"] == 1
    assert report["affected_quotes"][0]["original_bid"] == .01
    assert report["affected_quotes"][0]["ask"] == ask


def test_refresh_repairs_quote_without_fabrication():
    refreshed = frames()
    refreshed.loc[1, "ask"] = .02
    result, report, calls = run(frames(), refreshed)
    assert len(result) == 2 and calls == 1
    assert result.loc[1, "ask"] == .02
    assert report["quarantined_count"] == 0


@pytest.mark.parametrize("field,value", [("strike", 111), ("bid", -1), ("contract_symbol", "bad")])
def test_other_corruption_cannot_hide_behind_quote_failure(field, value):
    frame = frames()
    frame.loc[1, field] = value
    with pytest.raises(OptionChainDataIntegrityError):
        run(frame)


def test_widespread_quote_failures_remain_fatal():
    frame = frames()
    frame.loc[0, "ask"] = 0
    with pytest.raises(OptionChainDataIntegrityError):
        run(frame)


def test_disappearing_quote_remains_quarantined():
    result, report, calls = run(frames(), _frame())
    assert len(result) == 1 and calls == 1
    assert not report["affected_quotes"][0]["refresh_found"]


def test_duplicate_contract_is_fatal_before_quarantine():
    with pytest.raises(OptionChainDataIntegrityError):
        run(pd.concat([frames(), frames().iloc[[1]]], ignore_index=True))


@pytest.mark.parametrize("error", [
    TimeoutError, ConnectionError, httpx.ReadTimeout, httpx.ConnectError,
    httpx.RemoteProtocolError, RequestsConnectionError, RequestsTimeout,
])
def test_transport_failure_preserves_valid_rows_and_safe_evidence(error):
    provider = Provider(frames())

    def fail(ticker, expiration):
        assert (ticker, expiration) == ("SYNTH", "2026-08-21")
        provider.calls += 1
        raise error("secret credentialed URL must not reach the report")

    provider.load_option_chain = fail
    result, raw_report = quarantine_unusable_quotes(
        frames(), provider=provider, ticker="SYNTH", underlying_price=105,
    )
    report = json.loads(raw_report)
    assert len(result) == 1 and provider.calls == 1
    assert report["quarantined_count"] == 1
    assert report["affected_quotes"][0]["refresh_error"] == error.__name__
    assert report["affected_quotes"][0]["refresh_found"] is False
    assert "secret" not in raw_report


@pytest.mark.parametrize("error", [ProviderAuthenticationError, ProviderQuotaError, ValueError])
def test_nontransport_refresh_failure_propagates(error):
    provider = Provider(frames())

    def fail(ticker, expiration):
        assert (ticker, expiration) == ("SYNTH", "2026-08-21")
        raise error("stop")

    provider.load_option_chain = fail
    with pytest.raises(error):
        quarantine_unusable_quotes(
            frames(), provider=provider, ticker="SYNTH", underlying_price=105,
        )


def test_refresh_normalization_failure_is_not_swallowed():
    provider = Provider(frames())

    def fail(**kwargs):
        raise ValueError("invalid mapping")

    provider.normalize_option_frame = fail
    with pytest.raises(ValueError, match="invalid mapping"):
        quarantine_unusable_quotes(
            frames(), provider=provider, ticker="SYNTH", underlying_price=105,
        )

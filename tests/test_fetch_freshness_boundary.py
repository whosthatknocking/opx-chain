"""Acquisition clocks must not turn received quotes into future quotes."""

import logging

import pandas as pd
import pytest

from conftest import make_runtime_config
from test_fetch import StubProvider
from opx_chain import fetch, metrics, normalize
from opx_chain.runlog import RUN_LOGGER_NAME
from opx_chain.storage.cache import FilesystemCache


@pytest.mark.parametrize("refresh", [False, True])
@pytest.mark.parametrize("quote_offset,stale", [(2, False), (-7200, True), (10, True)])
def test_freshness_uses_post_acquisition_clock(
    monkeypatch, caplog, tmp_path, refresh, quote_offset, stale,
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Include refresh latency but retain old and truly future quote flags."""
    start = pd.Timestamp("2026-03-20T14:27:58.460Z")
    received = start + pd.Timedelta(seconds=5)
    clock = [start]

    class TimedProvider(StubProvider):
        """Advance the clock only after the last provider response."""

        def __init__(self):
            super().__init__()
            self.chain_calls = 0

        def load_option_chain(self, ticker, expiration_date):
            self.chain_calls += 1
            chain = super().load_option_chain(ticker, expiration_date)
            for frame in (chain.calls, chain.puts):
                frame["option_quote_time"] = start + pd.Timedelta(seconds=quote_offset)
            if refresh and self.chain_calls == 1:
                chain.calls.loc[0, "ask"] = 0.0
            else:
                clock[0] = received
            return chain

        def load_underlying_snapshot(self, ticker):
            snapshot = super().load_underlying_snapshot(ticker)
            snapshot["underlying_price_time"] = start - pd.Timedelta(seconds=900)
            return snapshot

    config = make_runtime_config(today=start.date(), stale_quote_seconds=3600)
    provider = TimedProvider()
    for module in (fetch, metrics, normalize):
        monkeypatch.setattr(module, "get_runtime_config", lambda: config)
    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)
    monkeypatch.setattr(fetch, "get_provider_cache", lambda _config: FilesystemCache(tmp_path))
    monkeypatch.setattr(fetch, "utc_now_timestamp", lambda: clock[0])
    caplog.set_level("INFO", logger=RUN_LOGGER_NAME)
    result = fetch.fetch_ticker_option_chain("TEST", logger=logging.getLogger(RUN_LOGGER_NAME))

    assert not result.empty
    assert provider.chain_calls == (2 if refresh else 1)
    assert result["quote_age_seconds"].eq(5 - quote_offset).all()
    assert result["is_stale_quote"].eq(stale).all()
    assert result["underlying_price_age_seconds"].eq(905).all()
    assert result["is_stale_underlying_price"].eq(False).all()
    assert "fetch_started_at=2026-03-20T14:27:58Z" in caplog.text
    assert "freshness_assessed_at=2026-03-20T14:28:03Z" in caplog.text
    if not refresh:
        clock[0] = start + pd.Timedelta(seconds=7205)
        cached = fetch.fetch_ticker_option_chain("TEST")
        assert provider.chain_calls == 1
        assert cached["quote_age_seconds"].eq(7205 - quote_offset).all()
        assert cached["is_stale_quote"].eq(True).all()
        assert cached["underlying_price_age_seconds"].eq(8105).all()
        assert cached["is_stale_underlying_price"].eq(True).all()

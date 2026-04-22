"""Fetch-path tests covering raw provider row-count logging."""

import logging
from datetime import date

import numpy as np
import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain import fetch
from opx_chain.fetch import append_ticker_event_fields
import opx_chain.metrics
import opx_chain.normalize
from opx_chain.positions import EMPTY_POSITION_SET, OptionPositionKey, PositionSet
from opx_chain.providers.base import OptionChainFrames


def make_vendor_frame(rows):
    """Build a provider-normalized frame that still exercises later filters."""
    return pd.DataFrame(rows)


class StubProvider:
    """Minimal provider stub for fetch-path tests."""

    name = "stub"

    def load_underlying_snapshot(self, ticker):
        """Return a small underlying snapshot."""
        assert ticker == "TEST"
        return {
            "underlying_price": 100.0,
            "underlying_price_time": pd.Timestamp("2026-03-20T13:45:00Z"),
            "underlying_day_change_pct": 0.01,
            "historical_volatility": 0.2,
        }

    def list_option_expirations(self, ticker):
        """Return one supported expiration."""
        assert ticker == "TEST"
        return ["2026-04-17"]

    def load_option_chain(self, ticker, expiration_date):
        """Return a small raw call/put payload."""
        assert ticker == "TEST"
        assert expiration_date == "2026-04-17"
        calls = make_vendor_frame(
            [
                {
                    "contract_symbol": "TESTC1",
                    "option_quote_time": "2026-03-20T13:40:00Z",
                    "bid": 1.0,
                    "ask": 1.1,
                    "strike": 100.0,
                    "last_trade_price": 1.05,
                    "open_interest": 10,
                    "volume": 5,
                    "implied_volatility": 0.3,
                    "change": 0.1,
                    "percent_change": 0.02,
                    "is_in_the_money": False,
                    "contract_size": "REGULAR",
                },
                {
                    "contract_symbol": "TESTC2",
                    "option_quote_time": "2026-03-20T13:40:00Z",
                    "bid": 0.0,
                    "ask": 0.2,
                    "strike": 140.0,
                    "last_trade_price": 0.1,
                    "open_interest": 0,
                    "volume": 0,
                    "implied_volatility": 0.35,
                    "change": 0.0,
                    "percent_change": 0.0,
                    "is_in_the_money": False,
                    "contract_size": "REGULAR",
                },
            ]
        )
        puts = make_vendor_frame(
            [
                {
                    "contract_symbol": "TESTP1",
                    "option_quote_time": "2026-03-20T13:40:00Z",
                    "bid": 0.5,
                    "ask": 0.7,
                    "strike": 95.0,
                    "last_trade_price": 0.6,
                    "open_interest": 8,
                    "volume": 3,
                    "implied_volatility": 0.28,
                    "change": -0.05,
                    "percent_change": -0.01,
                    "is_in_the_money": False,
                    "contract_size": "REGULAR",
                },
            ]
        )
        return OptionChainFrames(calls=calls, puts=puts)

    def load_ticker_events(self, ticker):  # pylint: disable=unused-argument
        """Return blank event data so fetch-path tests are not affected by event fetching."""
        return {
            "next_earnings_date": None,
            "next_ex_div_date": None,
            "dividend_amount": float("nan"),
        }

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df,
        underlying_price,
        expiration_date,
        option_type,
        ticker,
    ):
        """Add the canonical fields a provider adapter is responsible for."""
        frame = df.copy()
        frame["option_type"] = option_type
        frame["underlying_symbol"] = ticker
        frame["expiration_date"] = expiration_date
        frame["days_to_expiration"] = 28
        frame["time_to_expiration_years"] = 28 / 365.0
        frame["data_source"] = self.name
        frame["risk_free_rate_used"] = 0.045
        frame["underlying_price"] = underlying_price
        frame["option_quote_time"] = pd.to_datetime(
            frame["option_quote_time"], utc=True, errors="coerce"
        )
        return frame


class ErrorProvider:  # pylint: disable=too-few-public-methods
    """Provider stub that fails after selection so shared error logging can be asserted."""

    name = "broken"

    def load_underlying_snapshot(self, ticker):
        """Raise a provider error so shared logging can include provider context."""
        raise RuntimeError(f"provider exploded for {ticker}")


def test_fetch_ticker_option_chain_logs_raw_provider_row_counts(monkeypatch, caplog):
    """Log raw provider counts before app-side filtering changes the row set."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("INFO", logger="opx_chain.run")
    logger = logging.getLogger("opx_chain.run")

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert not result.empty
    assert (
        "provider=stub expiration=2026-04-17 status=raw_provider_rows "
        "call_rows=2 put_rows=1 total_rows=3 "
        "call_bid_rows=2 put_bid_rows=1 call_ask_rows=2 put_ask_rows=1 "
        "call_trade_rows=2 put_trade_rows=1"
    ) in caplog.text
    assert "status=ok" in caplog.text
    assert "raw_provider_rows=3 raw_expirations=1" in caplog.text


def test_fetch_ticker_option_chain_prints_stage_counts(monkeypatch, capsys):
    """Console output should show per-stage fetch counts for each ticker."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)

    def config_factory():
        """Return the standard filtered runtime config for fetch-stage diagnostics."""
        return make_runtime_config(today=pd.Timestamp("2026-03-20").date())

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    result = fetch.fetch_ticker_option_chain("TEST")

    stdout = capsys.readouterr().out
    assert not result.empty
    assert "Loading TEST  (stub)" in stdout
    assert "TEST: expirations  usable=1/1" in stdout
    assert "TEST: chain  2026-04-17  rows=3" in stdout
    assert "TEST: normalize  rows=3" in stdout
    assert "TEST: filter  rows=1  dropped=2" in stdout
    assert "TEST: done  rows=1  expirations=1  raw=3" in stdout


def test_fetch_ticker_option_chain_explains_when_filters_remove_everything(monkeypatch, capsys):
    """Console output should explain empty results after provider data is filtered out."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    def config_factory():
        """Return a stricter runtime config that filters all quotes."""
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            max_spread_pct_of_mid=0.01,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    result = fetch.fetch_ticker_option_chain("TEST")

    stdout = capsys.readouterr().out
    assert result.empty
    assert "TEST: chain  2026-04-17  rows=3" in stdout
    assert "TEST: normalize  rows=3" in stdout
    assert (
        "TEST: all provider rows were filtered out by the shared normalization and "
        "screening pipeline"
    ) in stdout


def test_fetch_ticker_option_chain_can_disable_post_download_filters(monkeypatch):
    """Disabling post-download filters should keep rows that filters would normally drop."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)

    def config_factory():
        """Return a runtime config with post-download filters disabled."""
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            enable_filters=False,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    result = fetch.fetch_ticker_option_chain("TEST")

    assert len(result) == 3
    assert set(result["contract_symbol"]) == {"TESTC1", "TESTC2", "TESTP1"}


def test_fetch_ticker_option_chain_validates_rows_before_filtering(monkeypatch):
    """Validation should see invalid rows even when post-download filters later remove them."""
    class InvalidBeforeFilterProvider(StubProvider):
        """Provider variant with one invalid quote that still gets filtered after validation."""

        def load_option_chain(self, ticker, expiration_date):
            chain = super().load_option_chain(ticker, expiration_date)
            chain.calls.loc[1, "ask"] = None
            return chain

    monkeypatch.setattr(fetch, "get_data_provider", InvalidBeforeFilterProvider)

    def config_factory():
        """Return the standard filtered runtime config for row validation checks."""
        return make_runtime_config(today=pd.Timestamp("2026-03-20").date())

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)
    findings = []

    result = fetch.fetch_ticker_option_chain("TEST", validation_findings=findings)

    assert not result.empty
    assert any(
        finding.code == "missing_required_field"
        and finding.field == "ask"
        and finding.contract_symbol == "TESTC2"
        for finding in findings
    )


def test_fetch_ticker_option_chain_can_disable_validation(monkeypatch):
    """Disabling validation should skip row-level findings entirely."""
    class InvalidBeforeFilterProvider(StubProvider):
        """Provider variant with one invalid quote that would fail validation if enabled."""

        def load_option_chain(self, ticker, expiration_date):
            chain = super().load_option_chain(ticker, expiration_date)
            chain.calls.loc[1, "ask"] = None
            return chain

    monkeypatch.setattr(fetch, "get_data_provider", InvalidBeforeFilterProvider)

    def config_factory():
        """Return a runtime config with validation disabled."""
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            enable_validation=False,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)
    findings = []

    result = fetch.fetch_ticker_option_chain("TEST", validation_findings=findings)

    assert not result.empty
    assert not findings


def test_fetch_ticker_option_chain_logs_provider_name_on_error(monkeypatch, caplog):
    """Shared error logs should stay provider-neutral while preserving provider context."""
    monkeypatch.setattr(fetch, "get_data_provider", ErrorProvider)
    monkeypatch.setattr(fetch, "get_runtime_config", make_runtime_config)

    caplog.set_level("ERROR", logger="opx_chain.run")
    logger = logging.getLogger("opx_chain.run")

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert result.empty
    assert "provider=broken status=error" in caplog.text


def test_append_ticker_event_fields_broadcasts_day_counts_to_all_rows():
    """Event fields should be broadcast to every row with correct day-count arithmetic."""
    today = date(2026, 4, 16)
    events = {
        "next_earnings_date": "2026-04-23",
        "next_earnings_date_is_estimated": True,
        "next_ex_div_date": "2026-04-18",
        "dividend_amount": 0.75,
    }
    frame = pd.DataFrame([{"strike": 100.0}, {"strike": 105.0}, {"strike": 110.0}])

    result = append_ticker_event_fields(frame.copy(), events, today)

    assert (result["next_earnings_date"] == "2026-04-23").all()
    assert result["next_earnings_date_is_estimated"].tolist() == [True, True, True]
    assert (result["next_ex_div_date"] == "2026-04-18").all()
    assert result["dividend_amount"].tolist() == pytest.approx([0.75, 0.75, 0.75])
    assert (result["days_to_earnings"] == 7).all()
    assert (result["days_to_ex_div"] == 2).all()


def test_append_ticker_event_fields_handles_blank_events():
    """Missing event data should produce NaN day-count fields without raising."""
    today = date(2026, 4, 16)
    events = {
        "next_earnings_date": None,
        "next_earnings_date_is_estimated": None,
        "next_ex_div_date": None,
        "dividend_amount": np.nan,
    }
    frame = pd.DataFrame([{"strike": 100.0}])

    result = append_ticker_event_fields(frame.copy(), events, today)

    assert result.loc[0, "next_earnings_date"] is None
    assert result.loc[0, "next_earnings_date_is_estimated"] is None
    assert result.loc[0, "next_ex_div_date"] is None
    assert pd.isna(result.loc[0, "days_to_earnings"])
    assert pd.isna(result.loc[0, "days_to_ex_div"])


class TodayExpirationProvider(StubProvider):
    """Provider variant that returns only a today-dated expiration."""

    def list_option_expirations(self, ticker):
        return ["2026-03-20"]  # same as config today

    def load_option_chain(self, ticker, expiration_date):
        assert expiration_date == "2026-03-20"
        calls = make_vendor_frame([
            {
                "contract_symbol": "TODAY_CALL",
                "option_quote_time": "2026-03-20T13:40:00Z",
                "bid": 1.0,
                "ask": 1.2,
                "strike": 100.0,
                "last_trade_price": 1.1,
                "open_interest": 50,
                "volume": 5,
                "implied_volatility": 0.3,
                "change": 0.0,
                "percent_change": 0.0,
                "is_in_the_money": False,
                "contract_size": "REGULAR",
            }
        ])
        return OptionChainFrames(calls=calls, puts=make_vendor_frame([]))


def _patch_config_20260320(monkeypatch):
    def config_factory():
        return make_runtime_config(today=pd.Timestamp("2026-03-20").date())
    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)


def test_today_expiration_dropped_without_position_set(monkeypatch):
    """Expirations on today's date must be skipped when the ticker is not a portfolio stock."""
    monkeypatch.setattr(fetch, "get_data_provider", TodayExpirationProvider)
    _patch_config_20260320(monkeypatch)

    result = fetch.fetch_ticker_option_chain("TEST", position_set=EMPTY_POSITION_SET)

    assert result.empty


def test_today_expiration_kept_for_portfolio_stock(monkeypatch):
    """Expirations on today's date must be kept when the ticker is a portfolio stock."""
    monkeypatch.setattr(fetch, "get_data_provider", TodayExpirationProvider)
    _patch_config_20260320(monkeypatch)

    position_set = PositionSet(stock_tickers=frozenset({"TEST"}), option_keys=frozenset())
    result = fetch.fetch_ticker_option_chain("TEST", position_set=position_set)

    assert not result.empty
    assert "TODAY_CALL" in result["contract_symbol"].values


def test_position_option_survives_filters(monkeypatch):
    """An option matching a portfolio position must not be dropped even with bid==0."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    _patch_config_20260320(monkeypatch)

    # TESTC2 has bid=0 and strike=140 (outside 30% band) — normally filtered;
    # it should survive because it matches a position key.
    position_set = PositionSet(
        stock_tickers=frozenset(),
        option_keys=frozenset([
            OptionPositionKey(
                ticker="TEST", expiration_date="2026-04-17", option_type="call", strike=140.0
            )
        ]),
    )
    result = fetch.fetch_ticker_option_chain("TEST", position_set=position_set)

    assert "TESTC2" in result["contract_symbol"].values

"""Fetch-path tests covering raw provider row-count logging."""

import logging

import pandas as pd

from conftest import make_runtime_config
from opx import fetch
from opx.providers.base import OptionChainFrames


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
            "underlying_market_state": "REGULAR",
            "underlying_day_change_pct": 0.01,
            "historical_volatility": 0.2,
            "vix_level": 18.5,
            "vix_quote_time": pd.Timestamp("2026-03-20T13:45:00Z"),
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


def test_fetch_ticker_option_chain_logs_raw_provider_row_counts(monkeypatch, caplog):
    """Log raw provider counts before app-side filtering changes the row set."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("INFO", logger="opx.run")
    logger = logging.getLogger("opx.run")

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert not result.empty
    assert (
        "provider=stub expiration=2026-04-17 status=raw_provider_rows "
        "call_rows=2 put_rows=1 total_rows=3"
    ) in caplog.text
    assert "status=ok" in caplog.text
    assert "raw_provider_rows=3 raw_expirations=1" in caplog.text

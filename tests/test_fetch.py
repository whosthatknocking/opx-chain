import logging
from types import SimpleNamespace

import pandas as pd

from options_fetcher import fetch


class StubStock:
    def __init__(self):
        self.fast_info = {"lastPrice": 100.0, "previousClose": 99.0}
        self.info = {"regularMarketTime": "2026-03-20T13:45:00Z", "marketState": "REGULAR"}
        self.options = ["2026-04-17"]

    def history(self, period, interval, auto_adjust):
        return pd.DataFrame({"Close": [100.0, 101.0, 102.0, 103.0]})

    def option_chain(self, expiration_date):
        calls = pd.DataFrame(
            [
                {
                    "contractSymbol": "TESTC1",
                    "lastTradeDate": "2026-03-20T13:40:00Z",
                    "bid": 1.0,
                    "ask": 1.1,
                    "strike": 100.0,
                    "lastPrice": 1.05,
                    "openInterest": 10,
                    "volume": 5,
                    "impliedVolatility": 0.3,
                    "change": 0.1,
                    "percentChange": 0.02,
                    "inTheMoney": False,
                    "contractSize": "REGULAR",
                },
                {
                    "contractSymbol": "TESTC2",
                    "lastTradeDate": "2026-03-20T13:40:00Z",
                    "bid": 0.0,
                    "ask": 0.2,
                    "strike": 140.0,
                    "lastPrice": 0.1,
                    "openInterest": 0,
                    "volume": 0,
                    "impliedVolatility": 0.35,
                    "change": 0.0,
                    "percentChange": 0.0,
                    "inTheMoney": False,
                    "contractSize": "REGULAR",
                },
            ]
        )
        puts = pd.DataFrame(
            [
                {
                    "contractSymbol": "TESTP1",
                    "lastTradeDate": "2026-03-20T13:40:00Z",
                    "bid": 0.5,
                    "ask": 0.7,
                    "strike": 95.0,
                    "lastPrice": 0.6,
                    "openInterest": 8,
                    "volume": 3,
                    "impliedVolatility": 0.28,
                    "change": -0.05,
                    "percentChange": -0.01,
                    "inTheMoney": False,
                    "contractSize": "REGULAR",
                },
            ]
        )
        return SimpleNamespace(calls=calls, puts=puts)


def test_fetch_ticker_option_chain_logs_raw_yfinance_row_counts(monkeypatch, caplog):
    monkeypatch.setattr(fetch.yf, "Ticker", lambda ticker: StubStock())
    monkeypatch.setattr(
        fetch,
        "load_vix_snapshot",
        lambda: {"vix_level": 18.5, "vix_quote_time": pd.Timestamp("2026-03-20T13:45:00Z")},
    )

    caplog.set_level("INFO", logger="options_fetcher.run")
    logger = logging.getLogger("options_fetcher.run")

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert not result.empty
    assert "status=raw_yfinance_rows call_rows=2 put_rows=1 total_rows=3" in caplog.text
    assert "status=ok" in caplog.text
    assert "raw_yfinance_rows=3 raw_expirations=1" in caplog.text

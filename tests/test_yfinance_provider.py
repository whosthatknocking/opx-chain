"""YFinance provider tests covering shared debug payload dumping."""

from pathlib import Path
import json

import pandas as pd

from conftest import make_runtime_config
from opx_chain.providers.yfinance import YFinanceProvider


class FakeChain:  # pylint: disable=too-few-public-methods
    """Minimal yfinance option-chain stand-in."""

    def __init__(self):
        self.calls = pd.DataFrame([{"contractSymbol": "TSLACALL", "bid": 1.0, "ask": 1.1}])
        self.puts = pd.DataFrame([{"contractSymbol": "TSLAPUT", "bid": 0.9, "ask": 1.0}])


class FakeTicker:  # pylint: disable=too-few-public-methods
    """Minimal yfinance ticker stand-in."""

    def __init__(self, ticker):
        self.ticker = ticker
        self.fast_info = {"lastPrice": 101.5, "previousClose": 100.0}
        self.info = {
            "regularMarketTime": "2026-03-23T13:30:00Z",
            "regularMarketPrice": 101.5,
            "previousClose": 100.0,
        }
        self.options = ("2026-04-17",)

    def option_chain(self, expiration_date):
        """Return a minimal option-chain payload."""
        assert expiration_date == "2026-04-17"
        return FakeChain()

    def history(self, **_kwargs):
        """Return enough daily bars for HV calculation."""
        return pd.DataFrame({"Close": [100.0, 101.0, 99.5, 102.0] * 30})


def test_yfinance_provider_can_dump_raw_payloads(monkeypatch, tmp_path: Path, capsys):
    """Shared provider debug mode should dump raw yfinance payloads to JSON."""
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )
    monkeypatch.setattr("opx_chain.providers.base.get_runtime_config", lambda: make_runtime_config(
        debug_dump_provider_payload=True,
        debug_dump_dir=tmp_path,
    ))
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FakeTicker)

    provider = YFinanceProvider()
    provider.load_underlying_snapshot("TSLA")
    expirations = provider.list_option_expirations("TSLA")
    chain = provider.load_option_chain("TSLA", expirations[0])

    assert not chain.calls.empty
    assert not chain.puts.empty
    dumped_files = sorted(tmp_path.glob("yfinance_TSLA_*.json"))
    assert len(dumped_files) == 3
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in dumped_files]
    labels = {payload["label"] for payload in payloads}
    assert labels == {"underlying_snapshot", "expirations", "option_chain_2026-04-17"}
    assert "yfinance debug: dumped underlying_snapshot payload to" in capsys.readouterr().out

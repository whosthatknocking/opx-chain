"""Standalone price-context CLI tests."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from conftest import BoundaryTickDateTime, make_option_chain_frame, make_runtime_config
from opx_chain.price_context import PRICE_CONTEXT_SCHEMA_VERSION
from opx_chain.positions import EMPTY_POSITION_SET
from opx_chain.storage.memory import MemoryBackend


class StubProvider:  # pylint: disable=too-few-public-methods
    """Provider stub that captures per-ticker preparation calls."""

    name = "stub"

    def __init__(self):
        self.prepared_tickers = []
        self.history_calls = []

    def prepare_ticker_fetch(self, ticker):
        """Record ticker preparation before a standalone context fetch."""
        self.prepared_tickers.append(ticker)

    def load_price_history(self, ticker, *, lookback_days):
        """Return deterministic daily OHLCV history for standalone context fetches."""
        self.history_calls.append((ticker, lookback_days))
        frame = pd.DataFrame({"date": pd.bdate_range(end="2026-03-20", periods=lookback_days)})
        frame["close"] = 90.0 + frame.index.to_series(index=frame.index) * 0.05
        frame["open"] = frame["close"] - 0.25
        frame["high"] = frame["close"] + 0.75
        frame["low"] = frame["close"] - 0.75
        frame["volume"] = 1000 + frame.index
        return frame


class PriceContextFetchStub:  # pylint: disable=too-few-public-methods
    """Callable price-context fetch stub with captured config arguments."""

    def __init__(self):
        self.seen_configs = []

    def __call__(  # pylint: disable=too-many-arguments
        self,
        ticker,
        *,
        provider=None,
        logger=None,
        config=None,
        store=None,
    ):
        """Return deterministic price context for the requested ticker."""
        del provider, logger, store
        self.seen_configs.append(config)
        return {
            "support_1": 90.0,
            "support_2": 85.0,
            "resistance_1": 110.0,
            "resistance_2": 115.0,
            "20d_high": 112.0,
            "20d_low": 88.0,
            "50dma": 101.0,
            "200dma": 99.0,
            "vwap": 100.0,
            "volume_profile_high_volume_node": 98.0,
            "gap_fill_level": None,
            "pre_earnings_move_pct": None,
            "price_context_as_of": "2026-03-20",
            "price_context_age_days": 0,
            "price_context_source": "stub",
            "price_context_lookback_trading_days": 260,
            "price_context_calculation_method": "daily_ohlcv_v1",
            "price_context_staleness_status": "FRESH",
            "ticker_seen": ticker,
        }


def test_price_context_only_writes_json_without_option_export(tmp_path: Path, capsys):
    """--price-context-only must fetch daily context without option-chain side effects."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    storage_dir = tmp_path / "data"
    config = make_runtime_config(
        storage_enabled=True,
        storage_dir=storage_dir,
        tickers=("AAA", "BBB"),
        price_context_enable=False,
    )
    provider = StubProvider()

    with (
        patch.object(fetcher, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock"),
        patch.object(fetcher, "acquire_fetcher_lock", return_value=MagicMock()),
        patch.object(fetcher, "release_fetcher_lock"),
        patch.object(fetcher, "get_runtime_config", return_value=config),
        patch.object(fetcher, "set_runtime_config_override"),
        patch.object(
            fetcher,
            "create_run_logger",
            return_value=(MagicMock(), tmp_path / "run.log"),
        ),
        patch.object(fetcher, "get_storage_backend", return_value=backend),
        patch.object(
            backend,
            "interrupt_stale_runs",
            wraps=backend.interrupt_stale_runs,
        ) as mock_interrupt_stale,
        patch.object(fetcher, "load_positions", return_value=EMPTY_POSITION_SET),
        patch.object(fetcher, "get_data_provider", return_value=provider),
        patch.object(fetcher, "fetch_ticker_option_chain") as mock_option_chain,
        patch.object(fetcher, "write_options_csv") as mock_write_csv,
    ):
        result = fetcher.main(["--price-context-only"])

    captured = capsys.readouterr()
    assert result == 0
    assert "CLI override: price_context_enable=true, price_context_only=true" in captured.out
    assert "Saved price context:" in captured.out
    assert "AAA: price_context  status=FRESH  as_of=2026-03-20" in captured.out
    assert "BBB: price_context  status=FRESH  as_of=2026-03-20" in captured.out
    assert provider.prepared_tickers == ["AAA", "BBB"]
    assert provider.history_calls == [("AAA", 260), ("BBB", 260)]
    mock_option_chain.assert_not_called()
    mock_write_csv.assert_not_called()
    mock_interrupt_stale.assert_not_called()
    assert not backend.list_datasets()
    assert (storage_dir / "price-history.db").exists()

    latest_path = storage_dir / "runs" / "price_context_latest.json"
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "price_context"
    assert payload["schema_version"] == PRICE_CONTEXT_SCHEMA_VERSION
    assert payload["provider"] == "yfinance"
    assert payload["tickers"] == ["AAA", "BBB"]
    assert [record["ticker"] for record in payload["records"]] == ["AAA", "BBB"]
    assert all(
        record["price_context_staleness_status"] == "FRESH"
        for record in payload["records"]
    )


def test_enabled_price_context_option_run_writes_independent_json(
    tmp_path: Path,
    capsys,
):
    """Normal option-chain runs should keep price context as an independent artifact."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    storage_dir = tmp_path / "data"
    config = make_runtime_config(
        storage_enabled=False,
        storage_dir=storage_dir,
        tickers=("AAA",),
        price_context_enable=True,
    )
    provider = StubProvider()
    price_fetch = PriceContextFetchStub()
    option_frame = make_option_chain_frame(
        rows=1,
        ticker="AAA",
        provider=config.data_provider,
        expiration="2026-04-17",
    )

    with (
        patch.object(fetcher, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock"),
        patch.object(fetcher, "acquire_fetcher_lock", return_value=MagicMock()),
        patch.object(fetcher, "release_fetcher_lock"),
        patch.object(fetcher, "get_runtime_config", return_value=config),
        patch.object(fetcher, "set_runtime_config_override"),
        patch.object(
            fetcher,
            "create_run_logger",
            return_value=(MagicMock(), tmp_path / "run.log"),
        ),
        patch.object(fetcher, "get_storage_backend", return_value=None),
        patch.object(fetcher, "load_positions", return_value=EMPTY_POSITION_SET),
        patch.object(fetcher, "get_data_provider", return_value=provider),
        patch.object(fetcher, "fetch_ticker_price_context", side_effect=price_fetch),
        patch.object(fetcher, "fetch_ticker_option_chain", return_value=option_frame),
    ):
        result = fetcher.main([])

    captured = capsys.readouterr()
    assert result == 0
    assert "Price context:" in captured.out
    assert "Saved:" in captured.out
    assert price_fetch.seen_configs[0].price_context_enable is True

    latest_path = storage_dir / "runs" / "price_context_latest.json"
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == PRICE_CONTEXT_SCHEMA_VERSION
    assert payload["tickers"] == ["AAA"]
    assert payload["records"][0]["ticker"] == "AAA"


def test_price_context_artifact_reuses_timestamp_for_filename_and_payload(
    monkeypatch,
    tmp_path: Path,
):
    """The timestamped artifact name and fetched_at metadata must stay paired."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    BoundaryTickDateTime.reset()
    config = make_runtime_config(
        storage_enabled=False,
        storage_dir=tmp_path / "data",
        tickers=("AAA",),
        price_context_enable=True,
    )
    store = MagicMock()
    provider = StubProvider()
    price_fetch = PriceContextFetchStub()

    monkeypatch.setattr(fetcher, "datetime", BoundaryTickDateTime)
    monkeypatch.setattr(fetcher, "get_data_provider", MagicMock(return_value=provider))
    monkeypatch.setattr(fetcher, "get_price_history_store", MagicMock(return_value=store))
    monkeypatch.setattr(fetcher, "fetch_ticker_price_context", price_fetch)

    output_path = fetcher._run_price_context_fetch(  # pylint: disable=protected-access
        config,
        ("AAA",),
        logger=None,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    latest_payload = json.loads(
        (tmp_path / "data" / "runs" / "price_context_latest.json").read_text(
            encoding="utf-8",
        ),
    )
    assert BoundaryTickDateTime.calls == 1
    assert output_path.name == "price_context_20260509_055959.json"
    assert payload["fetched_at"] == "2026-05-09T05:59:59Z"
    assert latest_payload["fetched_at"] == "2026-05-09T05:59:59Z"


def test_price_context_only_conflicts_with_disable_flag():
    """Standalone price-context mode cannot also disable price-context fetching."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    with pytest.raises(SystemExit):
        fetcher.parse_args(["--price-context-only", "--disable-price-context"])

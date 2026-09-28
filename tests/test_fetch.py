"""Fetch-path tests covering raw provider row-count logging."""

# pylint: disable=too-many-lines

import logging
from datetime import date

import numpy as np
import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain import fetch
from opx_chain.fetch import append_ticker_event_fields
from opx_chain.integrity import OptionChainDataIntegrityError, OptionChainIntegrityCode
import opx_chain.metrics
import opx_chain.normalize
from opx_chain.price_context import PriceContextStatus
from opx_chain.positions import EMPTY_POSITION_SET, OptionPositionKey, PositionSet
from opx_chain.providers.base import OptionChainFrames
from opx_chain.runlog import RUN_LOGGER_NAME
from opx_chain.storage.cache import FilesystemCache


def make_vendor_frame(rows):
    """Build a provider-normalized frame that still exercises later filters."""
    return pd.DataFrame(rows)


class StubProvider:
    """Minimal provider stub for fetch-path tests."""

    name = "stub"

    def __init__(self):
        self.prepared_tickers = []

    def prepare_ticker_fetch(self, ticker):
        """Track the per-ticker fetch boundary hook."""
        self.prepared_tickers.append(ticker)

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
                    "contract_symbol": "TEST260417C00100000",
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
                    "contract_symbol": "TEST260417C00140000",
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
                    "contract_symbol": "TEST260417P00095000",
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

    def load_price_history(self, ticker, *, lookback_days):  # pylint: disable=unused-argument
        """Return daily OHLCV data for price-context tests."""
        dates = pd.bdate_range(end="2026-03-20", periods=lookback_days)
        closes = [90.0 + index * 0.05 for index in range(lookback_days)]
        return pd.DataFrame(
            {
                "Date": dates,
                "Open": [close - 0.25 for close in closes],
                "High": [close + 0.75 for close in closes],
                "Low": [close - 0.75 for close in closes],
                "Close": closes,
                "Volume": [1000 + index for index in range(lookback_days)],
            }
        )

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


def test_json_cache_round_trips_pandas_snapshot_values(tmp_path):
    """Snapshot cache writes should preserve pandas scalar semantics."""
    cache = FilesystemCache(tmp_path)
    value = {
        "underlying_price": np.float64(100.5),
        "underlying_price_time": pd.Timestamp("2026-03-20T13:45:00Z"),
        "missing_time": pd.NaT,
        "nested": {"values": [np.int64(3), pd.Timestamp("2026-03-20T13:46:00Z")]},
    }

    fetch._cache_put_json(cache, "snapshot:stub:TEST", value, ttl=300)  # pylint: disable=protected-access
    cached = fetch._cache_get_json(cache, "snapshot:stub:TEST")  # pylint: disable=protected-access

    assert cached is not None
    assert cached["underlying_price"] == 100.5
    assert cached["underlying_price_time"] == pd.Timestamp("2026-03-20T13:45:00Z")
    assert cached["missing_time"] is pd.NaT
    assert cached["nested"]["values"] == [3, pd.Timestamp("2026-03-20T13:46:00Z")]


def test_json_cache_sanitizes_non_finite_values(tmp_path):
    """Snapshot cache content must never emit non-standard JSON numbers."""
    cache = FilesystemCache(tmp_path)
    value = {
        "underlying_price": np.float64(np.inf),
        "change": float("-inf"),
        "dividend_amount": np.float64(np.nan),
        "nested": {"values": [1.0, float("nan"), True]},
    }

    fetch._cache_put_json(cache, "snapshot:stub:INF", value, ttl=300)  # pylint: disable=protected-access
    raw = cache.get("snapshot:stub:INF")
    cached = fetch._cache_get_json(cache, "snapshot:stub:INF")  # pylint: disable=protected-access

    assert raw is not None
    text = raw.decode()
    assert "Infinity" not in text
    assert "NaN" not in text
    assert cached == {
        "underlying_price": None,
        "change": None,
        "dividend_amount": None,
        "nested": {"values": [1.0, None, True]},
    }


def test_json_cache_rejects_non_standard_json_literals(tmp_path):
    """Legacy corrupt cache content should miss instead of restoring inf/nan."""
    cache = FilesystemCache(tmp_path)
    cache.put("snapshot:stub:BAD", b'{"underlying_price": Infinity}', ttl_seconds=300)

    assert fetch._cache_get_json(cache, "snapshot:stub:BAD") is None  # pylint: disable=protected-access


def test_json_cache_logs_unserializable_values(tmp_path, caplog):
    """Unsupported cache values should be visible rather than silently skipped."""
    cache = FilesystemCache(tmp_path)
    caplog.set_level("WARNING")

    fetch._cache_put_json(  # pylint: disable=protected-access
        cache,
        "snapshot:stub:BAD",
        {"unsupported": object()},
        ttl=300,
    )

    assert cache.get("snapshot:stub:BAD") is None
    assert "cache put skipped for key=snapshot:stub:BAD" in caplog.text


def test_chain_cache_logs_write_failures(caplog):
    """Option-chain cache write failures should be visible to operators."""
    class FailingCache:  # pylint: disable=too-few-public-methods
        """Cache stub that fails writes."""

        def put(self, _key, _value, _ttl):
            """Raise to exercise cache error logging."""
            raise OSError("disk full")

    chain = OptionChainFrames(
        calls=pd.DataFrame([{"contractSymbol": "TSLACALL"}]),
        puts=pd.DataFrame([{"contractSymbol": "TSLAPUT"}]),
    )

    with caplog.at_level(logging.WARNING):
        fetch._cache_put_chain(  # pylint: disable=protected-access
            FailingCache(),
            "chain:stub:TSLA:2026-04-17",
            chain,
            ttl=300,
        )

    assert "cache put skipped for key=chain:stub:TSLA:2026-04-17" in caplog.text
    assert "disk full" in caplog.text


def test_fetch_ticker_option_chain_logs_raw_provider_row_counts(monkeypatch, caplog):
    """Log raw provider counts before app-side filtering changes the row set."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("INFO", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

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


def test_fetch_ticker_option_chain_skips_non_finite_underlying_price(
    monkeypatch,
    caplog,
):
    """Non-finite snapshot prices should fail before enrichment runs."""

    class InfiniteSnapshotProvider(StubProvider):
        """Provider that returns a corrupt spot quote."""

        def load_underlying_snapshot(self, ticker):
            snapshot = super().load_underlying_snapshot(ticker)
            snapshot["underlying_price"] = float("inf")
            return snapshot

        def list_option_expirations(self, ticker):
            raise AssertionError("invalid snapshot should skip expiration loading")

    monkeypatch.setattr(fetch, "get_data_provider", InfiniteSnapshotProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("INFO", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert result.empty
    assert result.attrs["fetch_status"] == "skipped"
    assert "reason=invalid_underlying_price" in caplog.text


def test_fetch_ticker_option_chain_counts_vendor_trade_aliases(monkeypatch, caplog):
    """Raw diagnostics should count vendor-specific last-trade price columns."""

    class AliasTradeProvider(StubProvider):
        """Provider that uses yfinance/marketdata-style last-trade names."""

        name = "alias"

        def load_option_chain(self, ticker, expiration_date):
            chain = super().load_option_chain(ticker, expiration_date)
            return OptionChainFrames(
                calls=chain.calls.rename(columns={"last_trade_price": "lastPrice"}),
                puts=chain.puts.rename(columns={"last_trade_price": "last"}),
            )

        def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            df,
            underlying_price,
            expiration_date,
            option_type,
            ticker,
        ):
            frame = df.rename(
                columns={
                    "lastPrice": "last_trade_price",
                    "last": "last_trade_price",
                }
            )
            return super().normalize_option_frame(
                frame,
                underlying_price,
                expiration_date,
                option_type,
                ticker,
            )

    monkeypatch.setattr(fetch, "get_data_provider", AliasTradeProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("INFO", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert not result.empty
    assert "provider=alias expiration=2026-04-17 status=raw_provider_rows" in caplog.text
    assert "call_trade_rows=2 put_trade_rows=1" in caplog.text


def test_fetch_ticker_option_chain_logs_skipped_when_provider_has_no_frames(
    monkeypatch, caplog
):
    """Empty provider payloads should use the same skipped status in logs and storage."""

    class EmptyChainProvider(StubProvider):
        """Provider that reports an expiration but no usable option rows."""

        def load_option_chain(self, ticker, expiration_date):
            assert ticker == "TEST"
            assert expiration_date == "2026-04-17"
            return OptionChainFrames(calls=pd.DataFrame(), puts=pd.DataFrame())

    monkeypatch.setattr(fetch, "get_data_provider", EmptyChainProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    caplog.set_level("WARNING", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert result.empty
    assert result.attrs["fetch_status"] == "skipped"
    assert (
        "ticker=TEST provider=stub status=skipped rows=0 expirations=0 "
        "raw_provider_rows=0 raw_expirations=1"
    ) in caplog.text
    assert "ticker=TEST provider=stub status=ok rows=0" not in caplog.text


def test_fetch_ticker_option_chain_prepares_provider_before_loading(monkeypatch):
    """Each ticker fetch should mark a boundary for provider-local memory caches."""
    provider = StubProvider()
    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    result = fetch.fetch_ticker_option_chain("TEST")

    assert not result.empty
    assert provider.prepared_tickers == ["TEST"]


def test_fetch_ticker_option_chain_can_skip_provider_events(monkeypatch, capsys):
    """Downstream callers can defer corporate-event enrichment to a separate overlay."""

    class EventCountingProvider(StubProvider):
        """Provider that fails the test if option-chain fetch asks for events."""

        def load_ticker_events(self, ticker):
            raise AssertionError(f"unexpected event fetch for {ticker}")

    monkeypatch.setattr(fetch, "get_data_provider", EventCountingProvider)
    monkeypatch.setattr(
        fetch,
        "get_runtime_config",
        lambda: make_runtime_config(today=pd.Timestamp("2026-03-20").date()),
    )

    result = fetch.fetch_ticker_option_chain("TEST", skip_events=True)

    stdout = capsys.readouterr().out
    assert not result.empty
    assert "TEST: events skipped by caller" in stdout
    assert result["next_earnings_date"].isna().all()
    assert result["next_ex_div_date"].isna().all()
    assert result["days_to_earnings"].isna().all()
    assert result["days_to_ex_div"].isna().all()


@pytest.mark.parametrize("bad_skip_events", ["false", "0", 0, 1, None])
def test_fetch_ticker_option_chain_rejects_non_bool_skip_events(
    monkeypatch,
    bad_skip_events,
):
    """Direct fetch helper mode flags should fail before provider work."""

    def fail_provider():
        raise AssertionError("provider should not be resolved for invalid skip_events")

    monkeypatch.setattr(fetch, "get_data_provider", fail_provider)

    with pytest.raises(ValueError, match="skip_events must be true or false"):
        fetch.fetch_ticker_option_chain("TEST", skip_events=bad_skip_events)


@pytest.mark.parametrize("bad_ticker", [None, False, 0, "", "bad ticker", "AAA1"])
def test_fetch_ticker_option_chain_rejects_invalid_ticker_before_provider(
    monkeypatch,
    bad_ticker,
):
    """Direct ticker helper input should fail before provider/cache work."""

    def fail_provider():
        raise AssertionError("provider should not be resolved for invalid ticker")

    monkeypatch.setattr(fetch, "get_data_provider", fail_provider)

    with pytest.raises(ValueError, match="ticker must be"):
        fetch.fetch_ticker_option_chain(bad_ticker)  # type: ignore[arg-type]


def test_fetch_ticker_option_chain_reuses_serialized_snapshot_cache(monkeypatch, tmp_path):
    """Cached snapshots should avoid repeated provider calls and remain timestamp-like."""
    class CountingProvider(StubProvider):
        """Provider that records snapshot calls across fetches."""

        def __init__(self):
            super().__init__()
            self.snapshot_calls = 0

        def load_underlying_snapshot(self, ticker):
            self.snapshot_calls += 1
            return super().load_underlying_snapshot(ticker)

    provider = CountingProvider()
    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)

    def config_factory():
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            provider_cache_backend="filesystem",
            provider_cache_dir=tmp_path,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    first = fetch.fetch_ticker_option_chain("TEST")
    second = fetch.fetch_ticker_option_chain("TEST")

    assert not first.empty
    assert not second.empty
    assert provider.snapshot_calls == 1
    assert second["underlying_price_time"].iloc[0] == pd.Timestamp("2026-03-20T13:45:00Z")


def test_fetch_ticker_option_chain_does_not_project_price_context(monkeypatch, capsys):
    """Enabled price context should not alter option-chain row schema."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)

    def config_factory():
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            enable_filters=False,
            price_context_enable=True,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    result = fetch.fetch_ticker_option_chain("TEST")

    stdout = capsys.readouterr().out
    assert not result.empty
    assert "price_context" not in stdout
    assert "price_context_staleness_status" not in result.columns
    assert "price_context_source" not in result.columns
    assert "support_1" not in result.columns
    assert "20d_high" not in result.columns


def test_fetch_ticker_price_context_uses_price_history_store(monkeypatch, tmp_path):
    """Price context should derive from stored daily bars after first reconciliation."""
    class CountingProvider(StubProvider):
        """Provider that records price-history calls across context fetches."""

        def __init__(self):
            super().__init__()
            self.history_calls = 0

        def load_price_history(self, ticker, *, lookback_days):
            self.history_calls += 1
            return super().load_price_history(ticker, lookback_days=lookback_days)

    provider = CountingProvider()
    config = make_runtime_config(
        today=pd.Timestamp("2026-03-20").date(),
        storage_dir=tmp_path / "data",
        provider_price_context_ttl=86400,
        price_context_lookback_days=260,
    )
    monkeypatch.setattr(fetch, "get_runtime_config", lambda: config)

    first = fetch.fetch_ticker_price_context("TEST", provider=provider, config=config)
    second = fetch.fetch_ticker_price_context("TEST", provider=provider, config=config)

    assert first["price_context_staleness_status"] == PriceContextStatus.FRESH.value
    assert second["price_context_staleness_status"] == PriceContextStatus.FRESH.value
    assert provider.history_calls == 1


@pytest.mark.parametrize("max_age_days", [None, "7", True, -1])
def test_fetch_ticker_price_context_validates_max_age_before_provider_work(
    max_age_days,
):
    """Direct config-like max-age values should fail before provider/store calls."""
    class CountingProvider(StubProvider):
        """Provider stub that records whether price-history loading was reached."""

        def __init__(self):
            super().__init__()
            self.history_calls = 0

        def load_price_history(self, ticker, *, lookback_days):
            self.history_calls += 1
            return super().load_price_history(ticker, lookback_days=lookback_days)

    provider = CountingProvider()
    config = make_runtime_config(price_context_max_age_days=max_age_days)

    with pytest.raises(ValueError, match="price_context_max_age_days"):
        fetch.fetch_ticker_price_context("TEST", provider=provider, config=config)

    assert provider.history_calls == 0


def test_fetch_ticker_price_context_returns_error_payload_on_failure():
    """Optional price-context failures should return an ERROR payload."""
    class BrokenPriceContextProvider(StubProvider):
        """Provider with usable option data but failing price history."""

        def load_price_history(self, ticker, *, lookback_days):
            raise RuntimeError(f"history unavailable for {ticker}")

    config = make_runtime_config(today=pd.Timestamp("2026-03-20").date())
    context = fetch.fetch_ticker_price_context(
        "TEST",
        provider=BrokenPriceContextProvider(),
        config=config,
    )

    assert context["price_context_staleness_status"] == PriceContextStatus.ERROR.value
    assert context["support_1"] is None


def test_marketdata_cache_keys_include_mode(monkeypatch, tmp_path):
    """Market Data cache entries must not cross live/delayed/cached modes."""

    class CountingMarketDataProvider(StubProvider):
        """Provider that records calls across mode changes."""

        name = "marketdata"

        def __init__(self):
            super().__init__()
            self.snapshot_calls = 0
            self.event_calls = 0
            self.chain_calls = 0

        def load_underlying_snapshot(self, ticker):
            self.snapshot_calls += 1
            return super().load_underlying_snapshot(ticker)

        def load_ticker_events(self, ticker):
            self.event_calls += 1
            return super().load_ticker_events(ticker)

        def load_option_chain(self, ticker, expiration_date):
            self.chain_calls += 1
            return super().load_option_chain(ticker, expiration_date)

    provider = CountingMarketDataProvider()
    selected_mode = "delayed"
    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)

    def config_factory():
        return make_runtime_config(
            today=pd.Timestamp("2026-03-20").date(),
            marketdata_mode=selected_mode,
            provider_cache_backend="filesystem",
            provider_cache_dir=tmp_path,
        )

    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)

    delayed = fetch.fetch_ticker_option_chain("TEST")
    selected_mode = "live"
    live = fetch.fetch_ticker_option_chain("TEST")

    assert not delayed.empty
    assert not live.empty
    assert provider.snapshot_calls == 2
    assert provider.event_calls == 2
    assert provider.chain_calls == 2


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
    assert result.attrs["raw_row_count"] == 3
    assert result.attrs["normalized_row_count"] == 3
    assert result.attrs["filtered_row_count"] == 2
    assert result.attrs["raw_expiration_count"] == 1


def test_fetch_ticker_option_chain_explains_when_filters_remove_everything(
    monkeypatch, capsys, caplog
):
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

    caplog.set_level("INFO", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    stdout = capsys.readouterr().out
    assert result.empty
    assert result.attrs["fetch_status"] == "skipped"
    assert result.attrs["raw_row_count"] == 3
    assert result.attrs["normalized_row_count"] == 3
    assert result.attrs["filtered_row_count"] == 3
    assert (
        "ticker=TEST provider=stub status=skipped fetched_at="
        in caplog.text
    )
    assert "reason=all_rows_filtered" in caplog.text
    assert "ticker=TEST provider=stub status=ok" not in caplog.text
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
    assert set(result["contract_symbol"]) == {
        "TEST260417C00100000",
        "TEST260417C00140000",
        "TEST260417P00095000",
    }


def test_fetch_ticker_option_chain_rejects_invalid_rows_before_filtering(monkeypatch):
    """The hard gate rejects corrupt rows before filters could hide them."""
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

    with pytest.raises(OptionChainDataIntegrityError) as captured:
        fetch.fetch_ticker_option_chain("TEST", validation_findings=findings)

    assert (
        captured.value.summary.counts_by_code[
            OptionChainIntegrityCode.REQUIRED_FIELD_INVALID
        ]
        == 1
    )
    assert not findings


def test_fetch_ticker_option_chain_rejects_provider_ticker_identity_mismatch(monkeypatch):
    """Provider-normalized rows for another underlying must not be returned as ok rows."""
    class WrongTickerProvider(StubProvider):
        """Provider variant that labels normalized rows with another underlying."""

        def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            df,
            underlying_price,
            expiration_date,
            option_type,
            ticker,
        ):
            frame = super().normalize_option_frame(
                df, underlying_price, expiration_date, option_type, ticker
            )
            frame["underlying_symbol"] = "OTHER"
            return frame

    monkeypatch.setattr(fetch, "get_data_provider", WrongTickerProvider)
    monkeypatch.setattr(fetch, "get_runtime_config", make_runtime_config)

    with pytest.raises(OptionChainDataIntegrityError) as captured:
        fetch.fetch_ticker_option_chain("TEST")

    assert (
        captured.value.summary.counts_by_code[
            OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH
        ]
        >= 1
    )


def test_fetch_ticker_option_chain_cannot_disable_integrity_gate(monkeypatch):
    """Optional diagnostics do not disable fatal provider integrity checks."""
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

    with pytest.raises(OptionChainDataIntegrityError):
        fetch.fetch_ticker_option_chain("TEST", validation_findings=findings)

    assert not findings


def test_fetch_ticker_option_chain_logs_provider_name_on_error(monkeypatch, caplog):
    """Shared error logs should stay provider-neutral while preserving provider context."""
    monkeypatch.setattr(fetch, "get_data_provider", ErrorProvider)
    monkeypatch.setattr(fetch, "get_runtime_config", make_runtime_config)

    caplog.set_level("ERROR", logger=RUN_LOGGER_NAME)
    logger = logging.getLogger(RUN_LOGGER_NAME)

    result = fetch.fetch_ticker_option_chain("TEST", logger=logger)

    assert result.empty
    assert result.attrs["fetch_status"] == "error"
    assert result.attrs["fetch_error_summary"] == "RuntimeError: provider exploded for TEST"
    assert "provider=broken status=error" in caplog.text


def test_append_ticker_event_fields_broadcasts_day_counts_to_all_rows():
    """Event fields should be broadcast to every row with correct day-count arithmetic."""
    today = date(2026, 4, 16)
    events = {
        "next_earnings_date": "2026-04-23",
        "next_earnings_date_is_estimated": True,
        "next_earnings_date_source": "marketdata.reportDate",
        "next_earnings_date_confidence": "estimated",
        "next_ex_div_date": "2026-04-18",
        "next_ex_div_date_source": "marketdata.exDate",
        "next_ex_div_date_confidence": "confirmed",
        "dividend_amount": 0.75,
    }
    frame = pd.DataFrame([{"strike": 100.0}, {"strike": 105.0}, {"strike": 110.0}])

    result = append_ticker_event_fields(frame.copy(), events, today)

    assert (result["next_earnings_date"] == "2026-04-23").all()
    assert result["next_earnings_date_is_estimated"].tolist() == [True, True, True]
    assert (result["next_earnings_date_source"] == "marketdata.reportDate").all()
    assert (result["next_earnings_date_confidence"] == "estimated").all()
    assert (result["next_ex_div_date"] == "2026-04-18").all()
    assert (result["next_ex_div_date_source"] == "marketdata.exDate").all()
    assert (result["next_ex_div_date_confidence"] == "confirmed").all()
    assert result["dividend_amount"].tolist() == pytest.approx([0.75, 0.75, 0.75])
    assert (result["days_to_earnings"] == 7).all()
    assert (result["days_to_ex_div"] == 2).all()


def test_append_ticker_event_fields_handles_blank_events():
    """Missing event data should produce NaN day-count fields without raising."""
    today = date(2026, 4, 16)
    blank_event_keys = [
        "next_earnings_date",
        "next_earnings_date_is_estimated",
        "next_earnings_date_source",
        "next_earnings_date_confidence",
        "next_ex_div_date",
        "next_ex_div_date_source",
        "next_ex_div_date_confidence",
    ]
    events = {key: None for key in blank_event_keys}
    events["dividend_amount"] = np.nan
    frame = pd.DataFrame([{"strike": 100.0}])

    result = append_ticker_event_fields(frame.copy(), events, today)

    assert result.loc[0, "next_earnings_date"] is None
    assert result.loc[0, "next_earnings_date_is_estimated"] is None
    assert result.loc[0, "next_earnings_date_source"] is None
    assert result.loc[0, "next_earnings_date_confidence"] is None
    assert result.loc[0, "next_ex_div_date"] is None
    assert result.loc[0, "next_ex_div_date_source"] is None
    assert result.loc[0, "next_ex_div_date_confidence"] is None
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
                "contract_symbol": "TEST260320C00100000",
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


@pytest.mark.parametrize("held", [False, True])
def test_isolated_crossed_quote_is_quarantined_with_durable_warning(monkeypatch, held):
    """A held corrupt quote is missing, never preserved by the position corridor."""
    import json  # pylint: disable=import-outside-toplevel

    class CrossedProvider(StubProvider):
        """Reproduce a provider's zero offer with a positive bid."""

        def load_option_chain(self, ticker, expiration_date):
            chain = super().load_option_chain(ticker, expiration_date)
            chain.calls.loc[0, "bid"] = .01
            chain.calls.loc[0, "ask"] = 0
            return chain

    provider = CrossedProvider()
    monkeypatch.setattr(fetch, "get_data_provider", lambda: provider)
    _patch_config_20260320(monkeypatch)
    def config_factory():
        return make_runtime_config(today=date(2026, 3, 20), enable_filters=False)
    monkeypatch.setattr(fetch, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.normalize, "get_runtime_config", config_factory)
    monkeypatch.setattr(opx_chain.metrics, "get_runtime_config", config_factory)
    positions = PositionSet(
        stock_tickers=frozenset({"TEST"}),
        option_keys=frozenset({OptionPositionKey("TEST", "2026-04-17", "call", 100)}),
    ) if held else EMPTY_POSITION_SET
    result = fetch.fetch_ticker_option_chain("TEST", position_set=positions)
    assert "TEST260417C00100000" not in set(result["contract_symbol"])
    assert result.attrs["fetch_status"] == "ok_with_warnings"
    assert json.loads(result.attrs["fetch_error_summary"])["quarantined_count"] == 1
    assert result.attrs["normalized_row_count"] == len(result) + result.attrs["filtered_row_count"]
    assert provider.prepared_tickers == ["TEST", "TEST"]


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
    assert "TEST260320C00100000" in result["contract_symbol"].values


def test_today_expiration_kept_for_portfolio_option(monkeypatch):
    """Expirations on today's date must be kept when the ticker has held options."""
    monkeypatch.setattr(fetch, "get_data_provider", TodayExpirationProvider)
    _patch_config_20260320(monkeypatch)

    position_set = PositionSet(
        stock_tickers=frozenset(),
        option_keys=frozenset(
            [
                OptionPositionKey(
                    ticker="TEST",
                    expiration_date="2026-03-20",
                    option_type="call",
                    strike=100.0,
                )
            ]
        ),
    )
    result = fetch.fetch_ticker_option_chain("TEST", position_set=position_set)

    assert not result.empty
    assert "TEST260320C00100000" in result["contract_symbol"].values


def test_position_option_survives_filters(monkeypatch):
    """An option matching a portfolio position must not be dropped even with bid==0."""
    monkeypatch.setattr(fetch, "get_data_provider", StubProvider)
    _patch_config_20260320(monkeypatch)

    # The 140 call has bid=0 and is outside the 30% band — normally filtered;
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

    assert "TEST260417C00140000" in result["contract_symbol"].values


def test_append_underlying_snapshot_fields_is_stale_underlying_price_stays_nullable_boolean():
    """is_stale_underlying_price must be bool/None (object dtype), never float.
    When `underlying_price_time` is NaT the result cell is None (unknown); when
    it is present the cell is a Python bool derived from the staleness compare.
    """
    df = pd.DataFrame([{"contract_symbol": "X"}, {"contract_symbol": "Y"}])
    fresh_snapshot = {
        "underlying_price_time": pd.Timestamp("2026-03-20T15:55:00Z"),
        "underlying_day_change_pct": 0.0,
        "historical_volatility": 0.25,
    }
    fetched_at = pd.Timestamp("2026-03-20T16:00:00Z")

    fresh = fetch.append_underlying_snapshot_fields(
        df.copy(), fresh_snapshot, fetched_at, stale_quote_seconds=3600
    )
    assert fresh["is_stale_underlying_price"].dtype == object
    assert fresh.loc[0, "is_stale_underlying_price"] is False
    assert fresh.loc[1, "is_stale_underlying_price"] is False

    future_snapshot = {
        "underlying_price_time": pd.Timestamp("2026-03-20T16:05:00Z"),
        "underlying_day_change_pct": 0.0,
        "historical_volatility": 0.25,
    }
    future = fetch.append_underlying_snapshot_fields(
        df.copy(), future_snapshot, fetched_at, stale_quote_seconds=3600
    )
    assert future["is_stale_underlying_price"].dtype == object
    assert future.loc[0, "is_stale_underlying_price"] is True
    assert future.loc[1, "is_stale_underlying_price"] is True

    missing_snapshot = {
        "underlying_price_time": pd.NaT,
        "underlying_day_change_pct": 0.0,
        "historical_volatility": 0.25,
    }
    missing = fetch.append_underlying_snapshot_fields(
        df.copy(), missing_snapshot, fetched_at, stale_quote_seconds=3600
    )
    assert missing["is_stale_underlying_price"].dtype == object
    assert missing.loc[0, "is_stale_underlying_price"] is None
    assert missing.loc[1, "is_stale_underlying_price"] is None

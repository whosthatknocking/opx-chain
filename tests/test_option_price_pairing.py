"""Provider-native price/time pairs survive normalization and enrichment."""

from datetime import date

import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain.export import prepare_export_frame
from opx_chain.fetch import append_underlying_snapshot_fields
from opx_chain.greeks import compute_greeks
from opx_chain.normalize import enrich_option_frame, filter_strikes_near_spot
from opx_chain.providers.base import OptionChainFrames
from opx_chain.providers.marketdata import MarketDataProvider
from opx_chain.quote_quarantine import quarantine_unusable_quotes


@pytest.fixture(name="paired_frame")
def make_paired_frame(monkeypatch):
    """Keep differing contract snapshots, not a single broadcast stock price."""
    config = make_runtime_config(today=date(2026, 11, 5))
    for module in ("normalize", "metrics"):
        monkeypatch.setattr(f"opx_chain.{module}.get_runtime_config", lambda: config)
    raw = pd.DataFrame({
        "optionSymbol": ["INTC271217P00090000", "INTC271217P00085000"],
        "strike": [90., 85.], "underlyingPrice": [60., 61.],
        "updated": ["2026-11-05T19:56:46Z", "2026-11-05T19:56:47Z"],
        "bid": [38.1, 34.], "ask": [39.2, 35.], "last": [38.7, 34.5],
        "iv": [.65, .66], "volume": [200, 100], "openInterest": [3000, 2000],
    }, index=[9, 3])
    return MarketDataProvider().normalize_option_frame(
        raw, 54.46, "2027-12-17", "put", "INTC",
    )


def test_native_pairs_drive_intrinsic_extrinsic_greeks_and_export(paired_frame):
    """The independent delayed spot must never affect paired row valuation."""
    before = paired_frame.copy(deep=True)
    result = enrich_option_frame(paired_frame, 54.46, pd.Timestamp("2026-11-05T19:58:00Z"))
    result = append_underlying_snapshot_fields(result, {
        "underlying_price_time": pd.Timestamp("2026-11-05T19:41:36Z"),
        "underlying_day_change_pct": -.0654, "historical_volatility": float("nan"),
    }, pd.Timestamp("2026-11-05T19:58:00Z"), 900)
    assert result.underlying_price.tolist() == [60., 61.]
    assert (result.underlying_price_time == result.option_quote_time).all()
    assert result.intrinsic_value.tolist() == pytest.approx([30., 24.])
    assert result.extrinsic_value_mid.tolist() == pytest.approx([8.65, 10.5])
    assert result.underlying_price_age_seconds.tolist() == [74., 73.]
    assert not result.is_stale_underlying_price.any()
    assert result.underlying_day_change_pct.isna().all()
    for index in result.index:
        reference = compute_greeks(before.loc[[index]].drop(columns="underlying_price"),
                                   before.loc[index, "underlying_price"], .045)
        assert result.loc[index, "delta"] == pytest.approx(reference.loc[index, "delta"])
        assert result.loc[index, "gamma"] == pytest.approx(reference.loc[index, "gamma"])
    exported = prepare_export_frame([result])
    assert exported.underlying_price.tolist() == [60., 61.]
    assert (exported.underlying_price_time == exported.option_quote_time).all()
    assert exported.extrinsic_value_mid.tolist() == pytest.approx([8.65, 10.5])


@pytest.mark.parametrize("native", [None, "invalid", float("inf"), -1., 0.])
def test_invalid_native_price_never_falls_back_to_independent_spot(native):
    """Bad native evidence remains invalid instead of silently changing source."""
    frame = MarketDataProvider().normalize_option_frame(pd.DataFrame({
        "underlyingPrice": [native], "updated": ["2026-11-05T19:56:46Z"],
        "strike": [90.], "bid": [38.1], "ask": [39.2],
    }), 54.46, "2027-12-17", "put", "INTC")
    value = frame.iloc[0].underlying_price
    assert pd.isna(value) or value <= 0
    assert value != 54.46


def test_missing_native_price_preserves_independent_fallback_clock():
    """Legacy provider rows retain honest independent snapshot timing."""
    frame = MarketDataProvider().normalize_option_frame(pd.DataFrame({
        "updated": ["2026-11-05T19:56:46Z"], "strike": [90.],
        "bid": [38.1], "ask": [39.2],
    }), 54.46, "2027-12-17", "put", "INTC")
    stock_time = pd.Timestamp("2026-11-05T19:41:36Z")
    result = append_underlying_snapshot_fields(frame, {
        "underlying_price_time": stock_time,
        "underlying_day_change_pct": -.0654, "historical_volatility": float("nan"),
    }, pd.Timestamp("2026-11-05T19:58:00Z"), 900)
    assert result.iloc[0].underlying_price == 54.46
    assert result.iloc[0].underlying_price_time == stock_time
    assert result.iloc[0].underlying_price_time != result.iloc[0].option_quote_time


def test_canonical_independent_price_is_not_relabelled_as_native():
    """Canonical field presence alone is not proof of a native chain pair."""
    stock_time = pd.Timestamp("2026-11-05T19:41:36Z")
    raw = pd.DataFrame({"underlying_price": [54.46],
        "underlying_price_time": [stock_time], "updated": ["2026-11-05T19:56:46Z"],
        "strike": [90.], "bid": [38.1], "ask": [39.2]})
    result = MarketDataProvider().normalize_option_frame(raw, 54.46,
        "2027-12-17", "put", "INTC")
    assert result.iloc[0].underlying_price_time == stock_time
    assert result.iloc[0].underlying_price_time != result.iloc[0].option_quote_time


def test_missing_native_clock_is_not_fabricated_from_stock_snapshot():
    """A known row price with no time must not acquire a fabricated clock."""
    frame = MarketDataProvider().normalize_option_frame(pd.DataFrame({
        "underlyingPrice": [60.], "strike": [90.], "bid": [38.1], "ask": [39.2],
    }), 54.46, "2027-12-17", "put", "INTC")
    result = append_underlying_snapshot_fields(frame, {
        "underlying_price_time": pd.Timestamp("2026-11-05T19:41:36Z"),
        "underlying_day_change_pct": -.0654, "historical_volatility": float("nan"),
    }, pd.Timestamp("2026-11-05T19:58:00Z"), 900)
    assert pd.isna(result.iloc[0].underlying_price_time)
    assert pd.isna(result.iloc[0].underlying_price_age_seconds)


def test_strike_filter_uses_each_rows_paired_spot(monkeypatch):
    """Scope filtering must use the same row spot as pricing metrics."""
    config = make_runtime_config(max_strike_distance_pct=.1)
    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", lambda: config)
    frame = pd.DataFrame({"strike": [90., 90.], "underlying_price": [90., 60.]})
    assert filter_strikes_near_spot(frame, 54.46).index.tolist() == [0]


def test_quote_refresh_replaces_price_and_clock_together(paired_frame, monkeypatch):
    """Bounded quote repair must replace the entire paired observation."""
    frame = paired_frame.reset_index(drop=True)
    frame.loc[0, "ask"] = 0.
    raw = frame.rename(columns={"contract_symbol": "optionSymbol",
        "underlying_price": "underlyingPrice", "option_quote_time": "updated"}).copy()
    raw.loc[0, "underlyingPrice"] = 62.
    raw.loc[0, "updated"] = pd.Timestamp("2026-11-05T19:59:00Z")
    raw.loc[0, "ask"] = 39.2
    provider = MarketDataProvider()
    monkeypatch.setattr(provider, "prepare_ticker_fetch", lambda _ticker: None)
    monkeypatch.setattr(provider, "load_option_chain", lambda *_args: OptionChainFrames(
        calls=pd.DataFrame(), puts=raw))
    result, evidence = quarantine_unusable_quotes(
        frame, provider=provider, ticker="INTC", underlying_price=54.46)
    assert evidence is not None
    assert result.loc[0, "underlying_price"] == 62.
    assert result.loc[0, "underlying_price_time"] == result.loc[0, "option_quote_time"]
    assert result.loc[0, "underlying_price_time"] == pd.Timestamp("2026-11-05T19:59:00Z")
    assert result.loc[1, "underlying_price"] == 61.

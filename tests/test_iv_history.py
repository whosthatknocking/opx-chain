"""Tests for the durable implied-volatility history store."""

from datetime import date, datetime, timedelta, timezone
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from opx_chain.iv_history import (
    IVHistoryStore,
    build_iv_observation_frame,
    get_iv_history_read_store,
)
from opx_chain.price_history import PriceHistoryStore
from opx_chain.volatility_features import (
    SOURCE_READY,
    build_ticker_volatility_features,
)


def _chain(*, ticker: str = "TSLA", iv_offset: float = 0.0) -> pd.DataFrame:
    rows = []
    for expiration, dte, ivs in (
        ("2026-06-05", 14, [0.24, 0.26, 0.28]),
        ("2026-06-19", 28, [0.29, 0.31, 0.33]),
    ):
        for index, iv in enumerate(ivs):
            rows.append(
                {
                    "underlying_symbol": ticker,
                    "expiration_date": expiration,
                    "days_to_expiration": dte,
                    "option_type": "CALL" if index % 2 else "PUT",
                    "delta": 0.15 + index * 0.1,
                    "implied_volatility": iv + iv_offset,
                }
            )
    return pd.DataFrame(rows)


def _price_history(*, periods: int = 120, end: str = "2026-05-22") -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=periods)
    closes = [100.0 + index * 0.1 for index in range(periods)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": [close + 1.0 for close in closes],
            "low": [close - 1.0 for close in closes],
            "close": closes,
            "volume": [1000 + index for index in range(periods)],
        }
    )


def test_read_store_keeps_missing_path_absent_and_reads_existing_history(tmp_path):
    """Report reads preserve both missing paths and existing history."""
    directory = tmp_path / "history with spaces"
    config = SimpleNamespace(storage_dir=directory)
    with pytest.raises(FileNotFoundError):
        get_iv_history_read_store(config)
    assert not directory.exists()

    path = directory / "iv-history.db"
    writer = IVHistoryStore(path)
    observed = date(2026, 5, 22)
    observations = build_iv_observation_frame(
        _chain(),
        provider="marketdata",
        dataset_id="dataset-1",
        run_id="run-1",
        observed_at=observed,
    )
    writer.upsert_observations(observations)
    expected = writer.load_history(
        provider="marketdata", ticker="TSLA", lookback_days=30, end_date=observed,
    )
    writer.close()
    before = path.read_bytes()
    reader = get_iv_history_read_store(config)
    try:
        assert reader.stats(provider="marketdata", ticker="TSLA").latest_date == observed
        history = reader.load_history(
            provider="marketdata", ticker="TSLA", lookback_days=30, end_date=observed,
        )
        pd.testing.assert_frame_equal(history, expected)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.upsert_observations(observations)
    finally:
        reader.close()
    assert path.read_bytes() == before
    assert list(directory.iterdir()) == [path]


def test_iv_history_store_persists_ticker_and_dte_aggregates(tmp_path):
    """Stored IV observations should preserve ticker-wide and DTE-bucket history."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    observations = build_iv_observation_frame(
        _chain(),
        provider="marketdata",
        dataset_id="dataset-1",
        run_id="run-1",
        observed_at=date(2026, 5, 22),
    )

    stored_rows = store.upsert_observations(observations)
    history = store.load_history(
        provider="marketdata",
        ticker="TSLA",
        lookback_days=365,
        end_date=date(2026, 5, 22),
    )
    stats = store.stats(provider="marketdata", ticker="TSLA")

    assert stored_rows == len(observations)
    assert {"ALL", "8_14", "15_30"} <= set(history["dte_bucket"])
    assert stats.observation_dates == 1
    assert stats.latest_date == date(2026, 5, 22)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("provider", "", "provider must be a non-empty string"),
        ("provider", False, "provider must be a non-empty string"),
        ("ticker", "BAD/TICKER", "ticker must be a valid stock ticker symbol"),
        ("ticker", "A.", "ticker must be a valid stock ticker symbol"),
        ("observation_date", "not-a-date", "observation_date must be YYYY-MM-DD"),
        ("observation_count", "3", "observation_count must be a positive integer"),
        ("observation_count", True, "observation_count must be a positive integer"),
    ],
)
def test_iv_history_store_validates_observation_metadata(
    tmp_path,
    field,
    value,
    message,
) -> None:
    """Direct observation writes should fail at a stable store boundary."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    observations = build_iv_observation_frame(
        _chain(),
        provider="marketdata",
        dataset_id="dataset-1",
        run_id="run-1",
        observed_at=date(2026, 5, 22),
    )
    observations[field] = observations[field].astype(object)
    observations.loc[0, field] = value

    with pytest.raises(ValueError, match=message):
        store.upsert_observations(observations)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"provider": "", "ticker": "TSLA"}, "provider must be a non-empty string"),
        ({"provider": "marketdata", "ticker": "TSLA1"}, "valid stock ticker"),
        ({"provider": "marketdata", "ticker": "BRK..B"}, "valid stock ticker"),
    ],
)
def test_iv_history_store_query_helpers_validate_identity(
    tmp_path,
    kwargs,
    message,
) -> None:
    """Read helpers should reject malformed provider/ticker inputs consistently."""
    store = IVHistoryStore(tmp_path / "iv-history.db")

    with pytest.raises(ValueError, match=message):
        store.stats(**kwargs)

    with pytest.raises(ValueError, match=message):
        store.has_observation_date(
            **kwargs,
            observation_date=date(2026, 5, 22),
        )


def test_iv_history_store_has_observation_date_validates_date(tmp_path) -> None:
    """Observation-date probes should not leak raw attribute errors."""
    store = IVHistoryStore(tmp_path / "iv-history.db")

    with pytest.raises(ValueError, match="observation_date must be a date"):
        store.has_observation_date(
            provider="marketdata",
            ticker="TSLA",
            observation_date="2026-05-22",
        )


def test_iv_history_store_rejects_datetime_date_boundaries(tmp_path) -> None:
    """Date-only read/write helper arguments must not truncate datetimes."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    timestamp = datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="end_date must be a date"):
        store.load_history(
            provider="marketdata",
            ticker="TSLA",
            lookback_days=365,
            end_date=timestamp,
        )
    with pytest.raises(ValueError, match="observation_date must be a date"):
        store.has_observation_date(
            provider="marketdata",
            ticker="TSLA",
            observation_date=timestamp,
        )
    with pytest.raises(ValueError, match="observation_date must be a date"):
        store.record_sync(
            dataset_id="dataset-1",
            provider="marketdata",
            run_id=None,
            status="INGESTED",
            observation_date=timestamp,
            source_rows=3,
            stored_rows=8,
        )


@pytest.mark.parametrize("bad_lookback", [0, -1, True, "365", 1.5])
def test_iv_history_store_load_history_validates_lookback(
    tmp_path,
    bad_lookback,
) -> None:
    """Stored IV-history windows should require explicit positive integers."""
    store = IVHistoryStore(tmp_path / "iv-history.db")

    with pytest.raises(ValueError, match="lookback_days"):
        store.load_history(
            provider="marketdata",
            ticker="TSLA",
            lookback_days=bad_lookback,
            end_date=date(2026, 5, 22),
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"dataset_id": "", "provider": "marketdata", "status": "INGESTED"}, "dataset_id"),
        ({"dataset_id": "dataset-1", "provider": "", "status": "INGESTED"}, "provider"),
        ({"dataset_id": "dataset-1", "provider": "marketdata", "status": "ok"}, "status"),
    ],
)
def test_iv_history_store_record_sync_validates_required_metadata(
    tmp_path,
    kwargs,
    message,
) -> None:
    """Sync writes should not persist malformed ingestion metadata."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    params = {
        "run_id": None,
        "observation_date": date(2026, 5, 22),
        "source_rows": 3,
        "stored_rows": 8,
    }
    params.update(kwargs)

    with pytest.raises(ValueError, match=message):
        store.record_sync(**params)  # pylint: disable=missing-kwoa


def test_ticker_volatility_features_use_durable_iv_history(tmp_path):
    """The public feature builder should load historical IV from the durable store."""
    iv_store = IVHistoryStore(tmp_path / "iv-history.db")
    price_store = PriceHistoryStore(tmp_path / "price-history.db")
    price_store.upsert_bars(
        provider="marketdata",
        ticker="TSLA",
        history=_price_history(),
    )
    start_day = date(2026, 4, 20)
    for index in range(25):
        observed_at = start_day + timedelta(days=index)
        observations = build_iv_observation_frame(
            _chain(iv_offset=index * 0.002),
            provider="marketdata",
            dataset_id=f"dataset-{index}",
            run_id=f"run-{index}",
            observed_at=observed_at,
        )
        iv_store.upsert_observations(observations)

    snapshot = build_ticker_volatility_features(
        ticker="TSLA",
        chain=_chain(iv_offset=0.06),
        price_history_store=price_store,
        iv_history_store=iv_store,
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert snapshot["source_status"] == SOURCE_READY
    assert snapshot["iv"]["source_status"] == SOURCE_READY
    assert snapshot["iv"]["iv_source_method"] == "durable_iv_history"
    assert snapshot["iv"]["iv_history_observation_count"] == 25
    assert snapshot["iv"]["iv_percentile_1y"] is not None

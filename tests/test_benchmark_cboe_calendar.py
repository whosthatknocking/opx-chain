"""A wider official Cboe publication calendar must not erase XNYS evidence."""
# pylint: disable=missing-function-docstring

from dataclasses import replace
from datetime import date, datetime, timezone
from time import monotonic

import pytest

from opx_chain import benchmark_series as b
from opx_chain.providers.benchmark_history import parse_cboe_vix

NOW = datetime(2025, 5, 28, 21, tzinfo=timezone.utc)
CSV = (b"DATE,OPEN,HIGH,LOW,CLOSE\n05/23/2025,20,21,19,20\n"
       b"05/26/2025,20.75,20.83,20.30,20.57\n05/27/2025,19,20,18,19\n")


def request(tmp_path):
    sessions = tuple(b.SessionClose(date(2025, 5, day), datetime(
        2025, 5, day, 20, tzinfo=timezone.utc)) for day in (23, 27))
    return b.DailySeriesRequest(series_id="VIX_SPOT", provider_id="cboe_daily_vix",
        price_basis="INDEX_CLOSE", unit="VOLATILITY_POINTS",
        calendar_id="XNYS", calendar_revision="fixture",
        sessions=sessions, latest_allowed_session=date(2025, 5, 27), acquisition_started_at=NOW,
        deadline_monotonic=monotonic()+5, cache_dir=tmp_path)


def test_official_holiday_observation_is_excluded_not_assigned_a_close(tmp_path):
    value = request(tmp_path)
    rows = parse_cboe_vix(CSV)
    snapshot = b._normalize(value, rows, NOW)  # pylint: disable=protected-access
    assert [row.session_date for row in snapshot.observations] == [
        date(2025, 5, 23), date(2025, 5, 27)]
    assert [row.close for row in snapshot.observations] == [20, 19]
    without_holiday = b._normalize(value, [rows[0], rows[2]], NOW)  # pylint: disable=protected-access
    assert snapshot == without_holiday


def test_public_approved_acquisition_publishes_only_caller_sessions(tmp_path, monkeypatch):
    value = request(tmp_path)
    monkeypatch.setattr(b, "utc_now", lambda: NOW)
    monkeypatch.setattr(b, "_fetch", lambda *_: parse_cboe_vix(CSV))
    value = replace(value, fetch_mode="reuse_or_fetch", network_scope=b.ApprovedNetworkScope(
        "cboe_daily_vix", "VIX_SPOT", b.CBOE_VIX_URL, "offline-fixture-consent"))
    result = b.acquire_daily_series(value)
    assert (result.refresh_status, result.error_code) == ("SUCCEEDED", None)
    assert len(result.snapshot.observations) == 2
    assert b.acquire_daily_series(value).refresh_status == "REUSED"


def test_spy_still_rejects_out_of_calendar_data(tmp_path):
    value = replace(request(tmp_path), series_id="SPY", provider_id="marketdata",
                    price_basis="SPLIT_ADJUSTED_PRICE", unit="USD")
    with pytest.raises(b.BenchmarkSeriesError, match="INVALID_SOURCE"):
        b._normalize(value, parse_cboe_vix(CSV), NOW)  # pylint: disable=protected-access


@pytest.mark.parametrize("row", [
    {"session_date": "2025-05-29", "close": 20},
    {"session_date": "bad-date", "close": 20},
    {"session_date": "2025-05-26", "close": float("nan")},
    {"session_date": "2025-05-26", "close": 0},
    {"session_date": "2025-05-26", "close": 99},
])
def test_calendar_filter_does_not_weaken_integrity_checks(tmp_path, row):
    with pytest.raises(b.BenchmarkSeriesError, match="INVALID_SOURCE"):
        b._normalize(request(tmp_path), parse_cboe_vix(CSV)+[row], NOW)  # pylint: disable=protected-access

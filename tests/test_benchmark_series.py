"""Offline producer contract, parser, failure, process and concurrency coverage."""
# pylint: disable=missing-function-docstring,missing-class-docstring
# pylint: disable=protected-access,redefined-outer-name,import-outside-toplevel
# pylint: disable=cyclic-import

from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import multiprocessing
import time

import httpx
import pytest

import opx_chain.benchmark_series as b
from opx_chain import config
from opx_chain.providers import benchmark_history as sources
from opx_chain.storage.atomic import atomic_write_bytes

NOW = datetime(2026, 11, 2, 22, tzinfo=timezone.utc)


@pytest.fixture
def request_series(tmp_path, monkeypatch):
    """A real-session synthetic schedule; no calendar/network inference in tests."""
    monkeypatch.setattr(b, "utc_now", lambda: NOW)
    sessions = tuple(b.SessionClose(date(2026, 10, day), datetime(
        2026, 10, day, 20, tzinfo=timezone.utc)) for day in (27, 28, 29, 30))
    return b.DailySeriesRequest(
        series_id="VIX_SPOT", provider_id="cboe_daily_vix", price_basis="INDEX_CLOSE",
        unit="VOLATILITY_POINTS", calendar_id="XNYS", calendar_revision="fixture-v1",
        sessions=sessions, latest_allowed_session=sessions[-1].session_date,
        acquisition_started_at=NOW, deadline_monotonic=time.monotonic() + 5,
        cache_dir=tmp_path / "benchmark-series")


def approved(request):
    """Synthetic permission only: every provider operation is stubbed offline."""
    return replace(request, fetch_mode="reuse_or_fetch", network_scope=b.ApprovedNetworkScope(
        request.provider_id, request.series_id, b._source(request), "offline-fixture-approval"))


def rows_for(request, *, omit_last=False):
    sessions = request.sessions[:-1] if omit_last else request.sessions
    return [{"session_date": s.session_date.isoformat(), "close": 20.0 + i}
            for i, s in enumerate(sessions)]


def seed(request, *, rows=None, receipt=None, versions=None):
    """Publish only to the test namespace, using the exact producer bytes."""
    receipt = receipt or NOW - timedelta(seconds=1)
    build = replace(request, acquisition_started_at=receipt - timedelta(seconds=1))
    snapshot = b._normalize(build, rows or rows_for(request), receipt)
    root = request.cache_dir / b._hash(b._identity(request))
    atomic_write_bytes(root / (snapshot.source_snapshot_id + ".json"), b._encode(asdict(snapshot)))
    atomic_write_bytes(root / "state.json", b._encode({
        "versions": versions or [snapshot.source_snapshot_id], "last_attempt_at": None,
        "lease_until": 0.0}))
    return snapshot, root


def test_default_denied_and_reuse_only_are_read_only(request_series, monkeypatch):
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    result = b.acquire_daily_series(request_series)
    assert (result.refresh_status, result.error_code) == ("NOT_REQUESTED", "MISSING_SOURCE")
    denied = b.acquire_daily_series(replace(request_series, fetch_mode="reuse_or_fetch"))
    assert (denied.refresh_status, denied.error_code) == ("NOT_PERMITTED", "SOURCE_NOT_APPROVED")
    assert not request_series.cache_dir.exists()


@pytest.mark.parametrize("field,value", [
    ("provider_id", "cboe"), ("series_id", "VXX"), ("unit", "USD"),
    ("price_basis", "FUTURES_CLOSE"), ("calendar_revision", ""),
    ("sessions", ()), ("max_sessions", True), ("max_sessions", 301),
    ("deadline_monotonic", float("nan")), ("fetch_mode", "refresh"),
    ("acquisition_started_at", NOW.replace(tzinfo=None)),
    ("latest_allowed_session", date(2026, 11, 2)),
    ("target_session", date(2026, 11, 3)),
])
def test_invalid_requests_never_fetch(request_series, monkeypatch, field, value):
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    assert b.acquire_daily_series(replace(request_series, **{field: value})).snapshot is None
    assert not request_series.cache_dir.exists()


def test_yfinance_basis_is_explicitly_unsupported(request_series):
    request = replace(request_series, series_id="SPY", provider_id="yfinance",
                      price_basis="SPLIT_ADJUSTED_PRICE", unit="USD")
    assert b.acquire_daily_series(request).error_code == "UNSUPPORTED_BASIS"


def test_permission_is_exact_not_a_boolean(request_series, monkeypatch):
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    for scope in (True, b.ApprovedNetworkScope("cboe", "VIX_SPOT", b.CBOE_VIX_URL, "yes"),
                  b.ApprovedNetworkScope("cboe_daily_vix", "VIX_SPOT",
                                         "https://invalid.test", "yes"),
                  b.ApprovedNetworkScope("cboe_daily_vix", "VIX_SPOT", b.CBOE_VIX_URL, "")):
        result = b.acquire_daily_series(replace(
            request_series, fetch_mode="reuse_or_fetch", network_scope=scope))
        assert result.error_code == "SOURCE_NOT_APPROVED"


def test_reuse_and_publication_grace_never_wait_or_write(request_series, monkeypatch):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    state = (root / "state.json").read_bytes()
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    monkeypatch.setattr(b, "acquire_nonblocking_file_lock", lambda *_: pytest.fail("lock wait"))
    request = replace(approved(request_series),
                      target_session=request_series.sessions[-2].session_date)
    result = b.acquire_daily_series(request)
    assert result.refresh_status == "REUSED"
    assert result.snapshot == old
    assert (root / "state.json").read_bytes() == state


def test_fetch_then_offline_reuse_and_exact_hash(request_series, monkeypatch):
    calls = []
    monkeypatch.setattr(b, "_fetch", lambda *_: calls.append(1) or rows_for(request_series))
    result = b.acquire_daily_series(approved(request_series))
    assert result.refresh_status == "SUCCEEDED"
    snapshot = result.snapshot
    assert snapshot.source_url == b.CBOE_VIX_URL
    content = {"series_id": "VIX_SPOT", "price_basis": "INDEX_CLOSE", "unit": "VOLATILITY_POINTS",
               "observations": [{"session_date": o.session_date.isoformat(),
                                 "close": round(o.close, 8)} for o in snapshot.observations]}
    assert snapshot.content_sha256 == hashlib.sha256(json.dumps(
        content, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    assert all(o.observed_at <= NOW == o.available_at for o in snapshot.observations)
    reused = b.acquire_daily_series(request_series)
    assert reused.refresh_status == "REUSED" and reused.snapshot == snapshot
    assert len(calls) == 1
    assert json.dumps(snapshot.to_dict(), allow_nan=False)


@pytest.mark.parametrize("close", [float("nan"), float("inf"), -1, 0, True, None, "NaN"])
def test_strict_closes(request_series, close):
    rows = rows_for(request_series)
    rows[0]["close"] = close
    with pytest.raises(b.BenchmarkSeriesError, match="INVALID_SOURCE"):
        b._normalize(request_series, rows, NOW)


def test_duplicates_gaps_future_and_no_intraday_admission(request_series):
    rows = rows_for(request_series)
    snapshot = b._normalize(request_series, rows[::-1] + rows, NOW)
    assert len(snapshot.observations) == 4
    conflicting = rows + [{**rows[0], "close": 99}]
    with pytest.raises(b.BenchmarkSeriesError):
        b._normalize(request_series, conflicting, NOW)
    gap = b._normalize(request_series, [rows[0], rows[-1]], NOW)
    assert len(gap.observations) == 2  # no forward fill
    with pytest.raises(b.BenchmarkSeriesError):
        b._normalize(request_series, [{"session_date": "2026-11-03", "close": 20}], NOW)
    # A close during acquisition is not eligible, even if receipt is later.
    early = replace(request_series, acquisition_started_at=datetime(
        2026, 10, 30, 19, tzinfo=timezone.utc), latest_allowed_session=date(2026, 10, 29))
    assert b._normalize(early, rows, NOW).observations[-1].session_date == date(2026, 10, 29)


def test_failed_refresh_retains_fallback_and_durable_throttle(request_series, monkeypatch):
    old, _ = seed(request_series, rows=rows_for(request_series, omit_last=True))
    def fail(*_):
        raise b.BenchmarkSeriesError("SOURCE_ERROR")
    monkeypatch.setattr(b, "_fetch", fail)
    result = b.acquire_daily_series(approved(request_series))
    assert (result.refresh_status, result.error_code, result.snapshot) == (
        "FAILED", "SOURCE_ERROR", old)
    assert result.retained_from.source_snapshot_id == old.source_snapshot_id
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("duplicate attempt"))
    result = b.acquire_daily_series(approved(request_series))
    assert result.refresh_status == "THROTTLED" and result.snapshot == old
    assert result.last_attempt_at == NOW


def test_corrupt_newest_version_uses_independently_valid_older(request_series):
    old, root = seed(request_series)
    bad = "a" * 64
    atomic_write_bytes(root / (bad + ".json"), b"{broken")
    atomic_write_bytes(root / "state.json", b._encode({"versions": [bad, old.source_snapshot_id],
                       "last_attempt_at": None, "lease_until": 0}))
    assert b.acquire_daily_series(request_series).snapshot == old


def test_sliced_reuse_keeps_original_provenance(request_series):
    old, _ = seed(request_series)
    result = b.acquire_daily_series(replace(request_series, max_sessions=2))
    assert len(result.snapshot.observations) == 2
    assert result.snapshot.source_snapshot_id != old.source_snapshot_id
    assert result.retained_from.source_snapshot_id == old.source_snapshot_id
    assert result.retained_from.content_sha256 == old.content_sha256


def test_supplied_early_close_is_used_without_replacing_with_regular_close(request_series):
    short = b.SessionClose(date(2026, 11, 27), datetime(
        2026, 11, 27, 18, tzinfo=timezone.utc))
    request = replace(request_series, sessions=(short,), latest_allowed_session=short.session_date,
                      acquisition_started_at=datetime(2026, 11, 27, 18, 1, tzinfo=timezone.utc))
    snapshot = b._normalize(request, [{"session_date": "2026-11-27", "close": 20}],
                            request.acquisition_started_at)
    assert snapshot.observations[0].observed_at == short.observed_at


def test_lease_wait_is_bounded_and_does_not_fetch(request_series, monkeypatch):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    atomic_write_bytes(root / "state.json", b._encode({"versions": [old.source_snapshot_id],
        "last_attempt_at": NOW.isoformat(), "lease_until": time.monotonic() + 1}))
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    start = time.monotonic()
    result = b.acquire_daily_series(approved(replace(
        request_series, deadline_monotonic=start + 0.1)))
    assert result.error_code == "DEADLINE_EXCEEDED" and result.snapshot == old
    assert time.monotonic() - start < 0.5


def test_expired_deadline_does_not_launch_worker(request_series, monkeypatch):
    monkeypatch.setattr(b, "_worker", lambda *_: pytest.fail("worker launched"))
    with pytest.raises(b.BenchmarkSeriesError, match="DEADLINE_EXCEEDED"):
        b._fetch(request_series, time.monotonic() - 1)


def test_normalized_size_failure_is_typed(request_series, monkeypatch):
    monkeypatch.setattr(b, "MAX_NORMALIZED_BYTES", 100)
    monkeypatch.setattr(b, "_fetch", lambda *_: rows_for(request_series))
    result = b.acquire_daily_series(approved(request_series))
    assert result.snapshot is None and result.error_code == "SOURCE_TOO_LARGE"


def test_revision_lease_token_prevents_out_of_order_publication(request_series, monkeypatch):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    def lose_lease(request, _deadline):
        path = root / "state.json"
        state = json.loads(path.read_bytes())
        state["lease_token"] = "later-owner"
        atomic_write_bytes(path, b._encode(state))
        return rows_for(request)
    monkeypatch.setattr(b, "_fetch", lose_lease)
    result = b.acquire_daily_series(approved(request_series))
    assert result.error_code == "DEADLINE_EXCEEDED" and result.snapshot == old
    assert json.loads((root / "state.json").read_bytes())["versions"] == [old.source_snapshot_id]


def test_corrupt_control_state_fails_closed_without_reset(request_series, monkeypatch):
    _, root = seed(request_series)
    atomic_write_bytes(root / "state.json", b"broken")
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    assert b.acquire_daily_series(approved(request_series)).error_code == "CACHE_ERROR"
    assert (root / "state.json").read_bytes() == b"broken"


def test_old_date_replay_never_backdates_later_receipt(request_series, monkeypatch):
    seed(request_series)
    historical = replace(request_series, acquisition_started_at=NOW - timedelta(minutes=1))
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    result = b.acquire_daily_series(approved(historical))
    assert result.snapshot is None and result.error_code == "HISTORICAL_REPLAY_ONLY"


def test_corrections_preserve_first_availability_and_both_versions(request_series, monkeypatch):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    rows = rows_for(request_series)
    rows[1]["close"] = 88
    monkeypatch.setattr(b, "_fetch", lambda *_: rows)
    new = b.acquire_daily_series(approved(request_series)).snapshot
    assert new.observations[0].available_at == old.observations[0].available_at
    assert new.observations[1].available_at == NOW
    assert (root / (old.source_snapshot_id + ".json")).exists()
    assert old.source_snapshot_id != new.source_snapshot_id


def test_cache_write_failure_preserves_fallback(request_series, monkeypatch):
    old, _ = seed(request_series, rows=rows_for(request_series, omit_last=True))
    monkeypatch.setattr(b, "_fetch", lambda *_: rows_for(request_series))
    real_write = b.atomic_write_bytes
    def fail_snapshot(path, raw):
        if path.name != "state.json":
            raise OSError("test disk failure")
        return real_write(path, raw)
    monkeypatch.setattr(b, "atomic_write_bytes", fail_snapshot)
    result = b.acquire_daily_series(approved(request_series))
    assert result.error_code == "CACHE_ERROR" and result.snapshot == old


def test_empty_and_older_responses_do_not_replace_cache(request_series, monkeypatch):
    old, _ = seed(request_series, rows=rows_for(request_series, omit_last=True))
    monkeypatch.setattr(b, "_fetch", lambda *_: rows_for(request_series)[:1])
    result = b.acquire_daily_series(approved(request_series))
    assert result.error_code == "INVALID_SOURCE" and result.snapshot == old


def test_cboe_schema_and_numeric_parsing():
    raw = b"DATE,OPEN,HIGH,LOW,CLOSE\r\n10/30/2026,20,22,19,21.5\r\n"
    assert sources.parse_cboe_vix(raw) == [{"session_date": "2026-10-30", "close": 21.5}]
    for broken in (b"<html>denied</html>", raw.replace(b"DATE", b"SYMBOL"),
                   raw.replace(b"21.5", b"NaN"), raw.replace(b"21.5", b"21.5,extra")):
        with pytest.raises(b.BenchmarkSeriesError):
            sources.parse_cboe_vix(broken)


def test_marketdata_midnight_eastern_dst_and_identity():
    def payload(stamp, **extra):
        return json.dumps({"s": "ok", "t": [stamp], "c": [500.1], **extra}).encode()
    assert sources.parse_marketdata_spy(payload(1772773200))[0]["session_date"] == "2026-03-06"
    # Midnight Eastern after spring DST transition.
    assert sources.parse_marketdata_spy(payload(1773028800))[0]["session_date"] == "2026-03-09"
    for raw in (payload(1772773201), payload(1772773200, symbol="VXX"), b'{"s":"no_data"}'):
        with pytest.raises(b.BenchmarkSeriesError):
            sources.parse_marketdata_spy(raw)


def test_marketdata_explicit_basis_bounds_and_existing_credentials(request_series, monkeypatch):
    calls = []
    monkeypatch.setattr(config, "get_provider_credentials",
                        lambda _: {"api_token": "fixture-token"})
    class Config:  # pylint: disable=too-few-public-methods
        marketdata_mode = "delayed"
    monkeypatch.setattr(config, "get_runtime_config", Config)
    def download(url, **kwargs):
        calls.append((url, kwargs))
        return b'{"s":"ok","t":[],"c":[]}'
    monkeypatch.setattr(sources, "bounded_download", download)
    request = approved(replace(request_series, series_id="SPY", provider_id="marketdata",
                               price_basis="SPLIT_ADJUSTED_PRICE", unit="USD"))
    sources.load_marketdata_spy(request)
    url, kwargs = calls[0]
    assert url == b.MARKETDATA_SPY_URL
    assert kwargs["params"]["adjustdividends"] == "false"
    assert kwargs["params"]["adjustsplits"] == "true"
    assert kwargs["params"]["extended"] == "false"
    assert kwargs["params"]["mode"] == "delayed"
    assert (date.fromisoformat(kwargs["params"]["to"]) - date.fromisoformat(
        kwargs["params"]["from"])).days == 599


def _slow_worker(_request, _output):
    time.sleep(10)


def test_actual_worker_deadline_stops_process(request_series, monkeypatch):
    monkeypatch.setattr(b, "_worker", _slow_worker)
    start = time.monotonic()
    with pytest.raises(b.BenchmarkSeriesError, match="DEADLINE_EXCEEDED"):
        b._fetch(request_series, start + 0.15)
    assert time.monotonic() - start < 1.0
    assert not multiprocessing.active_children()


def _counted_fetch(request, _deadline):
    counter = request.cache_dir / "counter.txt"
    count = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(count + 1))
    time.sleep(0.15)
    return rows_for(request)


def _acquire_process(request, queue):
    b.utc_now = lambda: NOW
    b._fetch = _counted_fetch
    result = b.acquire_daily_series(replace(
        request, deadline_monotonic=time.monotonic() + 5))
    digest = result.snapshot.content_sha256 if result.snapshot else None
    queue.put((result.refresh_status, result.error_code, digest))


def test_crossprocess_singleflight_rechecks_public_cache(request_series):
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    request = approved(replace(request_series, deadline_monotonic=time.monotonic() + 5))
    processes = [context.Process(target=_acquire_process, args=(request, queue)) for _ in range(2)]
    for process in processes:
        process.start()
    results = [queue.get(timeout=8) for _ in processes]
    for process in processes:
        process.join(5)
        assert process.exitcode == 0
        process.close()
    queue.close()
    queue.join_thread()
    assert sorted(r[0] for r in results) == ["REUSED", "SUCCEEDED"]
    assert results[0][2] == results[1][2]
    assert (request.cache_dir / "counter.txt").read_text() == "1"


def test_expired_owner_lease_does_not_reset_attempt_throttle(request_series, monkeypatch):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    atomic_write_bytes(root / "state.json", b._encode({"versions": [old.source_snapshot_id],
        "last_attempt_at": (NOW - timedelta(seconds=2)).isoformat(),
        "lease_until": time.monotonic() - 1, "lease_token": "dead-owner"}))
    monkeypatch.setattr(b, "_fetch", lambda *_: pytest.fail("network work"))
    assert b.acquire_daily_series(approved(request_series)).refresh_status == "THROTTLED"


def test_denied_caller_does_not_wait_for_active_lease(request_series):
    old, root = seed(request_series, rows=rows_for(request_series, omit_last=True))
    atomic_write_bytes(root / "state.json", b._encode({"versions": [old.source_snapshot_id],
        "last_attempt_at": NOW.isoformat(), "lease_until": time.monotonic() + 60}))
    start = time.monotonic()
    result = b.acquire_daily_series(replace(request_series, fetch_mode="reuse_or_fetch"))
    assert result.refresh_status == "NOT_PERMITTED" and result.snapshot == old
    assert time.monotonic() - start < 0.2


def test_stream_decompressed_size_bound_and_no_redirect_retry(monkeypatch):
    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield gzip.compress(b"x" * 100000)
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(200, headers={"content-encoding": "gzip"}, stream=Stream())
    real_client = httpx.Client
    monkeypatch.setattr(sources.httpx, "Client", lambda **kwargs: real_client(
        **kwargs, transport=httpx.MockTransport(respond)))
    monkeypatch.setattr(sources, "MAX_SOURCE_BYTES", 100)
    with pytest.raises(b.BenchmarkSeriesError, match="SOURCE_TOO_LARGE"):
        sources.bounded_download(b.CBOE_VIX_URL, deadline_monotonic=time.monotonic() + 1)
    assert len(calls) == 1


@pytest.mark.parametrize("url,status,success", [
    (b.MARKETDATA_SPY_URL, 200, True), (b.MARKETDATA_SPY_URL, 203, True),
    (b.MARKETDATA_SPY_URL, 204, False), (b.MARKETDATA_SPY_URL, 401, False),
    (b.MARKETDATA_SPY_URL, 500, False), (b.CBOE_VIX_URL, 200, True),
    (b.CBOE_VIX_URL, 203, False),
])
def test_transport_honors_provider_specific_success_status(monkeypatch, url, status, success):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(status, stream=httpx.ByteStream(b"source-bytes"))

    client = httpx.Client
    monkeypatch.setattr(sources.httpx, "Client", lambda **kwargs: client(
        **kwargs, transport=httpx.MockTransport(respond)))
    if success:
        assert sources.bounded_download(
            url, deadline_monotonic=time.monotonic() + 1) == b"source-bytes"
    else:
        with pytest.raises(b.BenchmarkSeriesError, match="SOURCE_ERROR"):
            sources.bounded_download(url, deadline_monotonic=time.monotonic() + 1)
    assert len(calls) == 1

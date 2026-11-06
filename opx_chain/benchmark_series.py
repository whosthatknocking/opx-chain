"""Public, opt-in daily benchmark acquisition; no portfolio or strategy policy.

The caller supplies a versioned exchange schedule and explicit source-use approval.
Provider work runs in a disposable process, never a non-cancellable thread.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import tempfile
import time
from typing import Literal

from opx_chain.locks import acquire_nonblocking_file_lock, release_file_lock
from opx_chain.paths import get_cache_dir
from opx_chain.storage.atomic import atomic_write_bytes
from opx_chain.timestamps import utc_now

BENCHMARK_SERIES_SCHEMA_VERSION = 1
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_NORMALIZED_BYTES = 256 * 1024
MIN_ATTEMPT_INTERVAL_SECONDS = 900
MAX_REQUEST_SECONDS = 5.0
MAX_CACHE_VERSIONS = 16
ADAPTER_REVISION = "daily-benchmark-v1"
CBOE_VIX_URL = "https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
MARKETDATA_SPY_URL = "https://api.marketdata.app/v1/stocks/candles/D/SPY/"
DAILY_SERIES_FETCH_MODES = ("reuse_only", "reuse_or_fetch", "fetch_latest")

RefreshStatus = Literal[
    "NOT_REQUESTED", "REUSED", "SUCCEEDED", "FAILED", "NOT_PERMITTED", "THROTTLED"
]
ErrorCode = Literal[
    "INVALID_REQUEST", "UNSUPPORTED_PROVIDER", "UNSUPPORTED_BASIS", "INVALID_SOURCE",
    "SOURCE_NOT_APPROVED", "DEADLINE_EXCEEDED", "SOURCE_TOO_LARGE", "SOURCE_ERROR",
    "CACHE_ERROR", "THROTTLED", "MISSING_SOURCE", "HISTORICAL_REPLAY_ONLY",
]


@dataclass(frozen=True)
class SessionClose:
    """One real regular-session close from the caller's versioned calendar."""

    session_date: date
    observed_at: datetime


@dataclass(frozen=True)
class ApprovedNetworkScope:
    """Explicit recorded approval, including caching/backup/display/run retention.

    Merely enabling a consumer is not source permission. URLs are exact allowlist
    metadata, not arbitrary destinations. No credentials belong in this record.
    """

    provider_id: str
    series_id: str
    source_url: str
    approval_reference: str


@dataclass(frozen=True)
class DailySeriesRequest:  # pylint: disable=too-many-instance-attributes
    """A bounded request; deadline_monotonic is an absolute time.monotonic value."""

    series_id: str
    provider_id: str
    price_basis: str
    unit: str
    calendar_id: str
    calendar_revision: str
    sessions: tuple[SessionClose, ...]
    latest_allowed_session: date
    acquisition_started_at: datetime
    deadline_monotonic: float
    max_sessions: int = 300
    fetch_mode: Literal["reuse_only", "reuse_or_fetch", "fetch_latest"] = "reuse_only"
    network_scope: ApprovedNetworkScope | None = None
    cache_dir: Path | None = None
    target_session: date | None = None


@dataclass(frozen=True)
class DailyObservation:
    """Close and first verified local availability, never backdated to session close."""

    session_date: date
    observed_at: datetime
    available_at: datetime
    close: float


@dataclass(frozen=True)
class DailySeriesSnapshot:  # pylint: disable=too-many-instance-attributes
    """Immutable normalized source evidence (at most 300 observations)."""

    schema_version: int
    series_id: str
    provider_id: str
    source_url: str
    price_basis: str
    unit: str
    exchange_calendar: str
    calendar_revision: str
    provider_revision: str
    fetched_at: datetime
    observations: tuple[DailyObservation, ...]
    source_revision: str
    content_sha256: str
    source_snapshot_id: str

    def to_dict(self) -> dict:
        """Return strict JSON-compatible evidence without losing timestamp precision."""
        return json.loads(_encode(asdict(self)))


@dataclass(frozen=True)
class RetainedFrom:
    """Original snapshot provenance, separate from the latest attempt."""

    source_snapshot_id: str
    content_sha256: str
    fetched_at: datetime


@dataclass(frozen=True)
class DailySeriesResult:
    """Terminal outcome; a failed refresh can still carry valid retained evidence."""

    snapshot: DailySeriesSnapshot | None
    refresh_status: RefreshStatus
    error_code: ErrorCode | None = None
    last_attempt_at: datetime | None = None
    retained_from: RetainedFrom | None = None


class BenchmarkSeriesError(ValueError):
    """Sanitized source/validation failure; never contains a provider response."""

    def __init__(self, code: ErrorCode):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class DailySeriesCapability:
    """Pure source support, not permission, freshness or evidence availability."""

    series_id: str
    provider_id: str
    price_basis: str | None
    unit: str | None
    source_url: str | None
    error_code: ErrorCode | None
    fetch_modes: tuple[str, ...]


def daily_series_capability(series_id: str, provider_id: str) -> DailySeriesCapability:
    """Describe a source without inspecting configuration, cache or network."""
    basis: str | None = None
    unit: str | None = None
    url: str | None = None
    error: ErrorCode | None = "UNSUPPORTED_PROVIDER"
    if series_id == "SPY":
        basis, unit = "SPLIT_ADJUSTED_PRICE", "USD"
        if provider_id == "marketdata":
            url, error = MARKETDATA_SPY_URL, None
        elif provider_id == "yfinance":
            error = "UNSUPPORTED_BASIS"
    elif series_id == "VIX_SPOT":
        basis, unit = "INDEX_CLOSE", "VOLATILITY_POINTS"
        if provider_id == "cboe_daily_vix":
            url, error = CBOE_VIX_URL, None
    modes = DAILY_SERIES_FETCH_MODES if error is None else ()
    return DailySeriesCapability(series_id, provider_id, basis, unit, url, error, modes)


def _encode(value) -> bytes:
    def scalar(item):
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        raise TypeError("unsupported benchmark scalar")
    return json.dumps(value, default=scalar, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _aware(value) -> bool:
    return (isinstance(value, datetime) and value.tzinfo is not None
            and value.utcoffset() is not None)


def _source(request: DailySeriesRequest) -> str:
    capability = daily_series_capability(request.series_id, request.provider_id)
    if capability.error_code is not None or capability.source_url is None:
        raise BenchmarkSeriesError(capability.error_code or "UNSUPPORTED_PROVIDER")
    if (request.price_basis, request.unit) != (capability.price_basis, capability.unit):
        raise BenchmarkSeriesError("UNSUPPORTED_BASIS")
    return capability.source_url


def _network_permitted(request: DailySeriesRequest, source_url: str) -> bool:
    scope = request.network_scope
    return (request.fetch_mode in ("reuse_or_fetch", "fetch_latest")
            and isinstance(scope, ApprovedNetworkScope)
            and (scope.provider_id, scope.series_id, scope.source_url) == (
                request.provider_id, request.series_id, source_url)
            and isinstance(scope.approval_reference, str)
            and bool(scope.approval_reference.strip()))


# Exact types deliberately exclude bool-as-int and datetime-as-date coercion.
# pylint: disable=unidiomatic-typecheck,too-many-boolean-expressions
def _validate_request(request: DailySeriesRequest) -> None:
    if (not isinstance(request, DailySeriesRequest)
            or type(request.max_sessions) is not int or not 1 <= request.max_sessions <= 300
            or request.fetch_mode not in DAILY_SERIES_FETCH_MODES
            or not _aware(request.acquisition_started_at)
            or type(request.latest_allowed_session) is not date
            or type(request.deadline_monotonic) not in (int, float)
            or not math.isfinite(request.deadline_monotonic)
            or not isinstance(request.calendar_id, str) or not request.calendar_id
            or not isinstance(request.calendar_revision, str) or not request.calendar_revision
            or not isinstance(request.sessions, tuple) or not request.sessions
            or len(request.sessions) > 700):
        raise BenchmarkSeriesError("INVALID_REQUEST")
    previous = None
    for session in request.sessions:
        if (not isinstance(session, SessionClose) or type(session.session_date) is not date
                or not _aware(session.observed_at)
                or session.observed_at.astimezone(timezone.utc).date() != session.session_date
                or (previous is not None and (session.session_date <= previous.session_date
                                             or session.observed_at <= previous.observed_at))):
            raise BenchmarkSeriesError("INVALID_REQUEST")
        previous = session
    schedule = {s.session_date: s.observed_at for s in request.sessions}
    if (request.latest_allowed_session not in schedule
            or schedule[request.latest_allowed_session] > request.acquisition_started_at):
        raise BenchmarkSeriesError("INVALID_REQUEST")
    if request.target_session is not None and (
            type(request.target_session) is not date
            or request.target_session not in schedule
            or request.target_session > request.latest_allowed_session):
        raise BenchmarkSeriesError("INVALID_REQUEST")


def _identity(request: DailySeriesRequest) -> dict:
    return {"schema_version": BENCHMARK_SERIES_SCHEMA_VERSION,
            "adapter_revision": ADAPTER_REVISION, "series_id": request.series_id,
            "provider_id": request.provider_id, "price_basis": request.price_basis,
            "unit": request.unit, "calendar_id": request.calendar_id,
            "calendar_revision": request.calendar_revision}


def _content(snapshot: DailySeriesSnapshot) -> dict:
    return {"series_id": snapshot.series_id, "price_basis": snapshot.price_basis,
            "unit": snapshot.unit,
            "observations": [{"session_date": o.session_date, "close": round(o.close, 8)}
                             for o in snapshot.observations]}


def _snapshot_id(snapshot: DailySeriesSnapshot) -> str:
    return _hash({k: v for k, v in asdict(snapshot).items() if k != "source_snapshot_id"})


# pylint: disable-next=too-many-locals,too-many-branches
def _normalize(request, rows, fetched_at, previous=None):
    schedule = {s.session_date: s.observed_at for s in request.sessions}
    first_day = min(schedule)
    accepted = {}
    seen = {}
    if not _aware(fetched_at) or fetched_at < request.acquisition_started_at:
        raise BenchmarkSeriesError("INVALID_SOURCE")
    old = {o.session_date: o for o in previous.observations} if previous else {}
    for row in rows:
        try:
            day = date.fromisoformat(row["session_date"])
            raw_close = row["close"]
            if isinstance(raw_close, bool) or not isinstance(raw_close, (int, float, str)):
                raise ValueError
            close = float(raw_close)
            if not math.isfinite(close) or close <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise BenchmarkSeriesError("INVALID_SOURCE") from exc
        if day in seen and seen[day] != close:
            raise BenchmarkSeriesError("INVALID_SOURCE")
        seen[day] = close
        if day > request.acquisition_started_at.astimezone(timezone.utc).date():
            raise BenchmarkSeriesError("INVALID_SOURCE")
        if day < first_day:
            continue
        if day not in schedule:
            raise BenchmarkSeriesError("INVALID_SOURCE")
        # A valid daily file may contain a close newer than this request's cutoff.
        if day > request.latest_allowed_session or schedule[day] > request.acquisition_started_at:
            continue
        available = fetched_at
        prior = old.get(day)
        if prior and prior.close == close and prior.observed_at == schedule[day]:
            available = prior.available_at
        accepted[day] = DailyObservation(day, schedule[day], available, close)
    observations = tuple(accepted[d] for d in sorted(accepted)[-request.max_sessions:])
    if not observations:
        raise BenchmarkSeriesError("MISSING_SOURCE")
    snapshot = DailySeriesSnapshot(
        BENCHMARK_SERIES_SCHEMA_VERSION, request.series_id, request.provider_id,
        _source(request), request.price_basis, request.unit, request.calendar_id,
        request.calendar_revision, ADAPTER_REVISION, fetched_at, observations, "", "", "")
    digest = _hash(_content(snapshot))
    snapshot = replace(snapshot, source_revision=digest, content_sha256=digest)
    snapshot = replace(snapshot, source_snapshot_id=_snapshot_id(snapshot))
    if len(_encode(asdict(snapshot))) > MAX_NORMALIZED_BYTES:
        raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
    return snapshot


def _read_snapshot(path, request, cutoff):
    with path.open("rb") as handle:
        raw = handle.read(MAX_NORMALIZED_BYTES + 1)
    if len(raw) > MAX_NORMALIZED_BYTES:
        raise ValueError("oversized cache")
    data = json.loads(raw)
    data["fetched_at"] = datetime.fromisoformat(data["fetched_at"])
    data["observations"] = tuple(DailyObservation(
        date.fromisoformat(o["session_date"]), datetime.fromisoformat(o["observed_at"]),
        datetime.fromisoformat(o["available_at"]), o["close"]) for o in data["observations"])
    snapshot = DailySeriesSnapshot(**data)
    origin = RetainedFrom(snapshot.source_snapshot_id, snapshot.content_sha256, snapshot.fetched_at)
    if (not _aware(snapshot.fetched_at) or snapshot.fetched_at > cutoff
            or not 1 <= len(snapshot.observations) <= 300
            or snapshot.schema_version != BENCHMARK_SERIES_SCHEMA_VERSION
            or snapshot.provider_revision != ADAPTER_REVISION
            or (snapshot.series_id, snapshot.provider_id, snapshot.source_url,
                snapshot.price_basis, snapshot.unit, snapshot.exchange_calendar,
                snapshot.calendar_revision) != (
                    request.series_id, request.provider_id, _source(request),
                    request.price_basis, request.unit, request.calendar_id,
                    request.calendar_revision)
            or snapshot.content_sha256 != _hash(_content(snapshot))
            or snapshot.source_revision != snapshot.content_sha256
            or snapshot.source_snapshot_id != _snapshot_id(snapshot)):
        raise ValueError("incompatible cache")
    schedule = {s.session_date: s.observed_at for s in request.sessions}
    previous = None
    selected = []
    for obs in snapshot.observations:
        if (not _aware(obs.observed_at) or not _aware(obs.available_at)
                or type(obs.close) not in (int, float) or not math.isfinite(obs.close)
                or obs.close <= 0 or obs.available_at < obs.observed_at
                or obs.available_at > snapshot.fetched_at
                or (previous is not None and obs.session_date <= previous)):
            raise ValueError("invalid cache")
        previous = obs.session_date
        if obs.session_date < min(schedule):
            continue
        if obs.session_date not in schedule or obs.observed_at != schedule[obs.session_date]:
            raise ValueError("calendar mismatch")
        if (obs.session_date <= request.latest_allowed_session
                and obs.observed_at <= request.acquisition_started_at
                and obs.available_at <= cutoff):
            selected.append(obs)
    # Do not rewrite a cached evidence hash by silently slicing its observations.
    # Derive a new immutable view with explicit provenance in the result instead.
    if not selected:
        return None, origin
    if tuple(selected[-request.max_sessions:]) == snapshot.observations:
        return snapshot, origin
    view = replace(snapshot, observations=tuple(selected[-request.max_sessions:]),
                   source_revision="", content_sha256="", source_snapshot_id="")
    digest = _hash(_content(view))
    view = replace(view, source_revision=digest, content_sha256=digest)
    return replace(view, source_snapshot_id=_snapshot_id(view)), origin


def _state(path):
    if not path.exists():
        return {"versions": [], "last_attempt_at": None, "lease_until": 0.0}
    with path.open("rb") as handle:
        raw = handle.read(32769)
    if len(raw) > 32768:
        raise ValueError("invalid cache state")
    state = json.loads(raw)
    if (not isinstance(state["versions"], list) or len(state["versions"]) > MAX_CACHE_VERSIONS
            or type(state["lease_until"]) not in (int, float)
            or not math.isfinite(state["lease_until"])):
        raise ValueError("invalid cache state")
    if state["last_attempt_at"] is not None:
        if not _aware(datetime.fromisoformat(state["last_attempt_at"])):
            raise ValueError("invalid cache state")
    return state


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def _cached(root, state, request, cutoff, deadline, origins):
    candidates = []
    for digest in state["versions"]:
        if time.monotonic() >= deadline:
            break
        if not isinstance(digest, str) or len(digest) != 64 or any(
                c not in "0123456789abcdef" for c in digest):
            continue
        try:
            snapshot, origin = _read_snapshot(root / (digest + ".json"), request, cutoff)
            if snapshot:
                candidates.append(snapshot)
                origins[snapshot.source_snapshot_id] = origin
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            continue
    return max(candidates, key=lambda s: (s.observations[-1].session_date, s.fetched_at),
               default=None)


def _worker(request, output):
    # Import only inside the worker: offline/cache reads never initialize providers.
    try:
        from opx_chain.providers.benchmark_history import (  # pylint: disable=import-outside-toplevel
            load_benchmark_rows,
        )
        rows = load_benchmark_rows(request)
        validated = _normalize(request, rows, utc_now())
        payload = {"rows": [{"session_date": o.session_date, "close": o.close}
                            for o in validated.observations]}
        raw = _encode(payload)
        if len(raw) > MAX_NORMALIZED_BYTES:
            raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
    except BenchmarkSeriesError as exc:
        raw = _encode({"error_code": exc.code})
    except Exception:  # pylint: disable=broad-exception-caught
        raw = _encode({"error_code": "SOURCE_ERROR"})
    atomic_write_bytes(Path(output), raw)


def _fetch(request, deadline):
    # Spawn avoids inherited provider sessions, locks and account/portfolio state.
    context = multiprocessing.get_context("spawn")
    if time.monotonic() >= deadline:
        raise BenchmarkSeriesError("DEADLINE_EXCEEDED")
    with tempfile.TemporaryDirectory(prefix="opx-benchmark-") as directory:
        output = Path(directory) / "result.json"
        process = context.Process(target=_worker, args=(request, str(output)), daemon=True)
        try:
            process.start()
            process.join(max(0.0, deadline - time.monotonic()))
            if process.is_alive() or time.monotonic() >= deadline:
                raise BenchmarkSeriesError("DEADLINE_EXCEEDED")
            if process.exitcode != 0 or not output.exists():
                raise BenchmarkSeriesError("SOURCE_ERROR")
            with output.open("rb") as handle:
                raw = handle.read(MAX_NORMALIZED_BYTES + 1)
            if len(raw) > MAX_NORMALIZED_BYTES:
                raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
            data = json.loads(raw)
            if "error_code" in data:
                raise BenchmarkSeriesError(data["error_code"])
            return data["rows"]
        finally:
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join()
                process.close()


def _result(snapshot, status, error=None, attempt=None, origin=None):
    retained = None
    if snapshot is not None and status != "SUCCEEDED":
        retained = origin or RetainedFrom(snapshot.source_snapshot_id, snapshot.content_sha256,
                                         snapshot.fetched_at)
    return DailySeriesResult(snapshot, status, error, attempt, retained)


def acquire_daily_series(request: DailySeriesRequest) -> DailySeriesResult:
    # pylint: disable=too-many-locals,too-many-branches,too-many-return-statements,too-many-statements
    """Reuse or acquire one source, with strict opt-in, singleflight and fallback.

    No network, lock waiting, or cache writes for reuse_only/denied callers.
    max_sessions is a ceiling, not a request for additional provider pagination.
    """
    started = time.monotonic()
    fallback = None
    attempt = None
    origins = {}

    def finish(snapshot, status, error=None, attempt=None):
        origin = origins.get(snapshot.source_snapshot_id) if snapshot else None
        return _result(snapshot, status, error, attempt, origin)

    try:
        _validate_request(request)
        source_url = _source(request)
        deadline = min(request.deadline_monotonic, started + MAX_REQUEST_SECONDS)
        root = ((request.cache_dir or get_cache_dir() / "benchmark-series")
                / _hash(_identity(request)))
        state_path = root / "state.json"
        state = _state(state_path)
        # Old-date replay can only use evidence actually received by that cutoff.
        live = 0 <= (utc_now() - request.acquisition_started_at).total_seconds() <= 10
        cutoff = utc_now() if live else request.acquisition_started_at
        fallback = _cached(root, state, request, cutoff, deadline, origins)
        target = request.target_session or request.latest_allowed_session
        if (request.fetch_mode != "fetch_latest" and fallback
                and fallback.observations[-1].session_date >= target):
            return finish(fallback, "REUSED")
        if request.fetch_mode == "reuse_only":
            return finish(fallback, "NOT_REQUESTED", None if fallback else "MISSING_SOURCE")
        if not _network_permitted(request, source_url):
            return finish(fallback, "NOT_PERMITTED", "SOURCE_NOT_APPROVED")
        if not live:
            return finish(fallback, "NOT_PERMITTED", "HISTORICAL_REPLAY_ONLY")
        while time.monotonic() < deadline:
            handle = acquire_nonblocking_file_lock(root / "lease.lock")
            if handle is None:
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                continue
            try:
                state = _state(state_path)
                newer = _cached(root, state, request, utc_now(), deadline, origins)
                fallback = newer or fallback
                if (fallback and fallback.observations[-1].session_date >= target
                        and (request.fetch_mode != "fetch_latest"
                             or fallback.fetched_at >= request.acquisition_started_at)):
                    return finish(fallback, "REUSED")
                if time.monotonic() >= deadline:
                    return finish(fallback, "FAILED", "DEADLINE_EXCEEDED", attempt)
                # A durable lease prevents duplicate requests across processes;
                # expiry and attempt time survive a dead owner independently.
                if time.monotonic() < state["lease_until"] <= started + MAX_REQUEST_SECONDS:
                    pass
                else:
                    attempt = (datetime.fromisoformat(state["last_attempt_at"])
                               if state["last_attempt_at"] else None)
                    if (attempt and (utc_now() - attempt).total_seconds()
                            < MIN_ATTEMPT_INTERVAL_SECONDS):
                        return finish(fallback, "THROTTLED", "THROTTLED", attempt)
                    attempt = utc_now()
                    token = _hash({"attempt": attempt, "deadline": deadline})
                    state.update(last_attempt_at=attempt.isoformat(), lease_until=deadline,
                                 lease_token=token)
                    atomic_write_bytes(state_path, _encode(state))
                    break
            finally:
                release_file_lock(handle)
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        else:
            return finish(fallback, "FAILED", "DEADLINE_EXCEEDED", attempt)
        error = None
        snapshot = None
        try:
            rows = _fetch(replace(request, deadline_monotonic=deadline), deadline)
            snapshot = _normalize(request, rows, utc_now(), fallback)
        except BenchmarkSeriesError as exc:
            error = exc.code
        except (RuntimeError, AssertionError):
            error = "SOURCE_ERROR"
        # No scan after a deadline; only the already-read fallback can be returned.
        while time.monotonic() < deadline:
            handle = acquire_nonblocking_file_lock(root / "lease.lock")
            if handle is None:
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                continue
            try:
                state = _state(state_path)
                if state.get("lease_token") != token:
                    return finish(fallback, "FAILED", "DEADLINE_EXCEEDED", attempt)
                if snapshot is not None:
                    # Never replace a newer valid session with a shorter response.
                    if (fallback and snapshot.observations[-1].session_date
                            < fallback.observations[-1].session_date):
                        return finish(fallback, "FAILED", "INVALID_SOURCE", attempt)
                    atomic_write_bytes(root / (snapshot.source_snapshot_id + ".json"),
                                       _encode(asdict(snapshot)))
                    state["versions"] = ([snapshot.source_snapshot_id] + [
                        v for v in state["versions"] if v != snapshot.source_snapshot_id
                    ])[:MAX_CACHE_VERSIONS]
                state["lease_until"] = 0.0
                atomic_write_bytes(state_path, _encode(state))
                status = ("THROTTLED" if error == "THROTTLED" else
                          "FAILED" if error else "SUCCEEDED")
                return finish(snapshot or fallback, status, error, attempt)
            finally:
                release_file_lock(handle)
        return finish(fallback, "FAILED", error or "DEADLINE_EXCEEDED", attempt)
    except BenchmarkSeriesError as exc:
        return finish(fallback, "FAILED", exc.code, attempt)
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return finish(fallback, "FAILED", "CACHE_ERROR", attempt)


__all__ = [
    "BENCHMARK_SERIES_SCHEMA_VERSION", "ApprovedNetworkScope", "SessionClose",
    "DailySeriesRequest", "DailyObservation", "DailySeriesSnapshot", "DailySeriesResult",
    "RetainedFrom", "BenchmarkSeriesError", "acquire_daily_series",
    "DailySeriesCapability", "daily_series_capability", "DAILY_SERIES_FETCH_MODES",
    "CBOE_VIX_URL", "MARKETDATA_SPY_URL",
]

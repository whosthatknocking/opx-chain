"""Bounded provider-specific benchmark history transports and strict parsers.

These are not independently authorized entry points: the public benchmark
producer checks source permission before launching this module in its worker.
"""
# The producer imports this worker module lazily; no runtime import cycle occurs.
# pylint: disable=cyclic-import

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import io
import math
import time
from zoneinfo import ZoneInfo
import zlib

import httpx

from opx_chain.json_utils import loads_strict_json
from opx_chain.benchmark_series import (
    BenchmarkSeriesError, CBOE_VIX_URL, MARKETDATA_SPY_URL, MAX_SOURCE_BYTES,
    _network_permitted, _source,
)


def bounded_download(url, *, deadline_monotonic, headers=None, params=None):
    # pylint: disable=too-many-locals
    """One HTTPS request, no redirect/retry; cap decompressed bytes while streaming.

    The producer's killable worker enforces the total wall deadline, including
    DNS/TLS and a peer trickling bytes; HTTPX timeouts alone are not a total limit.
    """
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise BenchmarkSeriesError("DEADLINE_EXCEEDED")
    try:
        request_headers = {"Accept-Encoding": "gzip, deflate, identity", **(headers or {})}
        with httpx.Client(follow_redirects=False, timeout=remaining, trust_env=False) as client:
            with client.stream("GET", url, headers=request_headers, params=params) as response:
                if response.status_code == 429:
                    raise BenchmarkSeriesError("THROTTLED")
                if response.status_code != 200 and not (
                        url == MARKETDATA_SPY_URL and response.status_code == 203):
                    raise BenchmarkSeriesError("SOURCE_ERROR")
                content_type = response.headers.get("content-type", "").lower()
                if "html" in content_type:
                    raise BenchmarkSeriesError("INVALID_SOURCE")
                chunks = []
                size = 0
                encoding = response.headers.get("content-encoding", "identity").lower()
                if encoding not in ("identity", "gzip", "deflate"):
                    raise BenchmarkSeriesError("INVALID_SOURCE")
                decoder = (zlib.decompressobj(16 + zlib.MAX_WBITS if encoding == "gzip"
                                              else zlib.MAX_WBITS)
                           if encoding != "identity" else None)
                for raw in response.iter_raw(chunk_size=16384):
                    if time.monotonic() >= deadline_monotonic:
                        raise BenchmarkSeriesError("DEADLINE_EXCEEDED")
                    # Bound allocation during decompression, not just after the
                    # HTTP library has already expanded a compressed response.
                    chunk = (decoder.decompress(raw, MAX_SOURCE_BYTES - size + 1)
                             if decoder is not None else raw)
                    size += len(chunk)
                    if size > MAX_SOURCE_BYTES:
                        raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
                    chunks.append(chunk)
                if decoder is not None and (not decoder.eof or decoder.unused_data):
                    raise BenchmarkSeriesError("INVALID_SOURCE")
                return b"".join(chunks)
    except zlib.error as exc:
        raise BenchmarkSeriesError("INVALID_SOURCE") from exc
    except httpx.TimeoutException as exc:
        raise BenchmarkSeriesError("DEADLINE_EXCEEDED") from exc
    except httpx.HTTPError as exc:
        raise BenchmarkSeriesError("SOURCE_ERROR") from exc


def _positive(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise BenchmarkSeriesError("INVALID_SOURCE")
    try:
        number = float(value)
    except ValueError as exc:
        raise BenchmarkSeriesError("INVALID_SOURCE") from exc
    if not math.isfinite(number) or number <= 0:
        raise BenchmarkSeriesError("INVALID_SOURCE")
    return number


def parse_cboe_vix(raw: bytes) -> list[dict]:
    """Parse only the official daily spot-index OHLC schema, never futures/ETFs."""
    if len(raw) > MAX_SOURCE_BYTES:
        raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
    try:
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text), strict=True)
        if reader.fieldnames != ["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]:
            raise BenchmarkSeriesError("INVALID_SOURCE")
        rows = []
        for row in reader:
            if len(rows) >= 25000 or None in row or any(v is None for v in row.values()):
                raise BenchmarkSeriesError("INVALID_SOURCE")
            day = datetime.strptime(row["DATE"], "%m/%d/%Y").date()
            # Validate OHLC too: the CLOSE column cannot hide a shifted/malformed row.
            for key in ("OPEN", "HIGH", "LOW", "CLOSE"):
                _positive(row[key])
            rows.append({"session_date": day.isoformat(), "close": _positive(row["CLOSE"])})
        return rows
    except (UnicodeError, ValueError, csv.Error) as exc:
        raise BenchmarkSeriesError("INVALID_SOURCE") from exc


def parse_marketdata_spy(raw: bytes) -> list[dict]:
    """Map daily split-adjusted SPY candles from the exact provider endpoint."""
    if len(raw) > MAX_SOURCE_BYTES:
        raise BenchmarkSeriesError("SOURCE_TOO_LARGE")
    try:
        payload = loads_strict_json(raw)
        if not isinstance(payload, dict) or payload.get("s") != "ok":
            raise ValueError
        for key in ("symbol", "ticker"):
            if key in payload and payload[key] not in ("SPY", ["SPY"]):
                raise ValueError
        for key, expected in (("provider_id", "marketdata"), ("unit", "USD"),
                              ("price_basis", "SPLIT_ADJUSTED_PRICE")):
            if key in payload and payload[key] != expected:
                raise ValueError
        times, closes = payload["t"], payload["c"]
        if (not isinstance(times, list) or not isinstance(closes, list)
                or len(times) != len(closes) or len(times) > 601):
            raise ValueError
        rows = []
        for stamp, close in zip(times, closes):
            if type(stamp) not in (int, float) or not math.isfinite(stamp):
                raise ValueError
            instant = datetime.fromtimestamp(stamp, tz=timezone.utc)
            # Market Data daily candle labels are midnight America/New_York,
            # not an intraday quote or the time of the bar's receipt.
            local = instant.astimezone(ZoneInfo("America/New_York"))
            if (local.hour, local.minute, local.second, local.microsecond) != (0, 0, 0, 0):
                raise ValueError
            rows.append({"session_date": local.date().isoformat(), "close": _positive(close)})
        return rows
    except (UnicodeError, ValueError, TypeError, KeyError, OverflowError, OSError) as exc:
        raise BenchmarkSeriesError("INVALID_SOURCE") from exc


def load_marketdata_spy(request):
    """Use the existing provider's daily candle endpoint, split-only and regular session.

    This isolated path deliberately avoids the SDK's retries, worker pools and
    debug dumps. It never writes or reconciles the ordinary daily history store.
    """
    if (request.series_id, request.provider_id) != ("SPY", "marketdata"):
        raise BenchmarkSeriesError("UNSUPPORTED_PROVIDER")
    if not _network_permitted(request, _source(request)):
        raise BenchmarkSeriesError("SOURCE_NOT_APPROVED")
    from opx_chain.config import (  # pylint: disable=import-outside-toplevel
        get_provider_credentials, get_runtime_config,
    )
    token = get_provider_credentials("marketdata").get("api_token")
    if not token:
        raise BenchmarkSeriesError("SOURCE_NOT_APPROVED")
    end = request.latest_allowed_session
    # Inclusive endpoints span no more than 600 calendar dates.
    params = {"from": (end - timedelta(days=599)).isoformat(), "to": end.isoformat(),
              "adjustsplits": "true", "adjustdividends": "false", "extended": "false"}
    mode = get_runtime_config().marketdata_mode
    if mode is not None:
        params["mode"] = mode
    return parse_marketdata_spy(bounded_download(
        MARKETDATA_SPY_URL, deadline_monotonic=request.deadline_monotonic,
        headers={"Authorization": f"Bearer {token}"}, params=params))


def load_benchmark_rows(request):
    """Dispatch exact identities; never fall back to another provider or symbol."""
    if not _network_permitted(request, _source(request)):
        raise BenchmarkSeriesError("SOURCE_NOT_APPROVED")
    if request.series_id == "VIX_SPOT" and request.provider_id == "cboe_daily_vix":
        return parse_cboe_vix(bounded_download(
            CBOE_VIX_URL, deadline_monotonic=request.deadline_monotonic))
    if request.series_id == "SPY" and request.provider_id == "marketdata":
        from opx_chain.providers import (  # pylint: disable=import-outside-toplevel
            get_data_provider_by_name,
        )
        provider = get_data_provider_by_name(request.provider_id)
        if provider.name != request.provider_id:
            raise BenchmarkSeriesError("UNSUPPORTED_PROVIDER")
        return provider.load_benchmark_price_history(request)
    raise BenchmarkSeriesError("UNSUPPORTED_PROVIDER")

"""Public, process-local provider-credit evidence; never strategy policy.

Reads are network-free. Credential-generation handles are opaque and must not
be serialized. Explicit refresh makes one bounded request without SDK retries.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import math
import re
import secrets
import threading
from typing import Any, Mapping

import httpx

from opx_chain.config import get_provider_credentials, get_runtime_config


@dataclass(frozen=True)
class ProviderUsageState:
    """Normalized evidence for the current configured provider credential."""
    capability: str
    observation: dict[str, Any] | None = None
    entitlement: dict[str, Any] | None = None
    generation: object = field(default=None, repr=False)


@dataclass(frozen=True)
class ProviderUsageRefreshResult:
    """Refresh evidence plus an allowlisted, non-secret failure code."""
    state: ProviderUsageState
    error_code: str | None = None


_LOCK = threading.RLock()
_SECRET = secrets.token_bytes(32)
@dataclass
class _Registry:
    identity: bytes | None = None
    generation: object = field(default_factory=object)
    observation: dict[str, Any] | None = None
    entitlement: dict[str, Any] | None = None


_STATE = _Registry()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _activate(token: str) -> object:
    identity = hmac.digest(_SECRET, token.encode(), hashlib.sha256)
    with _LOCK:
        if identity != _STATE.identity:
            _STATE.identity, _STATE.generation = identity, object()
            _STATE.observation = _STATE.entitlement = None
        return _STATE.generation


def get_provider_usage(provider: str) -> ProviderUsageState:
    """Read without initializing an SDK client or making a provider request."""
    if provider not in {"marketdata", "massive", "yfinance"}:
        raise ValueError("Unknown data provider")
    if provider != "marketdata":
        return ProviderUsageState("not_supported")
    token = get_provider_credentials(provider).get("api_token", "")
    with _LOCK:
        generation = _activate(token)
        return ProviderUsageState(
            "supported" if token else "not_configured",
            deepcopy(_STATE.observation), deepcopy(_STATE.entitlement), generation,
        )


def response_observer(token: str):
    """Bind passive SDK telemetry to the credential used to create that client."""
    with _LOCK:
        state = get_provider_usage("marketdata")
        generation = (state.generation if token == get_provider_credentials(
            "marketdata").get("api_token") else None)

    def observe(response):
        try:
            capture_response(generation, response.headers)
        except Exception:  # pylint: disable=broad-exception-caught
            # Optional telemetry cannot alter ordinary provider success or failure.
            pass
    return observe


def _integer(value: object, minimum: int | None = None) -> int | None:
    if not isinstance(value, str) or not re.fullmatch(r"-?[0-9]{1,16}", value.strip()):
        return None
    number = int(value)
    if abs(number) > 2**53 - 1 or (minimum is not None and number < minimum):
        return None
    return number


def _parse_headers(headers: Mapping[str, str], source: str) -> dict[str, Any] | None:
    normalized = {str(k).lower(): v for k, v in headers.items()}
    prefix = "x-api-ratelimit-"
    if not any(prefix + k in normalized for k in ("limit", "remaining", "consumed", "reset")):
        return None
    epoch = _integer(normalized.get(prefix + "reset"), 1)
    try:
        reset = datetime.fromtimestamp(epoch, timezone.utc).isoformat() if epoch else None
    except (ValueError, OverflowError, OSError):
        reset = None
    return {
        "observed_at": _now().isoformat(), "source": source,
        "credit_limit": _integer(normalized.get(prefix + "limit"), 1),
        "credits_remaining": _integer(normalized.get(prefix + "remaining")),
        "credits_consumed_by_request": _integer(normalized.get(prefix + "consumed"), 0),
        "reset_at": reset,
    }


def _complete(value: dict[str, Any]) -> bool:
    remaining, limit = value["credits_remaining"], value["credit_limit"]
    return remaining is not None and value["reset_at"] is not None and (
        limit is None or remaining <= limit
    )


# Multiple exits preserve the explicit observation-ordering precedence.
# pylint: disable-next=too-many-return-statements
def _prefer(candidate: dict[str, Any], current: dict[str, Any] | None) -> bool:
    if current is None:
        return True
    new_reset, old_reset = candidate["reset_at"], current["reset_at"]
    if new_reset != old_reset:
        if new_reset is None:
            return False
        return old_reset is None or new_reset > old_reset
    if _complete(candidate) != _complete(current):
        return _complete(candidate)
    if _complete(candidate):
        new_balance, old_balance = candidate["credits_remaining"], current["credits_remaining"]
        if new_balance != old_balance:
            return new_balance < old_balance
    else:
        new_fields = sum(candidate[k] is not None for k in ("reset_at", "credits_remaining"))
        old_fields = sum(current[k] is not None for k in ("reset_at", "credits_remaining"))
        if new_fields != old_fields:
            return new_fields > old_fields
    return candidate["observed_at"] >= current["observed_at"]


def capture_response(generation: object, headers: Mapping[str, str], *,
                     source: str = "response_headers") -> dict[str, Any] | None:
    """Capture one response atomically; stale credential generations are ignored.

    Package-internal producer hook. It must never affect ordinary HTTP behavior.
    """
    candidate = _parse_headers(headers, source)
    with _LOCK:
        current = get_provider_usage("marketdata")
        if generation is current.generation and candidate is not None:
            if _prefer(candidate, _STATE.observation):
                _STATE.observation = candidate
    return candidate


async def _request_user(token: str, timeout_seconds: float) -> tuple[int, Mapping[str, str], Any]:
    # Async cancellation gives a total deadline, including pacing and body reads.
    async def request() -> tuple[int, Mapping[str, str], Any]:
        interval = get_runtime_config().marketdata_request_interval_seconds
        if interval > 0:
            await asyncio.sleep(interval)
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=False) as client:
            async with client.stream("GET", "https://api.marketdata.app/user/",
                                     headers={"Authorization": f"Bearer {token}"}) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 65536:
                        return response.status_code, response.headers, _INVALID_BODY
                try:
                    payload = httpx.Response(200, content=body).json()
                except ValueError:
                    payload = _INVALID_BODY
                return response.status_code, response.headers, payload
    return await asyncio.wait_for(request(), timeout=timeout_seconds)


_INVALID_BODY = object()


def _error_code(status: int, payload: Any) -> str | None:
    if status in {200, 203}:
        return None
    if status in {401, 403}:
        return "AUTHENTICATION_FAILED"
    if status == 429 and isinstance(payload, dict):
        message = str(payload.get("errmsg", payload.get("message", ""))).lower()
        if "credit limit" in message or "credits exhausted" in message:
            return "CREDIT_EXHAUSTED"
        if "concurren" in message or "simultaneous" in message:
            return "CONCURRENCY_LIMIT"
    return "PROVIDER_ERROR"


def refresh_provider_usage(
    provider: str, *, timeout_seconds: float = 5.0,
) -> ProviderUsageRefreshResult:
    """Explicit one-request refresh. Call off the server event loop."""
    if (isinstance(timeout_seconds, bool) or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0):
        raise ValueError("timeout_seconds must be finite and positive")
    with _LOCK:
        state = get_provider_usage(provider)
        if state.capability != "supported":
            return ProviderUsageRefreshResult(state)
        token = get_provider_credentials(provider).get("api_token", "")
        if _activate(token) is not state.generation:
            return ProviderUsageRefreshResult(get_provider_usage(provider))
    try:
        status, headers, payload = asyncio.run(_request_user(token, min(timeout_seconds, 5.0)))
    except (TimeoutError, asyncio.TimeoutError, httpx.TimeoutException):
        return ProviderUsageRefreshResult(get_provider_usage(provider), "TIMEOUT")
    except Exception:  # pylint: disable=broad-exception-caught
        # Optional telemetry must not expose transport/config exceptions.
        return ProviderUsageRefreshResult(get_provider_usage(provider), "PROVIDER_ERROR")
    candidate = capture_response(state.generation, headers, source="user_endpoint")
    error = _error_code(status, payload)
    if error is None:
        if (payload is _INVALID_BODY or candidate is None or not _complete(candidate)
                or candidate["reset_at"] <= candidate["observed_at"]):
            error = "INVALID_USAGE_RESPONSE"
        if isinstance(payload, dict):
            permission = payload.get("optionsDataPermissions")
            if isinstance(permission, str):
                # Allow only bounded printable text; never retain the raw user body.
                entitlement = {"options_data_permissions": "".join(
                    c for c in permission if c.isprintable())[:256],
                    "observed_at": _now().isoformat()}
                with _LOCK:
                    if get_provider_usage(provider).generation is state.generation:
                        _STATE.entitlement = entitlement
    return ProviderUsageRefreshResult(get_provider_usage(provider), error)

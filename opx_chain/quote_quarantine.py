"""Bounded acquisition-only quarantine; published datasets remain strict."""

from __future__ import annotations

import pandas as pd
import httpx
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from opx_chain._integrity_validation import (
    collect_option_chain_frame_findings,
    project_option_chain_integrity_summary,
    validate_option_chain_provider_response,
)
from opx_chain.integrity import (
    OptionChainDataIntegrityError,
    OptionChainIntegrityBoundary,
)
from opx_chain.json_utils import dumps_strict_json
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT
from opx_chain.providers.base import OptionChainFrames
from opx_chain.timestamps import utc_now


_TRANSPORT_ERRORS = (
    TimeoutError, ConnectionError, httpx.TimeoutException, httpx.NetworkError,
    httpx.RemoteProtocolError, RequestsConnectionError, RequestsTimeout,
)


def _quote_failures(frame, ticker, provider, *, ticker_row_count=None):
    findings = collect_option_chain_frame_findings(
        frame, boundary=OptionChainIntegrityBoundary.PRE_FILTER,
        requested_tickers=(ticker,),
    )
    failures = [item for item in findings if item.severity.value == "fatal"]
    quote_rows = {item.row_index for item in failures if item.field == "bid_ask"}
    total = len(frame) if ticker_row_count is None else ticker_row_count
    if any(item.field != "bid_ask" for item in failures) or (
        len(quote_rows) > min(10, max(1, total // 10))
    ):
        raise OptionChainDataIntegrityError(project_option_chain_integrity_summary(
            findings, total_rows=len(frame), provider=provider,
        ))
    return sorted(quote_rows)


def quarantine_unusable_quotes(frame, *, provider, ticker, underlying_price):  # pylint: disable=too-many-locals
    """Refresh quote-only failures once; return valid rows and durable evidence.

    At most ten rows and ten percent (with a one-row floor) may be isolated.
    A zero ask is never repaired or reinterpreted as an executable offer.
    """
    frame = frame.reset_index(drop=True).copy()
    bad = _quote_failures(frame, ticker, provider.name)
    if not bad:
        return frame, None
    initial = frame.loc[bad].copy()
    prepare = getattr(provider, "prepare_ticker_fetch", None)
    if callable(prepare):
        prepare(ticker)
    evidence = []
    for expiration in sorted(initial["expiration_date"].unique()):
        # Bypass the persistent chain cache. Providers may fetch a whole ticker
        # internally, but only affected expirations are requested here.
        refresh_error = None
        try:
            chain = provider.load_option_chain(ticker, expiration)
        except _TRANSPORT_ERRORS as exc:
            # Do not persist upstream messages: they can include credentialed URLs.
            refresh_error = type(exc).__name__
            chain = OptionChainFrames(calls=pd.DataFrame(), puts=pd.DataFrame())
        validate_option_chain_provider_response(
            chain.calls, chain.puts, ticker=ticker, provider=provider.name,
        )
        normalized = []
        for side, rows in ((OPTION_TYPE_CALL, chain.calls), (OPTION_TYPE_PUT, chain.puts)):
            if not rows.empty:
                normalized.append(provider.normalize_option_frame(
                    df=rows, underlying_price=underlying_price,
                    expiration_date=expiration, option_type=side, ticker=ticker,
                ))
        refreshed = pd.concat(normalized, ignore_index=True) if normalized else pd.DataFrame()
        if not refreshed.empty:
            _quote_failures(refreshed, ticker, provider.name, ticker_row_count=len(frame))
        for index, original in initial.loc[initial["expiration_date"] == expiration].iterrows():
            matches = (refreshed.loc[refreshed["contract_symbol"] == original["contract_symbol"]]
                       if not refreshed.empty else refreshed)
            replacement = matches.iloc[0] if len(matches) == 1 else None
            if replacement is not None:
                for column in frame.columns:
                    frame.at[index, column] = replacement.get(column)
            current = frame.loc[index]
            evidence.append({
                "contract_symbol": str(original["contract_symbol"]),
                "expiration_date": str(expiration),
                "original_bid": float(original["bid"]),
                "original_ask": float(original["ask"]),
                "original_bid_size": str(original.get("bidSize", "")),
                "original_ask_size": str(original.get("askSize", "")),
                "original_provider_mid": str(original.get("mid", "")),
                "original_quote_time": str(original.get("option_quote_time")),
                "bid": float(current["bid"]), "ask": float(current["ask"]),
                "bid_size": str(current.get("bidSize", "")),
                "ask_size": str(current.get("askSize", "")),
                "quote_time": str(current.get("option_quote_time")),
                "refresh_found": replacement is not None,
                "refresh_error": refresh_error,
                "reason": "ZERO_ASK" if current["ask"] == 0 else "CROSSED_QUOTE",
                "quarantined": bool(current["bid"] > current["ask"]),
            })
    remaining = _quote_failures(frame, ticker, provider.name)
    report = {
        "schema_version": 1, "ticker": ticker, "provider": provider.name,
        "checked_at": utc_now().isoformat(), "refresh_attempts": 1,
        "quarantined_count": len(remaining), "affected_quotes": evidence,
    }
    return frame.drop(index=remaining).reset_index(drop=True), dumps_strict_json(report)

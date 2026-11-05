"""Bounded, offline market-data evidence for option valuation recalculation."""

from __future__ import annotations

import hashlib

import pandas as pd

from opx_chain.integrity import ValidatedOptionChainDataset
from opx_chain.json_utils import dumps_sanitized_json, loads_strict_json, sanitize_json_payload
from opx_chain.schema import SOURCE_QUOTE_COLUMN
from opx_chain.storage.integrity import _assert_canonical_values_preserved
from opx_chain.version import __version__

OPTION_VALUATION_EVIDENCE_SCHEMA_VERSION = 1
MAX_VALUATION_EVIDENCE_BYTES = 128 * 1024 * 1024
SOURCE_QUOTE_FIELDS = (
    "kind", "contract_symbol", "underlying_price", "underlying_price_time",
    "option_quote_time", "bid", "ask", "provider_intrinsic_value",
    "provider_extrinsic_value", "provider_delta", "provider_iv",
)
MARKET_VALUATION_FIELDS = (
    "contract_symbol", "underlying_symbol", "option_type", "strike", "expiration_date",
    "days_to_expiration", "contract_size", "underlying_price", "underlying_price_time",
    "option_quote_time", "bid", "ask", "bid_ask_spread_pct_of_mid", "delta", "gamma",
    "theta", "vega", "implied_volatility", "quote_age_seconds", "is_stale_quote",
    "underlying_price_age_seconds", "is_stale_underlying_price", "data_source",
    "risk_free_rate_used",
)
VALUATION_EVIDENCE_FIELDS = (
    *MARKET_VALUATION_FIELDS,
    "next_earnings_date", "next_earnings_date_is_estimated", "next_earnings_date_source",
    "next_earnings_date_confidence", "days_to_earnings", "earnings_within_5d",
    "earnings_within_10d", "next_ex_div_date", "next_ex_div_date_source",
    "next_ex_div_date_confidence", "days_to_ex_div", "ex_div_within_3d", "dividend_amount",
    "event_risk_score",
)


def _digest(value: object) -> str:
    return hashlib.sha256(dumps_sanitized_json(value, sort_keys=True,
                                              separators=(",", ":")).encode()).hexdigest()


def _assert_source_pairs(rows: list[dict]) -> None:
    allowed = {"OPTION_CHAIN_SNAPSHOT", "INDEPENDENT_STOCK_SNAPSHOT", "CANONICAL_ONLY"}
    if any(row["source_quote"].get("kind") not in allowed for row in rows):
        raise ValueError("valuation evidence source kind is invalid")
    paired = [row for row in rows if row["source_quote"].get("kind") == "OPTION_CHAIN_SNAPSHOT"]
    if paired:
        fields = ("contract_symbol", "underlying_price", "underlying_price_time",
                  "option_quote_time", "bid", "ask")
        native = pd.DataFrame([
            {key: row["source_quote"].get(key) for key in fields} for row in paired])
        canonical = pd.DataFrame([row["canonical"] for row in paired])
        _assert_canonical_values_preserved(native, canonical)


def build_option_valuation_evidence(  # pylint: disable=too-many-arguments
    source: pd.DataFrame, canonical: pd.DataFrame, *, dataset_id: str,
    dataset_content_hash: str, provider: str, assessed_at: str,
    positions_content_hash: str | None = None,
) -> bytes:
    """Capture allowlisted native observations before export discards extras."""
    if (len(source) != len(canonical)
            or source.contract_symbol.tolist() != canonical.contract_symbol.tolist()):
        raise ValueError("valuation source rows do not match the published dataset")
    rows = []
    for native, row in zip(source.to_dict("records"), canonical.to_dict("records"), strict=True):
        quote = native.get(SOURCE_QUOTE_COLUMN)
        rows.append({
            "canonical": {key: row.get(key) for key in VALUATION_EVIDENCE_FIELDS},
            "source_quote": {key: quote[key] for key in SOURCE_QUOTE_FIELDS if key in quote}
                if isinstance(quote, dict) else {"kind": "CANONICAL_ONLY"},
        })
    payload = sanitize_json_payload({
        "schema_version": OPTION_VALUATION_EVIDENCE_SCHEMA_VERSION,
        "dataset_id": dataset_id, "dataset_content_hash": dataset_content_hash,
        "provider": provider, "script_version": __version__, "assessed_at": assessed_at,
        "positions_content_hash": positions_content_hash,
        "scope": "retained_chain_rows", "rows": rows,
    })
    _assert_source_pairs(payload["rows"])
    payload["evidence_sha256"] = _digest(payload)
    content = dumps_sanitized_json(payload, sort_keys=True, separators=(",", ":")).encode()
    if len(content) > MAX_VALUATION_EVIDENCE_BYTES:
        raise ValueError("valuation evidence exceeds the retained artifact size limit")
    return content


def parse_option_valuation_evidence(content: bytes, dataset: ValidatedOptionChainDataset) -> dict:
    """Verify an offline receipt against the exact freshly validated dataset."""
    if len(content) > MAX_VALUATION_EVIDENCE_BYTES:
        raise ValueError("valuation evidence exceeds the retained artifact size limit")
    payload = loads_strict_json(content.decode())
    if not isinstance(payload, dict):
        raise ValueError("valuation evidence must be an object")
    checksum = payload.pop("evidence_sha256", None)
    if checksum != _digest(payload):
        raise ValueError("valuation evidence checksum mismatch")
    handle = dataset.handle
    version = payload.get("schema_version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("valuation evidence schema version must be an integer")
    if (version != OPTION_VALUATION_EVIDENCE_SCHEMA_VERSION
            or payload.get("dataset_id") != handle.dataset_id
            or payload.get("dataset_content_hash") != handle.content_hash
            or payload.get("provider") != handle.provider):
        raise ValueError("valuation evidence dataset binding mismatch")
    rows = payload.get("rows")
    if not isinstance(rows, list) or len(rows) != len(dataset.frame):
        raise ValueError("valuation evidence row coverage mismatch")
    if any(not isinstance(row, dict) or not isinstance(row.get("canonical"), dict)
           or not isinstance(row.get("source_quote"), dict) for row in rows):
        raise ValueError("valuation evidence row shape mismatch")
    restored = pd.DataFrame([row["canonical"] for row in rows])
    columns = [key for key in VALUATION_EVIDENCE_FIELDS if key in dataset.frame]
    _assert_canonical_values_preserved(dataset.frame[columns], restored)
    _assert_source_pairs(rows)
    payload["evidence_sha256"] = checksum
    return payload

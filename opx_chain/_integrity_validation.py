"""Package-private option-chain frame validators and canonical projector."""
# pylint: disable=too-many-arguments,too-many-branches,too-many-locals

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
import re

import numpy as np
import pandas as pd

from opx_chain.integrity import (
    CONTRACT_STRIKE_TOLERANCE,
    OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
    OptionChainDataIntegrityError,
    OptionChainIntegrityBoundary,
    OptionChainIntegrityCode,
    OptionChainIntegrityFinding,
    OptionChainIntegritySeverity,
    OptionChainIntegritySummary,
    canonical_option_contract_key,
)
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT
from opx_chain.schema import BOOLEAN_FIELDS, TIMESTAMP_FIELDS
from opx_chain.validate import NUMERIC_FIELDS, REQUIRED_CORE_FIELDS


_COMPACT_OSI_RE = re.compile(
    r"^(?P<root>[A-Z0-9]{1,6})(?P<expiration>[0-9]{6})"
    r"(?P<side>[CP])(?P<strike>[0-9]{8})$"
)
_IDENTITY_FIELDS = frozenset(
    {"underlying_symbol", "contract_symbol", "option_type", "expiration_date", "strike"}
)
_RAW_SYMBOL_FIELDS = ("contract_symbol", "contractSymbol", "optionSymbol")
_RAW_REQUIRED_NUMERIC_FIELDS = ("strike", "bid", "ask")
_RAW_OPTIONAL_NUMERIC_FIELDS = (
    "last_trade_price",
    "lastPrice",
    "last",
    "open_interest",
    "openInterest",
    "volume",
    "implied_volatility",
    "impliedVolatility",
    "iv",
    "change",
    "percent_change",
    "percentChange",
    "delta",
    "gamma",
    "theta",
    "vega",
)
_RAW_TIMESTAMP_FIELDS = ("option_quote_time", "lastTradeDate", "updated")
_RAW_BOOLEAN_FIELDS = ("is_in_the_money", "inTheMoney")
_RAW_OPTION_TYPE_FIELDS = ("option_type", "side", "contract_type")


def _timestamp_invalid_checker():
    """Memoize exact strings only, bounded to one validation invocation."""
    @lru_cache(maxsize=1024)
    def invalid_string(value: str) -> bool:
        return bool(pd.isna(pd.to_datetime(value, utc=True, errors="coerce")))

    def invalid(value: object):
        # Only built-in strings: subclasses can customize equality and hashing.
        if type(value) is str:  # pylint: disable=unidiomatic-typecheck
            return invalid_string(value)
        return pd.isna(pd.to_datetime(value, utc=True, errors="coerce"))

    return invalid


def _missing(value: object) -> bool:
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and not value.strip()


def _safe_text(value: object) -> str | None:
    return None if _missing(value) else str(value)


def _finding(
    boundary: OptionChainIntegrityBoundary,
    code: OptionChainIntegrityCode,
    *,
    row_index: int | None = None,
    row: pd.Series | None = None,
    field: str | None = None,
    expected: object | None = None,
    actual: object | None = None,
    severity: OptionChainIntegritySeverity = OptionChainIntegritySeverity.FATAL,
) -> OptionChainIntegrityFinding:
    return OptionChainIntegrityFinding(
        severity=severity,
        boundary=boundary,
        code=code,
        row_index=row_index,
        ticker=_safe_text(row.get("underlying_symbol")) if row is not None else None,
        contract_symbol=_safe_text(row.get("contract_symbol")) if row is not None else None,
        field=field,
        expected=_safe_text(expected),
        actual=_safe_text(actual),
    )


def _raw_finding(
    code: OptionChainIntegrityCode,
    *,
    row_index: int | None = None,
    ticker: str | None = None,
    contract_symbol: object | None = None,
    field: str | None = None,
    expected: object | None = None,
    actual: object | None = None,
) -> OptionChainIntegrityFinding:
    """Build one bounded provider-response finding before canonical mapping."""
    return OptionChainIntegrityFinding(
        severity=OptionChainIntegritySeverity.FATAL,
        boundary=OptionChainIntegrityBoundary.PROVIDER_RESPONSE,
        code=code,
        row_index=row_index,
        ticker=ticker,
        contract_symbol=_safe_text(contract_symbol),
        field=field,
        expected=_safe_text(expected),
        actual=_safe_text(actual),
    )


def _raw_contract_symbol(row: pd.Series) -> tuple[str | None, object | None]:
    for field in _RAW_SYMBOL_FIELDS:
        if field in row.index:
            return field, row.get(field)
    return None, None


def provider_payload_to_frame(
    payload: object,
    *,
    ticker: str,
    provider: str,
) -> pd.DataFrame:
    """Align one mapping-of-columns payload or raise a typed shape failure."""
    try:
        if not isinstance(payload, Mapping):
            raise TypeError(f"expected mapping, got {type(payload).__name__}")
        return pd.DataFrame(dict(payload))
    except (TypeError, ValueError) as exc:
        finding = _raw_finding(
            OptionChainIntegrityCode.RESPONSE_SHAPE_INVALID,
            ticker=ticker,
            field="response",
            expected="aligned tabular column arrays",
            actual=str(exc),
        )
        summary = project_option_chain_integrity_summary(
            (finding,),
            total_rows=0,
            provider=provider,
        )
        raise OptionChainDataIntegrityError(summary) from exc


def validate_option_chain_provider_response(  # pylint: disable=too-many-statements
    calls: object,
    puts: object,
    *,
    ticker: str,
    provider: str,
) -> OptionChainIntegritySummary:
    """Validate aligned raw provider frames before lossy canonical coercion."""
    timestamp_invalid = _timestamp_invalid_checker()
    findings: list[OptionChainIntegrityFinding] = []
    frames: list[pd.DataFrame] = []
    row_offset = 0
    raw_symbols: list[tuple[int, str]] = []
    for side, raw_frame in ((OPTION_TYPE_CALL, calls), (OPTION_TYPE_PUT, puts)):
        if not isinstance(raw_frame, pd.DataFrame):
            findings.append(
                _raw_finding(
                    OptionChainIntegrityCode.RESPONSE_SHAPE_INVALID,
                    ticker=ticker,
                    field=side,
                    expected="pandas DataFrame",
                    actual=type(raw_frame).__name__,
                )
            )
            continue
        frame = raw_frame.reset_index(drop=True)
        frames.append(frame)
        for local_index, row in frame.iterrows():
            row_index = row_offset + int(local_index)
            symbol_field, symbol = _raw_contract_symbol(row)
            if symbol_field is None or _missing(symbol):
                findings.append(
                    _raw_finding(
                        OptionChainIntegrityCode.REQUIRED_IDENTITY_FIELD_MISSING,
                        row_index=row_index,
                        ticker=ticker,
                        field="contract_symbol",
                        expected="independent provider contract identifier",
                        actual="missing",
                    )
                )
            else:
                raw_symbols.append((row_index, str(symbol).strip().upper()))

            for field in _RAW_REQUIRED_NUMERIC_FIELDS:
                if field not in frame.columns or _missing(row.get(field)):
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.REQUIRED_FIELD_INVALID,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="present finite number",
                            actual="missing",
                        )
                    )

            for field in (*_RAW_REQUIRED_NUMERIC_FIELDS, *_RAW_OPTIONAL_NUMERIC_FIELDS):
                if field not in frame.columns or _missing(row.get(field)):
                    continue
                try:
                    numeric = float(row.get(field))
                except (TypeError, ValueError):
                    numeric = float("nan")
                if not np.isfinite(numeric):
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="finite number",
                            actual=row.get(field),
                        )
                    )

            for field in _RAW_TIMESTAMP_FIELDS:
                if field not in frame.columns or _missing(row.get(field)):
                    continue
                if timestamp_invalid(row.get(field)):
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="timestamp",
                            actual=row.get(field),
                        )
                    )

            for field in _RAW_BOOLEAN_FIELDS:
                if field not in frame.columns or _missing(row.get(field)):
                    continue
                if not isinstance(row.get(field), (bool, np.bool_)):
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="boolean",
                            actual=row.get(field),
                        )
                    )

            for field in _RAW_OPTION_TYPE_FIELDS:
                if field not in frame.columns:
                    continue
                if _missing(row.get(field)):
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.REQUIRED_IDENTITY_FIELD_MISSING,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="call or put",
                            actual="missing",
                        )
                    )
                    continue
                raw_side = str(row.get(field)).strip().lower()
                normalized_side = {
                    "c": OPTION_TYPE_CALL,
                    OPTION_TYPE_CALL: OPTION_TYPE_CALL,
                    "p": OPTION_TYPE_PUT,
                    OPTION_TYPE_PUT: OPTION_TYPE_PUT,
                }.get(raw_side)
                if normalized_side is None:
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected="call or put",
                            actual=row.get(field),
                        )
                    )
                elif normalized_side != side:
                    findings.append(
                        _raw_finding(
                            OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH,
                            row_index=row_index,
                            ticker=ticker,
                            contract_symbol=symbol,
                            field=field,
                            expected=side,
                            actual=normalized_side,
                        )
                    )
        row_offset += len(frame)

    symbol_counts = Counter(symbol for _, symbol in raw_symbols)
    for row_index, symbol in raw_symbols:
        if symbol_counts[symbol] > 1:
            findings.append(
                _raw_finding(
                    OptionChainIntegrityCode.DUPLICATE_CONTRACT_SYMBOL,
                    row_index=row_index,
                    ticker=ticker,
                    contract_symbol=symbol,
                    field="contract_symbol",
                    expected="unique provider contract identifier",
                    actual=symbol,
                )
            )

    total_rows = sum(len(frame) for frame in frames)
    summary = project_option_chain_integrity_summary(
        findings,
        total_rows=total_rows,
        provider=provider,
    )
    if summary.status == "invalid":
        raise OptionChainDataIntegrityError(summary)
    return summary


def _parse_compact_osi(contract_symbol: object) -> tuple[str, str, Decimal, object]:
    symbol = str(contract_symbol or "").strip().upper()
    match = _COMPACT_OSI_RE.fullmatch(symbol)
    if match is None:
        raise ValueError("contract symbol is not compact OSI")
    root = match.group("root")
    if any(character.isdigit() for character in root):
        raise NotImplementedError("adjusted/non-standard option root")
    expiration = datetime.strptime(match.group("expiration"), "%y%m%d").date()
    side = OPTION_TYPE_CALL if match.group("side") == "C" else OPTION_TYPE_PUT
    strike = Decimal(match.group("strike")) / Decimal(1000)
    return root, side, strike, expiration


def _append_field_findings(
    findings: list[OptionChainIntegrityFinding],
    frame: pd.DataFrame,
    boundary: OptionChainIntegrityBoundary,
) -> None:
    timestamp_invalid = _timestamp_invalid_checker()
    for row_index, row in frame.iterrows():
        for field in REQUIRED_CORE_FIELDS:
            if field not in frame.columns or _missing(row.get(field)):
                code = (
                    OptionChainIntegrityCode.REQUIRED_IDENTITY_FIELD_MISSING
                    if field in _IDENTITY_FIELDS
                    else OptionChainIntegrityCode.REQUIRED_FIELD_INVALID
                )
                findings.append(
                    _finding(
                        boundary,
                        code,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected="present",
                        actual="missing",
                    )
                )

        for field in NUMERIC_FIELDS:
            if field not in frame.columns or _missing(row.get(field)):
                continue
            value = row.get(field)
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                numeric = float("nan")
            if not np.isfinite(numeric):
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected="finite number",
                        actual=value,
                    )
                )

        for field in TIMESTAMP_FIELDS:
            if field not in frame.columns or _missing(row.get(field)):
                continue
            if timestamp_invalid(row.get(field)):
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected="timestamp",
                        actual=row.get(field),
                    )
                )

        for field in BOOLEAN_FIELDS:
            if field not in frame.columns or _missing(row.get(field)):
                continue
            if not isinstance(row.get(field), (bool, np.bool_)):
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected="boolean",
                        actual=row.get(field),
                    )
                )


def _append_identity_findings(
    findings: list[OptionChainIntegrityFinding],
    frame: pd.DataFrame,
    boundary: OptionChainIntegrityBoundary,
    *,
    requested_tickers: tuple[str, ...] | None,
) -> None:
    canonical_keys: list[tuple | None] = []
    requested = None
    if requested_tickers is not None:
        requested = {
            canonical_option_contract_key(
                ticker,
                OPTION_TYPE_CALL,
                1,
                "2000-01-01",
            )[0]
            for ticker in requested_tickers
        }

    for row_index, row in frame.iterrows():
        if any(_missing(row.get(field)) for field in _IDENTITY_FIELDS):
            canonical_keys.append(None)
            continue
        try:
            canonical_key = canonical_option_contract_key(
                row.get("underlying_symbol"),
                row.get("option_type"),
                row.get("strike"),
                row.get("expiration_date"),
            )
        except ValueError as exc:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.REQUIRED_FIELD_INVALID,
                    row_index=int(row_index),
                    row=row,
                    field="contract_key",
                    expected="canonical standard option identity",
                    actual=str(exc),
                )
            )
            canonical_keys.append(None)
            continue
        canonical_keys.append(canonical_key)

        if requested is not None and canonical_key[0] not in requested:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH,
                    row_index=int(row_index),
                    row=row,
                    field="underlying_symbol",
                    expected=",".join(sorted(requested)),
                    actual=canonical_key[0],
                )
            )

        if "contract_size" in frame.columns and not _missing(row.get("contract_size")):
            contract_size = row.get("contract_size")
            if isinstance(contract_size, str) and contract_size.strip().upper() == "REGULAR":
                standard_contract = True
            else:
                try:
                    standard_contract = float(contract_size) == 100.0
                except (TypeError, ValueError):
                    standard_contract = False
            if not standard_contract:
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.UNSUPPORTED_CONTRACT_IDENTITY,
                        row_index=int(row_index),
                        row=row,
                        field="contract_size",
                        expected="100",
                        actual=row.get("contract_size"),
                    )
                )

        try:
            root, side, strike, expiration = _parse_compact_osi(row.get("contract_symbol"))
        except NotImplementedError:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.UNSUPPORTED_CONTRACT_IDENTITY,
                    row_index=int(row_index),
                    row=row,
                    field="contract_symbol",
                    expected="standard compact OSI identity",
                    actual=row.get("contract_symbol"),
                )
            )
            continue
        except ValueError:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.CONTRACT_SYMBOL_INVALID,
                    row_index=int(row_index),
                    row=row,
                    field="contract_symbol",
                    expected="<root><YYMMDD><C|P><8-digit strike>",
                    actual=row.get("contract_symbol"),
                )
            )
            continue

        comparisons = (
            ("underlying_symbol", root, canonical_key[0]),
            ("option_type", side, canonical_key[1]),
            ("expiration_date", expiration.isoformat(), canonical_key[3].isoformat()),
        )
        for field, expected, actual in comparisons:
            if expected != actual:
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected=expected,
                        actual=actual,
                    )
                )
        if abs(strike - canonical_key[2]) >= CONTRACT_STRIKE_TOLERANCE:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH,
                    row_index=int(row_index),
                    row=row,
                    field="strike",
                    expected=str(strike),
                    actual=str(canonical_key[2]),
                )
            )

    if "contract_symbol" in frame.columns:
        normalized_symbols = frame["contract_symbol"].map(
            lambda value: str(value).strip().upper() if not _missing(value) else None
        )
        duplicate_symbols = normalized_symbols.duplicated(keep=False) & normalized_symbols.notna()
        for row_index in duplicate_symbols[duplicate_symbols].index:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.DUPLICATE_CONTRACT_SYMBOL,
                    row_index=int(row_index),
                    row=frame.loc[row_index],
                    field="contract_symbol",
                    expected="unique",
                    actual=normalized_symbols.loc[row_index],
                )
            )

    key_counts = Counter(key for key in canonical_keys if key is not None)
    for row_index, key in enumerate(canonical_keys):
        if key is not None and key_counts[key] > 1:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.DUPLICATE_CONTRACT_KEY,
                    row_index=row_index,
                    row=frame.iloc[row_index],
                    field="contract_key",
                    expected="unique",
                    actual="|".join(str(value) for value in key),
                )
            )


def _append_quote_findings(
    findings: list[OptionChainIntegrityFinding],
    frame: pd.DataFrame,
    boundary: OptionChainIntegrityBoundary,
) -> None:
    for row_index, row in frame.iterrows():
        for field, minimum, strict in (
            ("strike", 0.0, True),
            ("underlying_price", 0.0, True),
            ("bid", 0.0, False),
            ("ask", 0.0, False),
        ):
            if field not in frame.columns or _missing(row.get(field)):
                continue
            try:
                numeric = float(row.get(field))
            except (TypeError, ValueError):
                continue
            if not np.isfinite(numeric) or (numeric <= minimum if strict else numeric < minimum):
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                        row_index=int(row_index),
                        row=row,
                        field=field,
                        expected=f"> {minimum}" if strict else f">= {minimum}",
                        actual=row.get(field),
                    )
                )
        if all(field in frame.columns and not _missing(row.get(field)) for field in ("bid", "ask")):
            try:
                crossed = float(row.get("bid")) > float(row.get("ask"))
            except (TypeError, ValueError):
                crossed = False
            if crossed:
                findings.append(
                    _finding(
                        boundary,
                        OptionChainIntegrityCode.FIELD_VALUE_INVALID,
                        row_index=int(row_index),
                        row=row,
                        field="bid_ask",
                        expected="bid <= ask",
                        actual=f"{row.get('bid')} > {row.get('ask')}",
                    )
                )


def collect_option_chain_frame_findings(
    frame: pd.DataFrame,
    *,
    boundary: OptionChainIntegrityBoundary,
    requested_tickers: tuple[str, ...] | None = None,
) -> tuple[OptionChainIntegrityFinding, ...]:
    """Collect exhaustive applicable findings for one aligned canonical frame."""
    boundary = OptionChainIntegrityBoundary(boundary)
    if not isinstance(frame, pd.DataFrame):
        return (
            _finding(
                boundary,
                OptionChainIntegrityCode.RESPONSE_SHAPE_INVALID,
                expected="pandas DataFrame",
                actual=type(frame).__name__,
            ),
        )
    canonical = frame.reset_index(drop=True)
    findings: list[OptionChainIntegrityFinding] = []
    missing_columns = [field for field in REQUIRED_CORE_FIELDS if field not in canonical.columns]
    for field in missing_columns:
        findings.append(
            _finding(
                boundary,
                OptionChainIntegrityCode.DATASET_SCHEMA_INVALID,
                field=field,
                expected="required canonical column",
                actual="missing",
            )
        )
    if canonical.empty:
        findings.append(
            _finding(
                boundary,
                OptionChainIntegrityCode.DATASET_SCHEMA_INVALID,
                expected="at least one canonical row",
                actual="0 rows",
            )
        )
        return tuple(findings)
    _append_field_findings(findings, canonical, boundary)
    _append_identity_findings(
        findings,
        canonical,
        boundary,
        requested_tickers=requested_tickers,
    )
    _append_quote_findings(findings, canonical, boundary)
    if "data_source" in canonical.columns:
        sources = {
            str(value).strip().lower()
            for value in canonical["data_source"]
            if not _missing(value)
        }
        if len(sources) > 1:
            findings.append(
                _finding(
                    boundary,
                    OptionChainIntegrityCode.DATASET_SCHEMA_INVALID,
                    field="data_source",
                    expected="one provider per dataset",
                    actual=",".join(sorted(sources)),
                )
            )
    return tuple(findings)


def _finding_sort_key(finding: OptionChainIntegrityFinding) -> tuple:
    return (
        finding.code.value,
        finding.boundary.value,
        finding.row_index is None,
        finding.row_index if finding.row_index is not None else 0,
        finding.ticker or "",
        finding.contract_symbol or "",
        finding.field or "",
        finding.expected or "",
        finding.actual or "",
        finding.severity.value,
    )


def project_option_chain_integrity_summary(
    findings: tuple[OptionChainIntegrityFinding, ...] | list[OptionChainIntegrityFinding],
    *,
    total_rows: int,
    checked_at: datetime | None = None,
    dataset_id: str | None = None,
    content_hash: str | None = None,
    provider: str | None = None,
) -> OptionChainIntegritySummary:
    """Aggregate raw findings once into the canonical bounded summary."""
    ordered = tuple(sorted(tuple(findings), key=_finding_sort_key))
    counts = Counter(finding.code for finding in ordered)
    fatal = tuple(
        finding for finding in ordered
        if finding.severity is OptionChainIntegritySeverity.FATAL
    )
    warning = tuple(
        finding for finding in ordered
        if finding.severity is OptionChainIntegritySeverity.WARNING
    )
    samples: list[OptionChainIntegrityFinding] = []
    samples_per_code: dict[OptionChainIntegrityCode, int] = defaultdict(int)
    for finding in ordered:
        if len(samples) >= 5:
            break
        if samples_per_code[finding.code] >= 2:
            continue
        samples.append(finding)
        samples_per_code[finding.code] += 1
    return OptionChainIntegritySummary(
        schema_version=OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
        validator_version=OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
        status="invalid" if fatal else "valid",
        checked_at=checked_at or datetime.now(tz=timezone.utc),
        dataset_id=dataset_id,
        content_hash=content_hash,
        provider=provider,
        total_rows=total_rows,
        invalid_row_count=len({item.row_index for item in fatal if item.row_index is not None}),
        fatal_finding_count=len(fatal),
        warning_finding_count=len(warning),
        affected_ticker_count=len({item.ticker for item in ordered if item.ticker is not None}),
        counts_by_code=counts,
        samples=tuple(samples),
    )


def validate_option_chain_frame(
    frame: pd.DataFrame,
    *,
    boundary: OptionChainIntegrityBoundary,
    requested_tickers: tuple[str, ...] | None = None,
    checked_at: datetime | None = None,
    dataset_id: str | None = None,
    content_hash: str | None = None,
    provider: str | None = None,
) -> OptionChainIntegritySummary:
    """Validate a frame and raise the public typed error on fatal findings."""
    findings = collect_option_chain_frame_findings(
        frame,
        boundary=boundary,
        requested_tickers=requested_tickers,
    )
    summary = project_option_chain_integrity_summary(
        findings,
        total_rows=len(frame) if isinstance(frame, pd.DataFrame) else 0,
        checked_at=checked_at,
        dataset_id=dataset_id,
        content_hash=content_hash,
        provider=provider,
    )
    if summary.status == "invalid":
        raise OptionChainDataIntegrityError(summary)
    return summary

"""Shared row-level and file-level validation for canonical option data."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from opx_chain.schema import QUALITY_FLAG_FIELDS


REQUIRED_CORE_FIELDS = (
    "data_source",
    "underlying_symbol",
    "contract_symbol",
    "option_type",
    "expiration_date",
    "strike",
    "underlying_price",
    "bid",
    "ask",
)
NUMERIC_FIELDS = (
    "strike",
    "underlying_price",
    "bid",
    "ask",
    "last_trade_price",
    "volume",
    "open_interest",
    "implied_volatility",
)
TIMESTAMP_FIELDS = (
    "option_quote_time",
    "underlying_price_time",
)
BOOLEAN_FIELDS = (
    "is_in_the_money",
    "next_earnings_date_is_estimated",
    *QUALITY_FLAG_FIELDS,
    "near_expiry_near_money_flag",
    "passes_primary_screen",
    "is_stale_quote",
    "is_stale_underlying_price",
)


@dataclass(frozen=True)
class ValidationFinding:
    """One validation finding emitted during row or file checks."""

    severity: str
    code: str
    message: str
    row_index: int | None = None
    contract_symbol: str | None = None
    field: str | None = None

    def format_for_output(self) -> str:
        """Return a compact human-readable validation line."""
        bits = [self.severity.upper()]
        if self.row_index is not None:
            bits.append(f"row={self.row_index}")
        if self.contract_symbol:
            bits.append(f"contract={self.contract_symbol}")
        bits.append(f"code={self.code}")
        if self.field:
            bits.append(f"field={self.field}")
        bits.append(self.message)
        return " ".join(bits)


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return bool(pd.isna(value))


def _coerce_numeric(value):
    coerced = pd.to_numeric(value, errors="coerce")
    return np.nan if pd.isna(coerced) else float(coerced)


def _is_boolean_like(value) -> bool:
    return isinstance(value, (bool, np.bool_))


def _make_finding(  # pylint: disable=too-many-arguments
    severity, code, message, *, row_index=None, contract_symbol=None, field=None
):
    return ValidationFinding(
        severity=severity,
        code=code,
        message=message,
        row_index=row_index,
        contract_symbol=contract_symbol,
        field=field,
    )


def validate_option_rows(  # pylint: disable=too-many-branches
    df: pd.DataFrame,
) -> list[ValidationFinding]:
    """Validate individual option rows before shared post-download filtering."""
    findings: list[ValidationFinding] = []
    if df.empty:
        return findings

    for row_index, row in df.iterrows():
        contract_symbol = None if "contract_symbol" not in row else row.get("contract_symbol")

        for field in REQUIRED_CORE_FIELDS:
            if field not in df.columns or _is_missing(row.get(field)):
                findings.append(
                    _make_finding(
                        "error",
                        "missing_required_field",
                        f"Required field '{field}' is empty.",
                        row_index=row_index,
                        contract_symbol=contract_symbol,
                        field=field,
                    )
                )

        option_type = row.get("option_type")
        if not _is_missing(option_type) and option_type not in {"call", "put"}:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_option_type",
                    "option_type must be 'call' or 'put'.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="option_type",
                )
            )

        expiration_date = row.get("expiration_date")
        if not _is_missing(expiration_date):
            parsed_expiration = pd.to_datetime(
                expiration_date,
                format="%Y-%m-%d",
                errors="coerce",
            )
            if pd.isna(parsed_expiration):
                findings.append(
                    _make_finding(
                        "error",
                        "invalid_expiration_date",
                        "expiration_date must parse as YYYY-MM-DD.",
                        row_index=row_index,
                        contract_symbol=contract_symbol,
                        field="expiration_date",
                    )
                )

        for field in NUMERIC_FIELDS:
            if field not in df.columns or _is_missing(row.get(field)):
                continue
            numeric_value = _coerce_numeric(row.get(field))
            if pd.isna(numeric_value):
                findings.append(
                    _make_finding(
                        "error",
                        "invalid_numeric_field",
                        f"Field '{field}' must be numeric.",
                        row_index=row_index,
                        contract_symbol=contract_symbol,
                        field=field,
                    )
                )

        for field in TIMESTAMP_FIELDS:
            if field not in df.columns or _is_missing(row.get(field)):
                continue
            parsed_timestamp = pd.to_datetime(row.get(field), utc=True, errors="coerce")
            if pd.isna(parsed_timestamp):
                findings.append(
                    _make_finding(
                        "error",
                        "invalid_timestamp_field",
                        f"Field '{field}' must be a valid timestamp.",
                        row_index=row_index,
                        contract_symbol=contract_symbol,
                        field=field,
                    )
                )

        for field in BOOLEAN_FIELDS:
            if field not in df.columns or _is_missing(row.get(field)):
                continue
            if not _is_boolean_like(row.get(field)):
                findings.append(
                    _make_finding(
                        "warning",
                        "invalid_boolean_field",
                        f"Field '{field}' should be boolean-like.",
                        row_index=row_index,
                        contract_symbol=contract_symbol,
                        field=field,
                    )
                )

        strike = _coerce_numeric(row.get("strike"))
        if pd.notna(strike) and strike <= 0:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_strike",
                    "strike must be greater than zero.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="strike",
                )
            )

        underlying_price = _coerce_numeric(row.get("underlying_price"))
        if pd.notna(underlying_price) and underlying_price <= 0:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_underlying_price",
                    "underlying_price must be greater than zero.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="underlying_price",
                )
            )

        bid = _coerce_numeric(row.get("bid"))
        ask = _coerce_numeric(row.get("ask"))
        if pd.notna(bid) and bid < 0:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_bid",
                    "bid must be non-negative.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="bid",
                )
            )
        if pd.notna(ask) and ask < 0:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_ask",
                    "ask must be non-negative.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="ask",
                )
            )
        if pd.notna(bid) and pd.notna(ask) and bid > ask:
            findings.append(
                _make_finding(
                    "error",
                    "invalid_quote_order",
                    "bid must be less than or equal to ask.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="bid_ask",
                )
            )

    return findings


def validate_export_frame(df: pd.DataFrame) -> list[ValidationFinding]:
    """Validate the combined export frame before CSV write."""
    findings: list[ValidationFinding] = []
    if df.empty:
        findings.append(
            _make_finding(
                "warning",
                "empty_export_frame",
                "Combined export frame is empty.",
            )
        )
        return findings

    for field in REQUIRED_CORE_FIELDS:
        if field not in df.columns:
            findings.append(
                _make_finding(
                    "error",
                    "missing_required_column",
                    f"Required column '{field}' is missing from the export frame.",
                    field=field,
                )
            )

    if "data_source" in df.columns and df["data_source"].dropna().nunique() > 1:
        findings.append(
            _make_finding(
                "error",
                "mixed_data_sources",
                "Export frame contains rows from multiple data sources.",
                field="data_source",
            )
        )

    if {"data_source", "contract_symbol"}.issubset(df.columns):
        duplicates = df[df.duplicated(subset=["data_source", "contract_symbol"], keep=False)]
        for row_index, row in duplicates.iterrows():
            findings.append(
                _make_finding(
                    "error",
                    "duplicate_contract_row",
                    "Duplicate contract_symbol detected within the same data source.",
                    row_index=row_index,
                    contract_symbol=row.get("contract_symbol"),
                    field="contract_symbol",
                )
            )

    return findings


def summarize_validation_findings(findings: list[ValidationFinding]) -> tuple[int, int]:
    """Return warning/error counts for a collection of findings."""
    warnings = sum(1 for finding in findings if finding.severity == "warning")
    errors = sum(1 for finding in findings if finding.severity == "error")
    return warnings, errors


def emit_validation_report(findings: list[ValidationFinding], *, logger=None) -> None:
    """Print and optionally log a validation summary and detailed findings."""
    warnings, errors = summarize_validation_findings(findings)
    print("Validation summary:")
    print(f"  warnings: {warnings}")
    print(f"  errors: {errors}")
    if logger:
        logger.info("validation status=completed warnings=%s errors=%s", warnings, errors)

    if not findings:
        return

    print("Validation findings:")
    for finding in findings:
        line = f"  {finding.format_for_output()}"
        print(line)
        if logger:
            log_method = logger.error if finding.severity == "error" else logger.warning
            log_method("validation_%s %s", finding.severity, finding.format_for_output())

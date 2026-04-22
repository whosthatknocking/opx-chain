"""Validation tests for shared row-level and file-level checks."""

import pandas as pd

from opx_chain.validate import (
    emit_validation_report,
    summarize_validation_findings,
    validate_export_frame,
    validate_option_rows,
)


def make_valid_row(**overrides):
    """Build one canonical row that satisfies the shared validation contract."""
    row = {
        "data_source": "stub",
        "underlying_symbol": "TEST",
        "contract_symbol": "TEST260417C00100000",
        "option_type": "call",
        "expiration_date": "2026-04-17",
        "strike": 100.0,
        "underlying_price": 101.0,
        "bid": 1.0,
        "ask": 1.2,
        "last_trade_price": 1.1,
        "volume": 10,
        "open_interest": 20,
        "implied_volatility": 0.3,
        "option_quote_time": pd.Timestamp("2026-03-20T13:40:00Z"),
        "underlying_price_time": pd.Timestamp("2026-03-20T13:45:00Z"),
        "is_in_the_money": False,
        "has_valid_quote": True,
        "passes_primary_screen": True,
    }
    row.update(overrides)
    return row


def test_validate_option_rows_flags_missing_required_bid_and_ask():
    """Missing shared required quote fields should be row-level errors."""
    frame = pd.DataFrame([make_valid_row(bid=None, ask=None)])

    findings = validate_option_rows(frame)

    assert any(f.code == "missing_required_field" and f.field == "bid" for f in findings)
    assert any(f.code == "missing_required_field" and f.field == "ask" for f in findings)


def test_validate_option_rows_flags_invalid_types_and_quote_order():
    """Malformed shared field values should surface as validation findings."""
    frame = pd.DataFrame(
        [
            make_valid_row(
                option_type="CALL",
                expiration_date="04/17/2026",
                bid=2.0,
                ask=1.0,
                has_valid_quote="yes",
            )
        ]
    )

    findings = validate_option_rows(frame)

    assert any(f.code == "invalid_option_type" for f in findings)
    assert any(f.code == "invalid_expiration_date" for f in findings)
    assert any(f.code == "invalid_quote_order" for f in findings)
    assert any(f.code == "invalid_boolean_field" for f in findings)


def test_validate_export_frame_flags_missing_columns_and_duplicates():
    """Combined export validation should catch file-level schema and duplicate issues."""
    frame = pd.DataFrame(
        [
            make_valid_row(),
            make_valid_row(),
        ]
    ).drop(columns=["bid"])
    frame.loc[1, "contract_symbol"] = frame.loc[0, "contract_symbol"]

    findings = validate_export_frame(frame)

    assert any(f.code == "missing_required_column" and f.field == "bid" for f in findings)
    assert any(f.code == "duplicate_contract_row" for f in findings)


def test_emit_validation_report_prints_counts(capsys):
    """Validation reporting should print a stable summary even when findings are empty."""
    emit_validation_report([])

    stdout = capsys.readouterr().out
    assert "Validation summary:" in stdout
    assert "warnings: 0" in stdout
    assert "errors: 0" in stdout


def test_summarize_validation_findings_counts_warning_and_error():
    """Severity counts should be easy to aggregate for the run summary."""
    findings = validate_option_rows(
        pd.DataFrame([make_valid_row(option_type="CALL", has_valid_quote="yes")])
    )

    warnings, errors = summarize_validation_findings(findings)

    assert warnings == 1
    assert errors == 1

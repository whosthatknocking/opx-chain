"""Public and package-private option-chain integrity contract tests."""
# pylint: disable=duplicate-code,line-too-long,missing-function-docstring,protected-access

from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from opx_chain import _integrity_validation as validation

from opx_chain._integrity_validation import (
    collect_option_chain_frame_findings,
    provider_payload_to_frame,
    project_option_chain_integrity_summary,
    validate_option_chain_frame,
    validate_option_chain_provider_response,
)
from opx_chain.integrity import (
    OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
    OptionChainDataIntegrityError,
    OptionChainDatasetFacts,
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityBoundary,
    OptionChainIntegrityCode,
    OptionChainIntegrityFinding,
    OptionChainIntegritySeverity,
    OptionChainIntegrityStatus,
    OptionChainIntegritySummary,
    OptionChainSchemaCompatibilityError,
    OptionChainTickerTimeBounds,
    canonical_option_contract_key,
    compute_option_chain_dataset_facts,
)


def _frame(**overrides) -> pd.DataFrame:
    row = {
        "data_source": "synthetic-provider",
        "underlying_symbol": "SYNTH",
        "contract_symbol": "SYNTH260821C00100000",
        "option_type": "call",
        "expiration_date": "2026-08-21",
        "strike": 100.0,
        "underlying_price": 105.0,
        "underlying_price_time": "2026-08-21T15:30:00Z",
        "bid": 4.5,
        "ask": 4.7,
        "contract_size": 100,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_timestamp_memo_preserves_findings_and_summary(monkeypatch):
    values = ["2026-08-21T15:30:00Z", "2026-08-21T08:30:00-07:00",
              "08/21/2026", "invalid", "", None, pd.NaT,
              datetime(2026, 8, 21, tzinfo=timezone.utc), 123456, float("inf")]
    frame = pd.concat([_frame(underlying_price_time=value) for value in values] * 2)
    frame.iloc[0, frame.columns.get_loc("strike")] = float("nan")
    frame.iloc[1, frame.columns.get_loc("contract_symbol")] = "OTHER260821C00100000"
    boundary = OptionChainIntegrityBoundary.STORED_ARTIFACT
    actual = collect_option_chain_frame_findings(frame, boundary=boundary)
    monkeypatch.setattr(validation, "_timestamp_invalid_checker", lambda: lambda value: pd.isna(pd.to_datetime(value, utc=True, errors="coerce")))
    expected = collect_option_chain_frame_findings(frame, boundary=boundary)
    assert actual == expected
    checked = datetime(2026, 8, 21, tzinfo=timezone.utc)
    assert project_option_chain_integrity_summary(actual, total_rows=len(frame), checked_at=checked) == project_option_chain_integrity_summary(expected, total_rows=len(frame), checked_at=checked)


def test_timestamp_memo_is_bounded_exact_string_and_invocation_local(monkeypatch):
    original = pd.to_datetime
    calls = []

    def parse(value, **kwargs):
        calls.append(value)
        return original(value, **kwargs)

    monkeypatch.setattr(pd, "to_datetime", parse)
    check = validation._timestamp_invalid_checker()
    for _ in range(3):
        assert not check("2026-08-21")
        assert check("bad")
        assert not check(123)
    assert calls == ["2026-08-21", "bad", 123, 123, 123]
    for index in range(1025):
        check(f"invalid-{index}")
    before = len(calls)
    check("2026-08-21")
    validation._timestamp_invalid_checker()("2026-08-21")
    assert len(calls) == before + 2


def test_canonical_validation_parses_each_repeated_timestamp_once_per_pass(monkeypatch):
    original = pd.to_datetime
    calls = []

    def parse(value, **kwargs):
        calls.append(value)
        return original(value, **kwargs)

    monkeypatch.setattr(pd, "to_datetime", parse)
    frame = pd.concat([_frame()] * 100, ignore_index=True)
    for expected in (1, 2):
        collect_option_chain_frame_findings(frame, boundary=OptionChainIntegrityBoundary.EXPORT)
        assert len(calls) == expected


def test_provider_timestamp_memo_keeps_raw_errors_and_pass_local_scope(monkeypatch):
    original = pd.to_datetime
    calls = []

    def parse(value, **kwargs):
        calls.append(value)
        return original(value, **kwargs)

    monkeypatch.setattr(pd, "to_datetime", parse)
    raw = pd.DataFrame([{"strike": 100.0, "bid": 1.0, "ask": 2.0,
                         "lastTradeDate": "invalid"}] * 20)
    for expected in (1, 2):
        with pytest.raises(OptionChainDataIntegrityError) as caught:
            validate_option_chain_provider_response(raw, pd.DataFrame(), ticker="SYNTH", provider="yfinance")
        assert len(calls) == expected
        assert caught.value.summary.invalid_row_count == 20


def test_public_constants_and_enum_sets_are_exact():
    assert OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION == 1
    assert OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION == 1
    assert OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION == 1
    assert {item.value for item in OptionChainIntegrityStatus} == {"valid", "invalid", "unknown"}
    assert {item.value for item in OptionChainDatasetFactsStatus} == {"available", "unknown"}
    assert {item.value for item in OptionChainIntegrityBoundary} == {
        "provider_response", "pre_filter", "export", "serialized_artifact", "stored_artifact"
    }
    assert {item.value for item in OptionChainIntegrityCode} == {
        "RESPONSE_SHAPE_INVALID", "REQUIRED_IDENTITY_FIELD_MISSING",
        "CONTRACT_SYMBOL_INVALID", "CONTRACT_IDENTITY_MISMATCH",
        "UNSUPPORTED_CONTRACT_IDENTITY", "REQUIRED_FIELD_INVALID",
        "FIELD_VALUE_INVALID", "DUPLICATE_CONTRACT_SYMBOL",
        "DUPLICATE_CONTRACT_KEY", "DATASET_SCHEMA_INVALID",
        "SERIALIZATION_INTEGRITY_FAILED", "DATASET_CONTENT_HASH_MISMATCH",
        "OPTIONAL_FIELD_DEGRADED",
    }


def test_canonical_option_contract_key_normalizes_class_share_and_thousandths():
    assert canonical_option_contract_key(" brk.b ", "C", "412.1254", "2026-08-21") == (
        "BRKB", "call", Decimal("412.125"), date(2026, 8, 21)
    )
    assert canonical_option_contract_key("BRK-B", "put", 412.125, date(2026, 8, 21))[0] == "BRKB"


@pytest.mark.parametrize("strike", [0, -1, float("nan"), float("inf"), "100.0005"])
def test_canonical_option_contract_key_rejects_invalid_or_unrepresentable_strike(strike):
    with pytest.raises(ValueError):
        canonical_option_contract_key("SYNTH", "call", strike, "2026-08-21")


def test_finding_and_summary_are_bounded_immutable_and_round_trip():
    finding = OptionChainIntegrityFinding(
        severity=OptionChainIntegritySeverity.FATAL,
        boundary=OptionChainIntegrityBoundary.EXPORT,
        code=OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH,
        row_index=3,
        ticker="SYNTH\x00",
        contract_symbol="X" * 140,
        field="option_type",
        expected="call",
        actual="put",
    )
    source_counts = {finding.code: 1}
    summary = OptionChainIntegritySummary(
        schema_version=1,
        validator_version=1,
        status="invalid",
        checked_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
        dataset_id="dataset-1",
        content_hash="abc",
        provider="synthetic-provider",
        total_rows=4,
        invalid_row_count=1,
        fatal_finding_count=1,
        warning_finding_count=0,
        affected_ticker_count=1,
        counts_by_code=source_counts,
        samples=(finding,),
    )
    source_counts[finding.code] = 99
    assert summary.counts_by_code[finding.code] == 1
    assert "\x00" not in summary.samples[0].ticker
    assert len(summary.samples[0].contract_symbol) == 128
    assert OptionChainIntegritySummary.from_dict(summary.to_dict()) == summary


def test_projector_is_permutation_stable_exhaustive_and_sample_bounded():
    findings = []
    for row_index in range(8):
        findings.append(OptionChainIntegrityFinding(
            severity=OptionChainIntegritySeverity.FATAL,
            boundary=OptionChainIntegrityBoundary.EXPORT,
            code=(OptionChainIntegrityCode.FIELD_VALUE_INVALID
                  if row_index < 4 else OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH),
            row_index=row_index,
            ticker=f"T{row_index}",
            contract_symbol=f"C{row_index}",
            field="strike",
            expected="valid",
            actual="invalid",
        ))
    first = project_option_chain_integrity_summary(findings, total_rows=8,
        checked_at=datetime(2026, 8, 21, tzinfo=timezone.utc))
    second = project_option_chain_integrity_summary(list(reversed(findings)), total_rows=8,
        checked_at=first.checked_at)
    assert first.to_dict() == second.to_dict()
    assert first.fatal_finding_count == 8
    assert first.invalid_row_count == 8
    assert len(first.samples) == 4
    assert all(sum(item.code == code for item in first.samples) <= 2 for code in OptionChainIntegrityCode)


@pytest.mark.parametrize(
    ("overrides", "code", "field"),
    [
        ({"contract_symbol": "bad"}, OptionChainIntegrityCode.CONTRACT_SYMBOL_INVALID, "contract_symbol"),
        ({"option_type": "put"}, OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH, "option_type"),
        ({"strike": 101}, OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH, "strike"),
        ({"expiration_date": "2026-08-22"}, OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH, "expiration_date"),
        ({"underlying_symbol": "OTHER"}, OptionChainIntegrityCode.CONTRACT_IDENTITY_MISMATCH, "underlying_symbol"),
        ({"contract_symbol": "SYNTH1260821C00100000"}, OptionChainIntegrityCode.UNSUPPORTED_CONTRACT_IDENTITY, "contract_symbol"),
        ({"contract_size": 150}, OptionChainIntegrityCode.UNSUPPORTED_CONTRACT_IDENTITY, "contract_size"),
        ({"bid": -1}, OptionChainIntegrityCode.FIELD_VALUE_INVALID, "bid"),
        ({"bid": 5, "ask": 4}, OptionChainIntegrityCode.FIELD_VALUE_INVALID, "bid_ask"),
        ({"underlying_price_time": "not-a-time"}, OptionChainIntegrityCode.FIELD_VALUE_INVALID, "underlying_price_time"),
    ],
)
def test_complete_frame_validation_rejects_identity_and_value_corruption(overrides, code, field):
    findings = collect_option_chain_frame_findings(
        _frame(**overrides), boundary=OptionChainIntegrityBoundary.PRE_FILTER
    )
    assert any(item.code is code and item.field == field for item in findings)


def test_duplicate_symbol_and_semantic_key_are_both_fatal():
    duplicate = pd.concat([_frame(), _frame()], ignore_index=True)
    findings = collect_option_chain_frame_findings(
        duplicate, boundary=OptionChainIntegrityBoundary.EXPORT
    )
    assert sum(item.code is OptionChainIntegrityCode.DUPLICATE_CONTRACT_SYMBOL for item in findings) == 2
    assert sum(item.code is OptionChainIntegrityCode.DUPLICATE_CONTRACT_KEY for item in findings) == 2


def test_one_fatal_row_raises_typed_error_for_whole_dataset():
    frame = pd.concat([_frame(), _frame(
        contract_symbol="OTHER260821P00095000",
        underlying_symbol="OTHER", option_type="call", strike=95,
    )], ignore_index=True)
    with pytest.raises(OptionChainDataIntegrityError) as captured:
        validate_option_chain_frame(frame, boundary=OptionChainIntegrityBoundary.EXPORT)
    assert captured.value.summary.status == "invalid"
    assert captured.value.summary.invalid_row_count == 1
    assert captured.value.summary.total_rows == 2
    assert "OTHER" in {item.ticker for item in captured.value.summary.samples}


def test_clean_frame_and_class_share_root_validate():
    summary = validate_option_chain_frame(
        _frame(
            underlying_symbol="BRK.B",
            contract_symbol="BRKB260821C00400000",
            strike=400,
        ),
        boundary=OptionChainIntegrityBoundary.EXPORT,
    )
    assert summary.status == "valid"
    assert summary.fatal_finding_count == 0


def test_dataset_facts_are_sorted_content_bound_and_round_trip():
    frame = pd.concat([
        _frame(underlying_symbol="ZZZ", contract_symbol="ZZZ260918P00090000",
               option_type="put", strike=90, expiration_date="2026-09-18",
               underlying_price_time="2026-08-21T16:00:00Z"),
        _frame(underlying_price_time="2026-08-21T15:00:00Z"),
    ], ignore_index=True)
    facts = compute_option_chain_dataset_facts(frame, content_hash="sha256")
    assert facts.tickers == ("SYNTH", "ZZZ")
    assert facts.expiration_dates == (date(2026, 8, 21), date(2026, 9, 18))
    assert facts.underlying_price_time_bounds[0] == OptionChainTickerTimeBounds(
        "SYNTH",
        datetime(2026, 8, 21, 15, tzinfo=timezone.utc),
        datetime(2026, 8, 21, 15, tzinfo=timezone.utc),
    )
    assert OptionChainDatasetFacts.from_dict(facts.to_dict()) == facts


def test_schema_compatibility_error_rejects_bool_versions_and_keeps_attributes():
    error = OptionChainSchemaCompatibilityError("dataset", 2, (1, 2))
    assert error.dataset_id == "dataset"
    assert error.declared_version == 2
    assert error.supported_versions == (1, 2)
    with pytest.raises(ValueError):
        _ = OptionChainSchemaCompatibilityError("dataset", True, (1,))


def test_provider_response_rejects_malformed_present_optional_numeric():
    calls = _frame().copy()
    calls.loc[0, "implied_volatility"] = "not-a-number"

    with pytest.raises(OptionChainDataIntegrityError) as captured:
        validate_option_chain_provider_response(
            calls,
            pd.DataFrame(),
            ticker="SYNTH",
            provider="synthetic-provider",
        )

    assert (
        captured.value.summary.counts_by_code[
            OptionChainIntegrityCode.FIELD_VALUE_INVALID
        ]
        == 1
    )
    assert captured.value.summary.samples[0].boundary is (
        OptionChainIntegrityBoundary.PROVIDER_RESPONSE
    )


def test_provider_response_rejects_duplicate_independent_identifiers():
    calls = pd.concat((_frame(), _frame()), ignore_index=True)

    with pytest.raises(OptionChainDataIntegrityError) as captured:
        validate_option_chain_provider_response(
            calls,
            pd.DataFrame(),
            ticker="SYNTH",
            provider="synthetic-provider",
        )

    assert (
        captured.value.summary.counts_by_code[
            OptionChainIntegrityCode.DUPLICATE_CONTRACT_SYMBOL
        ]
        == 2
    )


def test_provider_payload_alignment_raises_typed_shape_error():
    with pytest.raises(OptionChainDataIntegrityError) as captured:
        provider_payload_to_frame(
            {"optionSymbol": ["ONE", "TWO"], "strike": [100]},
            ticker="SYNTH",
            provider="synthetic-provider",
        )

    assert captured.value.summary.counts_by_code == {
        OptionChainIntegrityCode.RESPONSE_SHAPE_INVALID: 1
    }
    assert captured.value.summary.samples[0].boundary is (
        OptionChainIntegrityBoundary.PROVIDER_RESPONSE
    )

"""Offline receipts retain native quotes and bind to the saved dataset."""

from datetime import date
import hashlib
import json

import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain import SCHEMA_VERSION
from opx_chain.export import prepare_export_frame
from opx_chain.normalize import enrich_option_frame
from opx_chain.providers.marketdata import MarketDataProvider
from opx_chain.schema import SOURCE_QUOTE_COLUMN
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary
from opx_chain.valuation_evidence import (
    build_option_valuation_evidence, parse_option_valuation_evidence,
)


@pytest.fixture(name="retained")
def make_retained(tmp_path, monkeypatch):
    """Round-trip real provider normalization through a disposable dataset."""
    config = make_runtime_config(today=date(2026, 11, 5))
    for module in ("normalize", "metrics"):
        monkeypatch.setattr(f"opx_chain.{module}.get_runtime_config", lambda: config)
    raw = pd.DataFrame({"optionSymbol": ["INTC271217P00090000"], "strike": [90.],
        "underlyingPrice": [60.], "updated": ["2026-11-05T19:56:46Z"],
        "bid": [38.1], "ask": [39.2], "last": [38.7], "iv": [.65],
        "delta": [-.62], "volume": [200], "openInterest": [3000],
        "intrinsicValue": [30.], "extrinsicValue": [8.65],
        "credentialed_url": ["secret-never-retain"]})
    source = MarketDataProvider().normalize_option_frame(raw, 54.46, "2027-12-17", "put", "INTC")
    source = enrich_option_frame(source, 54.46, pd.Timestamp("2026-11-05T19:58:00Z"))
    canonical = prepare_export_frame([source])
    backend = FilesystemBackend(tmp_path / "runs", tmp_path / "debug")
    run = backend.create_run(RunContext("marketdata", ("INTC",), "config", "positions"))
    record = backend.write_dataset(run, DatasetWrite(canonical, "marketdata", SCHEMA_VERSION))
    backend.finalize_run(run, RunSummary("complete"))
    dataset = backend.load_validated_option_chain_dataset(record.dataset_id)
    content = build_option_valuation_evidence(source, canonical,
        dataset_id=record.dataset_id, dataset_content_hash=record.content_hash,
        provider="marketdata", assessed_at="2026-11-05T19:58:00Z", positions_content_hash="abc")
    return source, dataset, content


def test_saved_receipt_retains_native_and_canonical_inputs(retained):
    """Raw paired values survive even though canonical export drops extras."""
    source, dataset, content = retained
    assert SOURCE_QUOTE_COLUMN not in dataset.frame
    receipt = parse_option_valuation_evidence(content, dataset)
    row = receipt["rows"][0]
    assert row["source_quote"]["underlying_price"] == 60.
    assert row["source_quote"]["provider_intrinsic_value"] == 30.
    assert row["source_quote"]["provider_extrinsic_value"] == 8.65
    assert row["canonical"]["bid"] == 38.1
    assert row["canonical"]["ask"] == 39.2
    assert row["canonical"]["days_to_expiration"] == 407
    assert row["canonical"]["underlying_price_time"] == row["canonical"]["option_quote_time"]
    assert "secret-never-retain" not in content.decode()
    assert receipt["positions_content_hash"] == "abc"
    assert source.iloc[0].underlying_price == 60.


def test_receipt_filters_unapproved_source_keys(retained):
    """The serialization boundary enforces the allowlist a second time."""
    source, dataset, _ = retained
    source.iloc[0][SOURCE_QUOTE_COLUMN]["Authorization"] = "secret-never-retain"
    content = build_option_valuation_evidence(source, dataset.frame,
        dataset_id=dataset.handle.dataset_id, dataset_content_hash=dataset.handle.content_hash,
        provider="marketdata", assessed_at="2026-11-05T19:58:00Z")
    assert "secret-never-retain" not in content.decode()
    parse_option_valuation_evidence(content, dataset)


def test_receipt_rejects_modified_bytes(retained):
    """Unbound edits must not become accepted offline evidence."""
    _, dataset, content = retained
    payload = json.loads(content)
    payload["rows"][0]["source_quote"]["underlying_price"] = 61.
    with pytest.raises(ValueError, match="checksum"):
        parse_option_valuation_evidence(json.dumps(payload).encode(), dataset)


@pytest.mark.parametrize("change", ["dataset", "provider", "hash", "canonical", "native",
                                    "identity", "coverage", "kind", "schema"])
def test_receipt_semantic_checks_survive_recomputed_checksum(retained, change):
    """Binding and source equality, not a self-checksum alone, establish trust."""
    _, dataset, content = retained
    payload = json.loads(content)
    if change in {"dataset", "provider", "hash"}:
        key = {"dataset": "dataset_id", "provider": "provider",
               "hash": "dataset_content_hash"}[change]
        payload[key] = "wrong"
    elif change == "coverage":
        payload["rows"] = []
    elif change == "schema":
        payload["schema_version"] = True
    elif change == "kind":
        payload["rows"][0]["source_quote"]["kind"] = "INVENTED"
    elif change == "identity":
        payload["rows"][0]["source_quote"]["contract_symbol"] = "wrong"
    else:
        key = "canonical" if change == "canonical" else "source_quote"
        payload["rows"][0][key]["underlying_price"] = 61.
    payload.pop("evidence_sha256")
    payload["evidence_sha256"] = hashlib.sha256(json.dumps(payload, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    with pytest.raises(ValueError):
        parse_option_valuation_evidence(json.dumps(payload).encode(), dataset)


def test_builder_rejects_source_corruption_before_publication(retained):
    """Source-to-canonical disagreement fails before any receipt is emitted."""
    source, dataset, _ = retained
    wrong = source.copy(deep=True)
    wrong.loc[0, "underlying_price"] = 61.
    with pytest.raises(ValueError, match="canonical values"):
        build_option_valuation_evidence(source, wrong, dataset_id=dataset.handle.dataset_id,
            dataset_content_hash=dataset.handle.content_hash, provider="marketdata",
            assessed_at="2026-11-05T19:58:00Z")

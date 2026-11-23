"""Producer-owned portable export/import, with no provider or strategy effects."""

from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import shutil

import pandas as pd
import pytest

from conftest import make_option_chain_frame
from opx_chain import SCHEMA_VERSION
from opx_chain.iv_history import IVHistoryStore
from opx_chain.price_history import PriceHistoryStore
from opx_chain.producer_bundle import (
    HistorySnapshotWindow,
    capture_history_component,
    encode_history_component,
    export_producer_bundle,
    import_producer_bundle,
)
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary, TickerFetchResult
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend

SCOPE = {"provider": "marketdata", "entitlement": "test-credential-scope"}
PREFIX = b"OPX-PRODUCER-1\n"


@pytest.fixture(name="retained", params=["fs", "sqlite"])
def _retained(tmp_path, request):
    root = tmp_path / "runs"
    if request.param == "fs":
        backend = FilesystemBackend(root, tmp_path / "debug")
    else:
        backend = SqliteIndexedBackend(tmp_path / "fixture.db", root, tmp_path / "debug")
    tickers = ("SYNTH",)
    run = backend.create_run(RunContext(SCOPE["provider"], tickers, "test", "fixture"))
    frame = make_option_chain_frame(rows=2, ticker=tickers[0], provider=SCOPE["provider"])
    backend.record_ticker_result(run, TickerFetchResult(tickers[0], 2, 2, 2, 0, 1, "ok"))
    dataset = backend.write_dataset(run, DatasetWrite(frame, SCOPE["provider"], SCHEMA_VERSION))
    backend.finalize_run(run, RunSummary(status="complete"))
    return backend, root, dataset, datetime.now(timezone.utc)


def snapshot(retained, **changes):
    """Export fixture bytes with explicit component availability."""
    backend, root, dataset, cutoff = retained
    components = {
        "event_snapshot": "not_configured",
        "price_context": "missing",
        "iv_history": "compacted",
        "price_history": "missing",
        **changes,
    }
    return export_producer_bundle(
        backend,
        dataset.dataset_id,
        runs_root=root,
        as_of=cutoff,
        entitlement=SCOPE["entitlement"],
        components=components,
    )


def test_round_trip_retains_original_bytes_and_provenance_without_source_directory(retained):
    """Offline readers can recover verified artifacts after the source is removed."""
    backend, root, dataset, cutoff = retained
    before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    day = cutoff.date().isoformat()
    history = encode_history_component(
        "price_history",
        provider=SCOPE["provider"],
        observed_at=cutoff,
        records=[
            {
                "provider": SCOPE["provider"],
                "ticker": "SYNTH",
                "date": day,
                "open": 100,
                "high": 102,
                "low": 98,
                "close": 101,
                "volume": 1000,
            }
        ],
    )
    content = snapshot(retained, price_history=history)
    assert snapshot(retained, price_history=history) == content
    assert {path: path.read_bytes() for path in before} == before
    original = backend.load_validated_option_chain_dataset(dataset.dataset_id)
    original_hash = sha256(Path(dataset.location).read_bytes()).hexdigest()
    shutil.rmtree(root)
    bundle = import_producer_bundle(
        content,
        dataset_id=dataset.dataset_id,
        **SCOPE,
        required=frozenset({"price_history"}),
        expected_sha256=sha256(content).hexdigest(),
    )
    actual = bundle.load_validated_option_chain_dataset(dataset.dataset_id)
    pd.testing.assert_frame_equal(actual.frame, original.frame)
    assert actual.handle.content_hash == original_hash and actual.handle.location.startswith(
        "bundle://"
    )
    (listed,) = bundle.list_datasets(limit=1, provider=SCOPE["provider"], ticker="SYNTH")
    assert (listed.dataset_id, listed.integrity_status, listed.dataset_facts) == (
        dataset.dataset_id,
        actual.handle.integrity_status,
        actual.dataset_facts,
    )
    assert bundle.list_datasets(ticker="OTHER") == []
    assert bundle.get_run(dataset.run_id).tickers == ("SYNTH",)
    assert bundle.get_ticker_results(dataset.run_id)[0].kept_row_count == 2
    assert bundle.components["price_history"] == history
    assert bundle.capabilities == {
        "price_history": "available",
        "iv_history": "compacted",
        "price_context": "missing",
        "event_snapshot": "not_configured",
    }
    with pytest.raises(KeyError):
        bundle.get_dataset("other")


def change_manifest(content, change):
    """Replace envelope metadata while preserving the original member bytes."""
    offset = len(PREFIX)
    size = int.from_bytes(content[offset : offset + 4], "big")
    manifest = json.loads(content[offset + 4 : offset + 4 + size])
    change(manifest)
    raw = json.dumps(manifest).encode()
    return PREFIX + len(raw).to_bytes(4, "big") + raw + content[offset + 4 + size :]


@pytest.mark.parametrize(
    "change",
    [
        lambda manifest: manifest.update(schema_version=True),
        lambda manifest: manifest.update(producer_version="0.8.2"),
        lambda manifest: manifest["members"][0].update(name="../chain"),
        lambda manifest: manifest["members"].append(manifest["members"][0]),
        lambda manifest: manifest["members"][0].update(size=True),
        lambda manifest: manifest["dataset"].update(row_count=True),
        lambda manifest: manifest["dataset"].update(row_count=250001),
        lambda manifest: manifest["dataset"]["integrity"].update(integrity_status="unknown"),
        lambda manifest: manifest["fetch_run"].update(status="running"),
        lambda manifest: manifest["capabilities"].update(price_history="available"),
    ],
)
def test_rejects_ambiguous_inventory_and_provenance(retained, change):
    """An inconsistent manifest cannot authorize a bundle read."""
    raw = change_manifest(snapshot(retained), change)
    with pytest.raises(ValueError):
        import_producer_bundle(raw, dataset_id=retained[2].dataset_id, **SCOPE)


def test_scope_corruption_and_absence_are_not_fallback_permissions(retained):
    """Selection mismatches and corrupted bytes reject rather than fall back."""
    content = snapshot(retained)
    selection = {**SCOPE, "dataset_id": retained[2].dataset_id}
    for changed in (
        {"entitlement": "foreign"},
        {"provider": "massive"},
        {"dataset_id": "foreign"},
        {"expected_sha256": "0" * 64},
        {"required": frozenset({"event_snapshot"})},
    ):
        with pytest.raises(ValueError):
            import_producer_bundle(content, **(selection | changed))
    for changed in (content + b"extra", content[:-1], content[:-2] + b"xx"):
        with pytest.raises(ValueError):
            import_producer_bundle(changed, **selection)


@pytest.mark.parametrize(
    "change",
    [
        {"provider": "foreign"},
        {"date": "2099-01-01"},
        {"high": 2},
        {"volume": True},
        {"close": float("inf")},
    ],
)
def test_history_validation_rejects_bad_rows(change):
    """Invalid history rows are rejected before export."""
    observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
    row = {
        "provider": SCOPE["provider"],
        "ticker": "SYNTH",
        "date": "2026-07-20",
        "open": 100,
        "high": 102,
        "low": 98,
        "close": 101,
        "volume": 1000,
    }
    with pytest.raises(ValueError):
        encode_history_component(
            "price_history",
            provider=SCOPE["provider"],
            observed_at=observed,
            records=[row | change],
        )


def test_history_export_bounds_provider_tickers_dates_and_exact_fetch_clock(tmp_path):
    """Capture only scoped observations available at the selected instant."""
    day = date(2026, 7, 20)
    observed = datetime(2026, 7, 21, 20, tzinfo=timezone.utc)
    later = observed + timedelta(microseconds=1)
    window = HistorySnapshotWindow("marketdata", ("SYNTH",), day, day, observed)
    prices = PriceHistoryStore(tmp_path / "prices.db")
    iv = IVHistoryStore(tmp_path / "volatility.db")
    try:
        bars = pd.DataFrame([{"date": day.isoformat(), "high": 110.0, "low": 90.0, "close": 100.0}])
        for provider, ticker, fetched_at in (
            ("marketdata", "SYNTH", observed),
            ("marketdata", "OTHER", observed),
            ("yfinance", "SYNTH", observed),
        ):
            prices.upsert_bars(
                provider=provider, ticker=ticker, history=bars, fetched_at=fetched_at
            )
        captured = json.loads(capture_history_component("price_history", prices, window))
        assert [(row["ticker"], row["date"], row["fetched_at"]) for row in captured["records"]] == [
            ("SYNTH", day.isoformat(), observed.isoformat())
        ]
        assert captured["records"][0]["open"] is None and captured["records"][0]["volume"] is None
        prices.upsert_bars(provider="marketdata", ticker="SYNTH", history=bars, fetched_at=later)
        assert capture_history_component("price_history", prices, window) == "missing"

        row = {
            "provider": "marketdata",
            "ticker": "SYNTH",
            "observation_date": day.isoformat(),
            "option_type": "ALL",
            "delta_bucket": "ALL",
            "dte_bucket": "ALL",
            "representative_iv": 0.4,
            "observation_count": 12,
            "dataset_id": "dataset",
            "run_id": "run",
            "source_created_at": observed.isoformat(),
            "fetched_at": observed.isoformat(),
        }
        iv.upsert_observations(
            pd.DataFrame(
                [
                    row,
                    {**row, "option_type": "CALL", "fetched_at": later.isoformat()},
                    {**row, "provider": "yfinance"},
                    {**row, "ticker": "OTHER"},
                ]
            )
        )
        captured = json.loads(capture_history_component("iv_history", iv, window))
        assert captured["records"] == [row]
    finally:
        iv.close()
        prices.close()


def test_parquet_inventory_is_checked_before_decode(tmp_path, monkeypatch):
    """Reject oversized Parquet before building an in-memory frame."""
    pytest.importorskip("pyarrow")
    backend = FilesystemBackend(tmp_path / "runs", tmp_path / "debug")
    run = backend.create_run(RunContext(SCOPE["provider"], ("SYNTH",), "test", "fixture"))
    frame = make_option_chain_frame(rows=2, ticker="SYNTH", provider=SCOPE["provider"])
    dataset = backend.write_dataset(
        run, DatasetWrite(frame, SCOPE["provider"], SCHEMA_VERSION, format="parquet")
    )
    backend.finalize_run(run, RunSummary(status="complete"))
    retained = backend, tmp_path / "runs", dataset, datetime.now(timezone.utc)
    content = snapshot(retained)
    opened = import_producer_bundle(content, dataset_id=dataset.dataset_id, **SCOPE)
    assert opened.get_dataset(dataset.dataset_id).format == "parquet"
    monkeypatch.setattr("opx_chain.producer_bundle._MAX_PARQUET_DECODED_BYTES", 1)
    with pytest.raises(ValueError, match="decode budget"):
        import_producer_bundle(content, dataset_id=dataset.dataset_id, **SCOPE)

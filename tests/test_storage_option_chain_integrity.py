"""Cross-backend publication and validated-load integrity contract tests."""
# pylint: disable=protected-access

from pathlib import Path
from dataclasses import replace
from types import SimpleNamespace

import pytest

from conftest import make_option_chain_frame
from opx_chain import SCHEMA_VERSION
from opx_chain.integrity import (
    OptionChainDataIntegrityError,
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityCode,
    OptionChainIntegrityStatus,
)
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage._disk import content_hash_for_bytes
from opx_chain.storage.integrity import validate_stored_option_chain_snapshot
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary
from opx_chain.storage.serializers import get_serializer
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _memory_backend(_tmp_path: Path):
    return MemoryBackend()


def _filesystem_backend(tmp_path: Path):
    return FilesystemBackend(tmp_path / "runs", tmp_path / "debug")


def _sqlite_backend(tmp_path: Path):
    return SqliteIndexedBackend(
        tmp_path / "chain.db",
        tmp_path / "runs",
        tmp_path / "debug",
    )


BACKENDS = (_memory_backend, _filesystem_backend, _sqlite_backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize("format_name", ["csv", "parquet"])
@pytest.mark.parametrize("field,value", [
    ("underlying_price", 126.), ("bid", 3.1), ("ask", 3.3),
    ("option_quote_time", "2026-08-21T15:31:00Z"),
    ("underlying_price_time", "2026-08-21T15:31:00Z"),
    ("delta", .5), ("days_to_expiration", 408), ("contract_size", None),
    ("option_quote_time", None), ("option_quote_time", "DROP_COLUMN"),
])
def test_serialized_valuation_input_changes_are_rejected(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    backend_factory, format_name, field, value, tmp_path, monkeypatch,
):
    """Plausible quote corruption must fail even when identity remains valid."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(RunContext("synthetic-provider", ("SYNTH",), "config", "positions"))
    frame = make_option_chain_frame(rows=1, ticker="SYNTH", provider="synthetic-provider")
    frame["option_quote_time"] = "2026-08-21T15:30:00Z"
    frame["delta"] = .4
    frame["days_to_expiration"] = 407
    serializer = get_serializer(format_name)

    def corrupt(content):
        restored = serializer.deserialize_bytes(content)
        if value == "DROP_COLUMN":
            return restored.drop(columns=field)
        restored.loc[0, field] = value
        return restored

    monkeypatch.setattr("opx_chain.storage.integrity.get_serializer", lambda _format:
        SimpleNamespace(serialize_bytes=serializer.serialize_bytes, deserialize_bytes=corrupt))
    with pytest.raises(OptionChainDataIntegrityError) as error:
        backend.write_dataset(run_id, DatasetWrite(frame, "synthetic-provider", SCHEMA_VERSION,
                                                  format=format_name))
    assert "SERIALIZATION_INTEGRITY_FAILED" in error.value.summary.counts_by_code
    assert backend.list_datasets() == []


def _write(backend, *, frame=None):
    run_id = backend.create_run(
        RunContext("synthetic-provider", ("SYNTH",), "config", "positions")
    )
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            frame if frame is not None else make_option_chain_frame(
                rows=2,
                ticker="SYNTH",
                provider="synthetic-provider",
            ),
            "synthetic-provider",
            SCHEMA_VERSION,
        ),
    )
    return run_id, record


def _replace_bytes(backend, record, content: bytes) -> None:
    if isinstance(backend, MemoryBackend):
        backend._dataset_bytes[record.dataset_id] = content
    else:
        Path(record.location).write_bytes(content)


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_publication_is_hidden_until_complete_and_metadata_is_valid(
    backend_factory,
    tmp_path,
):
    """Only completed runs expose datasets with valid projected metadata."""
    backend = backend_factory(tmp_path)
    run_id, record = _write(backend)

    assert backend.list_datasets() == []
    with pytest.raises(KeyError):
        backend.get_dataset(record.dataset_id)

    backend.finalize_run(run_id, RunSummary("complete"))
    visible = backend.list_datasets(
        integrity_status=OptionChainIntegrityStatus.VALID,
        dataset_facts_status=OptionChainDatasetFactsStatus.AVAILABLE,
    )
    assert [item.dataset_id for item in visible] == [record.dataset_id]
    handle = backend.get_dataset(record.dataset_id)
    assert handle.integrity_status is OptionChainIntegrityStatus.VALID
    assert handle.integrity_summary.content_hash == handle.content_hash
    assert handle.dataset_facts.content_hash == handle.content_hash


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_validated_load_returns_fresh_frame_from_the_checked_snapshot(
    backend_factory,
    tmp_path,
):
    """Validated loads return a fresh frame from the bytes that were checked."""
    backend = backend_factory(tmp_path)
    run_id, record = _write(backend)
    backend.finalize_run(run_id, RunSummary("complete"))

    first = backend.load_validated_option_chain_dataset(record.dataset_id)
    first.frame.loc[0, "strike"] = 999
    second = backend.load_validated_option_chain_dataset(record.dataset_id)

    assert second.frame.loc[0, "strike"] == 100
    assert second.handle.content_hash == second.integrity.content_hash
    assert second.dataset_facts.content_hash == second.handle.content_hash


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_hash_mutation_persists_invalid_state_without_adopting_bytes(
    backend_factory,
    tmp_path,
):
    """Mutated bytes fail closed and never replace the committed hash identity."""
    backend = backend_factory(tmp_path)
    run_id, record = _write(backend)
    backend.finalize_run(run_id, RunSummary("complete"))
    _replace_bytes(backend, record, b"changed")

    with pytest.raises(OptionChainDataIntegrityError) as captured:
        backend.load_validated_option_chain_dataset(record.dataset_id)

    assert (
        captured.value.summary.counts_by_code[
            OptionChainIntegrityCode.DATASET_CONTENT_HASH_MISMATCH
        ]
        == 1
    )
    assert backend.list_datasets(
        integrity_status=OptionChainIntegrityStatus.VALID
    ) == []
    invalid = backend.list_datasets(
        integrity_status=OptionChainIntegrityStatus.INVALID,
        dataset_facts_status=OptionChainDatasetFactsStatus.UNKNOWN,
    )
    assert [item.dataset_id for item in invalid] == [record.dataset_id]


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_fatal_frame_prevents_any_dataset_record_or_visibility(
    backend_factory,
    tmp_path,
):
    """Fatal semantic findings prevent storage and discovery on every backend."""
    backend = backend_factory(tmp_path)
    frame = make_option_chain_frame(
        rows=1,
        ticker="SYNTH",
        provider="synthetic-provider",
    )
    frame.loc[0, "option_type"] = "put"
    run_id = backend.create_run(
        RunContext("synthetic-provider", ("SYNTH",), "config", "positions")
    )

    with pytest.raises(OptionChainDataIntegrityError):
        backend.write_dataset(
            run_id,
            DatasetWrite(frame, "synthetic-provider", SCHEMA_VERSION),
        )

    assert backend.get_run(run_id).dataset_id is None
    assert backend.list_datasets() == []


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_uniform_wrong_row_provider_prevents_publication(backend_factory, tmp_path):
    """Uniform rows from the wrong provider cannot acquire valid metadata."""
    backend = backend_factory(tmp_path)
    frame = make_option_chain_frame(rows=2, ticker="SYNTH", provider="other-provider")
    with pytest.raises(OptionChainDataIntegrityError) as captured:
        _write(backend, frame=frame)
    assert any(
        item.field == "data_source" and item.expected == "dataset provider synthetic-provider"
        for item in captured.value.summary.samples
    )
    assert backend.list_datasets() == []


def test_retained_matching_hash_cannot_bypass_provider_provenance():
    """A matching content hash does not validate contradictory provider metadata."""
    backend = MemoryBackend()
    _run_id, record = _write(backend)
    content = get_serializer(record.format).serialize_bytes(
        make_option_chain_frame(rows=2, ticker="SYNTH", provider="synthetic-provider")
    )
    record = replace(
        record, provider="other-provider", content_hash=content_hash_for_bytes(content)
    )
    outcome = validate_stored_option_chain_snapshot(record, content)
    assert outcome.error is not None
    assert outcome.frame is None
    assert outcome.record.integrity_status is OptionChainIntegrityStatus.INVALID

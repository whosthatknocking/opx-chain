"""Portable, read-only handoff of one exact retained market-data selection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType
from typing import Any

from opx_chain import SCHEMA_VERSION
from opx_chain.event_data import EVENT_DATA_SCHEMA_VERSION, normalize_event_data_provider
from opx_chain.integrity import (
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityStatus,
    ValidatedOptionChainDataset,
)
from opx_chain.price_context import PRICE_CONTEXT_SCHEMA_VERSION, PriceContextStatus
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.history_snapshot import HistorySnapshotWindow
from opx_chain.storage.integrity import (
    record_integrity_from_mapping,
    record_integrity_to_dict,
    validate_option_chain_row_scope,
    validate_stored_option_chain_snapshot,
    validated_dataset_from_outcome,
)
from opx_chain.storage.models import DatasetHandle, DatasetRecord, RunRecord, TickerRunRecord
from opx_chain.storage.validation import (
    validate_dataset_id,
    validate_dataset_list_filters,
    validate_run_id,
)
from opx_chain.version import __version__

# Exact types keep booleans and mutable byte buffers out of the wire format.
# pylint: disable=unidiomatic-typecheck

PRODUCER_BUNDLE_SCHEMA_VERSION = 1
MAX_BUNDLE_BYTES = 32 * 1024 * 1024
_MAX_MANIFEST_BYTES = 256 * 1024
_MAX_CHAIN_ROWS = 250_000
_MAX_PARQUET_DECODED_BYTES = 256 * 1024 * 1024
_MAGIC = b"OPX-PRODUCER-1\n"
_COMPONENTS = ("event_snapshot", "iv_history", "price_context", "price_history")
_ABSENT = {"missing", "compacted", "not_configured"}
_HISTORY_KEYS = {
    "price_history": ("ticker", "date"),
    "iv_history": ("ticker", "observation_date", "option_type", "dte_bucket", "delta_bucket"),
}


def _encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _decode(raw: bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result = dict(items)
        if len(result) != len(items):
            raise ValueError("duplicate producer bundle JSON key")
        return result

    def invalid(value: str) -> None:
        raise ValueError(f"nonfinite producer value: {value}")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
        if not isinstance(value, dict):
            raise ValueError("producer value must be an object")
        _encode(value)  # Also rejects numeric overflow (for example 1e999).
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("invalid producer JSON") from exc
    return value


def _text(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 256
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError("invalid producer identity")
    return value


def _instant(value: Any) -> datetime:
    parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("producer timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


def _positive(value: Any) -> bool:
    return type(value) is int and value > 0


def _digest(content: bytes) -> str:
    return sha256(content).hexdigest()


def _read_dataset_bytes(root: Path, location: str) -> bytes:
    path = Path(location)
    if (
        not root.is_absolute()
        or root.resolve(strict=True) != root
        or not path.is_absolute()
        or path.resolve(strict=True) != path
        or not path.is_relative_to(root)
    ):
        raise ValueError("producer dataset is outside its canonical retained root")
    # Anchor every component, so a replaced parent cannot redirect the read.
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(root, directory_flags)
    try:
        *parents, name = path.relative_to(root).parts
        for component in parents:
            following = os.open(component, directory_flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
        flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
        with os.fdopen(os.open(name, flags, dir_fd=descriptor), "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BUNDLE_BYTES:
                raise ValueError("retained dataset is not a bounded regular file")
            raw = stream.read(MAX_BUNDLE_BYTES + 1)
            after = os.fstat(stream.fileno())
            if (
                len(raw) != before.st_size
                or before.st_size != after.st_size
                or before.st_ctime_ns != after.st_ctime_ns
                or len(raw) > MAX_BUNDLE_BYTES
            ):
                raise ValueError("retained dataset changed during export")
            return raw
    finally:
        os.close(descriptor)


def _validate_history(kind: str, payload: dict[str, Any], provider: str, cutoff: datetime) -> None:
    if (
        set(payload) != {"schema_version", "provider", "observed_at", "records"}
        or type(payload["schema_version"]) is not int
        or payload["schema_version"] != 1
        or payload["provider"] != provider
        or _instant(payload["observed_at"]) > cutoff
    ):
        raise ValueError("producer history schema, provider or cutoff differs")
    keys = _HISTORY_KEYS[kind]
    rows = payload["records"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("available history needs rows; declare absence explicitly")
    seen = set()
    observed = _instant(payload["observed_at"])
    for row in rows:
        if (
            not isinstance(row, dict)
            or any(key not in row for key in keys)
            or row.get("provider") != provider
        ):
            raise ValueError("invalid producer history row")
        identity = tuple(_text(row[key]) for key in keys)
        day = date.fromisoformat(identity[1])
        if (
            identity in seen
            or day > observed.date()
            or day.isoformat() != identity[1]
            or identity[0] != identity[0].upper()
        ):
            raise ValueError("duplicate, malformed or future producer observation")
        seen.add(identity)
        for clock in ("fetched_at", "source_created_at"):
            if row.get(clock) is not None and _instant(row[clock]) > observed:
                raise ValueError("producer history was collected after its cutoff")
        numeric = ("high", "low", "close") if kind == "price_history" else ("representative_iv",)
        if any(type(row.get(key)) not in (int, float) or row[key] < 0 for key in numeric):
            raise ValueError("invalid producer history value")
        if kind == "price_history":
            if (
                not 0 < row["low"] <= row["close"] <= row["high"]
                or any(
                    row.get(key) is not None
                    and (type(row[key]) not in (int, float) or row[key] < 0)
                    for key in ("open", "volume")
                )
                or row.get("open") is not None
                and not row["low"] <= row["open"] <= row["high"]
            ):
                raise ValueError("contradictory price bar")
        elif (
            identity[2] not in {"ALL", "CALL", "PUT"}
            or row["representative_iv"] <= 0
            or not _positive(row.get("observation_count"))
        ):
            raise ValueError("invalid producer IV aggregate")


def encode_history_component(
    kind: str, *, provider: str, observed_at: datetime, records: list[dict[str, Any]]
) -> bytes:
    """Encode selected logical rows from the producer's public history reads."""
    if kind not in _HISTORY_KEYS or observed_at.utcoffset() is None:
        raise ValueError("unsupported history kind or observation cutoff")
    payload = {
        "schema_version": 1,
        "provider": _text(provider),
        "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
        "records": records,
    }
    _validate_history(kind, payload, provider, observed_at)
    return _encode(payload)


def capture_history_component(kind: str, store: Any, window: HistorySnapshotWindow) -> bytes | str:
    """Capture existing scoped producer-store rows without refreshing history."""
    if kind not in _HISTORY_KEYS or not isinstance(window, HistorySnapshotWindow):
        raise ValueError("unsupported retained history selection")
    rows = store.snapshot_bundle_rows(window)
    return (
        encode_history_component(
            kind, provider=window.provider, observed_at=window.observed_at, records=rows
        )
        if rows
        else "missing"
    )


def _validate_component(kind: str, raw: bytes, provider: str, cutoff: datetime) -> None:
    payload = _decode(raw)
    if kind in _HISTORY_KEYS:
        _validate_history(kind, payload, provider, cutoff)
        return
    version = (
        EVENT_DATA_SCHEMA_VERSION if kind == "event_snapshot" else PRICE_CONTEXT_SCHEMA_VERSION
    )
    if (
        type(payload.get("schema_version")) is not int
        or payload["schema_version"] != version
        or _instant(payload.get("fetched_at")) > cutoff
    ):
        raise ValueError("producer snapshot schema or cutoff differs")
    rows = payload.get("records")
    if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError("producer snapshot needs records")
    tickers = [_text(row.get("ticker")) for row in rows]
    if len(set(tickers)) != len(tickers):
        raise ValueError("duplicate producer snapshot ticker")
    if kind == "event_snapshot":
        selected, resolved = normalize_event_data_provider(
            payload.get("provider"), chain_provider=provider
        )
        provider_matches = (selected, resolved) == (
            payload.get("provider"),
            payload.get("resolved_provider"),
        )
        if (
            payload.get("artifact_type") != "event_data_snapshot"
            or not provider_matches
            or payload.get("status") not in {"ready", "partial"}
            or date.fromisoformat(_text(payload.get("trading_date"))) > cutoff.date()
            or any(
                row.get("provider_status")
                not in ("ready", "no_known_event", "provider_error", "invalid_payload")
                or not isinstance(row.get("event_fields"), dict)
                for row in rows
            )
        ):
            raise ValueError("event snapshot is not reusable")
        _text(payload.get("event_snapshot_id"))
    elif payload.get("provider") != provider or any(
        row.get("price_context_staleness_status") not in tuple(PriceContextStatus) for row in rows
    ):
        raise ValueError("price context provider or status differs")
    _check_snapshot_observations(payload, cutoff)


def _check_snapshot_observations(value: Any, cutoff: datetime) -> None:
    if isinstance(value, dict):
        for name, item in value.items():
            if (
                name
                in {
                    "fetched_at",
                    "event_data_fetched_at",
                    "market_known_at",
                    "observed_at",
                    "recorded_at",
                }
                and item is not None
            ):
                if _instant(item) > cutoff:
                    raise ValueError("producer snapshot includes a future observation")
            elif name == "price_context_as_of" and item is not None:
                if date.fromisoformat(_text(item)) > cutoff.date():
                    raise ValueError("producer snapshot includes a future market session")
            else:
                _check_snapshot_observations(item, cutoff)
    elif isinstance(value, list):
        for item in value:
            _check_snapshot_observations(item, cutoff)


def _check_dataset_budget(record: DatasetRecord, content: bytes) -> None:
    if not 0 < record.row_count <= _MAX_CHAIN_ROWS:
        raise ValueError("producer dataset exceeds its row budget")
    if record.format == "csv" and content.count(b"\n") > _MAX_CHAIN_ROWS + 1:
        raise ValueError("producer CSV exceeds its line budget")
    if record.format == "parquet":
        from pyarrow import parquet  # pylint: disable=import-outside-toplevel

        try:
            metadata = parquet.read_metadata(BytesIO(content))
        except (ValueError, OSError) as exc:
            raise ValueError("invalid retained Parquet metadata") from exc
        if (
            metadata.num_rows != record.row_count
            or metadata.num_row_groups > _MAX_CHAIN_ROWS
            or sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
            > _MAX_PARQUET_DECODED_BYTES
        ):
            raise ValueError("producer Parquet exceeds its declared decode budget")


@dataclass(frozen=True)
class ProducerBundle:  # pylint: disable=too-many-instance-attributes
    """Verified reader; location is a relocatable identity, never a host path."""

    sha256: str
    provider: str
    entitlement: str
    dataset_id: str
    as_of: datetime
    history_as_of: datetime | None
    capabilities: Mapping[str, str]
    components: Mapping[str, bytes]
    _dataset: DatasetRecord
    _content: bytes
    _run: RunRecord
    _ticker_results: tuple[TickerRunRecord, ...]

    def load_validated_option_chain_dataset(self, dataset_id: str) -> ValidatedOptionChainDataset:
        """Read only this dataset, revalidating its exact original bytes."""
        if dataset_id != self.dataset_id:
            raise KeyError("dataset is not in the selected producer bundle")
        outcome = validate_stored_option_chain_snapshot(
            self._dataset, self._content, checked_at=self.as_of
        )
        validate_option_chain_row_scope(
            outcome.record, ticker_results=list(self._ticker_results) or None
        )
        return validated_dataset_from_outcome(outcome)

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return the selected dataset's validated portable handle."""
        return self.load_validated_option_chain_dataset(dataset_id).handle

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
        integrity_status: OptionChainIntegrityStatus | None = None,
        dataset_facts_status: OptionChainDatasetFactsStatus | None = None,
    ) -> list[DatasetRecord]:
        """Filter the selected dataset only; never discover external history."""
        filters = validate_dataset_list_filters(
            limit=limit,
            provider=provider,
            since=since,
            until=until,
            ticker=ticker,
            integrity_status=integrity_status,
            dataset_facts_status=dataset_facts_status,
        )
        record = self._dataset
        identity_filters = (
            (filters.provider, record.provider),
            (filters.integrity_status, record.integrity_status),
            (filters.dataset_facts_status, record.dataset_facts_status),
        )
        if any(
            expected is not None and expected != actual for expected, actual in identity_filters
        ):
            return []
        if (
            filters.since is not None
            and record.created_at < filters.since
            or filters.until is not None
            and record.created_at > filters.until
        ):
            return []
        if filters.ticker is not None and filters.ticker not in self._run.tickers:
            return []
        return [replace(record)][: filters.limit]

    def get_run(self, run_id: str) -> RunRecord:
        """Return provenance for the retained fetch run."""
        if run_id != self._run.run_id:
            raise KeyError("run is not in the selected producer bundle")
        return replace(self._run)

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return the retained per-ticker fetch outcomes."""
        self.get_run(run_id)
        return [replace(row) for row in self._ticker_results]


def _run_metadata(
    data: Any, ticker_rows: Any, dataset: DatasetRecord, cutoff: datetime
) -> tuple[RunRecord, tuple[TickerRunRecord, ...]]:
    fields = {
        "run_id",
        "started_at",
        "finished_at",
        "status",
        "provider",
        "tickers",
        "config_fingerprint",
        "positions_fingerprint",
        "dataset_id",
        "error_summary",
        "script_version",
    }
    if not isinstance(data, dict) or data.keys() != fields:
        raise ValueError("producer fetch provenance differs from dataset")
    if (
        (data["run_id"], data["dataset_id"], data["provider"], data["status"])
        != (dataset.run_id, dataset.dataset_id, dataset.provider, "complete")
        or not isinstance(data["tickers"], list)
        or not isinstance(ticker_rows, list)
    ):
        raise ValueError("producer fetch provenance differs from dataset")
    started, finished = _instant(data["started_at"]), _instant(data["finished_at"])
    if not started <= dataset.created_at <= finished <= cutoff:
        raise ValueError("producer dataset or fetch finished outside its retained cutoff")
    tickers = tuple(_text(ticker) for ticker in data["tickers"])
    if len(set(tickers)) != len(tickers):
        raise ValueError("duplicate requested ticker")
    run = RunRecord(**{**data, "started_at": started, "finished_at": finished, "tickers": tickers})
    results = []
    seen = set()
    count_fields = {
        "raw_row_count",
        "normalized_row_count",
        "kept_row_count",
        "filtered_row_count",
        "expiration_count",
    }
    for row in ticker_rows:
        if not isinstance(row, dict) or row.keys() != count_fields | {
            "run_id",
            "ticker",
            "status",
            "error_summary",
        }:
            raise ValueError("invalid retained per-ticker producer provenance")
        if (
            row["run_id"] != run.run_id
            or _text(row["ticker"]) in seen
            or row["ticker"] not in tickers
            or row["status"] not in {"ok", "ok_with_warnings", "skipped", "error"}
            or any(type(row[key]) is not int or row[key] < 0 for key in count_fields)
        ):
            raise ValueError("invalid retained per-ticker producer provenance")
        seen.add(row["ticker"])
        results.append(TickerRunRecord(**row))
    return run, tuple(results)


def _read_manifest(content: bytes, provider: str, entitlement: str) -> tuple[dict, int]:
    start = len(_MAGIC)
    length = int.from_bytes(content[start : start + 4], "big")
    offset = start + 4 + length
    if not 0 < length <= _MAX_MANIFEST_BYTES or offset > len(content):
        raise ValueError("invalid producer manifest length")
    manifest = _decode(content[start + 4 : offset])
    fields = {
        "schema_version",
        "distribution",
        "producer_version",
        "provider",
        "entitlement",
        "as_of",
        "history_as_of",
        "dataset",
        "fetch_run",
        "ticker_results",
        "members",
        "capabilities",
    }
    if set(manifest) != fields:
        raise ValueError("producer bundle version or scope differs")
    if (
        type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != PRODUCER_BUNDLE_SCHEMA_VERSION
        or manifest["distribution"] != "opx-chain"
        or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", _text(manifest["producer_version"]))
        or tuple(map(int, manifest["producer_version"].split("."))) < (0, 8, 3)
    ):
        raise ValueError("producer bundle version or scope differs")
    if (manifest["provider"], manifest["entitlement"]) != (_text(provider), _text(entitlement)):
        raise ValueError("producer bundle version or scope differs")
    return manifest, offset


def _read_members(content: bytes, offset: int, manifest: dict, capabilities: dict) -> dict:
    members = manifest["members"]
    expected = ["chain", *(key for key in _COMPONENTS if capabilities[key] == "available")]
    if not isinstance(members, list) or len(members) != len(expected):
        raise ValueError("producer bundle inventory differs")
    blobs = {}
    for member, name in zip(members, expected):
        if (
            not isinstance(member, dict)
            or set(member) != {"name", "size", "sha256"}
            or member["name"] != name
            or not _positive(member["size"])
            or member["size"] > len(content) - offset
        ):
            raise ValueError("invalid producer bundle member")
        end = offset + member["size"]
        raw = content[offset:end]
        if _digest(raw) != member["sha256"]:
            raise ValueError("producer bundle member checksum differs")
        blobs[name] = raw
        offset = end
    if offset != len(content):
        raise ValueError("undeclared producer bundle bytes")
    return blobs


def _load_dataset(  # pylint: disable=too-many-arguments
    data: Any, content: bytes, *, provider: str, dataset_id: str, digest: str, cutoff: datetime
) -> DatasetRecord:
    fields = {
        "dataset_id",
        "run_id",
        "created_at",
        "format",
        "schema_version",
        "row_count",
        "content_hash",
        "script_version",
        "integrity",
    }
    if not isinstance(data, dict) or set(data) != fields:
        raise ValueError("producer dataset metadata differs")
    if (
        data["dataset_id"] != validate_dataset_id(dataset_id)
        or type(data["schema_version"]) is not int
        or data["schema_version"] != SCHEMA_VERSION
        or not _positive(data["row_count"])
        or data["format"] not in ("csv", "parquet")
    ):
        raise ValueError("producer dataset metadata differs")
    if (
        data["content_hash"] != _digest(content)
        or not isinstance(data["integrity"], dict)
        or _instant(data["created_at"]) > cutoff
    ):
        raise ValueError("producer dataset metadata differs")
    dataset = DatasetRecord(
        dataset_id,
        validate_run_id(data["run_id"]),
        _instant(data["created_at"]),
        provider,
        data["schema_version"],
        data["row_count"],
        data["format"],
        f"bundle://{digest}/chain",
        data["content_hash"],
        _text(data["script_version"]),
        **record_integrity_from_mapping(data["integrity"]),
    )
    _check_dataset_budget(dataset, content)
    return dataset


def import_producer_bundle(  # pylint: disable=too-many-arguments,too-many-locals
    content: bytes,
    *,
    provider: str,
    entitlement: str,
    dataset_id: str,
    required: frozenset[str] = frozenset(),
    expected_sha256: str | None = None,
) -> ProducerBundle:
    """Verify an envelope and return a file-free, effect-free portable reader."""
    if (
        type(content) is not bytes
        or len(content) > MAX_BUNDLE_BYTES
        or not content.startswith(_MAGIC)
        or not required <= set(_COMPONENTS)
    ):
        raise ValueError("unsupported or oversized producer bundle")
    digest = _digest(content)
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError("producer bundle digest differs from selected version")
    manifest, offset = _read_manifest(content, provider, entitlement)
    cutoff = _instant(manifest["as_of"])
    history_as_of = (
        _instant(manifest["history_as_of"]) if manifest["history_as_of"] is not None else None
    )
    if history_as_of is not None and history_as_of > cutoff:
        raise ValueError("history observation falls after the bundle cutoff")
    capabilities = manifest["capabilities"]
    if (
        not isinstance(capabilities, dict)
        or capabilities.keys() != set(_COMPONENTS)
        or any(
            not isinstance(value, str) or value not in _ABSENT | {"available"}
            for value in capabilities.values()
        )
        or any(capabilities[key] != "available" for key in required)
    ):
        raise ValueError("producer bundle lacks required component coverage")
    blobs = _read_members(content, offset, manifest, capabilities)
    data = manifest["dataset"]
    dataset = _load_dataset(
        data, blobs["chain"], provider=provider, dataset_id=dataset_id, digest=digest, cutoff=cutoff
    )
    run, ticker_results = _run_metadata(
        manifest["fetch_run"], manifest["ticker_results"], dataset, cutoff
    )
    outcome = validate_stored_option_chain_snapshot(dataset, blobs["chain"], checked_at=cutoff)
    validate_option_chain_row_scope(outcome.record, ticker_results=list(ticker_results) or None)
    validated_dataset_from_outcome(outcome)
    if record_integrity_to_dict(outcome.record) != data["integrity"]:
        raise ValueError("producer validation evidence contradicts dataset")
    bundle = ProducerBundle(
        digest,
        provider,
        entitlement,
        dataset_id,
        cutoff,
        history_as_of,
        MappingProxyType(capabilities),
        MappingProxyType({k: v for k, v in blobs.items() if k != "chain"}),
        outcome.record,
        blobs["chain"],
        run,
        ticker_results,
    )
    for name, raw in bundle.components.items():
        _validate_component(name, raw, provider, cutoff)
        if name in _HISTORY_KEYS and history_as_of is not None:
            if _instant(_decode(raw)["observed_at"]) != history_as_of:
                raise ValueError("history component differs from the frozen history clock")
    return bundle


def export_producer_bundle(  # pylint: disable=too-many-arguments,too-many-locals
    storage: StorageBackend,
    dataset_id: str,
    *,
    runs_root: Path,
    entitlement: str,
    as_of: datetime,
    components: Mapping[str, bytes | str],
    history_as_of: datetime | None = None,
) -> bytes:
    """Export exact retained bytes; never refresh source metadata or fetch data.

    Each named component is bytes or an explicit missing/compacted/not_configured
    status. The caller selects the original evidence and supplies a non-secret
    entitlement identifier, not a credential.
    """
    if (
        components.keys() != set(_COMPONENTS)
        or as_of.utcoffset() is None
        or history_as_of is not None
        and history_as_of.utcoffset() is None
    ):
        raise ValueError("producer export requires an explicit cutoff and complete inventory")
    handle = storage.get_dataset(validate_dataset_id(dataset_id))
    run = storage.get_run(handle.run_id)
    if (
        handle.dataset_id != dataset_id
        or run.dataset_id != dataset_id
        or run.status != "complete"
        or run.provider != handle.provider
    ):
        raise ValueError("producer run does not own the selected completed dataset")
    raw = _read_dataset_bytes(runs_root, handle.location)
    cutoff = as_of.astimezone(timezone.utc)
    record = DatasetRecord(**vars(handle))
    _check_dataset_budget(record, raw)
    outcome = validate_stored_option_chain_snapshot(record, raw, checked_at=cutoff)
    ticker_results = storage.get_ticker_results(handle.run_id)
    validate_option_chain_row_scope(outcome.record, ticker_results=ticker_results or None)
    validated_dataset_from_outcome(outcome)
    metadata = {
        key: getattr(handle, key)
        for key in (
            "dataset_id",
            "run_id",
            "format",
            "schema_version",
            "row_count",
            "content_hash",
            "script_version",
        )
    }
    metadata.update(
        created_at=handle.created_at.isoformat(), integrity=record_integrity_to_dict(outcome.record)
    )
    blobs = {"chain": raw}
    capabilities = {}
    for name in _COMPONENTS:
        value = components[name]
        if type(value) is bytes:
            blobs[name] = value
            capabilities[name] = "available"
        elif isinstance(value, str) and value in _ABSENT:
            capabilities[name] = value
        else:
            raise ValueError("invalid producer component or absence status")
    fetch_run = {
        **asdict(run),
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
    }
    manifest = _encode(
        {
            "schema_version": PRODUCER_BUNDLE_SCHEMA_VERSION,
            "distribution": "opx-chain",
            "producer_version": __version__,
            "provider": handle.provider,
            "entitlement": _text(entitlement),
            "as_of": cutoff.isoformat(),
            "history_as_of": (
                history_as_of.astimezone(timezone.utc).isoformat()
                if history_as_of is not None
                else None
            ),
            "dataset": metadata,
            "capabilities": capabilities,
            "fetch_run": fetch_run,
            "ticker_results": [asdict(row) for row in ticker_results],
            "members": [
                {"name": name, "size": len(raw), "sha256": _digest(raw)}
                for name, raw in blobs.items()
            ],
        }
    )
    if (
        len(manifest) > _MAX_MANIFEST_BYTES
        or len(_MAGIC) + 4 + len(manifest) + sum(map(len, blobs.values())) > MAX_BUNDLE_BYTES
    ):
        raise ValueError("producer bundle exceeds its byte limit")
    content = _MAGIC + len(manifest).to_bytes(4, "big") + manifest + b"".join(blobs.values())
    import_producer_bundle(
        content, provider=handle.provider, entitlement=entitlement, dataset_id=dataset_id
    )
    return content

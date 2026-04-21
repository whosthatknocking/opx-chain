# Storage Specification

This document specifies the storage design for `opx`. It defines the storage
interfaces, domain records, implementation strategy, and the order in which
changes should be executed.

It is intentionally forward-looking. Current behavior is filesystem-only.
This spec describes the target architecture and the path to reach it.

## 1. Goals

The storage design should:

- keep `opx` independent from any one storage implementation
- preserve the canonical exported dataset as the main integration contract
- support local-only operation first
- support later integration into a larger multi-component system
- avoid pushing `opx` into portfolio-decision or execution-engine scope

## 2. Non-Goals

This specification does not aim to:

- turn `opx` into a trading-state or order-management system
- store downstream decision-engine state inside `opx`
- make the viewer dependent on a specific database product
- remove filesystem exports as a supported artifact format

## 3. Design Principles

### 3.1 Storage Behind a Port

All runtime code should depend on a storage interface, not on direct filesystem,
SQLite, or network storage calls.

Rules:

- fetch orchestration should write through a storage port
- the viewer should read through a storage port
- storage implementations should be swappable without changing the fetch pipeline contract
- serialization format should be separable from storage location

### 3.2 Immutable Dataset Snapshots

The primary artifact produced by `opx` should remain an immutable dataset snapshot.

Implications:

- each successful fetch run produces one dataset snapshot
- snapshots are append-only artifacts, not mutable working state
- downstream systems should consume a stable dataset identifier or artifact location

### 3.3 Metadata Separate From Artifacts

Structured run metadata should be queryable independently from the artifact bytes.

Implications:

- large payloads such as CSV, Parquet, or raw provider dumps should not need to
  be embedded in a metadata database
- run history, validation summaries, and dataset discovery should be queryable
  through a compact index

### 3.4 Schema Version Tied to Export Contract

The canonical column order in `opx/export.py` (`CANONICAL_EXPORT_COLUMNS`) is the
schema. Every time a column is added, removed, or reordered, the schema version
must be incremented.

Rules:

- schema version is an integer, starting at `1`, stored in `opx/export.py`
- it is written into every `DatasetRecord` at write time
- the viewer and downstream consumers use it to detect schema drift between datasets
- backward-compatibility is not guaranteed across schema versions; consumers should
  re-fetch or re-export when versions differ

## 4. Logical Storage Interfaces

The application-facing storage boundary is divided into narrow, single-purpose
interfaces. They may share one backend technology but must not share one
application-level abstraction.

### 4.1 Run Store

Purpose:

- track one fetch run from start to finish

Responsibilities:

- create a run record and return a `run_id`
- mark run status transitions (`pending` → `running` → `complete` / `failed` / `interrupted`)
- record error details on failure
- persist resolved provider and config metadata
- persist per-ticker summary results
- persist validation summary
- persist filter summary
- finalize a run on clean exit

### 4.2 Dataset Store

Purpose:

- persist and retrieve canonical exported datasets

Responsibilities:

- write one immutable dataset artifact and return a `DatasetRecord`
- expose dataset metadata: row count, provider, schema version, format, content hash
- list available datasets for the viewer, with optional filtering by date, provider, or ticker
- return a handle or location for downstream consumers
- enforce a configurable retention policy (keep last N datasets, or TTL-based)

### 4.3 Artifact Store

Purpose:

- persist auxiliary artifacts that are not the canonical dataset itself

Responsibilities:

- write debug payload dumps
- write run logs or log references
- write optional serialized summaries or sidecars

### 4.4 Provider Cache

Purpose:

- cache upstream provider responses independently from run history

Responsibilities:

- store and retrieve quotes, event payloads, and historical candles
- enforce TTL or freshness semantics separately from dataset retention

This is a separate interface from `StorageBackend`. It must not be mixed into
the run or dataset stores. Provider cache concerns — TTL, invalidation, and
staleness — are distinct from run-lifecycle concerns.

### 4.5 Viewer Preference Store

Purpose:

- optionally persist user inspection preferences

Examples:

- saved filters
- column widths
- pinned symbols

Lower priority than run and dataset storage. The viewer currently reads datasets
directly from the filesystem; migrating it to the storage port should happen in
a separate step after the fetcher migration is complete.

## 5. Domain Records

The storage layer centers around storage-neutral records. These are plain
dataclasses or typed dicts — not ORM models.

### 5.1 Run Record

```python
@dataclass
class RunRecord:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str  # pending | running | complete | failed | interrupted
    provider: str
    config_fingerprint: str   # SHA-256 of the resolved config fields that affect output
    positions_fingerprint: str  # SHA-256 of the positions file bytes; empty string if absent
    dataset_id: str | None
    error_summary: str | None
```

`config_fingerprint` covers the fields that affect fetch output: provider,
tickers, expiration ceiling, filter settings, and scoring weights. It does not
cover log paths or debug flags. Two runs with the same fingerprint and the same
positions fingerprint should produce structurally comparable datasets.

`positions_fingerprint` is the SHA-256 of the raw positions file bytes. It changes
when any held position changes, making it easy to attribute output differences to
position changes vs. market changes.

### 5.2 Dataset Record

```python
@dataclass
class DatasetRecord:
    dataset_id: str
    run_id: str
    created_at: datetime
    provider: str
    schema_version: int
    row_count: int
    format: str   # csv | parquet
    location: str  # relative path or object-storage URI
    content_hash: str  # SHA-256 of artifact bytes, computed after write completes
```

`content_hash` is computed after the write completes, not before. For large files
this is acceptable overhead at the end of a run. It enables downstream deduplication
and artifact integrity checks.

### 5.3 Ticker Run Record

```python
@dataclass
class TickerRunRecord:
    run_id: str
    ticker: str
    raw_row_count: int
    normalized_row_count: int
    kept_row_count: int
    filtered_row_count: int
    expiration_count: int
    status: str  # ok | skipped | error
    error_summary: str | None
```

`normalized_row_count` captures the count after enrich/normalize and before the
filter step, making it possible to distinguish normalization losses from filter losses.

### 5.4 Validation Record

```python
@dataclass
class ValidationRecord:
    run_id: str
    severity: str   # error | warning | info
    code: str
    count: int
    sample: str | None  # optional JSON-encoded sample detail
```

### 5.5 Artifact Record

```python
@dataclass
class ArtifactRecord:
    artifact_id: str
    run_id: str
    artifact_type: str  # debug_payload | run_log | sidecar
    location: str
    content_hash: str
```

## 6. Write Payload Types

Callers pass write payloads into the storage port, not raw records. This keeps
the port stable even if record fields change.

```python
@dataclass
class RunContext:
    provider: str
    tickers: tuple[str, ...]
    config_fingerprint: str
    positions_fingerprint: str

@dataclass
class TickerFetchResult:
    ticker: str
    raw_row_count: int
    normalized_row_count: int
    kept_row_count: int
    filtered_row_count: int
    expiration_count: int
    status: str
    error_summary: str | None = None

@dataclass
class DatasetWrite:
    data: pd.DataFrame
    provider: str
    schema_version: int
    format: str = "csv"

@dataclass
class ArtifactWrite:
    artifact_type: str
    content: bytes
    filename: str

@dataclass
class RunSummary:
    status: str   # complete | failed | interrupted
    error_summary: str | None = None
```

`DatasetHandle` is returned by `get_dataset` and provides a stable reference
that callers can pass to downstream systems without coupling them to storage
implementation details:

```python
@dataclass
class DatasetHandle:
    dataset_id: str
    location: str
    schema_version: int
    row_count: int
    format: str
```

## 7. Storage Port Shape

The fetch pipeline and viewer depend on these two protocols:

```python
class StorageBackend(Protocol):
    def create_run(self, context: RunContext) -> str: ...
    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None: ...
    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord: ...
    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord: ...
    def list_datasets(
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]: ...
    def get_dataset(self, dataset_id: str) -> DatasetHandle: ...
    def finalize_run(self, run_id: str, summary: RunSummary) -> None: ...
    def fail_run(self, run_id: str, error: str) -> None: ...


class ProviderCache(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, value: bytes, ttl_seconds: int) -> None: ...
    def invalidate(self, key: str) -> None: ...
```

`fail_run` is separate from `finalize_run` to make the error path explicit.
It is called from the `except` blocks in `fetcher.py` and from the
`KeyboardInterrupt` handler for the `interrupted` status.

`list_datasets` accepts optional filters so callers are not forced to load all
records and filter in application code. Implementations that do not support
server-side filtering may apply them in memory, but the interface must be stable
from day one.

## 8. Concurrency and Run Lifecycle

The current fetcher lock (`logs/fetcher.lock`) prevents concurrent runs. Under
the storage model, `create_run` does not replace the lock — both coexist.

Rationale:

- the filesystem lock provides a fast, crash-safe pre-check before any storage
  I/O occurs
- `create_run` provides a persistent record of the run lifecycle after the lock
  is acquired
- on crash recovery, a `running` run record with no corresponding lock file
  signals an unclean exit; the backend may mark it `interrupted` on next startup

Run status transitions:

```
acquire lock → create_run (status=running)
  → per-ticker work
  → write_dataset
  → finalize_run (status=complete)
  → release lock

on error:
  → fail_run (status=failed, error=...)
  → release lock

on KeyboardInterrupt:
  → fail_run (status=interrupted)
  → release lock
```

## 9. Dataset Serialization Formats

The `DatasetWrite.format` field and `DatasetRecord.format` field anticipate
multiple serialization formats. The first supported format is CSV, matching
current behavior.

Parquet should be introduced as a first-class second format:

- the `DatasetSerializer` protocol controls format-specific write logic
- CSV and Parquet serializers share the same canonical column order from `export.py`
- the viewer and `opx-check` select format via config
- the `DatasetRecord.format` field distinguishes artifacts in the metadata index

This is better introduced alongside the storage port rather than as a later
migration, because the serializer boundary maps cleanly to the storage write path.

```python
class DatasetSerializer(Protocol):
    format: str
    def write(self, df: pd.DataFrame, path: Path) -> int: ...  # returns bytes written
```

## 10. Dataset Retention

The storage layer should enforce a retention policy to bound disk growth.

Suggested defaults:

- keep the last 30 datasets (configurable)
- on `write_dataset`, prune datasets beyond the retention limit
- pruning removes both the artifact file and the metadata record
- run records are retained independently of dataset retention (they are small)

The filesystem backend implements pruning by scanning the output directory.
The SQLite backend implements pruning with a `DELETE WHERE` on the dataset table.

## 11. Run Diffing

With structured `TickerRunRecord` entries stored per run, the SQLite backend
can support cross-run comparison queries without loading any artifact bytes.

Useful queries:

- row count delta per ticker between two runs
- tickers that appeared or disappeared between runs
- filter drop rate change over time
- validation error trends

These are not part of the initial implementation but are a primary motivating
use case for the SQLite backend. The `TickerRunRecord` fields should be designed
with these queries in mind from day one.

## 12. `opx-check` Integration

`opx-check` currently scans the output directory for the latest CSV by filename
timestamp. Under the storage model it should use `list_datasets(limit=1)` to
find the latest dataset and obtain its location from the returned `DatasetRecord`.

This decouples `opx-check` from the output directory naming convention and makes
it format-agnostic once Parquet is supported.

## 13. Testing Strategy

The storage layer should be tested through a `MemoryBackend`:

- `MemoryBackend` implements `StorageBackend` using in-memory dicts
- it is used in all existing and new fetch/viewer tests in place of filesystem mocks
- it does not write any files, making test isolation trivial
- it should be part of `opx/storage/` so it is importable by tests without patching

The filesystem and SQLite backends are tested with `tmp_path` fixtures. The
`MemoryBackend` is not a substitute for backend-specific tests but replaces the
current pattern of monkeypatching `write_options_csv` in integration tests.

## 14. Separation of Concerns

The following categories remain distinct:

- run history
- canonical dataset storage
- provider response cache
- viewer/user preference state
- downstream decision state

They may share one implementation technology but must not share one
application-level abstraction.

## 15. Suggested Module Layout

```text
opx/storage/
  __init__.py
  base.py          # StorageBackend and ProviderCache protocols
  models.py        # domain records and write payload types
  serializers.py   # DatasetSerializer protocol, CSV and Parquet implementations
  factory.py       # config-driven backend selection
  filesystem.py    # file-only backend (current behavior)
  sqlite_indexed.py  # SQLite metadata + file-artifact backend
  memory.py        # in-memory backend for tests
  cache.py         # ProviderCache implementations
```

## 16. Implementation Order

The changes should be executed in the following sequence. Each step is
independently shippable and leaves the system in a working state.

### Step 1 — Domain models and protocols (no behavior change)

- introduce `opx/storage/base.py` with `StorageBackend` and `ProviderCache` protocols
- introduce `opx/storage/models.py` with all records and write payloads
- introduce `opx/storage/serializers.py` with `DatasetSerializer` protocol and CSV implementation
- add `SCHEMA_VERSION` integer constant to `opx/export.py`
- add `MemoryBackend` in `opx/storage/memory.py`
- no changes to `fetcher.py`, `fetch.py`, or `viewer.py`
- tests: verify `MemoryBackend` satisfies the protocol and roundtrips all write operations

### Step 2 — Filesystem backend (replaces current direct writes)

- implement `FilesystemBackend` in `opx/storage/filesystem.py`
  - `write_dataset` calls the CSV serializer and writes to `output/`
  - `create_run`, `finalize_run`, `fail_run` write JSON sidecar files to `logs/`
  - `write_artifact` writes to `debug/`
  - `list_datasets` scans `output/` and parses sidecars
- implement dataset retention pruning in `FilesystemBackend`
- add `StorageFactory` in `opx/storage/factory.py` that returns `FilesystemBackend` by default
- no change to output format or directory layout visible to users

### Step 3 — Migrate `fetcher.py` to the storage port

- replace `write_options_csv` direct call with `storage.write_dataset`
- replace the per-ticker accumulation pattern with `storage.record_ticker_result` calls
- wrap the run lifecycle in `create_run` / `finalize_run` / `fail_run`
- replace `write_options_csv` monkeypatches in tests with `MemoryBackend` injection
- update `opx-check` to use `storage.list_datasets(limit=1)` for dataset discovery

### Step 4 — Parquet serializer

- add `ParquetSerializer` to `opx/storage/serializers.py`
- add `dataset_format` config option (`csv` default)
- `FilesystemBackend` selects serializer based on config
- viewer and `opx-check` handle both formats via `DatasetHandle.format`

### Step 5 — SQLite-indexed backend

- implement `SqliteIndexedBackend` in `opx/storage/sqlite_indexed.py`
  - stores run, dataset, ticker, validation, and artifact metadata in SQLite
  - artifact files remain on disk; SQLite holds only metadata
  - `list_datasets` queries SQLite with optional server-side filters
- add migration logic for the SQLite schema (simple version table)
- add `backend: sqlite` config option
- tests: verify run diffing queries against `SqliteIndexedBackend`

### Step 6 — Provider cache abstractions

- implement `ProviderCache` backends: `NullCache` (default) and `FilesystemCache`
- wire into provider `load_underlying_snapshot`, `load_option_chain`,
  `load_ticker_events` via an optional cache argument
- TTL configurable per call type (snapshot vs. chain vs. events)

### Step 7 — Viewer migration

- migrate `viewer.py` to read datasets through `StorageBackend.list_datasets`
  and `StorageBackend.get_dataset`
- add viewer preference store (low priority, can be a simple JSON file initially)

## 17. Open Questions

Before executing step 5, the main questions to settle are:

- What metadata fields are required by the downstream system on day one?
- Should run diffing queries be exposed through `StorageBackend` or through a
  separate read-model interface?
- Should `SqliteIndexedBackend` support multiple concurrent readers (WAL mode)?

## 18. Current Recommendation

Recommended path:

- execute steps 1 through 3 as the first milestone
- keep exported datasets as immutable file artifacts throughout
- defer SQLite until dataset discovery or run diffing becomes a concrete need
- introduce Parquet in step 4 before SQLite to validate the serializer abstraction

This gives `opx` a clean storage boundary for downstream integration without
overcommitting to one storage technology too early.

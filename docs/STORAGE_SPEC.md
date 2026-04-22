# Storage Specification

This document specifies the storage design for `opx`. It defines the storage
interfaces, domain records, implementation strategy, and the order in which
changes should be executed.

The storage layer is **opt-in and disabled by default.** The existing
filesystem-based runtime — direct `write_options_csv` calls, output-directory
scanning in `opx-check`, and the current viewer CSV discovery — is the default
and remains unchanged when storage is not enabled. Enabling storage is a
config-driven decision that activates the storage port alongside the existing
path; it does not replace or break it.

This spec is intentionally forward-looking. It describes the target architecture
and the path to reach it, independent from any downstream strategy or decision
engine.

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
- change the default runtime behavior when storage is not enabled

## 3. Design Principles

### 3.1 Storage Behind a Port

When storage is enabled, all runtime code should depend on a storage interface,
not on direct filesystem, SQLite, or network storage calls.

Rules:

- fetch orchestration should write through a storage port when enabled
- the viewer should read through a storage port when enabled
- storage implementations should be swappable without changing the fetch pipeline contract
- serialization format should be separable from storage location
- when storage is disabled, existing direct write and scan paths are used unchanged

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

- schema version is an integer, starting at `1`, defined as `SCHEMA_VERSION` in `opx/__init__.py`
- it is written into every `DatasetRecord` at write time
- the viewer and downstream consumers use it to detect schema drift between datasets
- backward-compatibility is not guaranteed across schema versions; consumers should
  re-fetch or re-export when versions differ

## 4. Config-Driven Enable/Disable

The storage layer is controlled by a `[storage]` section in
`~/.config/opx-chain/config.toml`.

```toml
[storage]
enable = false                 # default: storage disabled; existing runtime unchanged
backend = "filesystem"         # "filesystem" (default when enabled) | "sqlite"
dataset_format = "csv"         # "csv" (default) | "parquet"
max_runs_retained = 0          # 0 = keep all (default); positive integer = keep last N
also_write_csv = true          # also write <data-dir>/output/options_engine_output_<ts>.csv alongside the storage artifact
# dir = "/path/to/custom/dir"  # override XDG data dir (default: $XDG_DATA_HOME/opx-chain or ~/.local/share/opx-chain)

# Provider response cache (optional)
cache_backend = "none"         # "none" (default) | "filesystem"
cache_dir = "cache"            # path to cache directory (used when cache_backend = "filesystem")
snapshot_ttl = 300             # TTL in seconds for underlying snapshot cache entries
chain_ttl = 300                # TTL in seconds for option chain cache entries
events_ttl = 86400             # TTL in seconds for ticker events cache entries
```

Behavior:

- when `enable = false` (or the `[storage]` section is absent), `fetcher.py`
  calls `write_options_csv` directly, `opx-check` scans `output/` by filename,
  and the viewer discovers CSVs as today — no behavior change
- when `enable = true`, `fetcher.py` writes through the configured
  `StorageBackend`, `opx-check` uses `list_datasets(limit=1)`, and the Python
  package interface becomes available to downstream consumers
- `backend` is only read when `enable = true`; it is ignored otherwise
- `also_write_csv = false` suppresses the timestamped CSV; only the
  storage-managed artifact (e.g. `~/.local/share/opx-chain/runs/<run-id>/output/<uuid>.parquet`)
  is written; the viewer discovers it automatically via the storage backend; only
  meaningful when `enable = true`
- startup output always prints the resolved `Storage:` section; when disabled,
  it prints `enable: false`

The `enable` key must default to `false` in the config loader. Malformed or
unrecognised `backend` values fall back to `"filesystem"` with a warning.

## 5. Logical Storage Interfaces

The application-facing storage boundary is divided into narrow, single-purpose
interfaces. They may share one backend technology but must not share one
application-level abstraction.

### 5.1 Run Store

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

### 5.2 Dataset Store

Purpose:

- persist and retrieve canonical exported datasets

Responsibilities:

- write one immutable dataset artifact and return a `DatasetRecord`
- expose dataset metadata: row count, provider, schema version, format, content hash
- list available datasets for the viewer, with optional filtering by date, provider, or ticker
- return a handle or location for downstream consumers
- enforce a configurable retention policy (keep last N datasets, or TTL-based)

### 5.3 Artifact Store

Purpose:

- persist auxiliary artifacts that are not the canonical dataset itself

Responsibilities:

- write debug payload dumps
- write run logs or log references
- write optional serialized summaries or sidecars

### 5.4 Provider Cache

Purpose:

- cache upstream provider responses independently from run history

Responsibilities:

- store and retrieve quotes, event payloads, and historical candles
- enforce TTL or freshness semantics separately from dataset retention

This is a separate interface from `StorageBackend`. It must not be mixed into
the run or dataset stores. Provider cache concerns — TTL, invalidation, and
staleness — are distinct from run-lifecycle concerns.

### 5.5 Viewer Preference Store

Purpose:

- optionally persist user inspection preferences

Examples:

- saved filters
- column widths
- pinned symbols

Lower priority than run and dataset storage. The viewer currently reads datasets
directly from the filesystem; migrating it to the storage port should happen in
a separate step after the fetcher migration is complete.

## 6. Domain Records

The storage layer centers around storage-neutral records. These are plain
dataclasses or typed dicts — not ORM models.

### 6.1 Run Record

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

### 6.2 Dataset Record

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

### 6.3 Ticker Run Record

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

### 6.4 Validation Record

```python
@dataclass
class ValidationRecord:
    run_id: str
    severity: str   # error | warning | info
    code: str
    count: int
    sample: str | None  # optional JSON-encoded sample detail
```

### 6.5 Artifact Record

```python
@dataclass
class ArtifactRecord:
    artifact_id: str
    run_id: str
    artifact_type: str  # debug_payload | run_log | sidecar
    location: str
    content_hash: str
```

## 7. Write Payload Types

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
    content_hash: str   # SHA-256 of artifact bytes; for integrity checks
    created_at: datetime  # UTC timestamp; for freshness assessment
```

## 8. Storage Port Shape

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

## 9. Concurrency and Run Lifecycle

The current fetcher lock (`fetcher.lock`) prevents concurrent runs. Under
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

on unexpected exception:
  → fail_run (status=failed, error=<message>)
  → release lock

on KeyboardInterrupt:
  → finalize_run (status=interrupted, error_summary="interrupted")
  → release lock
```

The `pending` status value is reserved for future use; `create_run` sets
`status=running` immediately.

## 10. Missing Field Values

When a canonical field is not available from the active provider, it is left as
a type-native null in the in-memory DataFrame. The serializer is responsible for
writing that null in a format-appropriate way.

### 10.1 Type-native nulls by column kind

| Column kind | Python / pandas type | In-memory null |
|---|---|---|
| Numeric (`float`) | `float` | `float('nan')` / `np.nan` |
| Whole number (`int`) | `pd.Int64Dtype()` (nullable integer) | `pd.NA` |
| Boolean | `pd.BooleanDtype()` (nullable boolean) | `pd.NA` |
| Timestamp | `datetime64[ns, UTC]` | `pd.NaT` |
| String / categorical | `object` | `None` or `np.nan` |

Columns must not be coerced to a non-nullable dtype (e.g. plain `bool` or
plain `int64`) when the field can be absent for some providers. Use the
nullable pandas extension types (`Int64`, `boolean`) for fields that are
whole-number or boolean by contract but legitimately absent for some rows.

### 10.2 CSV serializer behavior

`pd.DataFrame.to_csv()` with no `na_rep` argument writes all null types as
an empty string. This is the current behavior and the contract for the CSV
format: **a blank cell means the field was not available for that row**.

Consumers reading the CSV must treat empty cells as absent values, not as
zero, false, or the empty string. Type inference is the consumer's
responsibility.

### 10.3 Parquet serializer behavior

The Parquet serializer must preserve type-native nulls. It must not coerce
nulls to sentinel values (e.g. `-1`, `0`, `""`). Parquet's native null
representation is used for each column type. Consumers reading Parquet get
properly-typed null values rather than empty strings.

### 10.4 Consistency rule

The same DataFrame must produce equivalent null semantics in both formats:
a field that is null for a given row in the CSV (empty cell) must also be
null for that row in the Parquet artifact. The serializer must not introduce
or remove nulls beyond what the DataFrame contains.

## 11. Dataset Serialization Formats

The serialization format is separate from the storage location. A
`DatasetSerializer` protocol defines the interface:

```python
class DatasetSerializer(Protocol):
    format: str  # "csv" | "parquet"
    def serialize(self, df: pd.DataFrame, path: str) -> int: ...
    # returns bytes written; raises on failure
```

Both `CsvSerializer` and `ParquetSerializer` are implemented in
`opx/storage/serializers.py`. `get_serializer(fmt)` returns the appropriate
instance. `FilesystemBackend` and `SqliteIndexedBackend` select the serializer
based on the `dataset_format` config option (`"csv"` default). The
`DatasetHandle.format` field tells downstream consumers which reader to use.

`ParquetSerializer` requires the optional `pyarrow` dependency
(`pip install 'opx[parquet]'`). Reading parquet files uses
`opx.utils.read_dataset_file(path)`, which dispatches on file extension.

## 12. Dataset Retention

Retention is configurable through `[storage]` in `~/.config/opx-chain/config.toml`.

```toml
[storage]
enable = false
backend = "filesystem"
max_runs_retained = 0   # 0 = keep all (default); positive integer = keep last N
```

Behavior:

- `max_runs_retained = 0` (the default) disables pruning; all datasets are kept
- a positive value causes `write_dataset` to prune the oldest datasets beyond
  the limit after each successful write
- pruning removes both the artifact file and the metadata record
- run records are retained independently of dataset pruning; they are small
- malformed or negative values fall back to `0` (no pruning) with a warning

The filesystem backend implements pruning by scanning the output directory and
sorting by filename timestamp. The SQLite backend implements pruning with a
`DELETE WHERE` on the dataset table ordered by `created_at`.

## 13. `opx-check` Integration

`opx-check` currently scans the output directory for the latest CSV by filename
timestamp. Under the storage model it should use `list_datasets(limit=1)` to
find the latest dataset and obtain its location from the returned `DatasetRecord`.

This decouples `opx-check` from the output directory naming convention and makes
it format-agnostic once Parquet is supported.

## 14. Testing Strategy

The storage layer should be tested through a `MemoryBackend`:

- `MemoryBackend` implements `StorageBackend` using in-memory dicts
- it is used in new tests that exercise the storage-enabled branch of `fetcher.py`
  and `opx-check`; existing tests that use `write_options_csv` directly are unchanged
- it does not write any files, making test isolation trivial
- it should be part of `opx/storage/` so it is importable by tests without patching

The filesystem and SQLite backends are tested with `tmp_path` fixtures.

## 15. Separation of Concerns

The following categories remain distinct:

- run history
- canonical dataset storage
- provider response cache
- viewer/user preference state
- downstream decision state

They may share one implementation technology but must not share one
application-level abstraction.

## 16. Suggested Module Layout

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

## 17. Implementation Status

All seven steps are complete and shipped.

### Step 1 — Domain models and protocols ✓

- `opx/storage/base.py` — `StorageBackend` and `ProviderCache` protocols
- `opx/storage/models.py` — all records and write payloads
- `opx/storage/serializers.py` — `DatasetSerializer` protocol and CSV implementation
- `SCHEMA_VERSION: int = 1` in `opx/__init__.py`
- `MemoryBackend` in `opx/storage/memory.py`

### Step 2 — Filesystem backend ✓

- `FilesystemBackend` in `opx/storage/filesystem.py`
- `StorageFactory` in `opx/storage/factory.py`
- `[storage]` parsing in `opx/config.py`

### Step 3 — Wire `fetcher.py` and `opx-check` ✓

- `fetcher.py` calls `create_run` / `record_ticker_result` / `write_dataset` /
  `finalize_run` / `fail_run` when storage is enabled
- `opx-check` uses `list_datasets(limit=1)` when storage is enabled
- `also_write_csv` config key (default `true`) controls whether the timestamped
  `output/options_engine_output_<ts>.csv` is also written alongside the storage artifact

### Step 4 — Parquet serializer ✓

- `ParquetSerializer` in `opx/storage/serializers.py`; requires `pyarrow`
- `dataset_format` config option (`"csv"` default)
- shared `read_dataset_file(path)` utility in `opx/utils.py` dispatches on extension

### Step 5 — SQLite-indexed backend ✓

- `SqliteIndexedBackend` in `opx/storage/sqlite_indexed.py`
- WAL mode, foreign keys, version table; schema defined in `docs/METADATA_SPEC.md`

### Step 6 — Provider cache abstractions ✓

- `NullCache` and `FilesystemCache` in `opx/storage/cache.py`
- wired in `fetch.py` at the fetch-orchestration level; caches snapshot, chain,
  and events responses with configurable TTLs
- config keys: `cache_backend`, `cache_dir`, `snapshot_ttl`, `chain_ttl`, `events_ttl`

### Step 7 — Viewer enhancements ✓

- `opx-view --data-dir DIR` scans an arbitrary directory for `.csv` and
  `.parquet` files; default discovery queries the storage backend, falling back to the timestamped CSV glob
- viewer preference store: `~/.config/opx-chain/viewer_prefs.json`,
  GET/POST `/api/prefs`

## 18. Open Questions

No open questions remain.

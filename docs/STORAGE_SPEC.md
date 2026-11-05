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

The canonical column order in `opx_chain/export.py` (`CANONICAL_EXPORT_COLUMNS`) is the
schema. Every time a column is added, removed, or reordered, the schema version
must be incremented.

Rules:

- schema version is an integer, starting at `1`, defined as `SCHEMA_VERSION` in `opx_chain/__init__.py`
- it is written into every `DatasetRecord` at write time
- the viewer and downstream consumers use it to detect schema drift between datasets
- backward-compatibility is not guaranteed across schema versions; consumers should
  re-fetch or re-export when versions differ

## 4. Config-Driven Enable/Disable

The storage layer is controlled by a `[storage]` section in
`$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`).

```toml
[storage]
enable = false                 # default: storage disabled; existing runtime unchanged
backend = "filesystem"         # "filesystem" (default when enabled) | "sqlite"
dataset_format = "csv"         # "csv" (default) | "parquet"
max_runs_retained = 0          # 0 = keep all (default); positive integer = keep last N
also_write_csv = true          # also write <data-dir>/runs/options_engine_output_<ts>.csv alongside the storage artifact
# dir = "/path/to/custom/dir"  # override XDG data dir (default: $XDG_DATA_HOME/opx-chain or ~/.local/share/opx-chain)

# Provider response cache (optional)
cache_backend = "none"         # "none" (default) | "filesystem"
cache_dir = "cache"            # relative paths resolve under $XDG_CACHE_HOME/opx-chain/
snapshot_ttl = 300             # TTL in seconds for underlying snapshot cache entries
chain_ttl = 300                # TTL in seconds for option chain cache entries
events_ttl = 86400             # TTL in seconds for ticker events cache entries
price_context_ttl = 86400      # TTL in seconds before retrying daily price-history reconciliation
```

Behavior:

- when `enable = false` (or the `[storage]` section is absent), `fetcher.py`
  calls `write_options_csv` directly, `opx-check` scans the configured data-dir
  `runs/` tree by filename, and the viewer discovers CSVs as today; if `dir`
  is omitted, the configured data dir is the XDG data dir
- `dir` overrides the fetcher lock location, timestamped CSV side-write
  location, `_latest` copy, and storage backend run/artifact location together
- `dir` also controls the base directory for `price-history.db`, the durable
  daily-OHLCV store used to derive price-context artifacts
- the `_latest` CSV pointer is a same-directory atomic file copy named
  `options_engine_output_latest.csv`, not a symlink; it remains readable even
  if the original timestamped CSV artifact is later removed
- when `enable = true`, `fetcher.py` writes through the configured
  `StorageBackend`; `opx-check` discovers complete, valid datasets and loads the
  selected identifier through `load_validated_option_chain_dataset(...)`, and
  the Python package interface becomes available to downstream consumers
- `get_storage_backend()` memoizes backend instances within the process, keyed
  by backend type, storage dir, debug dir, retention limit, and dataset format,
  so repeated viewer requests do not rebuild SQLite schema state on every call
- direct `get_storage_backend(config=...)` callers receive the same storage
  scalar boundaries as loaded config: `enable` accepts only recognized boolean
  values, `backend` must be `filesystem` or `sqlite`, and
  `max_runs_retained` must be a nonnegative non-boolean integer,
  while `storage_dataset_format` / `storage.dataset_format` must be `csv` or `parquet`
- `SqliteIndexedBackend` keeps one SQLite connection per backend instance,
  guarded by a re-entrant lock and opened with `check_same_thread = false`,
  so method calls amortize connection and PRAGMA setup while preserving
  serialized access inside the process
- `FilesystemBackend` serializes `run.json` read-modify-write updates inside a
  backend instance with a re-entrant lock, so concurrent per-ticker result,
  validation, artifact, dataset, and lifecycle writes in the fetcher process do
  not drop each other
- atomic filesystem helpers fsync the temporary file before replacement and
  fsync the parent directory after replacement where the platform supports it,
  preserving both concurrent-reader atomicity and power-loss durability
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
- keep sidecars storage-managed and associated with the owning run rather than
  treating them as ad hoc files
- delete storage-managed artifacts for a run when fetch fails before dataset
  publication, while preserving the run metadata needed for `fail_run`

### 5.4 Provider Cache

Purpose:

- cache upstream provider responses independently from run history

Responsibilities:

- store and retrieve quotes, event payloads, and historical candles
- enforce TTL or freshness semantics separately from dataset retention

This is a separate interface from `StorageBackend`. It must not be mixed into
the run or dataset stores. Provider cache concerns — TTL, invalidation, and
staleness — are distinct from run-lifecycle concerns.

Daily price-context history is stored durably in `price-history.db` under the
configured opx-chain data directory. It is not a provider response cache entry:
the store is keyed by provider, ticker, and trading date so old bars can be
reused across runs while reconciliation fetches only missing backfill or recent
tail data. `opx-price-history-backfill` uses the same store and keying to
populate `marketdata` and `yfinance` coverage independently without creating an
option-chain dataset or price-context artifact.

Durable implied-volatility history is stored in `iv-history.db` under the same
base directory. By default it is derived from retained option-chain datasets
rather than provider cache entries or new provider calls. Operators can also
explicitly seed it from provider historical option-chain snapshots; that path
writes only aggregate IV rows, may consume provider requests, and does not
create retained chain datasets. Rows are keyed by provider, ticker, observation
date, option type, DTE bucket, and delta bucket so historical-IV percentile
calculations can prefer ticker-wide and DTE-bucket history instead of using the
current-chain cross-section as a proxy.

### 5.5 Viewer Preference Store

Purpose:

- optionally persist user inspection preferences

Examples:

- saved filters
- column widths
- pinned symbols

Viewer dataset discovery is already storage-aware. When storage is enabled and
the viewer is not forced into CSV mode, the viewer discovers datasets through
`StorageBackend.list_datasets(limit=10000)` and resolves the returned artifact
locations. The explicit high limit keeps storage-backed discovery aligned with
CSV/fallback discovery, which scans all matching local files instead of
showing only the backend's small default page. It falls back to filesystem
discovery when storage is disabled, when CSV mode or an explicit data-dir
override is active, or when the storage index has no readable artifact paths.

Persisting viewer/user preferences remains lower priority than run and dataset
storage. Preference state is separate from dataset discovery and should be added
only after the storage-backed dataset path is stable.

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
    script_version: str  # opx-chain package version that opened the run
    tickers: tuple[str, ...]     # effective fetch universe for this run
    config_fingerprint: str   # SHA-256 of the resolved config fields that affect output
    positions_fingerprint: str  # SHA-256 of parsed positions; empty string if absent
    dataset_id: str | None
    error_summary: str | None
```

`tickers` records the effective fetch universe for this run, including configured
tickers and stock tickers expanded from the positions file. `config_fingerprint`
covers resolved runtime fields that affect fetch output: provider, tickers,
expiration ceiling, filter settings, validation setting, scoring weights,
Greek/HV constants, quote freshness threshold, provider modes, retry/backoff
settings, provider-cache settings, and storage/export settings. It excludes
credentials, local runtime paths, viewer bind settings, config warnings, and the
transient `today` value.
Two runs with the same fingerprint and the same positions fingerprint should
produce structurally comparable datasets.

`positions_fingerprint` is the SHA-256 of the canonical parsed positions
payload. It changes when any held stock ticker or option contract key changes,
making it easy to attribute output differences to position changes vs. market
changes without treating cosmetic CSV rewrites as portfolio changes.
`script_version` stores the opx-chain package version that created the run. Legacy
records that predate this field read back as `unknown`.

### 6.2 Dataset Record

```python
@dataclass
class DatasetRecord:
    dataset_id: str
    run_id: str
    created_at: datetime
    provider: str
    script_version: str  # opx-chain package version that wrote the dataset
    schema_version: int
    row_count: int
    format: str   # csv | parquet
    location: str  # relative path or object-storage URI
    content_hash: str  # SHA-256 of artifact bytes, computed after write completes
```

`content_hash` is computed after the write completes, not before. For large files
this is acceptable overhead at the end of a run. It enables downstream deduplication
and artifact integrity checks.
`script_version` lets downstream tools identify the exact opx-chain package
version that produced the dataset without relying on the shared run log.

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

`normalized_row_count` captures validated canonical rows after acquisition horizon
scoping and before generic post-download screening. Extra expirations fetched
for held contracts retain only those exact contracts; their other strikes are
acquisition exclusions, not generic-filter drops. `raw_row_count` still counts
the original provider rows. Thus disabled generic filters have zero
`filtered_row_count`, and normalized equals kept plus filtered. Structural
validation still runs before the lossy acquisition-scope restriction.

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
    run_id: str
    provider: str
    location: str
    schema_version: int
    row_count: int
    format: str
    content_hash: str   # SHA-256 of artifact bytes; for integrity checks
    created_at: datetime  # UTC timestamp; for freshness assessment
    row_scope_status: OptionChainRowScopeStatus
    row_scope: OptionChainRowScope | None
```

## 8. Storage Port Shape

The fetch pipeline and viewer depend on these two protocols:

```python
class StorageBackend(Protocol):
    def create_run(self, context: RunContext) -> str: ...
    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None: ...
    def record_validation(self, record: ValidationRecord) -> None: ...
    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord: ...
    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord: ...
    def delete_run_artifacts(self, run_id: str) -> None: ...
    def list_datasets(
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
        integrity_status: OptionChainIntegrityStatus | None = None,
        dataset_facts_status: OptionChainDatasetFactsStatus | None = None,
    ) -> list[DatasetRecord]: ...
    def get_dataset(self, dataset_id: str) -> DatasetHandle: ...
    def load_validated_option_chain_dataset(
        self,
        dataset_id: str,
    ) -> ValidatedOptionChainDataset: ...
    def get_run(self, run_id: str) -> RunRecord: ...
    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]: ...
    def finalize_run(self, run_id: str, summary: RunSummary) -> None: ...
    def fail_run(self, run_id: str, error: str) -> None: ...
    def interrupt_stale_runs(self, cutoff: datetime, error_summary: str) -> int: ...
    def count_runs_today(self, provider: str) -> int: ...


class ProviderCache(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, value: bytes, ttl_seconds: int) -> None: ...
    def invalidate(self, key: str) -> None: ...
```

Provider-cache implementations validate public method inputs consistently even
when caching is disabled: keys must be nonblank strings, payloads must be
`bytes`, and TTLs must be positive non-boolean integers. Direct
`get_provider_cache(config=...)` callers receive the same config boundary as
loaded config for `cache_backend` and `cache_dir`; relative cache directories
resolve under `$XDG_CACHE_HOME/opx-chain/`.

`FilesystemCache` deletes expired entries when they are read and prunes expired
or unreadable metadata once per cache directory per process. This keeps TTL
semantics from becoming append-only disk growth across normal fetch runs without
rescanning the same provider-cache directory for every ticker.

`fail_run` is separate from `finalize_run` to make the error path explicit.
It is called from the `except` blocks in `fetcher.py` and from the
`KeyboardInterrupt` handler for the `interrupted` status.

`get_run` returns the persisted `RunRecord` for downstream inspection.
`get_ticker_results(run_id)` returns the per-ticker fetch records for a run.
`interrupt_stale_runs(cutoff, error_summary)` marks stale `running` rows
older than `cutoff` as `interrupted`. `count_runs_today(provider)` returns
the number of complete runs started during the current US/Eastern calendar day
for the given provider and is used by `fetcher.py` to print same-day successful
run-count context before a fetch starts.

`list_datasets` accepts optional filters so callers are not forced to load all
records and filter in application code. Implementations that do not support
server-side filtering may apply them in memory, but the interface must be stable
from day one. The ticker filter is evaluated against the run's effective
`RunRecord.tickers` list; legacy records without persisted run tickers may fall
back to per-ticker result rows.

The filesystem backend maintains a derived `datasets.index.json` file under the
runs directory so common latest-dataset reads such as `list_datasets(limit=1)` do
not need to parse every `*.meta.json` sidecar. If the index is missing or
unreadable, the backend rebuilds it from the canonical per-dataset metadata.

## 9. Concurrency and Run Lifecycle

The current fetcher lock (`fetcher.lock`) prevents concurrent runs. Under
the storage model, `create_run` does not replace the lock — both coexist.
Within a single fetcher process, storage backends must also serialize run
sidecar read-modify-write paths so concurrent ticker workers cannot overwrite
each other's `ticker_results` or `validations` appends.

Rationale:

- the filesystem lock provides a fast, crash-safe pre-check before any storage
  I/O occurs
- `create_run` provides a persistent record of the run lifecycle after the lock
  is acquired
- `fetcher.lock` is a stable coordination path; active state is the advisory
  lock, not file existence, so release must not unlink the file under another
  contender

Run status transitions:

```
acquire lock → create_run (status=running)
  → per-ticker work
  → write_artifact for run sidecars required before dataset publication
  → write_dataset (staged, non-discoverable)
  → finalize_run (status=complete, atomically publish exact dataset id)
  → release lock

on unexpected exception:
  → fail_run (status=failed, error=<message>)
  → release lock

on KeyboardInterrupt:
  → finalize_run (status=interrupted, error_summary="interrupted")
  → release lock
```

Terminal transitions are guarded by the current run state. `finalize_run` and
`fail_run` only mutate rows whose status is still `running`; after a run is
`complete`, `failed`, or `interrupted`, later lifecycle calls must leave the
terminal status, `finished_at`, and `error_summary` unchanged. This prevents
late print/log/storage cleanup errors from demoting an already published
successful run.

`write_dataset` stages an exact immutable artifact and its validation metadata;
it is not the publication point. `finalize_run` requires the exact staged
dataset id, `integrity_status=valid`, and `dataset_facts_status=available`, then
performs the terminal publication transition. After completion,
`list_datasets` and `get_dataset` expose metadata for history and diagnostics
even if a later integrity evaluation becomes invalid or unknown; optional list
filters select current reusable candidates. Run sidecars that are part of the
successful fetch artifact set,
such as the positions snapshot and run-log reference, are written before the
transition. If those artifact writes fail, `delete_run_artifacts` removes any
earlier sidecar, run-log, or pre-publication output artifacts for that run,
then `fail_run` records the failure and no dataset is discoverable.

Serialization checks all supplied canonical values, including nullable
values and UTC clocks, not only contract identity/count, for every
backend and CSV/Parquet format. Changed values or lost columns fail
with SERIALIZATION_INTEGRITY_FAILED before publication.

The fetch coordinator also supplies one strict `OptionChainRowScope` in the
same `DatasetWrite`. Backends persist it atomically beside the dataset record
and mirror it onto every handle. It records the resolved generic-filter flag,
expiration-window setting (`0` means unbounded), and count-conserving totals
from the completed ticker results. Legacy absence loads as `unknown`/null;
contradictory current metadata fails validated load rather than degrading to
unknown.

All semantic readers use
`load_validated_option_chain_dataset(dataset_id)`. The loader resolves the
exact stored record, verifies supported schema and validator versions, checks
the artifact content hash, parses the declared format, revalidates the frame,
and returns one `ValidatedOptionChainDataset` containing the exact handle,
deep-copied frame, integrity summary, and neutral dataset facts. Missing,
tampered, schema-incompatible, incomplete, legacy-unknown, or otherwise invalid
datasets fail closed through provider-neutral typed errors. Raw artifact access
remains available only for inspection and explicit export/download surfaces.

The `pending` status value is reserved for future use; `create_run` sets
`status=running` immediately.

At startup for a real fetch, after the fetcher acquires the advisory lock and
before it opens the new run, it calls `interrupt_stale_runs` with a short cutoff
window. Any older `running` rows are treated as uncleanly terminated prior
processes, marked `interrupted`, and surfaced in startup output. Dry runs do not
mutate storage. Same-day run-count output counts only `complete` runs so stale
or recovered interrupted records do not inflate the operator-facing sequence.

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
zero, false, or the empty string. The supported `read_dataset_file(path)` helper
normalizes CSV reads back to the canonical nullable dtypes used by parquet for
whole-number, boolean, and quote timestamp fields. Direct raw `pd.read_csv`
consumers are responsible for applying equivalent dtype coercion themselves.

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
    def serialize_bytes(self, df: pd.DataFrame) -> bytes: ...
    def serialize(self, df: pd.DataFrame, path: str) -> int: ...
    # serialize_bytes returns the artifact bytes for hash-before-write paths.
    # serialize writes to a path and returns bytes written; both raise on failure.
```

Both `CsvSerializer` and `ParquetSerializer` are implemented in
`opx_chain/storage/serializers.py`. `get_serializer(fmt)` returns the appropriate
instance. `FilesystemBackend`, `SqliteIndexedBackend`, and `MemoryBackend`
select the serializer from `DatasetWrite.format`; `opx-fetch` populates that
field from the `dataset_format` config option (`"csv"` default). The
`DatasetHandle.format` field tells downstream consumers which reader to use and
must match the artifact bytes written by the backend.

`ParquetSerializer` requires the optional `pyarrow` dependency
(`pip install 'opx-chain[parquet]'`). The dependency is checked when parquet
storage is selected, so `opx-fetch --dry-run` and backend construction fail
fast before provider calls instead of failing after rows are fetched. Reading
parquet files uses `opx_chain.utils.read_dataset_file(path)`, which dispatches
on file extension and normalizes format-sensitive canonical dtypes so CSV and
parquet artifacts expose consistent nullable integer, nullable boolean, and UTC
quote timestamp columns.

## 12. Dataset Retention

Retention is configurable through `[storage]` in `$XDG_CONFIG_HOME/opx-chain/config.toml`
(default `~/.config/opx-chain/config.toml`).

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
- pruning removes both the dataset artifact file and the metadata record
- pruning also removes storage-managed sidecar and run-log artifacts associated
  with the pruned run
- run records are retained independently of dataset pruning; they are small
- malformed or negative values in config files fall back to `0` (no pruning)
  with a warning; direct public factory config objects raise a stable config
  validation error instead of constructing a backend with malformed retention

Both storage backends prune by the semantic dataset `created_at` timestamp.
The filesystem backend reads `created_at` from each dataset metadata sidecar;
the SQLite backend orders the dataset table by `created_at`.

## 13. `opx-check` Integration

When storage is disabled, `opx-check` scans the output directory for the latest
CSV by filename timestamp. That compatibility path validates the selected CSV
bytes before using them. When storage is enabled, `opx-check` discovers the
newest published dataset and calls
`load_validated_option_chain_dataset(dataset_id)` before checking position
coverage. Dataset selection is format-neutral; CSV and parquet use the same
validated loader and neither receives semantic preference.

This decouples `opx-check` from the output directory naming convention while
preventing coverage checks from consuming incomplete, unknown-integrity, or
tampered storage artifacts.

## 14. Testing Strategy

The storage layer should be tested through a `MemoryBackend`:

- `MemoryBackend` implements `StorageBackend` using in-memory dicts
- it is used in new tests that exercise the storage-enabled branch of `fetcher.py`
  and `opx-check`; existing tests that use `write_options_csv` directly are unchanged
- it does not write any files, making test isolation trivial
- it should be part of `opx_chain/storage/` so it is importable by tests without patching

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
opx_chain/storage/
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

- `opx_chain/storage/base.py` — `StorageBackend` and `ProviderCache` protocols
- `opx_chain/storage/models.py` — all records and write payloads
- `opx_chain/storage/serializers.py` — `DatasetSerializer` protocol and CSV implementation
- `SCHEMA_VERSION` in `opx_chain/__init__.py`
- `MemoryBackend` in `opx_chain/storage/memory.py`

### Step 2 — Filesystem backend ✓

- `FilesystemBackend` in `opx_chain/storage/filesystem.py`
- `StorageFactory` in `opx_chain/storage/factory.py`
- `[storage]` parsing in `opx_chain/config.py`

### Step 3 — Wire `fetcher.py` and `opx-check` ✓

- `fetcher.py` calls `create_run` / `record_ticker_result` /
  `record_validation` / `write_dataset` / `finalize_run` / `fail_run` when
  storage is enabled
- `opx-check` uses `list_datasets(limit=100)` when storage is enabled and
  loads the selected published identifier through
  `load_validated_option_chain_dataset(...)`; selection is format-neutral
- `also_write_csv` config key (default `true`) controls whether the timestamped
  `runs/options_engine_output_<ts>.csv` is also written alongside the storage artifact
- `storage.dir` controls the fetcher lock, timestamped CSV side write, `_latest`
  copy, and storage backend root as one data directory; relative values resolve
  under `$XDG_DATA_HOME/opx-chain/`
- `_latest` is a copy of the newest timestamped CSV, not a symlink to it

### Step 4 — Parquet serializer ✓

- `ParquetSerializer` in `opx_chain/storage/serializers.py`; requires `pyarrow`
  at serializer selection / backend construction time
- `dataset_format` config option (`"csv"` default)
- shared `read_dataset_file(path)` utility in `opx_chain/utils.py` dispatches on extension

### Step 5 — SQLite-indexed backend ✓

- `SqliteIndexedBackend` in `opx_chain/storage/sqlite_indexed.py`
- WAL mode, foreign keys, version table; schema defined in `docs/METADATA_SPEC.md`

### Step 6 — Provider cache abstractions ✓

- `NullCache` and `FilesystemCache` in `opx_chain/storage/cache.py`
- wired in `fetch.py` at the fetch-orchestration level; caches snapshot, chain,
  and events responses with configurable TTLs
- price context uses the separate durable `price-history.db` daily-bar store;
  `price_context_ttl` controls reconciliation attempts, not artifact retention
- IV history uses the separate durable `iv-history.db` aggregate store. Each
  successful storage-backed option-chain fetch automatically ingests the just-
  written dataset after run finalization; operators can also replay retained
  datasets or run explicit historical provider fetches with
  `opx-iv-history-backfill --fetch-historical`
- Market Data cache keys include the configured `[providers.marketdata].mode`
  (`live`, `cached`, `delayed`, or provider default) so changing mode does not
  reuse responses from a different recency mode
- provider-cache methods validate keys, payload bytes, and positive integer
  TTLs before disabled-cache or filesystem behavior; malformed direct cache
  calls fail consistently instead of being ignored by `NullCache`
- cached JSON payloads must restore to JSON objects; malformed reserved pandas
  timestamp markers and malformed chain pickle bytes are invalidated on read
- filesystem cache prunes expired/corrupt entries on startup and deletes an
  expired entry on read
- config keys: `cache_backend`, `cache_dir`, `snapshot_ttl`, `chain_ttl`,
  `events_ttl`, `price_context_ttl`

### Step 7 — Viewer enhancements ✓

- `opx-view --data-dir DIR` scans an arbitrary directory for `.csv` and
  `.parquet` files; default discovery queries the storage backend, falling back to the timestamped CSV glob
- viewer preference storage remains out of scope until there is a concrete UI
  consumer; no preference API is exposed by the current viewer

## 18. Open Questions

No open questions remain.

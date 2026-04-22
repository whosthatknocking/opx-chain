# External Interface Specification

This document specifies the stable external interface that `opx-chain` exposes to
downstream consumers. It covers the CLI invocation contract, the Python package
interface, and the schema versioning contract.

`opx-chain` does not own any downstream system and has no dependency on them. This
document describes what `opx-chain` commits to stabilizing so that consumers can
integrate without coupling to internal implementation details.

---

## 1. Scope

Two integration points are in scope:

1. **CLI invocation** — a downstream orchestrator can invoke `opx-fetcher` as a
   subprocess to trigger a fresh chain fetch
2. **Storage interface** — a downstream consumer can import `opx_chain` as a Python
   package and use `StorageBackend` to discover and read the latest chain dataset

Everything else — internal storage layout, provider adapters, scoring weights,
normalization logic — is internal to `opx-chain` and may change without notice.

---

## 2. CLI Invocation Contract

### 2.1 `opx-fetcher`

`opx-fetcher` is the entry point for triggering a fresh option-chain fetch.

A downstream orchestrator invokes it as a subprocess:

```
opx-fetcher [--positions <path>] [--enable-filters | --disable-filters]
```

The orchestrator must:
- wait for the process to exit before querying storage for the new dataset
- treat any non-zero exit code as a fetch failure
- not parse stdout or stderr for structured data; those streams are for logging only

**`--positions <path>` (optional)**

Overrides the default positions file path (`data/positions.csv`). When provided,
`opx-fetcher` uses this file to determine which option contracts must survive hard
filters regardless of screening criteria. When absent, behaviour is unchanged.

A downstream orchestrator that manages a per-run positions file passes the
run-specific path here:

```
opx-fetcher --positions data/runs/<run_id>/positions.csv
```

See `docs/PROJECT_SPEC.md` §7.3 for the full behaviour specification.

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Fetch completed; at least one dataset was written to storage |
| non-zero | Fetch failed or was interrupted; no new dataset should be assumed |

### 2.2 No other CLI arguments are part of the external interface

`--enable-filters` and `--disable-filters` are internal operational flags, not part
of the stable downstream interface. A downstream orchestrator should not set them.

---

## 3. Python Package Interface

**Prerequisite:** the Python package interface is only available when storage is
enabled in the `opx-chain` config (`[storage] enable = true`). When storage is
disabled (the default), `opx_chain.storage` modules are importable but
`get_storage_backend()` returns `None` and `list_datasets` is not meaningful.
A downstream consumer must ensure the `opx-chain` instance it connects to has storage
enabled before using this interface.

A downstream consumer may import `opx_chain` as a Python dependency to query the storage
layer without shelling out or scanning the filesystem directly.

### 3.1 Public surface

The stable public surface is limited to:

```python
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.models import DatasetHandle, DatasetRecord, RunRecord
from opx_chain.storage.factory import get_storage_backend
from opx_chain import SCHEMA_VERSION
```

All other modules are internal. Importing from `opx_chain.fetcher`, `opx_chain.normalize`,
`opx_chain.provider`, or any other internal module is not supported and may break across
releases.

### 3.2 Obtaining a backend instance

```python
backend: StorageBackend = get_storage_backend()
```

`get_storage_backend()` returns the configured backend (filesystem or SQLite) based
on the `opx-chain` config. No arguments are required. The consumer must not construct a
backend directly.

### 3.3 Discovering the latest dataset

```python
records: list[DatasetRecord] = backend.list_datasets(limit=1)
```

Returns the most recent successfully written dataset. Returns an empty list if no
datasets exist.

The consumer should validate:
- the list is non-empty (no datasets available → cannot proceed)
- `records[0].schema_version == SCHEMA_VERSION` (schema drift → must re-fetch or
  update the consumer to handle the new schema before proceeding)

### 3.4 Obtaining a dataset handle

```python
handle: DatasetHandle = backend.get_dataset(dataset_id)
```

Returns a `DatasetHandle` for the given `dataset_id`. The consumer reads the chain
artifact at `handle.location`.

### 3.5 Retrieving a run record

```python
run: RunRecord = backend.get_run(run_id)
```

Returns the `RunRecord` for the given `run_id`. Raises `KeyError` when the run
does not exist. Downstream consumers use this to retrieve
`RunRecord.positions_fingerprint` — the SHA-256 of the positions file that was
active when the chain was fetched — for cross-checking against the consumer's
own positions fingerprint.

`run_id` is available on `DatasetRecord.run_id` (returned by `list_datasets`).

```python
records = backend.list_datasets(limit=1)
run = backend.get_run(records[0].run_id)
assert run.positions_fingerprint == pipeline_positions_fingerprint
```

### 3.6 Reading the chain artifact

```python
from opx_chain.utils import read_dataset_file
df = read_dataset_file(handle.location)  # dispatches on .csv / .parquet extension
```

`read_dataset_file` is the recommended reader. It selects `pd.read_parquet` or
`pd.read_csv` based on the file extension, matching `handle.format`. Parquet
requires the optional `pyarrow` dependency (`pip install 'opx-chain[parquet]'`).

---

## 4. `DatasetHandle` Contract

`DatasetHandle` is the stable reference returned by `get_dataset`. The following
fields are part of the external interface contract:

```python
@dataclass
class DatasetHandle:
    dataset_id: str       # stable identifier for this dataset
    location: str         # absolute or relative path to the artifact file
    schema_version: int   # matches SCHEMA_VERSION at write time
    row_count: int        # total rows in the artifact
    format: str           # "csv" | "parquet"
    content_hash: str     # SHA-256 of artifact bytes; use for integrity checks
    created_at: datetime  # UTC timestamp when the dataset was written
```

**Change from STORAGE_SPEC §6:** `content_hash` and `created_at` are added to
`DatasetHandle`. They were previously only on `DatasetRecord`. Downstream consumers
need both for chain integrity verification and freshness checks without having to
fetch the full `DatasetRecord`.

`location` is an absolute path when the filesystem backend is active. Downstream
consumers must not construct or infer artifact paths independently — always use the
`location` field from the handle.

---

## 5. Schema Version Contract

### 5.1 `SCHEMA_VERSION` constant

```python
# opx_chain/__init__.py
SCHEMA_VERSION: int = 1   # incremented on every breaking schema change
```

This integer is the join key between the chain artifact and the consumer's field
expectations. It is written into every `DatasetRecord` and `DatasetHandle` at write
time.

### 5.2 Breaking vs non-breaking changes

| Change type | Version bump required |
|---|---|
| Column removed | Yes |
| Column renamed | Yes |
| Column order changed | Yes |
| Column added (appended) | Yes — downstream must handle unknown columns gracefully, but version still bumps |
| Value format change (e.g., date string format) | Yes |
| Internal scoring weight change | No |
| New provider added | No |

### 5.3 Consumer responsibility

A consumer that detects `schema_version != SCHEMA_VERSION` must not read the
artifact. It should surface a clear error: `chain schema version mismatch:
expected {expected}, got {actual}`. The operator must either re-fetch with the
current `opx-chain` version or update the consumer to support the new schema.

Backward compatibility across schema versions is not guaranteed.

---

## 6. Staleness Contract

A downstream consumer is responsible for determining whether the latest dataset
is fresh enough for its purposes. `opx-chain` does not enforce freshness on behalf of
consumers.

The consumer should use `DatasetHandle.created_at` as the dataset-level timestamp.
For per-ticker freshness, the chain artifact includes `underlying_price_time` per
row — the consumer applies its own staleness policy against that field.

`opx-chain` does not expose a staleness API. The consumer decides what "fresh enough"
means and blocks its own pipeline when the threshold is exceeded.

---

## 7. Changes Required

The following changes to `opx-chain` implement this interface. They are ordered by
dependency.

### 7.1 Add `SCHEMA_VERSION` public constant

- add `SCHEMA_VERSION: int = 1` to `opx_chain/__init__.py`; this is the
  canonical location — `from opx_chain import SCHEMA_VERSION` must work
- also update `opx_chain/export.py` to reference this constant rather than
  defining its own, so there is one source of truth
- write it into `DatasetRecord.schema_version` on every `write_dataset` call
- this is already described in STORAGE_SPEC §3.4 and §17 step 1; this spec
  makes it a named public constant importable from `opx_chain` directly

### 7.2 Add `content_hash` and `created_at` to `DatasetHandle`

Current `DatasetHandle` (STORAGE_SPEC §6):
```python
dataset_id, location, schema_version, row_count, format
```

Required addition:
```python
content_hash: str     # already on DatasetRecord; copy here
created_at: datetime  # already on DatasetRecord; copy here
```

`get_dataset` must populate both fields from the underlying `DatasetRecord`.
No storage schema change is required — both values are already persisted.

### 7.3 Add `--positions` argument to `opx-fetcher`

Implemented. Behaviour is specified in `docs/PROJECT_SPEC.md` §7.3.

### 7.4 Expose `get_storage_backend()` as a public factory function

Implemented. `opx_chain.storage.factory.get_storage_backend()` returns a
`StorageBackend` instance configured from the `opx-chain` config, or `None` when
storage is disabled.

### 7.5 `write_legacy_csv` config option

When `[storage] write_legacy_csv = false` (default `true`), `opx-fetcher` skips
writing the timestamped `output/options_engine_output_<ts>.csv` file. Only the
storage-managed artifact is written. Downstream orchestrators that depend on the
legacy filename pattern must either keep `write_legacy_csv = true` or switch to
reading through `get_storage_backend().list_datasets()`.

### 7.7 Add `get_run()` to `StorageBackend` protocol

Add `get_run(run_id: str) -> RunRecord` to the `StorageBackend` protocol in
`opx_chain/storage/base.py`. The method already exists on `FilesystemBackend` and
`SqliteIndexedBackend`; this change promotes it to the formal protocol so
downstream consumers can call it through the typed interface. `MemoryBackend`
must also implement it so the protocol conformance test passes.

### 7.6 `opx-viewer --data-dir`

`opx-viewer` accepts a `--data-dir DIR` argument that overrides all dataset
discovery — it scans `DIR` for `.csv` and `.parquet` files ordered by
modification time. This is the primary way to view datasets when legacy CSV
output is disabled. The default viewer behavior (no `--data-dir`) is to glob
`output/options_engine_output_*.csv` as before.

---

## 8. What Does Not Change

- CSV output format and column order (governed by `SCHEMA_VERSION`)
- output directory layout
- `opx-fetcher` fetch logic, provider adapters, scoring, or normalization
- `StorageBackend` write interface — consumers are read-only; they never call
  `create_run`, `write_dataset`, or any write method
- `opx-chain` config file format

---

## 9. Relationship to STORAGE_SPEC

This document and `docs/STORAGE_SPEC.md` are complementary:

- `STORAGE_SPEC.md` specifies the full internal storage architecture, all backends,
  the implementation order, and the testing strategy
- this document specifies the external-facing subset of that architecture that
  downstream consumers may depend on

When STORAGE_SPEC changes affect the public surface (e.g., a new field on
`DatasetHandle`), this document must be updated in the same commit.

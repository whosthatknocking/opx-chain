# Metadata Specification

This document answers the open question in STORAGE_SPEC.md §18: *"What
metadata fields are required by the downstream system on day one?"*

It provides the column-level contract for every domain record and SQLite
table in the storage layer: type, nullable status, which implementation
step introduces it, and why it is required. It also documents which
fields the downstream pipeline consumer (`opx-strategy`) depends on and
must be present before that consumer can function.

See STORAGE_SPEC.md for record descriptions, the storage port protocol,
and implementation ordering. See EXTERNAL_INTERFACE_SPEC.md for the
stable public surface exposed to downstream consumers.

---

## 1. `RunRecord`

One record per fetch run. Created by `create_run`, updated by
`finalize_run` and `fail_run`. Persisted by both the filesystem backend
(as a JSON sidecar at `runs/{run_id}/run.json`) and the SQLite backend (`runs` table).

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 2 | Primary key; unique identifier for this fetch run |
| `started_at` | `datetime` | NO | 2 | UTC timestamp when `create_run` was called |
| `finished_at` | `datetime` | YES | 2 | UTC timestamp when `finalize_run` or `fail_run` was called; `None` while running |
| `status` | `str` | NO | 2 | `running` / `complete` / `failed` / `interrupted`; `create_run` sets `running` immediately; `pending` is reserved |
| `provider` | `str` | NO | 2 | Data provider name (e.g., `marketdata`, `yfinance`); required for dataset provenance |
| `config_fingerprint` | `str` | NO | 2 | SHA-256 of the resolved config fields that affect output (provider, tickers, filter settings, scoring weights); two runs with the same fingerprint and positions fingerprint should produce structurally comparable datasets |
| `positions_fingerprint` | `str` | NO | 2 | SHA-256 of the raw positions file bytes; empty string when no positions file is present; changes when held positions change, making it easy to attribute output differences to position vs. market changes |
| `dataset_id` | `str` | YES | 2 | FK to `DatasetRecord`; `None` until `write_dataset` succeeds; a run may complete without a dataset if all tickers fail |
| `error_summary` | `str` | YES | 2 | Short error description when `status = failed` or `interrupted`; `None` otherwise |

**Required by downstream consumer**: `run_id`, `status`, `provider`,
`positions_fingerprint`. The pipeline reads `positions_fingerprint` to
detect whether the chain was collected against the same positions file
that is being processed in the current pipeline run.

---

## 2. `DatasetRecord`

One record per successfully written canonical dataset. Created by
`write_dataset` after the artifact file is written and its hash is
computed. This is the central record that downstream consumers discover
and reference.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `dataset_id` | `str` | NO | 2 | Primary key; stable identifier for this dataset snapshot |
| `run_id` | `str` | NO | 2 | FK to `RunRecord`; links the dataset to the fetch run that produced it |
| `created_at` | `datetime` | NO | 2 | UTC timestamp when the artifact was written; used by the downstream consumer for freshness assessment |
| `provider` | `str` | NO | 2 | Data provider that produced this dataset |
| `schema_version` | `int` | NO | 1 | Value of `SCHEMA_VERSION` at write time; consumer validates this before reading the artifact to detect schema drift |
| `row_count` | `int` | NO | 2 | Total rows in the artifact; used for basic sanity validation by the consumer |
| `format` | `str` | NO | 2 | `csv` (default) / `parquet`; tells the consumer which reader to use |
| `location` | `str` | NO | 2 | Absolute path to the artifact file; consumers must use this field — never construct or infer the path independently |
| `content_hash` | `str` | NO | 2 | SHA-256 of artifact bytes, computed after write completes; used by the downstream consumer for integrity verification and deduplication |

**All fields are required by the downstream consumer.** The pipeline
reads every field from `DatasetRecord` when resolving a chain to consume.
`schema_version` and `content_hash` are the two fields most critical for
correctness — schema drift or a corrupt artifact are fatal errors in the
pipeline.

---

## 3. `DatasetHandle`

Returned by `get_dataset`. The stable external reference passed to
downstream consumers. Defined by EXTERNAL_INTERFACE_SPEC.md §4 as the
public contract — these fields may not be removed or renamed without a
`SCHEMA_VERSION` bump.

| Field | Type | Nullable | Source |
|---|---|---|---|
| `dataset_id` | `str` | NO | `DatasetRecord.dataset_id` |
| `location` | `str` | NO | `DatasetRecord.location` |
| `schema_version` | `int` | NO | `DatasetRecord.schema_version` |
| `row_count` | `int` | NO | `DatasetRecord.row_count` |
| `format` | `str` | NO | `DatasetRecord.format` |
| `content_hash` | `str` | NO | `DatasetRecord.content_hash` |
| `created_at` | `datetime` | NO | `DatasetRecord.created_at` |

`content_hash` and `created_at` are required additions to `DatasetHandle`
(they were previously only on `DatasetRecord`). Downstream consumers need
both to perform integrity checks and freshness assessments without
fetching the full `DatasetRecord`. See EXTERNAL_INTERFACE_SPEC.md §7.2.

---

## 4. `TickerRunRecord`

One record per ticker per run. Written during the per-ticker fetch loop.
Used for run-level diagnostics and to attribute row count changes to
normalization losses vs. filter losses.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `ticker` | `str` | NO | 3 | Underlying symbol |
| `raw_row_count` | `int` | NO | 3 | Rows received from provider before any processing |
| `normalized_row_count` | `int` | NO | 3 | Rows after normalize/enrich and before filter step; isolates normalization losses |
| `kept_row_count` | `int` | NO | 3 | Rows after filters are applied; rows that reach the canonical export |
| `filtered_row_count` | `int` | NO | 3 | Rows removed by filters (`normalized_row_count - kept_row_count`) |
| `expiration_count` | `int` | NO | 3 | Distinct expiration dates in the kept rows |
| `status` | `str` | NO | 3 | `ok` / `skipped` / `error` |
| `error_summary` | `str` | YES | 3 | Short error description when `status = error`; `None` otherwise |

Not part of the downstream consumer's external interface. Used internally
for run diagnostics and the `opx-check` summary.

---

## 5. `ValidationRecord`

One record per validation finding per run. Written during the validate
step within `fetcher.py`. Multiple records with the same `code` may exist
for a single run (one per affected ticker or condition).

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `severity` | `str` | NO | 3 | `error` / `warning` / `info` |
| `code` | `str` | NO | 3 | Machine-readable validation code (e.g., `STALE_QUOTE`, `MISSING_GREEKS`) |
| `count` | `int` | NO | 3 | Number of rows or tickers affected |
| `sample` | `str` | YES | 3 | Optional JSON-encoded detail for the first affected row; `None` when count is the only useful signal |

Not part of the downstream consumer's external interface.

---

## 6. `ArtifactRecord`

One record per auxiliary artifact. Written by `write_artifact` for debug
payloads, run logs, and optional sidecars. Sidecars may live under the owning
run directory instead of the debug artifact directory.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `artifact_id` | `str` | NO | 3 | Primary key |
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `artifact_type` | `str` | NO | 3 | `debug_payload` / `run_log` / `sidecar` |
| `location` | `str` | NO | 3 | Path to the artifact file |
| `content_hash` | `str` | NO | 3 | SHA-256 of artifact bytes |

Not part of the downstream consumer's external interface.

---

## 7. SQLite Schema (Step 5)

When `backend = sqlite` is configured, the following tables are created
by the initial migration. Schema version is tracked in a `_schema_meta`
table; the migration increments it on any structural change.

```sql
CREATE TABLE _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- seed: INSERT INTO _schema_meta VALUES ('schema_version', '1');

CREATE TABLE runs (
    run_id               TEXT PRIMARY KEY,
    started_at           TEXT NOT NULL,   -- ISO 8601 UTC
    finished_at          TEXT,            -- NULL while running
    status               TEXT NOT NULL,
    provider             TEXT NOT NULL,
    config_fingerprint   TEXT NOT NULL,
    positions_fingerprint TEXT NOT NULL,
    dataset_id           TEXT,            -- NULL until write_dataset succeeds
    error_summary        TEXT
);

CREATE TABLE datasets (
    dataset_id      TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    created_at      TEXT NOT NULL,   -- ISO 8601 UTC
    provider        TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    format          TEXT NOT NULL,   -- 'csv' | 'parquet'
    location        TEXT NOT NULL,
    content_hash    TEXT NOT NULL
);

CREATE TABLE ticker_results (
    run_id               TEXT NOT NULL REFERENCES runs(run_id),
    ticker               TEXT NOT NULL,
    raw_row_count        INTEGER NOT NULL,
    normalized_row_count INTEGER NOT NULL,
    kept_row_count       INTEGER NOT NULL,
    filtered_row_count   INTEGER NOT NULL,
    expiration_count     INTEGER NOT NULL,
    status               TEXT NOT NULL,
    error_summary        TEXT,
    PRIMARY KEY (run_id, ticker)
);

CREATE TABLE validations (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL REFERENCES runs(run_id),
    severity TEXT NOT NULL,
    code     TEXT NOT NULL,
    count    INTEGER NOT NULL,
    sample   TEXT    -- JSON-encoded; NULL when not applicable
);

CREATE TABLE artifacts (
    artifact_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs(run_id),
    artifact_type TEXT NOT NULL,
    location      TEXT NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE INDEX idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX idx_datasets_run_id     ON datasets(run_id);
CREATE INDEX idx_runs_status         ON runs(status);
```

All writes use `INSERT OR REPLACE` (or equivalent upsert) so re-running
a fetch after an interrupted run overwrites the prior incomplete record
rather than accumulating duplicates.

---

## 8. Fields Required by the Downstream Consumer

The `opx-strategy` pipeline reads opx storage through `StorageBackend`
as a read-only consumer. These are the fields it depends on from day one:

| Field | Record | Why required |
|---|---|---|
| `dataset_id` | `DatasetRecord` / `DatasetHandle` | Stable reference stored in the pipeline's `runs` table to link every pipeline run to the exact chain it consumed |
| `location` | `DatasetHandle` | Absolute path used to read the chain artifact; must never be constructed independently |
| `schema_version` | `DatasetHandle` | Checked against `SCHEMA_VERSION` before reading; mismatch is a fatal error — the pipeline refuses to process a drifted schema |
| `content_hash` | `DatasetHandle` | Stored in `runs.chain_content_hash`; used for integrity verification and to detect whether a reused chain has been tampered with |
| `created_at` | `DatasetHandle` | Used for chain freshness assessment against the staleness thresholds in STRATEGY.md DATA AUTHORITY |
| `row_count` | `DatasetHandle` | Basic sanity check; a zero-row dataset is a fatal error at stage 3 |
| `format` | `DatasetHandle` | Selects the correct reader (`pd.read_csv` vs `pd.read_parquet`) |
| `positions_fingerprint` | `RunRecord` | Cross-checked against the pipeline's own positions fingerprint to detect chain/positions mismatch |

**`SCHEMA_VERSION`** (from `opx_chain/__init__.py`) is the most critical
single field. The downstream consumer imports it directly:

```python
from opx_chain import SCHEMA_VERSION
assert handle.schema_version == SCHEMA_VERSION
```

A mismatch means either the opx-chain package has been updated without
re-fetching, or a stale chain is being reused across a schema boundary.
Both cases are fatal — the pipeline stops with a clear error message
before any data is read.

---

## 9. Fields That Must Be Present Before `write_dataset` Returns

These fields must be successfully written or the storage backend must
raise before returning. A `DatasetRecord` with any of these fields absent
or zero is a storage bug, not an acceptable null.

- `dataset_id` — must be a non-empty unique string
- `schema_version` — must equal the current `SCHEMA_VERSION` constant
- `location` — must point to an existing file
- `content_hash` — must be a 64-character hex string (SHA-256)
- `row_count` — must be greater than zero; a zero-row dataset indicates
  a failed fetch that should not have called `write_dataset`
- `created_at` — must be a valid UTC timestamp

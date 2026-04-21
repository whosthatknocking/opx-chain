# opx-fetcher: `--positions` CLI Argument

**Status:** Ready to implement  
**Scope:** `opx/opx/fetcher.py` — two localized changes; no other files require modification

---

## Problem

`opx-fetcher` hardcodes the positions path to `data/positions.csv`. When the pipeline orchestrator creates a run-specific copy of positions.csv (at `data/runs/<run_id>/positions.csv`), it has no way to direct opx-fetcher to that file. The fetcher silently reads the wrong path.

---

## Solution

Add an optional `--positions` CLI argument to `opx-fetcher`. When provided, it overrides the default. When absent, behaviour is unchanged.

---

## Existing Infrastructure (no changes required)

| Location | What it provides |
|---|---|
| `positions.py: load_positions(path: Path \| None = None)` | Already accepts an optional path; falls back to `DEFAULT_POSITIONS_PATH` when `None` |
| `positions.py: DEFAULT_POSITIONS_PATH` | `Path("data/positions.csv")` — default preserved |
| `normalize.py: apply_post_download_filters(…, position_keys=None)` | Already bypasses hard filters for current position contracts |
| `check_positions.py: parse_args()` | Canonical `--positions` pattern already in this codebase |

---

## Changes Required

### 1. `parse_args()` — add the argument

```python
# Before (no --positions argument exists):
parser.add_argument("--enable-filters", ...)
parser.add_argument("--disable-filters", ...)

# After:
parser.add_argument("--enable-filters", ...)
parser.add_argument("--disable-filters", ...)
parser.add_argument(
    "--positions",
    type=Path,
    default=None,
    help="Path to positions CSV. Defaults to data/positions.csv.",
)
```

### 2. `main()` — resolve path from args

```python
# Before (line 126):
positions_path = DEFAULT_POSITIONS_PATH.expanduser()

# After:
positions_path = (args.positions or DEFAULT_POSITIONS_PATH).expanduser()
```

`load_positions(positions_path)` on line 127 is unchanged — it already accepts a resolved `Path`.

---

## Behaviour Specification

| Scenario | Behaviour |
|---|---|
| `--positions` not provided | Resolves to `DEFAULT_POSITIONS_PATH` (`data/positions.csv`) — identical to current behaviour |
| `--positions <path>` provided, file exists | Loads positions from `<path>`; position bypass filter applies to those positions |
| `--positions <path>` provided, file not found | `load_positions` returns `EMPTY_POSITION_SET`; fetcher continues with no position bypass — existing graceful handling |
| `--positions` provided without `--enable-filters` | Filter state is independent; positions path does not imply filter state |

---

## Logging

Log the resolved positions path at startup so pipeline runs are auditable:

```python
logger.info("positions path: %s", positions_path)
```

Place this immediately after the path resolution, before `load_positions()` is called.

---

## Pipeline Integration

The orchestrator passes the run-specific path at stage 3:

```
opx-fetcher --positions data/runs/<run_id>/positions.csv
```

When invoked standalone (outside the pipeline), the argument is omitted and the default path applies.

---

## Out of Scope

- No changes to `load_positions()`, `PositionSet`, or `apply_post_download_filters()`
- No path validation beyond what `load_positions()` already does (missing file → empty set)
- No support for multiple `--positions` files
- `opx-snapshot` receives its own `--positions` argument via an identical pattern (separate implementation)

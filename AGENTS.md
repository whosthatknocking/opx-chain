# AGENTS.md

This file gives project-specific guidance to AI agents working in this repository.

## Project Context

- Project: `opx`
- Purpose: fetch near-term option chains from one configured provider, normalize them into a canonical CSV schema, enrich them with shared analytics, and serve a local viewer for inspection
- Runtime: Python `3.10+`
- Main entrypoints:
  - `opx-fetcher` for data collection and CSV export
  - `opx-viewer` for the local HTTP viewer
- Packaging:
  - install with `python -m pip install -e .`
  - dev install with `python -m pip install -e ".[dev]"`

## Source of Truth

When behavior, naming, or scope is unclear, use these files in this order:

1. `docs/PROJECT_SPEC.md`
2. `docs/USER_GUIDE.md`
3. `docs/FIELD_REFERENCE.md`
4. `docs/DEVELOPMENT.md`
5. `README.md`
6. `docs/SYSTEM_SPEC.md` and `docs/DESIGN_SPEC.md` for downstream intent and UI direction

Keep those files aligned with the implementation. If you change canonical fields, provider behavior, config keys, CLI behavior, viewer behavior, or validation semantics, update the docs in the same task.

## Architecture Map

- `opx/fetcher.py`
  - CLI entrypoint for fetch runs
  - runtime config reporting
  - fetch lock handling
  - export writing and run-level validation
- `opx/fetch.py`
  - per-ticker fetch orchestration
  - expiration filtering
  - provider execution, normalization, filtering, and progress logging
- `opx/config.py`
  - config loading from `~/.config/opx/config.toml`
  - defaults, fallback warnings, provider selection, and runtime override support
- `opx/providers/`
  - provider contract in `base.py`
  - vendor implementations in `yfinance.py`, `massive.py`, and `marketdata.py`
- `opx/normalize.py`
  - canonical field normalization
  - shared post-download filters
  - enrichment handoff into pricing and freshness metrics
- `opx/metrics.py` and `opx/greeks.py`
  - derived analytics, scoring, and options math
- `opx/export.py`
  - canonical export column handling and CSV writing
- `opx/validate.py`
  - row-level and export-level validation
- `opx/viewer.py`
  - local HTTP server
  - CSV discovery and serialization
  - dataset summaries and reference content wiring
- `opx/viewer_static/`
  - frontend assets for the local viewer

## Non-Negotiable Design Rules

- Preserve the canonical CSV schema as the primary product contract unless the docs are updated deliberately.
- Keep exactly one active provider per run. Do not mix rows from multiple providers in one export.
- Prefer mapping provider-native values into canonical columns over adding provider-specific scratch fields.
- Keep shared metrics provider-agnostic once rows have been normalized.
- Do not add secrets to tracked files, logs, docs, or debug dumps.
- Do not silently reinterpret provider data when semantics do not match. Leave fields blank rather than map misleading values.
- Keep the viewer as an inspection tool, not a trading terminal or decision engine.
- Maintain stable output and behavior across fetch, export, validation, and viewer layers together.

## Provider and Pipeline Conventions

- Add or change provider-specific market-data logic under `opx/providers/`.
- Route provider payloads through the shared `DataProvider` contract; do not bypass it from fetch orchestration.
- Normalize vendor frames through `normalize_provider_frame(...)` or equivalent provider methods before enrichment.
- Keep provider debug dumps representative of the raw upstream payload shape.
- Respect config-driven pacing, retry, credential, and mode behavior already implemented in the provider layer.
- Use shared post-download filters as the main tradability gate unless there is a documented reason to narrow data earlier.
- If a provider plan or upstream API is delayed, sparse, or unreliable, document that caveat clearly instead of presenting the data as fresher or more complete than it is.

## Config and Runtime Rules

- Runtime settings come from `~/.config/opx/config.toml`; `config/example.toml` is the tracked template.
- Defaults and fallback warnings in `opx/config.py` are part of the product behavior. Keep startup reporting accurate if config handling changes.
- If the selected provider is misconfigured, preserve the current clear fallback or failure behavior rather than failing ambiguously.
- Secrets must stay redacted in any user-facing output.

## Viewer and Export Conventions

- Keep exported CSVs under `output/`, logs under `logs/`, and optional provider payload dumps under `debug/`.
- If you change exported columns, also update the viewer serialization assumptions and field-reference docs.
- Keep viewer endpoints and payloads aligned with the current tab model: `Dataset`, `Overview`, `Chain View`, and `Reference`.
- Use JSON-serializable payloads only when sending data to the browser.

## Error Handling and Stability

- Raise clear project-appropriate errors for config, authentication, mapping, and validation failures.
- Do not leak raw provider exceptions to users when the app can normalize them into a clearer failure mode.
- Preserve fetch locking, validation reporting, and run logging unless there is a strong reason to change them.
- Be careful with market-open and stale-quote edge cases. Freshness fields are user-facing and should remain trustworthy.

## Testing Expectations

Run the smallest relevant test set first, then broaden if needed.

- Main suite: `pytest`
- Lint: `pylint $(git ls-files '*.py')`

Testing guidance:

- Add or update tests for any behavior change in provider mappings, normalization, metrics, validation, export shape, or viewer payloads.
- Prefer offline, deterministic tests by default.
- If a change depends on live upstream provider behavior, say so explicitly and note what was not verified locally.
- If you change docs-visible output fields or viewer summaries, add or update focused tests where practical.

## Documentation Expectations

Update docs when any of these change:

- canonical field names or meanings
- provider selection or credential behavior
- config keys or defaults
- filter or validation behavior
- CLI flags or run instructions
- viewer tabs, summaries, or reference behavior
- supported or unsupported provider capabilities

Common files to update:

- `README.md`
- `docs/PROJECT_SPEC.md`
- `docs/USER_GUIDE.md`
- `docs/FIELD_REFERENCE.md`
- `docs/DEVELOPMENT.md`

## Practical Workflow

1. Read the affected code and the matching contract docs first.
2. Make the smallest coherent change that keeps fetch, export, validation, and viewer behavior aligned.
3. Update tests with the code change.
4. Update docs if user-facing behavior changed.
5. Run targeted verification, then broaden if warranted.

## Commit and PR Guidance

- Use imperative commit subjects, for example `docs: add provider-mapping guidance`.
- Keep commits small, single-purpose, and easy to review.
- Include tests with behavior changes in the same commit when practical.
- Avoid mixing unrelated refactors with schema or provider behavior changes.
- In PRs, summarize intent briefly and list the validation steps actually run.
- If validation was skipped or limited, say so explicitly.

## Repository-Specific Notes

- The package version is defined in `pyproject.toml`.
- Current supported providers are `yfinance`, `massive`, and `marketdata`.
- `viewer.py` remains the stable top-level viewer entrypoint name.
- This project is the data and screening layer. The portfolio decision engine described in `docs/SYSTEM_SPEC.md` is downstream and should not be collapsed into the fetch/viewer runtime.

## Good Changes

- tightening a provider-to-canonical field mapping with tests
- improving normalization, freshness, or scoring logic while preserving documented schema intent
- making config fallback behavior clearer and better documented
- fixing CSV serialization or viewer payload edge cases
- updating docs so they match the actual provider and viewer behavior

## Bad Changes

- mixing providers in one export file
- adding undocumented columns casually to the canonical CSV
- exposing secrets or raw credentials in config examples, logs, or dumps
- bypassing the provider contract from fetch orchestration
- changing viewer or export behavior without updating docs and tests

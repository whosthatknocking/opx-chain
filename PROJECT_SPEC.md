# Project Specification: Multi-Provider Options Fetcher

## 1. Goal

Extend the project from a Yahoo-only fetcher into a provider-driven options data pipeline that can run against exactly one market-data provider at a time.

Use the project and repository name `opx` so it is easier to find and type in terminal workflows.

Initial supported providers:

- `yfinance`
- `massive` (Polygon / Massive API)

The system must make it obvious:

- which provider produced a dataset
- which fields are fully populated by that provider
- which fields are unavailable, approximated, or intentionally left blank
- which credentials are required to run that provider
- when a user needs to complete provider onboarding before the provider can be used

Naming constraint:

- `viewer.py` remains unchanged

This spec defines the target behavior, implementation boundaries, and documentation requirements.

## 2. Problem Statement

The current codebase already has a provider abstraction, but the product behavior is still effectively Yahoo-centric:

- provider selection is hard-coded in config
- logging language assumes `yfinance`
- documentation describes a Yahoo-only data model
- there is no dedicated user config file pattern
- there is no provider coverage matrix describing data availability differences
- the project and repo naming remain longer than desired for terminal-oriented use

Adding Massive without tightening those contracts would create ambiguity in the exported data and in the user experience.

## 3. Product Outcome

After this work, a user should be able to:

1. Choose a single active provider for a run.
2. Store provider credentials outside tracked source files in a user config file.
3. Run the fetcher with either `yfinance` or `massive`.
4. See in the output and docs which provider generated the data.
5. Understand which fields are vendor-native, derived, unavailable, or lower-confidence for the selected provider.
6. Use the shorter project and repository name `opx` for day-to-day terminal work.

## 4. Non-Goals

This project does not aim to:

- merge data from multiple providers in one run
- reconcile rows across providers
- auto-fallback from one provider to another during a run
- build a secret vault or cloud secret manager
- normalize every possible field across all future vendors in this phase
- rename `viewer.py`

The runtime model is one provider per run, chosen explicitly.

## 5. Naming and Packaging

### 5.1 Project and Repository Rename

The project and repository name is `opx`.

Intent:

- shorten terminal commands
- make the project easier to locate in shell history, tab completion, and local workflows
- keep the public working name concise

This rename applies to:

- repository naming
- package/module naming where appropriate
- documentation references
- user-facing setup and command examples

Constraint:

- `viewer.py` remains unchanged

### 5.2 Rename Safety

The rename should be handled conservatively.

Rules:

- preserve behavior while shortening names
- update internal references only where needed to support the new name
- avoid unnecessary command churn for the viewer entrypoint
- document any compatibility implications if import paths or commands change

## 6. Provider Model

### 6.1 Single Active Provider

At runtime there must be exactly one active provider.

Allowed values:

- `yfinance`
- `massive`

Behavior:

- all fetch operations for a run use the active provider only
- no mixed-provider rows are allowed in one output file
- the run log records the active provider once at startup
- the CSV includes row-level `data_source`
- the dataset metadata shown in the viewer includes the active provider
- the default provider is `yfinance` so a new user can run the project with minimal setup

### 6.2 Provider Naming

Use the internal provider key:

- `massive`

Documentation should clarify the branding:

- `Massive (Polygon.io / Polygon rebrand)`

This avoids confusion between product naming and implementation naming.

## 7. Configuration and Secrets

### 7.1 User Config File

Runtime configuration should be stored in a user config file:

- `~/.config/opx/config.toml`

This file becomes the single source of truth for provider selection and user-local settings.

It should contain both non-secret settings and provider credentials.

Relevant settings to move into this file include:

- tickers
- spread thresholds
- staleness thresholds
- expiration window
- active provider name
- other user-tunable runtime settings currently held in code constants

The active provider is selected in this config file, not via environment variables.

Defaults:

- if the config file is absent or incomplete, defaults should favor `yfinance`
- `yfinance` remains the default provider because it requires the least effort to get started

There is no need for environment-variable overrides in this phase.

### 7.2 Credentials in Config File

Provider credentials must be stored in the user config file, not in tracked project files.

Target file:

- user-local config: `~/.config/opx/config.toml`

Requirements:

- credentials are never stored in repository source files
- credentials are never committed to git
- credentials are never copied into README examples as real values
- missing credentials produce a clear error only when the selected provider requires them

Initial secret keys:

- `massive_api_key = "..."`

`yfinance` should not require secrets.

### 7.3 Credential Handling Rules

- never hard-code API keys in source
- never store credentials in tracked repo files
- never print full secrets in logs or exceptions
- never share credentials in documentation, examples, commits, screenshots, or logs
- credentials should be treated as private local machine configuration only
- if logging config state, redact secret values

## 8. Provider Capability Requirements

### 8.1 Shared Contract

Each provider must implement the existing provider interface and return canonical normalized fields where possible.

The canonical schema remains the product contract. Providers map into it.

The generalized CSV schema should remain mostly unchanged.

Schema rule:

- do not add new CSV fields unless there is a clear, documented need
- prefer mapping provider-native fields into existing canonical columns
- prefer keeping downstream CSV consumers stable over exposing every provider-specific field
- if a new field is unavoidable, document why an existing canonical field could not represent it

The default implementation approach is schema preservation, not schema expansion.

Each provider implementation must clearly classify fields as:

- vendor-supplied
- derived by the app
- unavailable for that provider

### 8.2 Canonical Field Semantics

Canonical columns must preserve stable meaning across providers.

Rules:

- if a provider supplies a field whose meaning matches an existing canonical column, use the provider value directly
- if the provider does not supply the field, derive it in-app when possible
- if the provider supplies a similarly named field with materially different semantics, do not map it blindly
- when semantics differ, either transform the value into the canonical meaning or leave the canonical field blank

This matters especially for pricing and Greeks fields, where two providers may expose similarly named metrics that are not computed from the same assumptions.

### 7.3 YFinance Provider

Status:

- existing baseline provider
- unofficial / scraping-based
- slower and less reliable near market open

Expected characteristics:

- no API key
- supports underlying snapshot, option expirations, and option chains
- may provide delayed or stale quote times
- may have empty or incomplete chains

YFinance-specific note:

- some canonical metrics are currently derived in-app because Yahoo data does not provide them directly
- this includes derived fields such as Greeks and related analytics produced by the existing pipeline

### 7.4 Massive Provider

Status:

- new provider to implement
- API-key backed
- expected to be the more explicit and production-oriented data source
- requires provider onboarding before use

Initial required capabilities:

- load underlying snapshot for a ticker
- list option expirations
- load option chain for a ticker and expiration
- normalize Massive response fields into canonical columns

Massive-specific field policy:

- if Massive provides canonical fields directly, use those values directly
- if Massive provides Greeks or similar analytics whose meaning matches the current canonical columns, prefer the provider-supplied values over recomputing them
- if Massive provides a field that differs in definition, model, units, or timestamp basis from the canonical field, document that difference before mapping it
- only fall back to in-app derivation when the provider does not supply the canonical field or when the provider field cannot be safely mapped as-is

Onboarding documentation must explicitly state:

- `massive` is not zero-setup
- the user must create a Polygon / Massive account
- the user must obtain an API key during onboarding
- the API key is only required when `massive` is the selected provider
- a user running the default `yfinance` flow does not need to provide any API key

Provider setup docs should include a short provider-specific section such as:

- `yfinance`: no onboarding required beyond Python dependencies
- `massive`: onboarding required, API key required, key stored in `~/.config/opx/config.toml`

## 9. Data Coverage and Documentation Requirements

### 8.1 Field-Level Clarity

The documentation must explicitly distinguish:

- fields generated for all providers
- fields generated only for `yfinance`
- fields generated only for `massive`
- fields derived in-app and therefore available when required inputs exist
- fields supplied directly by the provider and passed through into canonical columns
- fields that may be blank for one provider because the source API does not expose them

This must be visible in:

- `README.md`
- any field reference shown in the viewer
- provider setup instructions

### 8.2 Required Coverage Matrix

Add a provider coverage section to the documentation with a matrix like:

- field group
- `yfinance` support
- `massive` support
- notes

Minimum field groups:

- underlying snapshot
- underlying market state
- underlying quote timestamp
- option bid/ask/last
- volume
- open interest
- implied volatility
- quote timestamp
- historical volatility
- derived Greeks
- expected move
- run-log diagnostics

### 8.3 “Generated vs Not Generated” Language

For each provider, docs must avoid implying that all fields are always present.

Use explicit language such as:

- “Generated for both providers”
- “Generated only when the provider supplies quote timestamps”
- “Not generated for `yfinance`”
- “Not currently generated for `massive` in phase 1”
- “Derived for `yfinance`, provider-supplied for `massive`”

The CSV browser reference tab should reflect the same wording.

## 10. Output Contract

### 9.1 CSV

Each exported row must continue to include:

- `data_source`

The CSV contract should otherwise remain as stable as possible.

Requirements:

- preserve the existing canonical column set unless there is a strong reason to change it
- avoid provider-specific CSV branches
- avoid adding new columns just because one provider exposes more raw fields
- when one provider offers richer native analytics, map them into existing canonical columns where semantics match

Additional desired dataset-level clarity:

- startup log entry showing active provider
- viewer summary card showing active provider
- optional future metadata file or dataset header summary if needed

### 9.2 Logging

Logging must become provider-neutral.

Current Yahoo-specific phrases such as `raw_yfinance_rows` should be generalized to names like:

- `raw_provider_rows`
- `provider=<name>`

Required log behavior:

- record active provider at run start
- record provider-specific raw row counts
- record provider-raised errors
- avoid vendor-specific wording in shared code paths

## 11. Architecture Changes

### 10.1 Provider Selection

Refactor provider selection into a small registry/factory model.

Requirements:

- central list of supported providers
- clear error for unsupported provider names
- easy addition of future providers without editing unrelated fetch logic
- selected provider is read from `~/.config/opx/config.toml`
- default provider is `yfinance`

### 10.2 Massive Provider Module

Add a new provider module:

- `opx/providers/massive.py`

Responsibilities:

- API client calls
- response parsing
- pagination handling if required by the API
- mapping external field names into canonical columns
- provider-specific timestamp normalization
- provider-specific market-state normalization if available
- deciding when provider-native analytics should populate existing canonical fields directly
- avoiding lossy or misleading mappings when Massive field semantics do not match canonical semantics

### 10.3 Secrets Loader

Add a small config-loading utility responsible for:

- reading `~/.config/opx/config.toml`
- exposing selected provider and runtime settings
- exposing provider credentials to providers
- distinguishing missing file from missing required key
- keeping credential access isolated from business logic

This should be simple and local to the repo, not a full configuration framework.

### 11.4 Rename Work

Rename-related implementation work should be scoped explicitly.

Expected changes:

- use the package/module path `opx`
- update imports and entrypoints that depend on the package name
- update documentation examples to use the new name where applicable
- keep `viewer.py` unchanged

Open implementation question:

- whether a short compatibility layer is needed during transition for imports or CLI usage

## 12. User Experience Requirements

### 11.1 Setup

A new user should be able to determine:

- which provider is active
- whether that provider needs an API key
- whether onboarding is required before the provider can be used
- where to place the key
- what data differences to expect from the provider

The onboarding flow must be explicit:

- `yfinance` is the default and requires no provider account
- `massive` requires account onboarding and API key setup before first use
- the point at which the API key becomes mandatory must be stated clearly: only when `massive` is selected in config

### 11.2 Errors

Errors should be explicit and actionable.

Examples:

- unsupported provider name
- missing `massive_api_key` when `massive` is selected
- provider authentication failure
- provider rate limit or empty response
- provider returned data but a field required for derivation is absent

### 11.3 Viewer

The viewer should surface provider identity in a visible place.

At minimum:

- active provider in dataset-level metadata
- field reference language that does not overclaim availability

## 13. Testing Requirements

Add or update tests for:

- provider factory returns `yfinance` and `massive`
- unsupported provider raises a clear error
- config loader reads `~/.config/opx/config.toml` correctly
- missing Massive key fails only when `massive` is selected
- fetch logging uses provider-neutral messages
- Massive normalization produces canonical columns
- Massive provider-native fields map into existing canonical columns without unnecessary schema growth
- provider-native Greeks are used directly when their meaning matches the canonical fields
- mismatched provider field semantics are not silently mapped into canonical columns
- renamed package/import paths resolve correctly after the `opx` rename
- `viewer.py` remains usable without rename-related regressions
- field coverage documentation stays aligned with implementation where practical

Prefer fixture-driven tests with provider stubs for shared fetch behavior.

## 14. Milestones

### Milestone 1: Config Migration Refactor

This is the required first step and must be completed before implementing the new provider.

Status: Implemented on 2026-03-23.

Implemented changes:

- project and runtime naming were updated to `opx` where applicable
- the Python package/module path was renamed to `opx`
- `viewer.py` remained unchanged as the entrypoint
- `~/.config/opx/config.toml` was introduced as the user config contract
- provider selection and user-tunable runtime settings were moved into the config loader model
- `yfinance` is the default provider when config is absent or incomplete
- config loading and validation were added
- provider credential access was isolated behind the config layer

Implemented exit criteria:

- the project consistently uses `opx` naming where applicable
- package imports work under the new name
- `viewer.py` still works without being renamed
- the application can run from the new config source
- provider selection is read from config
- Massive credentials are read from config when needed
- no environment-variable override path is required

Goals:

- rename the project and repository references to `opx`
- rename the Python package/module path where appropriate
- keep `viewer.py` unchanged
- introduce `~/.config/opx/config.toml` as the user config contract
- move provider selection and other relevant user-tunable runtime settings into that file
- establish `yfinance` as the default provider in the config model
- add config loading and validation
- isolate credential access from the rest of the fetch pipeline

Exit criteria:

- the project consistently uses `opx` naming where applicable
- package imports work under the new name
- `viewer.py` still works without being renamed
- the application can run from the new config source
- provider selection is read from config
- Massive credentials are read from config when needed
- no environment-variable override path is required

### Milestone 2: Provider Contract Cleanup

Status: Partially implemented.

Implemented changes:

- provider selection now uses a central factory/registry structure
- shared fetch logging was generalized from `raw_yfinance_rows` to `raw_provider_rows`

Still pending:

- full enforcement of schema-preservation rules in implementation and tests
- complete provider-neutral documentation around provider-supplied versus derived fields

Goals:

- generalize provider registry/factory behavior
- remove Yahoo-specific wording from shared logging and shared code paths
- preserve the canonical CSV schema as the primary contract
- document and enforce the rules for provider-supplied versus derived canonical fields

Exit criteria:

- shared code paths are provider-neutral
- schema preservation rules are enforced in implementation and tests
- the system is ready for a second provider without further config redesign

### Milestone 3: Massive Provider Implementation

Status: Not implemented.

Implemented prerequisite:

- Massive can be selected in config and validated for missing credentials, but runtime execution remains unimplemented

Goals:

- add `massive` provider module
- wire API key usage through the config loader
- implement expiration, snapshot, and option-chain retrieval
- normalize payload into canonical schema
- use Massive-native analytics directly where their meaning matches the canonical fields

Exit criteria:

- `massive` can be selected in config and execute a full fetch run
- missing or invalid Massive credentials fail clearly
- provider-native fields map safely into the existing schema

### Milestone 4: Documentation and Viewer Clarity

Status: Partially implemented.

Implemented changes:

- README now documents `~/.config/opx/config.toml`
- README now documents that `massive` uses an API key in config

Still pending:

- provider onboarding documentation
- provider coverage matrix
- provider-aware field reference updates
- viewer metadata surfacing for active provider beyond existing dataset cards

Goals:

- add provider setup and onboarding docs
- document that `massive` requires onboarding and API key setup
- add coverage matrix
- update field descriptions to mention provider limitations
- surface active provider in viewer metadata

Exit criteria:

- a new user can tell when an API key is required
- generated versus provider-supplied versus unavailable fields are documented clearly
- viewer reference content matches the written documentation

### Milestone 5: Validation

Status: Partially implemented.

Implemented changes:

- automated tests were added for config loading, provider selection, unsupported provider handling, and Massive-key validation
- package rename behavior and the unchanged `viewer.py` entrypoint are covered by the updated test suite

Still pending:

- validation of a working Massive fetch path
- broader provider-behavior coverage once the Massive implementation exists

Goals:

- add/update automated tests
- run fetch path against both providers where feasible
- verify exported CSV and viewer labels

Exit criteria:

- config migration is covered by tests
- provider behavior is covered by tests
- the documented contract matches actual outputs

## 15. Acceptance Criteria

The project is complete when:

1. A user can select `yfinance` or `massive` as the only provider for a run.
2. Massive credentials are loaded from `~/.config/opx/config.toml`, not tracked source files.
3. Running with `yfinance` requires no API key.
4. Running with `massive` fails fast with a clear message if `massive_api_key` is missing.
5. Shared logs and code paths no longer use Yahoo-specific naming.
6. The exported dataset clearly identifies its provider.
7. Documentation clearly states which fields are generated, conditionally generated, provider-supplied, or not generated for each provider.
8. The viewer reference content reflects the same provider-aware documentation.
9. Documentation clearly states that `massive` requires onboarding and an API key, while `yfinance` is the default low-friction starting point.
10. Credentials are never leaked, shared, or committed.
11. The canonical CSV schema remains mostly unchanged unless a new field is justified explicitly.
12. Provider-native analytics such as Greeks are used directly when they match canonical semantics, and are not remapped blindly when their meaning differs.
13. The project and repository naming are updated to `opx` where applicable.
14. `viewer.py` remains unchanged.

## 16. Open Questions

These should be resolved before or during implementation:

- Which Massive endpoints will be used for underlying snapshots, option contracts, and quotes?
- Does Massive provide all timestamps needed for current freshness metrics?
- Do we want provider-specific rate-limit backoff in phase 1, or can failures remain simple and explicit?
- No temporary compatibility layer is needed; the rename should remain a one-pass change.

## 17. Recommended Implementation Direction

Build this as a strict provider-contract cleanup, not as a large redesign.

Recommended sequence:

1. Introduce `~/.config/opx/config.toml` and move user-local runtime settings there.
2. Keep project/package references on `opx` while keeping `viewer.py` unchanged.
3. Generalize provider registry and logging names.
4. Implement `massive` provider behind the existing interface.
5. Update onboarding docs and viewer reference text to reflect provider-aware field coverage.

This keeps the current architecture intact while making the data source explicit, replaceable, and understandable.

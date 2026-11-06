# Benchmark Refresh Contract Audit

## Explicit Fetch Latest

Status: closed. Operator-approved implementation gap, 2026-11-05.

`DailySeriesRequest` currently admits only `reuse_only` and `reuse_or_fetch`.
`acquire_daily_series` returns any compatible current cache hit before reaching
the source-permission or acquisition path. A downstream control labeled Fetch
latest therefore cannot truthfully request refresh through this public API.
Provider support is currently discoverable only through acquisition or private
implementation details; configuration inspection needs a pure public contract.

Approved scope:

- Add `fetch_latest`, which skips ordinary pre-request cache reuse but retains
  valid evidence as a fallback. Automatic and reuse-only behavior stay compatible.
- Preserve exact source approval, replay isolation, deadlines, validation,
  minimum-attempt throttling, lease ownership and provider selection. Force does
  not grant permission or override quota safeguards.
- Coalesce simultaneous refreshes only when compatible evidence was fetched no
  earlier than the requesting acquisition start; never call a pre-request cache
  hit a successful forced refresh. Do not change immutable source identities.
- Expose source capability metadata without network, credentials, cache reads
  or writes. SPY/yfinance remains unsupported for the required split-only basis;
  SPY/marketdata and VIX_SPOT/cboe_daily_vix keep their existing exact sources.
- Document the public additions in EXTERNAL_INTERFACE_SPEC.md in the same code
  commit. No strategy imports or strategy policy belong in this package.

Verification must pair current-cache Auto reuse with forced refresh, denied and
historical callers, failed refresh with retained evidence, throttling, invalid
source data and simultaneous callers. All provider operations are stubbed.

## Resolution

Added `fetch_latest`, the public accepted-mode tuple and pure source capability
descriptor, with identical root/module exports. Ordinary cache reuse remains
unchanged; a current pre-request snapshot no longer short-circuits an explicit
refresh. Only compatible evidence received during the requesting acquisition
window can coalesce a concurrent refresh. Permission, replay, shared throttle,
lease, validation, receipt provenance and retained fallback remain enforced.

Focused benchmark/version tests pass (70 cases); the full offline suite passes
1,702 tests. Focused source/test lint scores 10/10. The normal commit gate also
runs the complete offline tests and tracked-source lint before publication.
No live source call,
credential change, provider switch or source-access approval is part of this
verification.

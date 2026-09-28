# Isolated unusable option quotes

## Follow-up audit: refresh failure boundaries

Closed: AUD-01a0e9e6-3408-79ae-a64b-3fcfb9c32a41 (strategy audit ledger).
A refresh transport exception escapes and causes fetch to discard previously
validated ticker rows. Preserve them, quarantine the unresolved quote, and report
the transport error class without leaking exception text. Authentication, quota,
structural and normalization failures remain fatal or retain existing handling.
Resolution: catch only recognized transport exceptions around the refresh call,
retain original quotes for quarantine, and persist a safe exception-class field.
Verified with seven transport variants, negative permission/quota/mapping cases,
held/unheld fetch-path tests, and the complete offline suite (1,504 passed before
adding two further passing fetch-path timeout variants).

Open: AUD-01a0e9e6-6497-7441-a6cb-4875d24c3c2c (same audit round).
Refresh incorrectly recalculates the ticker-wide quarantine limit from an
expiration subset. Twenty ticker rows with two crossed quotes in one three-row
expiration pass initial validation but fail unchanged refresh validation. Use
the original ticker denominator consistently, preserving structural checks.

## Original repair

Status: closed. Cross-package reference:
AUD-01a0e99f-d4b8-778d-ab29-ec7e0692c2c4 in opx-strategy.

On 2026-09-28 MarketData quote and chain endpoints both returned
UBER261120P00037500 with bid 0.01, ask 0, askSize 0 and midpoint 0.005.
The provider values survived normalization unchanged. Pre-filter bid/ask
validation aborted acquisition even though the contract was not held.

Required repair: a bounded quote-only quarantine after exhaustive structural
validation, one refresh of affected expirations, durable evidence and explicit
per-ticker warnings. Identity, schema, timestamp and widespread failures remain
fatal. Published rows must still pass the existing strict integrity gate.
Strategy owns missing-position action suppression and operator presentation.

Implemented acquisition-only quarantine of finite nonnegative crossed quotes,
including zero asks, with one refresh per affected expiration. Existing strict
validators still inspect every original row before this exception is considered.
Other fatal findings, widespread failures (over ten rows or ten percent with a
one-row floor), and an empty surviving dataset cannot pass. Original/refreshed
values and timestamps are retained in ticker warning metadata and logs.
No zero-ask market convention is assumed and no executable price is invented.

Verification: 1,489 offline tests passed, including both quote classes, refresh
recovery, disappearing contracts, unrelated corruption, widespread failures,
held-contract exclusion and warning round trips through all three backends.

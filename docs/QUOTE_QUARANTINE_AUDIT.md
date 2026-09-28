# Isolated unusable option quotes

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

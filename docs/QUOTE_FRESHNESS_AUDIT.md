# Quote freshness assessment boundary

Status: closed. Cross-package reference:
AUD-01a0f314-c015-71b4-af6a-e93b7ac394e7 in opx-strategy's audit ledger.

The September 30 SPCX fetch starts at 14:27:58.460 UTC. Its October 9 chain
arrives at 14:28:02.582 with a quote timestamp of 14:28:00. Using the start
instant as the freshness reference produces -1.540012 seconds and incorrectly
marks that received quote stale. Similar flags affect 359 unique candidates
in the downstream saved context.

Capture the assessment instant after acquisition, including bounded quote
refresh. Use it consistently for option and underlying quote ages. Preserve
the fetch-start timestamp in diagnostic logs. Do not clamp truly future-dated
quotes, change stale thresholds, replace cached source timestamps, or rewrite
previously published datasets.

Regression scope: ordinary acquisition across a clock boundary, refreshed
quotes, cached old quotes, old/future/missing timestamp flags and separate
option/underlying ages. Fix package-local acquisition, not downstream strategy.

Resolution: acquisition now captures its freshness assessment after quote
quarantine/refresh and validation, before enrichment. Both quote-age columns
use this instant. Existing future/old/missing metrics semantics are unchanged.
Fetch-start and assessment instants are logged separately. Offline regression
coverage includes six normal/refresh cases and cache reuse without refreshing
source timestamps.

Verification: full offline suite passes (1,515 tests; two existing pandas
warnings). Changed-module lint passes. Canonical and wheel-packaged reference
documentation are synchronized. Package version: 0.6.3.

## Follow-up audit

Reviewed the final acquisition path, both age consumers, refresh completion,
cache reuse and negative-age safeguards again after the fix. The assessment
clock is sampled after validation of the final acquired chain, before either
age consumer; no earlier consumer retains the fetch-start reference. Provider
timestamps are not rewritten, and stale thresholds and package boundaries are
unchanged. Empty/error paths do not publish partially assessed rows. Full
repository Pylint passes at 10/10. No additional actionable finding was found
in this scope; the full offline suite is repeated before this audit commit.

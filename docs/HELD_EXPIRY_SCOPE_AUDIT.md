# Held-expiry publication count audit

AUD-01a19b18-5d80-7c99-ab0e-f136d2b7ae32

## Finding

Two downstream pipeline runs fail durable publication after successful acquisition because exact-held expiry scoping is counted as generic filtering while generic filters are disabled. The pre-screen count is captured before the validated acquisition horizon restriction, so it includes unrelated strikes intentionally omitted from extra held expirations.

## Planned correction

Capture normalized screening input after acquisition scoping. Keep raw provider counts, validate the complete provider frame before lossy scope restriction, retain only exact held contracts beyond the horizon, and preserve strict row-scope conservation and zero generic drops when disabled. Cover enabled/disabled screening through durable publication and validated load, plus missing held contracts.

## Verification

Both enabled/disabled cases fail before correction at the conserved scope/publication boundary. The correction passes 66 focused fetch/storage controls, including exact-held contract retention, original raw counts, ordinary scope, missing held strike, durable publication and validated reload. The complete package suite passes 1,538 tests. Strict disabled-filter and malformed count rejection tests remain unchanged. A final complete suite and changed-file lint are required after metadata refresh and line wrapping before commit.

Final acceptance: 1,538 tests pass after the version metadata refresh (16.37s, two existing pandas warnings); changed-file pylint rates 10.00 with no diagnostics. No scope schema or validation waiver was introduced. Version 0.6.9 owns this correction.

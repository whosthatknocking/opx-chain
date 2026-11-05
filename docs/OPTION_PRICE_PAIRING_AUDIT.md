# Stock and option snapshot pairing

Status: closed. Cross-package tracking reference:
AUD-01a1ad68-0b19-79a4-8e26-b70f2e3fc174 in the opx-strategy audit ledger.

MarketData option-chain rows contain an underlyingPrice associated with the
option quote snapshot. Normalization currently replaces it with an independent
stock quote, while enrichment and snapshot metadata repeat that replacement.
Stock quotes can be delayed relative to the chain, so intrinsic value, time
premium, derived Greeks and assignment evidence can use a different market
instant from the option bid/ask. Latest retained INTC evidence has 910 seconds
of skew between the stock and option source timestamps.

Preserve valid provider-native row prices and their quote timestamps throughout
normalization, quote refresh, enrichment and export. Keep independent stock
snapshot metadata separate; never relabel its timestamp to imply pairing.
Other providers and rows without native paired evidence retain their explicit
fallback timing. Invalid native prices must not become trusted fallback data.

Saved canonical exports discarded the original native stock prices. An offline
recalculation must report missing paired evidence instead of reconstructing
prices from model Greeks, independently dated broker quotes, or timestamp edits.
Do not fetch or modify existing published datasets to recover missing evidence.

## Resolution

Version 0.7.1 preserves each native row's stock price and quote clock. Shared
pricing, derived Greeks, strike filtering, age assessment, bounded quote
refresh and canonical export preserve that authority. Independent fallback
keeps its actual stock clock; invalid native evidence never silently falls
back. Daily change from the independent snapshot is blank for paired rows.

Full offline suite: 1,606 passed. Changed-module Pylint: 10/10. Regression
coverage includes row-distinct pairs, intrinsic/extrinsic and derived Greeks,
export/storage, refresh, invalid/missing native values, missing clocks and
row-specific filtering. An unrelated cross-process benchmark-cache test
failed once under load, then passed both alone and in the full final suite
without changes. Existing published data is not modified.

Latest saved canonical data no longer contains the native stock prices and
raw payload dumping was disabled. No exact historical corrected premium or
risk calculation can be claimed. The separate offline assessment reports
missing paired values explicitly rather than relabeling independent prices.

## Pairing Re-audit: 0.7.2

Closed three independently documented findings in the downstream audit ledger:

- AUD-01a1ad76-aafc-7895-84cb-b36e4b24ae52: native pairing detection now checks
  the actual upstream field; canonical independent rows retain their clocks.
- AUD-01a1ad76-b05d-76fb-8307-ce4bedfe0d5c: serialization must preserve all
  supplied canonical values, nulls and clocks, not only contract identities.
- AUD-01a1ad76-b466-7850-adb0-f47a94a8bfa3: normal fetches always retain a
  bounded, allowlisted valuation receipt before publication, even with debug
  dumping off. It binds native/canonical observations to exact dataset hashes
  and positions bytes frozen at load time. CSV-only exports receive receipt
  and positions siblings. The public reader rejects mismatched/tampered evidence.

Final offline chain suite: 1,688 passed; changed-module/test Pylint: 10/10.
Downstream offline premium/timing/risk suite: 313 passed, including PUT and
dividend-sensitive CALL replay through CSV and Parquet. Receipt-write failures,
positions replacement, source allowlisting and checksum-recomputed semantic
tampering have regression coverage. No new fetch or historical rewrite occurred.
Two existing pandas timestamp warnings remain in the integrity test suite.

Market evidence alone does not own consumer strategy policy, quantities, exercise
style or authoritative event context. Replay keeps the frozen strategy run and
overlays only market fields; it must not replace those facts with chain nulls.
No corrected values can be recovered for legacy observations already discarded.

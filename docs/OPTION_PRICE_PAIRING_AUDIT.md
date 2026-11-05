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

## Native Source Detection Re-audit

AUD-01a1ad76-aafc-7895-84cb-b36e4b24ae52 is closed: canonical
independent rows retain their own clocks; native pairing requires
the upstream underlyingPrice field. Regression coverage includes
both positive native pairs and independent canonical input.

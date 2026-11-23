# Portable producer bundle audit

## Missing portable replay contract

The public storage backend can load a retained dataset only while its original
catalog and artifact are present. It cannot package the exact bytes and
relocatable metadata with selected event/price snapshots and price/IV history.
Copying a directory or SQLite file is not a portable validation contract.

Add a bounded, versioned producer-owned byte envelope with exact inventory and
checksums, explicit absent components, scope verification and an independent
read-only dataset loader. Validate in memory before returning any handle. Pin
round-trip, conflicting identity, corruption and unavailable-history behavior
with offline tests. No strategy imports or provider calls.

## Observed-at cutoff for native history export

Existing daily-bar and IV consumers filter by trading or observation date.
Those APIs omit when the current row was fetched, so using them to build a
historical bundle could include a correction acquired after the declared
cutoff. The history stores keep the current version per key, not a version
ledger. Export only current rows whose acquisition timestamps were already
known at the cutoff; a newer replacement cannot recreate the older row.
Keep source timestamps and test an old-dated, later-fetched replacement.

## Bound decoded datasets, not just the container

A bundle byte limit alone does not bound the size of a compressed Parquet
dataset or the number of decoded option rows. Check the row budget for every
dataset and the advertised Parquet row-group size before loading a frame.
Do not interpret oversized input as authority to omit option rows.

## Keep listing and validated reads consistent

Stage 3 preflight may inspect the catalog even when the dataset is explicit.
The portable reader must expose a one-record filtered view without opening the
original catalog. That view must use the same canonical integrity and dataset
facts as the verified read. Reject manifest fields that disagree rather than
letting a forged status affect preflight only.

## Verification

All findings above are addressed by the local 0.8.3 candidate. The complete
producer suite passed 1,780 tests (two existing warnings). Focused cases cover
physical filesystem and SQLite-indexed producer sources, exact-byte reopening,
member/scope/digest/schema rejection, decoded Parquet budgets, retained clocks
and metadata consistency. The wheel built offline; the consuming package's
disposable PostgreSQL suite passed all 1,694 tests against it, including eight
fresh-process success/rejection cases. No live provider or production database
was used. This verifies a local release candidate, not a published release.

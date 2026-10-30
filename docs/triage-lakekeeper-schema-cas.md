# Lakekeeper schema CAS errors

Status: investigation in progress. The existing schema workload observes HTTP
503 responses containing a duplicate `table_schema_pkey` error on Lakekeeper
v0.14.2. This is a lead, not yet a confirmed data-correctness bug or an upstream
report. See [the original observations](schema-workload.md#lakekeeper-losing-schema-commits-answer-503).

## Investigation plan

1. Reproduce with two concurrent guarded schema additions on a fresh table,
   then repeat the existing four-client workload without injected faults.
2. Retain raw HTTP histories, offline checker results, database errors, image
   identities and the exact Calve revision under `results/`.
3. Compare a sequential stale-schema control and inspect the database
   transaction and error mapping. A missing column alone does not resolve a
   commit that returned 503.
4. Check the latest release and current upstream source for an existing fix,
   minimize the requests, and distinguish this from the property-row deadlock
   reported in lakekeeper/lakekeeper#2201.

The initial stack needs only Lakekeeper, PostgreSQL and Silo. Requests are
metadata-only and sent once. No fault injection or upstream report is part of
the initial reproduction.

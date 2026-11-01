# Multi-table workload and checker

The skew workload's transactions each update only one table. The atomic
workload covers transactions that update every table they touch, and checks
that each one is all-or-nothing.

```sh
calve atomic --catalog http://localhost:8182/catalog --warehouse calve \
  --tables 2 --clients 4 --per-client 10 --out history-atomic.jsonl
calve check history-atomic.jsonl
```

Exit codes: 0 valid, 3 invalid, 4 inconclusive, 1 on errors.

## The workload

Every transaction sets one unique property, its marker, to `1` on every table,
guarded by each table's baseline UUID. Different transactions use different
keys, nothing removes a key, and every request is sent once. So each marker
points at exactly one request, and every acknowledged marker must stay on
every table.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--tables` | 2 | tables per transaction (2 to 32) |
| `--clients` | 4 | concurrent writers |
| `--per-client` | 10 | transactions per writer |
| `--readers` | 1 | concurrent readers; `0` turns them off |
| `--read-interval` | 10ms | pause between a reader's passes |

Transactions don't conflict with each other: their keys differ and the UUID
guards never change. A catalog may still reject some, and the checker doesn't
demand progress. But a run with no proven commit is inconclusive, because it
tested nothing.

Readers start with the writers. Each loads every table in order, pauses, and
repeats. They stop before the final reads. Separate loads are never treated as
one snapshot across tables.

The workload uses fresh random table names and never drops anything, so tables
stay available for inspection. Under `calve run --workload atomic` it runs with
faults; after a transaction gets no response, a writer waits until the catalog
answers again before sending the next (see [fault campaigns](fault-campaigns.md)).

## What the checker reports

| Kind | Basis | Evidence |
| --- | --- | --- |
| `lost` | `catalog-contract` | an acknowledged or proven transaction, followed by a load of the same table, same UUID, that lacks its marker |
| `phantom-failed` | `catalog-contract` | a load that started after a definite rejection shows the transaction's marker |
| `unexpected` | `catalog-contract` | a run marker that no earlier or overlapping request wrote |
| `single-table-regression` | `fresh-read-model` | a table shows a marker, then a later load of the same table doesn't |
| `non-atomic-visibility` | `atomic-visibility-model` | one table shows a marker, then a later load of another table doesn't, including while the transaction is still pending |

The first three break the [REST spec's atomic transaction][spec] directly. The
last two are leads: they assume table loads are fresh, which no catalog
promises. When both apply to one transaction, the same-table regression is
reported. `lost` and `phantom-failed` take precedence over both.

What doesn't count as a violation:

- **Old A, new B** in two separate reads. The transaction may have committed
  between them.
- **Overlapping reads** alone.
- **Effects seen before a rejection.** They're visibility evidence, but they
  don't show that the effect stayed. A rejected transaction stays rejected, and
  any forbidden effect is reported on its own.

## Unknown outcomes

A transaction with a timeout, 5xx or no completion stays `info`. Its marker
set to `1` on every original table proves it committed, and the resolution
lists the reads that show it. Absence, a partial effect, a failed read or the
end of the read-back budget never proves rejection. See
[unknown outcomes](unknown-outcomes.md).

Since no transaction removes another's marker, an unresolved peer can't
explain away a lost acknowledged marker.

## Coverage

With readers on, the run needs at least one usable read that overlaps a
transaction. A usable read succeeded, has the right UUID, and falls inside the
trusted window. Without one the run is inconclusive, though a finding still
makes it invalid. Reads before, after or during recovery don't count.

A failed read is not an empty table. A later good read can restore coverage. A
changed UUID ends the trusted window: only reads that finished before it count,
and transactions that cross it stay uncertain.

## Evidence rules

The run record names the tables, catalog, warehouse, namespace, client count
and attempts. Every transaction's request must update every listed table with
exactly its own marker and baseline UUID guard. The checker refuses unsupported
operations, duplicate markers, incomplete or mismatched requests, changed
routing, and labels that disagree with statuses. Nothing in a summary can
override the recorded requests and reads.

Invalid arguments are rejected before anything is written, so an existing
history file is never overwritten by a failed start. Setup errors redact the
token before showing response bodies.

## Tests

Tests use a correct fake catalog and planted lost, rejected, partial and
unknown effects. A real client-timeout test holds a transaction on the server
past the first final read, then applies it before the second, and checks that
recovery resolves it without a false partial-commit report. A controlled
frontend publishes one table, waits for a reader to see the other table's old
state, then publishes the second before acknowledging, which exercises the
visibility check with a complete final state. Other tests cover reader shutdown,
UUID changes, evidence IDs, redaction, exit codes and offline replay.

CI runs a small atomic workload and its offline replay on Lakekeeper, Nessie
and Polaris, and fails on a finding or an error. It also requires at least one
proven commit and one usable concurrent read on each.

## Results

The first runs of 40 two-table transactions passed on Lakekeeper v0.14.2,
Nessie 0.109.0 and Polaris 1.8.0, and the same verdicts came back on offline
replay. The reference fixture loses acknowledged markers
([triage](triage-reference-atomic.md)). The README has the current sweep
results.

General dependency-cycle detection (G0, G1c, G2) isn't implemented. These
checks are specific to this workload.

[spec]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441

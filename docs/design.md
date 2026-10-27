# Calve design doc

## Goal

Find isolation and durability anomalies in open-source Iceberg REST catalogs under concurrency and faults, with minimal standalone reproductions.

## Non-goals

- Testing managed cloud catalogs.
- Benchmarking performance.
- Testing query engines (Spark, Trino) rather than catalogs.

## What the catalogs promise

TODO: for each operation Calve tests (single-table commit, multi-table `transactions/commit`), quote what the Iceberg REST OpenAPI spec says about requirements, conflicts (409) and unknown outcomes (500/502/504). A finding is a bug only if it breaks one of these promises.

## History format

One JSON record per line (`internal/history`):

| Field | Meaning |
| --- | --- |
| index | position in the history |
| process | logical client |
| type | invoke, ok, fail, info (unknown) |
| f | operation name |
| value | workload-specific payload |
| time | nanoseconds since run start |
| status | HTTP status, 0 if no response |
| error | error text for fail and info |

## Workloads and checkers

| Workload | Checker | Anomalies |
| --- | --- | --- |
| append | `checker.CheckAppend` | lost, phantom-failed, duplicate, unexpected |
| write skew | TODO | both conflicting transactions committed |
| compare-and-set | TODO (Porcupine) | non-linearizable history |
| multi-table general | TODO (dependency graph + cycle search) | G0, G1c, G2 cycles |

## Faults

TODO: Toxiproxy (latency, reset, partition) between client and catalog and between catalog and Postgres; kill, pause, restart of the catalog process; every schedule derived from one seed.

## Open questions

- TODO

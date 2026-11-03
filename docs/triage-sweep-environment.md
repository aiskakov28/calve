# Sweep environment: Polaris setup timeouts and fixture lock-ups

Status: both triaged. Neither is a catalog bug; both came from how Calve ran the
catalogs, and both are fixed in Calve. Nothing to report upstream.

## Polaris `create table` slows down as a namespace fills

**Verdict:** Polaris design choice, exposed by Calve's sweep. Fixed in Calve.

In the v0.1.0 fault sweep, 10 Polaris runs ended in an error before their first
fault: the setup `create table` got no answer within the sweep's 5-second
request timeout. Over the job, the first `create table` in each run slowed from
about 10 ms to 4 or 5 seconds.

By default Polaris validates a new table's location against every sibling in
its namespace, so each create costs more as the namespace grows. Every sweep run
used the same `calve` namespace, which grew to thousands of tables. Timing 400
creates in one namespace on Polaris 1.8.0, with two otherwise identical
catalogs:

| Catalog | Tables 1–100 | 101–200 | 201–300 | 301–400 |
| --- | ---: | ---: | ---: | ---: |
| default, overlap check on | 9.6 ms | 14.3 ms | 21.5 ms | 31.9 ms |
| `polaris.config.allow.overlapping.table.location=true` | 7.7 ms | 7.0 ms | 7.0 ms | 7.2 ms |

Creates in a fresh namespace of the same catalog were back to 10 ms, so the cost
is per namespace. The check is deliberate: it stops tables from overlapping each
other's locations. Polaris also offers an indexed version,
`OPTIMIZED_SIBLING_CHECK`, which is off by default and needs acknowledging for
existing realms.

**Fix:** `scripts/sweep.sh` gives every run and every fault campaign its own
namespace, `sweep_<id>_<workload>[_<fault>]_<run>`, so no namespace grows beyond
one run's tables. Polaris keeps its default overlap check.

## The reference fixture stops answering with `SQLITE_BUSY`

**Verdict:** test environment. The fixture is Iceberg's test server, and newer
fixture images already avoid the cause. Fixed in Calve's Compose file.

Twice on 2026-11-02, the Compose stack's `apache/iceberg-rest-fixture:1.10.1`,
after hours of concurrent workloads, answered every write with HTTP 500
`SQLITE_BUSY` ("database is locked") until the container was recreated.

The 1.10.1 image stores the catalog in SQLite without WAL mode, where one
connection writing blocks every other. Iceberg switched the fixture to WAL in
[apache/iceberg#18044](https://github.com/apache/iceberg/pull/18044) (September
2026, after 1.10.1) because parallel clients hit `SQLITE_BUSY`
([apache/iceberg#18043](https://github.com/apache/iceberg/issues/18043)).

The lock-up did not reproduce on fresh 1.10.1 containers: eight rounds of
concurrent namespace creates and lost-update rounds, and a separate mixed run,
gave no lock-up and no `SQLITE_BUSY` in the server log, with or without WAL. So
it is unconfirmed that WAL prevents the long-running case; it does remove the
known cause.

**Fix:** `docker-compose.yml` sets the fixture's `CATALOG_URI` to
`jdbc:sqlite:/tmp/iceberg_catalog.db?journal_mode=WAL`, the default in newer
fixture images. Sweeps already recreate the fixture before each run.

## Checked

A small sweep after both fixes, the reference fixture and Polaris with `append`
and `snapshot`, plain and under latency faults: all 8 runs exited 0, each in its
own namespace, and the fixture ran with its WAL file.

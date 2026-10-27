# Calve

Jepsen-style correctness testing for Apache Iceberg REST catalogs.

Calve runs concurrent commits against open-source Iceberg catalogs while injecting faults, records every request and response, and then checks the history for lost commits, duplicates, and transactions that should have been rejected.

> **Status: early development (milestone M0).** The smoke test and a first append workload run against the reference catalog. Fault injection, the other catalogs and the serializability checker are next. No findings yet.

## Why

Every lakehouse vendor now ships an Iceberg REST catalog, and existing test suites check whether endpoints exist and return the right fields. Few check what happens when many writers commit at once while the network or the catalog fails. Concurrency bugs are still found by hand, for example [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552), where two conflicting multi-table transactions both commit on the reference catalog.

## Quick start

Requires Go 1.24+ and Docker.

```sh
docker compose up -d --wait
go run ./cmd/calve smoke  --catalog http://localhost:8181
go run ./cmd/calve smoke  --catalog http://localhost:8182/catalog --warehouse calve
go run ./cmd/calve append --catalog http://localhost:8181 --clients 8 --per-client 50
```

`append` saves the full history to `history.jsonl` and prints `VALID` or the anomalies it found.

## What Calve checks

| Workload | Anomaly | Status |
| --- | --- | --- |
| append | acknowledged commit missing, rejected commit present, duplicates | working |
| multi-table write skew (#18552) | both conflicting transactions commit | M2 |
| compare-and-set schema | more than one winner per round | M4 |
| maintenance races | snapshot expiration racing appends | M4 |

A commit answered with a timeout or 5xx is recorded as **unknown**, never as a failure: the Iceberg REST spec allows such a commit to have been applied. Checkers accept either outcome for unknown operations.

## Catalogs

| Catalog | Status |
| --- | --- |
| Reference (apache/iceberg-rest-fixture 1.10.1) | in docker-compose |
| Apache Polaris | M1 |
| Lakekeeper v0.14.2 (Postgres, Silo for S3) | in docker-compose |
| Nessie | M1 |
| Apache Gravitino | M1 |

Calve only tests catalogs you run yourself. It does not load-test managed cloud catalogs.

## Layout

```
cmd/calve/            CLI
internal/rest/        raw Iceberg REST client (no retries, sees every status)
internal/history/     invoke/ok/fail/info records, JSON Lines
internal/workload/    workload generators
internal/checker/     history checkers
internal/fakecatalog/ in-memory catalog with planted bugs, for testing checkers
docs/design.md        design doc
```

## Development

```sh
go test -race ./...
```

Every checker must catch the bugs planted in `internal/fakecatalog` before it is run against a real catalog.

## Reporting findings

Before filing anything upstream: reproduce it from a seed, shrink it to a standalone script, confirm the spec actually promises the violated behavior, and search existing issues.

## License

Apache-2.0

Done by [aiskakov28](https://github.com/aiskakov28) and [aldusialn](https://github.com/aldusialn).

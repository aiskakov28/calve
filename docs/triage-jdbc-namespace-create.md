# Concurrent namespace creates get HTTP 500 from Iceberg's JDBC catalog

Status: filed as [apache/iceberg#18759](https://github.com/apache/iceberg/issues/18759), with a fix in [apache/iceberg#18760](https://github.com/apache/iceberg/pull/18760) (open). Seen in Iceberg's `JdbcCatalog` through Gravitino 1.3.1 and the reference fixture 1.10.1. No data is damaged: every losing create left the winner's namespace intact.

## What we saw

This was found by hand while probing Gravitino. [`scripts/repro-jdbc-namespace-create.go`](../scripts/repro-jdbc-namespace-create.go) sends 6 concurrent `POST /v1/namespaces` for one fresh namespace, each with its own `owner` property, once per request and without redirects, over 20 rounds:

| Catalog | 200 | 409 | 500 |
| --- | ---: | ---: | ---: |
| Reference fixture 1.10.1 (SQLite) | 20 | 79 | 21 |
| Gravitino 1.3.1 (Postgres 17.6) | 20 | 81 | 19 |

Every round had exactly one 200, and the namespace read back afterwards held only the winner's `owner`. A sequential control on each catalog, one create and then the same create after it finished, got 200 then 409. Every 500 was an `UncheckedSQLException` for the property insert:

- Reference: `Caused by: org.sqlite.SQLiteException: [SQLITE_CONSTRAINT_PRIMARYKEY] A PRIMARY KEY constraint failed`
- Gravitino: `Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "iceberg_namespace_properties_pkey"`

Racing table creates (6 per round, 20 rounds) and table renames on Gravitino got one success and clean 409s or 404s in every round.

## Contract

The [REST spec at `be252818`](https://github.com/apache/iceberg/blob/be2528185a1d37e6590bce193c2760202d90a0cb/open-api/rest-catalog-open-api.yaml#L335-L336) gives createNamespace `409: Conflict - The namespace already exists`. Each losing create asked for a namespace that already existed, and got a 500 instead. A 5xx is allowed in general ("a server-side problem"), but it tells the client the outcome is unknown, while the catalog knew the answer: the namespace exists and this request created nothing.

This is a wrong status, not a lost or wrongly applied effect. Under Calve's rules a 500 is an unknown outcome, never a finding by itself.

## Cause

[`JdbcCatalog.createNamespace`](https://github.com/apache/iceberg/blob/be2528185a1d37e6590bce193c2760202d90a0cb/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java#L461-L478) checks `namespaceExists`, then calls [`insertProperties`](https://github.com/apache/iceberg/blob/be2528185a1d37e6590bce193c2760202d90a0cb/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java#L887-L907) as a separate step. Concurrent creates can all pass the check; the first insert wins and the rest hit the primary key on `iceberg_namespace_properties`. The insert runs through [`execute`](https://github.com/apache/iceberg/blob/be2528185a1d37e6590bce193c2760202d90a0cb/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java#L813-L831) with an error handler that does nothing, so every `SQLException`, the duplicate key included, becomes `UncheckedSQLException`, which REST servers return as 500. Losers that run the check after the winner's insert get the intended 409, which is why each round mixes the two.

The insert is a single statement, so a loser writes none of its properties.

The method is the same at the [1.10.1](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java#L403) tag the fixture runs, at [1.11.0](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java#L414-L431), which Gravitino 1.3.1 bundles (`iceberg-core-1.11.0.jar`), and on `main` at `be252818` (2026-10-30). Gravitino passes the call through unchanged: its 500 stack goes `IcebergNamespaceOperations.createNamespace` → `IcebergCatalogWrapper.createNamespace` → `CatalogHandlers.createNamespace` → `JdbcCatalog.createNamespace`. So the bug is Iceberg's. Other REST catalogs that use this `JdbcCatalog` path may show the same behavior if they pass the exception through the same way.

## Fix

[apache/iceberg#18760](https://github.com/apache/iceberg/pull/18760) catches the failed insert in `createNamespace` and checks again whether the namespace exists. If it does, it throws `AlreadyExistsException`, which REST servers return as 409; if the recheck finds no namespace, the original exception is rethrown. It does not depend on database-specific error codes, and it is the approach [apache/iceberg-go#1635](https://github.com/apache/iceberg-go/pull/1635) took for the same race in iceberg-go's SQL catalog.

Its regression test, `testCreateNamespaceLosingConcurrentCreate`, is deterministic: a spy makes the second create's first `namespaceExists` check return `false` for a namespace that already exists. Without the change it fails with `UncheckedSQLException` on the properties insert. Results on 2026-11-02, on Iceberg `main` `3ad26de`:

| Check | Without the fix | With the fix |
| --- | --- | --- |
| `TestJdbcCatalog`, JDK 21 | the new test fails | 157 pass, 12 skipped |
| Fixture images built from `3ad26de`, 6 clients x 20 rounds, 4 runs | 18–57 HTTP 500s per 120 creates | no 500s; one 200 and the rest 409 every round |

## Filing

- Calve has no namespace workload on purpose. This is a wrong status code with correct stored state, which falls outside Calve's isolation and durability goal, so the standalone script is the reproduction.
- No apache/iceberg issue or PR reported it before #18759. [apache/iceberg#13366](https://github.com/apache/iceberg/issues/13366) was a different, fixed fixture problem: in-memory SQLite gave each connection an empty database.
- Others had hit it: iceberg-go fixed the same race in its own SQL catalog ([apache/iceberg-go#1635](https://github.com/apache/iceberg-go/pull/1635)), and [bruin-data/ingestr#1056](https://github.com/bruin-data/ingestr/pull/1056) worked around this Java version after a first run failed with the same `UncheckedSQLException`.
- Before filing, it reproduced again on 2026-11-02: fixture 1.10.1 (4 runs, 7–28 HTTP 500s per 120 creates), a fixture built from `main` `b0df3ca` (16) and Gravitino 1.3.1 on Postgres (2 runs, 23–31). No Gravitino issue was filed; the fix belongs in Iceberg.

## Reproduce

Standard library only. With the reference fixture alone:

```sh
docker run -d --name ns-repro -p 8181:8181 apache/iceberg-rest-fixture:1.10.1
go run scripts/repro-jdbc-namespace-create.go -catalog http://localhost:8181
docker rm -f ns-repro
```

For Gravitino, start the Compose stack (`scripts/up.sh`) and pass `-catalog http://localhost:8185/iceberg`. The script prints a witness and writes `summary.json` with every response to a new directory (`-out`), then deletes the namespaces it created. A built binary exits 3 when it reproduces, 0 when it does not and 1 on any other failure; `go run` reports the 3 as `exit status 3`. Each run takes about a second.

# Lakekeeper schema CAS errors

Status: reproduced on Lakekeeper v0.14.2 and traced to database write order and
error mapping. Concurrent guarded schema additions hit a unique-key error that
rolls back the loser, but is returned as HTTP 503 rather than a conflict. No lost
update, multiple winner, or applied rejected commit was observed. No upstream
report or fix has been submitted.

This follows up [the original workload lead](schema-workload.md#lakekeeper-losing-schema-commits-answer-503).
The tested version is also the [latest published release][release] when checked
on 2026-10-30. Current main was inspected separately; it was not built or run.

## Reproduce

Only Lakekeeper, PostgreSQL and Silo are needed. With ports 8182 and 19000 free:

```sh
docker compose -p calve-schema-cas up -d --wait lakekeeper
docker compose -p calve-schema-cas run --rm lakekeeper-init
go run scripts/repro-lakekeeper-schema-cas.go -rounds 10 -out results/lakekeeper-schema-repro
```

The [script](../scripts/repro-lakekeeper-schema-cas.go) uses only Go's standard
library, sends each request once and refuses redirects. It saves request and
response bodies, statuses, headers and timings in `http.jsonl`, plus a
`summary.json`. The output directory must be new. Fresh test tables remain for
inspection. Stop the stack with `docker compose -p calve-schema-cas stop`;
its data is preserved.

The sequential control adds a column, waits for success, then sends a different
addition requiring the original schema: expected and observed 200, then 409,
with only the first column present.

Each race uses a fresh table with schema ID 0. Both writers require its UUID
and `current-schema-id: 0`. Each adds schema ID 1 containing the original
`id` field and its own optional long column (field ID 2), then sets the
current schema to `-1`. A barrier releases both requests; a final load checks
the result. Both allocate the same next schema ID from the same baseline, as
the [Java client][java-schema-ids] does. The winner invalidates the other's
requirement; the [single-table commit contract][single-commit] specifies 409
for failed requirements.

```text
Sequential control: first accepted, stale schema rejected with 409
race-0: [200 503]
...
duplicate-key 503s: 10; status pairs: map[200/503:10]; reproduced: true
```

The script exits 3 when it reproduces the duplicate-key 503, 0 when every race
ends 200/409, and 1 for a failed control, unexpected response or inconsistent
observed effect. These exit codes are not Calve verdicts.

## Results on 2026-10-30

Fresh dedicated stack; no injected faults or client retries. Calve CLI built
from `9edb44e` (Go source identical to main `8e1395b`). The standalone rows
used the first, Python version of the script.

On 2026-11-02 the Go port sent the same requests and gave the same results:
Lakekeeper v0.14.2 answered 200/503 in 10 of 10 races, each 503 naming
`table_schema_pkey`, and reference fixture 1.10.1 answered 200/409 in 10 of 10.
Both passed the sequential control.

| Test | Acknowledged | Rejected | HTTP 503 / unknown |
| --- | ---: | ---: | ---: |
| Calve: 2 writers, 10 rounds | 10 | 0 | 10 |
| Calve: 4 writers, 10 rounds | 10 | 0 | 30 |
| Standalone: 2 writers, 10 fresh tables | 10 | 0 | 10 |
| Standalone: sequential control | 1 | 1 (409) | 0 |
| Same script: reference fixture 1.10.1, 10 races | 10 | 10 (409) | 0 |

Every 503 contained `CatalogBackendError` / `Unexpected` and a duplicate
`table_schema_pkey` error. Each race had exactly one acknowledgment; only that
writer's column was visible. All ten standalone Lakekeeper request pairs
overlapped at the client. Both Calve histories replayed offline with the
same counts and `inconclusive` verdict (exit 4).

As an independent control, the same standalone requests against reference
fixture 1.10.1 produced 200/409 in all ten races, passed the sequential control
and reported `reproduced: false`.

Database tracing found SQLSTATE **23505**, followed by `ROLLBACK` on the same
backend for all ten standalone races. A shortened example:

```text
[95] ERROR: 23505: duplicate key value violates unique constraint "table_schema_pkey"
[95] DETAIL: Key (warehouse_id, table_id, schema_id)=(..., ..., 1) already exists.
[95] STATEMENT: INSERT INTO table_schema(schema_id, table_id, warehouse_id) ...
[95] LOG: statement: ROLLBACK
```

A database read found schema IDs `{0,1}` and current ID 1 for every standalone
table, including the control. The rollback conclusion has database evidence;
it is not inferred from missing columns. Calve correctly keeps a bare 503
unknown: other 503s can have different causes. No checker classification changed.

| Image | Local image / repository digest (`sha256:`) |
| --- | --- |
| `quay.io/lakekeeper/catalog:v0.14.2` | `9ad8de17a4d291d355215791ee7410409dcaea83cee04ed9279725b6905a4b85` |
| `postgres:17.6` | `00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929` |
| `pgsty/silo:RELEASE.2026-09-03T13-18-01Z` | `b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3` |

Local artifacts are in `results/lakekeeper-schema-cas-20261030/` (Git-ignored):
the two Calve JSONL histories and live/offline logs, `standalone/http.jsonl`,
`standalone/summary.json`, `standalone-services.log`, `rollback-excerpts.log`,
`persisted-schema-rows.txt`, `image-identities.txt`, and the reference comparison
in `reference-control/`. The three Lakekeeper-stack services used
approximately 280 MiB combined after the initial runs, excluding Docker/WSL
overhead; this is not a minimum memory requirement.

## Cause and related reports

On released source [`56bb203`][released-source]:

1. [`commit_table_transaction`][commit-order] applies sub-table changes before
   the top-level updates and metadata-location compare-and-set.
2. [`insert_schemas`][schema-insert] inserts `(warehouse_id, table_id, schema_id)`
   without conflict handling. One contender hits the winner's schema key before
   reaching the metadata-location check.
3. [`into_catalog_backend_error`][db-errors] does not classify SQLSTATE 23505 as
   concurrent modification. It becomes `Unexpected`, which the [HTTP
   conversion][http-errors] maps to 503.
4. The [retry loop][retry-loop] retries `ConcurrentUpdateError`, not this generic
   backend error. The [write transaction][write-transaction] returns before
   `commit()` when the insert fails; logs confirm rollback.

Current main `698be3e739a19ee3a2da9022d2f8baf92fc8a1a8` retains this production
schema insertion, sub-table ordering and 23505 mapping. New main handling for
uncertain errors from database `COMMIT` concerns a different failure point.
Runtime behavior on main has not been verified.

[#2201][properties-report] is a different failure: deadlocks from unsorted
property upserts (SQLSTATE 40P01), already returned as 409. Its proposed
key-sorting fix does not change schema insertion. Both involve sub-table
writes before the final compare-and-set. [#2110][refs-report] also concerned
deadlocks, on snapshot refs. A targeted issue search found no matching
schema-race report; that does not establish novelty.

The demonstrated impact is misleading failure classification during normal
schema contention. The server knows this transaction aborted, while the REST
response leaves clients uncertain. This is not evidence of data corruption.

A fix should reject or retry the loser as a normal commit conflict. Options
to evaluate upstream are moving the metadata-location compare-and-set before
sub-table writes, or translating this specific schema-key collision into the
existing concurrent-update path. Either needs a concurrent regression test;
globally converting unique violations could hide unrelated integrity errors.
No proposed fix has been runtime-validated here.

[release]: https://github.com/lakekeeper/lakekeeper/releases/tag/v0.14.2
[released-source]: https://github.com/lakekeeper/lakekeeper/tree/56bb20322fd58af82f933d84f9407bbea146823e
[commit-order]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/tabular/table/commit.rs#L78-L115
[schema-insert]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/tabular/table/common.rs#L63-L92
[db-errors]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/dbutils.rs#L44-L67
[http-errors]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/service/catalog_store/error.rs#L282-L294
[retry-loop]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/server/tables.rs#L1391-L1451
[write-transaction]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/server/tables.rs#L1832-L1858
[properties-report]: https://github.com/lakekeeper/lakekeeper/issues/2201
[refs-report]: https://github.com/lakekeeper/lakekeeper/issues/2110
[java-schema-ids]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651
[single-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml

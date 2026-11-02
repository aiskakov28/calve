# Lakekeeper schema CAS errors

Status: fixed upstream in [lakekeeper#2219][fix-pr] (open, not yet merged).
Filed as [lakekeeper#2216][upstream-report]. Concurrent guarded schema additions
hit a unique-key error that rolls back the loser, but is returned as HTTP 503
rather than a conflict. No lost update, multiple winner, or applied rejected
commit was observed. The maintainers traced it to the same write order as
[lakekeeper#2201][properties-report] and fixed both with an ordered lock on the
committed tables. We [validated that fix](#upstream-fix); it supersedes the
[reorder we tried first](#our-first-fix-superseded).

This follows up [the original workload lead](schema-workload.md#lakekeeper-losing-schema-commits-answer-503).
The tested version is also the [latest published release][release] when checked
on 2026-10-30. Current main was [built and run](#current-main) on 2026-11-02.

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


[#2201][properties-report] is a different failure: deadlocks from unsorted
property upserts (SQLSTATE 40P01), already returned as 409. Its proposed
key-sorting fix does not change schema insertion. Both involve sub-table
writes before the final compare-and-set. [#2110][refs-report] also concerned
deadlocks, on snapshot refs. A targeted issue search found no matching
schema-race report; that does not establish novelty.

The demonstrated impact is misleading failure classification during normal
schema contention. The server knows this transaction aborted, while the REST
response leaves clients uncertain. This is not evidence of data corruption.

## Current main

On 2026-11-02, Lakekeeper built from main [`0a86441`][main-source] with its own
`docker/full.Dockerfile` (arm64 image `sha256:5fd210e9…`) still answered
200/503 in 10 of 10 races, each 503 naming `table_schema_pkey`, and passed the
sequential control. None of the 12 main commits since `698be3e` touch
`commit.rs`, `common.rs`, `dbutils.rs`, `tables.rs` or the error conversion.

## Upstream fix

[lakekeeper#2219][fix-pr] (`97444b7`) makes a table commit lock the committed
tables' `tabular` rows in id order (`SELECT … ORDER BY tabular_id FOR NO KEY
UPDATE`) and check their metadata location before it writes any sub-table. The
loser of a race waits on that lock, fails with `ConcurrentUpdateError` before
writing anything, and is retried by the server; its `assert-current-schema-id`
then fails with 409. The same lock fixes #2201.

The maintainers chose this over our reorder for two reasons, both of which our
tests confirmed: moving the existing `UPDATE … FROM (VALUES …)` earlier locks
rows in request order, so multi-table commits that list tables in different
orders can still deadlock, and it locks `table` before `tabular`, the opposite
of a drop.

Results on 2026-11-02, Lakekeeper built from the PR (arm64 image
`sha256:03e6412c…`) against main `0a86441` ([posted on the PR](https://github.com/lakekeeper/lakekeeper/pull/2219#issuecomment-6589196427)):

| Check | main `0a86441` | #2219 |
| --- | --- | --- |
| #2216 schema race, bash and Go repros | 200/503 every round | 200/409 in 10 of 10 and 50 of 50 |
| #2201 script, 200 property commits from 8 writers, 3 runs | 154–182 deadlocks, 136–171s | 0 deadlocks, 2–3s; 181–184 applied, the rest `ConcurrentUpdateError` 409s |
| Multi-table commits, half listing `[A, B]` and half `[B, A]` | 10 deadlocks in 20 rounds | 0 deadlocks in 100 rounds, every transaction all or nothing |
| A commit racing a drop of the table | 18 × 503 (foreign key), 4 × 500 `RequiredTableComponentMissing`, 3 deadlocks in 40 rounds | no 5xx, no deadlocks in 80 rounds; 200 or 404 |
| Calve append, schema, atomic, skew, snapshot, maintenance | — | all `VALID`, 0 unknown, 0 deadlocks |

Our reorder, run through the same drop race, still deadlocked once and returned
two 500s in 40 rounds.

## Our first fix (superseded)

Before the maintainers replied, we tested moving the sub-table writes after the
compare-and-set. It fixes both single-table races, but not the drop race above.

The loser should fail as a normal commit conflict. Of the two options,
translating this one schema-key collision would leave the same race in place
for every other sub-table; converting all unique violations could hide
unrelated integrity errors. The fix tested here instead moves the sub-table
loop in `commit_table_transaction` after the top-level updates and
`verify_commit_completeness`. A concurrent commit then waits on the winner's
row lock, fails the metadata-location compare-and-set, and returns the
existing `ConcurrentUpdateError`. The retry loop reloads the table, and the
client's `assert-current-schema-id` fails with 409. A successful commit writes
the same rows in the same transaction, in a different order; no trigger or
deferred constraint depends on that order.

Results on main `0a86441`, with and without the fix:

| Test | Without the fix | With the fix |
| --- | --- | --- |
| New Rust test: two commits add schema 1, the loser waits on the winner's lock | fails: `CatalogBackendError`, 23505 `table_schema_pkey` | passes: `ConcurrentUpdateError` |
| Existing `commit.rs` tests | — | 4 of 4 pass |
| Go script, 2 writers | 200/503 in 10 of 10 | 200/409 in 60 of 60 |
| Calve schema workload, 4 writers, 20 rounds | — | 20 ok, 60 rejected, 0 unknown; `VALID` |

The same change also stops the deadlocks in
[lakekeeper#2201][properties-report]: commits to one table now queue on the
`table` row before touching any sub-table, so property rows are never written
by two commits at once. With the [#2201 script](triage-409-without-requirements.md),
200 property commits from 8 writers on main `0a86441`:

| Build | Runs | Deadlocks | Committed | Time per run |
| --- | --- | --- | --- | --- |
| main | 3 | 154–182 | 17–43 | 136–171s |
| with the fix | 4 | 0 | 177–183 | 2–3s |

The other commits with the fix were `ConcurrentUpdateError` 409s that lost
the race more often than the two server-side retries allow.

Every 409 with the fix reads `CatalogCommitConflicts … Requirement failed:
Current schema id does not match`. Lakekeeper's full test suite and clippy were
not run; `cargo fmt --check` passes.

[release]: https://github.com/lakekeeper/lakekeeper/releases/tag/v0.14.2
[released-source]: https://github.com/lakekeeper/lakekeeper/tree/56bb20322fd58af82f933d84f9407bbea146823e
[main-source]: https://github.com/lakekeeper/lakekeeper/tree/0a86441fc9472614d38bb247d159103594965d74
[commit-order]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/tabular/table/commit.rs#L78-L115
[schema-insert]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/tabular/table/common.rs#L63-L92
[db-errors]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper-storage-postgres/src/dbutils.rs#L44-L67
[http-errors]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/service/catalog_store/error.rs#L282-L294
[retry-loop]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/server/tables.rs#L1391-L1451
[write-transaction]: https://github.com/lakekeeper/lakekeeper/blob/56bb20322fd58af82f933d84f9407bbea146823e/crates/lakekeeper/src/server/tables.rs#L1832-L1858
[properties-report]: https://github.com/lakekeeper/lakekeeper/issues/2201
[fix-pr]: https://github.com/lakekeeper/lakekeeper/pull/2219
[upstream-report]: https://github.com/lakekeeper/lakekeeper/issues/2216
[refs-report]: https://github.com/lakekeeper/lakekeeper/issues/2110
[java-schema-ids]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651
[single-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml

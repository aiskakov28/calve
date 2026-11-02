# 409 on commits without requirements

Status: two upstream bugs filed from this triage, [apache/polaris#5907](https://github.com/apache/polaris/issues/5907) and [lakekeeper/lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201); a fix PR is open for the Polaris one, and the Lakekeeper one is fixed in [lakekeeper/lakekeeper#2219](https://github.com/lakekeeper/lakekeeper/pull/2219) (open, not yet merged). Nessie's behavior looks deliberate. Every 409 in these runs was a clean rejection: no rejected append appeared in the table.

## What Calve saw

`calve append --clients 8 --per-client 25` on the pinned Compose catalogs, no faults. Each append is a single-table `set-properties` commit of its own key with `"requirements": []`:

| Catalog | Rejected with 409 | What the 409 says | Other |
| --- | --- | --- | --- |
| Reference 1.10.1 | 0 of 200 | | |
| Lakekeeper v0.14.2 | 145 of 200 | 141 `CatalogBackendError (ConcurrentModification): ... deadlock detected`, 4 `ConcurrentUpdateError` | 10 unknown after the 10s request timeout; 2m15s run |
| Nessie 0.109.0 | 150 of 200 | empty `type`, "Values of existing and expected content ... are different" | 5 HTTP 500 writing to S3 |
| Polaris 1.8.0 | 147 of 200 | `CommitConflictException`, "concurrently modified" | |

## Contract

The pinned [REST spec](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1045) describes a table-commit 409 as "Conflict - CommitFailedException, one or more requirements failed. The client may retry." These commits have no requirements. The spec names no other reason for a 409, though "the client may retry" means a 409 is never a correctness problem by itself.

Iceberg's own [`CatalogHandlers.commit`](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java#L410-L453), which the reference catalog runs, shows the intended split: a failed requirement is wrapped so it is not retried and returns 409, while a storage conflict is retried on the server against refreshed metadata, up to 4 times with backoff. So a requirement-free commit gets a 409 only when every retry loses.

## Polaris: a refactor turned off server-side retries ([#5907](https://github.com/apache/polaris/issues/5907))

Polaris copies that retry loop in [`CatalogHandlerUtils.commit`](https://github.com/apache/polaris/blob/ee402d291826d6b1efada211e5a2ee5fbb5334de/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/CatalogHandlerUtils.java#L394-L404) (`ICEBERG_COMMIT_MAX_RETRIES`, default 4), retrying only `CommitFailedException`. The conflict from the final compare-and-set in [`updateTableLike`](https://github.com/apache/polaris/blob/ee402d291826d6b1efada211e5a2ee5fbb5334de/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/LocalIcebergCatalog.java#L3087-L3089) is a `CommitConflictException`, which extends `PolarisException`, so it skips the retry. The new metadata file is written between the earlier, retried check and this compare-and-set, so most conflicts take the unretried path.

[apache/polaris#2198](https://github.com/apache/polaris/pull/2198) (July 2025, first in 1.1.0) changed that throw from `CommitFailedException` to `CommitConflictException` to clean up exception types. The status stayed 409, so nothing visibly broke. Measured with a standalone script (200 concurrent requirement-free commits from 8 writers, official images, in-memory persistence, FILE storage):

| Polaris | Rejected with 409 |
| --- | --- |
| 1.0.1-incubating | 21 of 200 on all four runs, `CommitFailedException` after retries |
| 1.8.0 | 116, 143, 146 and 152 of 200, all `CommitConflictException` |

On the Compose service the server log showed 10 `Retrying task after failure` lines, all for the earlier path, against 147 unretried `CommitConflictException`s in the same run, and setting `polaris.config.iceberg-commit-max-retries` to 0, 4 or 20 on a catalog did not change the rate.

Fix PR open, awaiting review: [apache/polaris#5921](https://github.com/apache/polaris/pull/5921) retries `CommitConflictException` in both commit loops, the option a Polaris contributor preferred on the issue. Its tests inject the conflict once and failed on all four table and view runs without the change. With the standalone script against builds of Polaris `main` (`301708c3`, in-memory persistence, FILE storage), 114 to 135 of 200 commits were rejected before and 2 to 5 after.

## Lakekeeper: commits deadlock on `table_properties` ([#2201](https://github.com/lakekeeper/lakekeeper/issues/2201))

[`set_table_properties`](https://github.com/lakekeeper/lakekeeper/blob/8398c0b7a8c374d6a122903c414db94c625aaeae/crates/lakekeeper-storage-postgres/src/tabular/table/common.rs#L630-L639) upserts keys in `HashMap` order, which differs between requests, and [`commit_table_transaction`](https://github.com/lakekeeper/lakekeeper/blob/8398c0b7a8c374d6a122903c414db94c625aaeae/crates/lakekeeper-storage-postgres/src/tabular/table/commit.rs#L78-L92) writes these rows before the compare-and-set on the `table` row that decides the winner. Concurrent commits to one table lock the property rows in different orders and deadlock; all 151 deadlocks PostgreSQL logged were this statement on both sides. Lakekeeper maps `40P01` to a retryable 409, so the answers are honest, but each deadlock waits for `deadlock_timeout` (1s), which explains the slow run and the timeouts. The standalone script deadlocked 169 to 181 of 200 commits over four runs.

The same pattern on `table_refs` was [lakekeeper#2110](https://github.com/lakekeeper/lakekeeper/issues/2110), fixed by sorting in [#2117](https://github.com/lakekeeper/lakekeeper/pull/2117); properties were not sorted.

The fix we [offered on the issue](https://github.com/lakekeeper/lakekeeper/issues/2201#issuecomment-6498912446), not the one Lakekeeper chose: upsert the keys in `set_table_properties` in key order (`ORDER BY k`), as #2117 did for `table_refs`. A regression test with 4 writers upserting the same 50 keys failed 10 of 10 runs without the change, with the same `40P01` on `table_properties`, and passed 10 of 10 with it. With the standalone script against debug builds of Lakekeeper `main` (`8398c0b7`) on PostgreSQL 18:

| Build | Deadlocks | Other 409s | Committed | Time per run |
| --- | --- | --- | --- | --- |
| `main` | 181, 181, 172 of 200 | 0, 1, 4 | 19, 18, 24 | 156–166s |
| with the fix | 0, 0, 0 of 200 | 48, 45, 49 | 152, 155, 151 | 4.2–4.3s |

The remaining 409s are `ConcurrentUpdateError` from the compare-and-set on the `table` row, which is the intended rejection.

**Upstream fix.** [lakekeeper/lakekeeper#2219](https://github.com/lakekeeper/lakekeeper/pull/2219) locks the committed tables' `tabular` rows in id order and checks their metadata location before writing any sub-table, so commits to one table queue on that row and the loser fails with a retried `ConcurrentUpdateError` before it writes anything. The maintainers chose it over sorting the keys because the properties statement deletes and upserts in two passes, so a commit that removes a property could still deadlock with one that sets it, and because the same lock also fixes the schema 503 in [lakekeeper/lakekeeper#2216](https://github.com/lakekeeper/lakekeeper/issues/2216). With the standalone script against a build of the PR: 0 deadlocks in 3 runs, 181–184 of 200 commits applied and the rest `ConcurrentUpdateError` 409s, 2–3s per run. [The schema triage](triage-lakekeeper-schema-cas.md#upstream-fix) has the full validation, including multi-table commits in opposite orders and commits racing a drop.

## Nessie

Nessie rejects with its content compare-and-swap message and does not rebase on the server, which looks like a deliberate client-retry design. Its code was not examined. The empty error `type` is a separate conformance gap. The HTTP 500s were traced to a request context that is no longer active when Nessie writes table metadata, and filed as [projectnessie/nessie#13271](https://github.com/projectnessie/nessie/issues/13271) ([triage](triage-nessie-request-context.md)).

## What this means for Calve

Calve treats a 409 as a definite rejection. Every history in this triage supports that: across all four catalogs, the table always held exactly the acknowledged keys, and the append checker found no `phantom-failed`. Checkers keep verifying it rather than assuming it, since a 409 for a commit that did apply would be a real bug.

# Polaris commits both conflicting multi-table transactions

Status: known upstream bug, [apache/polaris#5807](https://github.com/apache/polaris/issues/5807), with an open fix in [apache/polaris#5820](https://github.com/apache/polaris/pull/5820). Calve reproduced it on Polaris 1.8.0 with Postgres persistence and [confirmed it on the issue](https://github.com/apache/polaris/issues/5807#issuecomment-6483615805). It is not a Calve finding. CI keeps a gate that expects it until a Polaris release fixes it.

## What Calve saw

`calve skew` against the pinned `apache/polaris:1.8.0` Compose service (relational-jdbc on Postgres 17, table files in Silo), two runs of ten concurrent rounds plus both sequential controls:

| | Result |
| --- | --- |
| Concurrent rounds | Both transactions acknowledged (HTTP 204) in 10/10, on both runs; final loads show both tables at schema 1 |
| Sequential controls | First acknowledged, stale second rejected with 409 in both directions |

In each round, one transaction adds a schema to table A and sends a requirement-only change for table B (`assert-table-uuid` and `assert-current-schema-id: 0`, `updates: []`); the other does the reverse. Whichever commits second asserts a schema ID that is no longer current, so a serializable catalog commits at most one. The controls show the requirement itself is validated; only the concurrent case gets through. Reference 1.10.1 shows the same pattern ([apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552)), but Polaris has its own commit path.

## Cause

In [`IcebergCatalogHandler.commitTransaction`](https://github.com/apache/polaris/blob/821d2a2357ba338a32a5e20b9ca3a247ad15ed68/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/IcebergCatalogHandler.java#L1521-L1590) at the 1.8.0 tag, each table is loaded and its requirements are validated against that loaded copy. `tableOps.commit` runs only `if (!currentMetadata.changes().isEmpty())`, and only that call adds the table to the transaction's [`pendingUpdates`](https://github.com/apache/polaris/blob/821d2a2357ba338a32a5e20b9ca3a247ad15ed68/polaris-core/src/main/java/org/apache/polaris/core/persistence/TransactionWorkspaceMetaStoreManager.java#L211-L215). The final atomic step, `updateEntitiesPropertiesIfNotChanged`, therefore checks the versions of the tables a transaction writes, never those it only asserts. Each transaction's check on the other table passes at load time, and each atomic commit covers only its own table.

## Contract

The pinned [REST spec](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1335) describes the endpoint as "Commit updates to multiple tables in an atomic operation", and [`AssertCurrentSchemaId`](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3175) says "The table's current schema id must match the requirement's `current-schema-id`". The second commit's requirement no longer held when it committed.

## Fix status

[#5820](https://github.com/apache/polaris/pull/5820) re-validates requirements for tables without updates against refreshed metadata and enrolls them in `pendingUpdates`, so the atomic step checks their versions too. Calve has not tested it: that needs a build of the contributor's branch. Worth checking when it is tested: enrolling assert-only tables may also make two transactions that only assert the same table conflict, which is stricter than needed.

## Reproduce

```sh
docker compose up -d --wait && docker compose run --rm polaris-init
calve skew --catalog http://localhost:8184/api/catalog --warehouse calve --token "$POLARIS_TOKEN" --rounds 10
```

Expect `INVALID: 10 write-skew` (exit 3). The CI gate runs the same command; when a pinned Polaris release passes it, update the gate and this note.

Without Calve, [`scripts/repro-multi-table-write-skew.sh`](../scripts/repro-multi-table-write-skew.sh) (bash, curl, jq) sends the two transactions directly and counts a round only when loads of both tables confirm both changes. It exits 3 when any round committed both. Results, 20 rounds unless noted: Polaris 1.8.0 both committed in 20 of 20; the reference fixture 1.10.1 ([apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552)) in 19 and 20 of 20, and 10 of 10; Lakekeeper v0.14.2 and Nessie 0.109.0 in 0 of 20. Polaris needs `WAREHOUSE=calve` and a `TOKEN`.

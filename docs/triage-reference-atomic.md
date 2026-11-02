# Transactions endpoint: acknowledged changes go missing

Status: reproduced and minimized to two concurrent requests to
`POST /v1/{prefix}/transactions/commit` on one table, on the reference fixture
1.10.1 and on Apache Hive 4.2.0 and 4.2.1's REST catalog. The cause is traced
in Iceberg's `BaseTransaction` retry. Not filed upstream.

When two transactions change the same table at once, both are answered 200, but
one of the two changes never lands. If both are guarded so that at most one may
commit, both are still answered 200. The single-table endpoint and sequential
transactions behave correctly.

## What Calve saw

`calve atomic` updates two fresh tables in each transaction, setting the same
unique property to `1` on both, guarded by their baseline UUIDs. Every
transaction is sent once, each uses its own key, and nothing removes keys. On
the reference fixture 1.10.1, two runs of four clients with ten transactions
each gave:

| Run | Acknowledged | Rejected / unknown | Markers kept on A / B | `lost` findings |
| --- | --- | --- | --- | --- |
| first | 40 (HTTP 200) | 0 / 0 | 30 / 29 of 40 | 11 |
| repeat, fresh tables | 40 (HTTP 200) | 0 / 0 | 32 / 32 of 40 | 8 |

The same workload passed on Lakekeeper v0.14.2, Nessie 0.109.0 and Polaris
1.8.0. Sweeps reproduce it on the fixture in every run, with and without faults
(see the README).

The [REST transaction endpoint][spec] promises an atomic multi-table
operation. An acknowledged change that is missing from a later load, when
nothing could have removed it, breaks that promise.

## Minimal reproduction

[`scripts/repro-transaction-lost-update.sh`](../scripts/repro-transaction-lost-update.sh)
(bash, curl, jq) creates a fresh table per round and sends two concurrent
transactions. Each requires the table's UUID and sets its own property. With
`MANIFEST_1` and `MANIFEST_2` set to two existing manifest lists, each instead
adds its own snapshot and requires `main` to still be empty, so at most one may
commit. `table` sends the same changes to the single-table endpoint, and
`sequential` sends the transactions one after the other.

```sh
scripts/repro-transaction-lost-update.sh http://localhost:8181 10
```

Results on 2026-11-02, 10 rounds each:

| Catalog | Change | Transactions, concurrent | Single-table endpoint | Transactions, sequential |
| --- | --- | --- | --- | --- |
| Reference fixture 1.10.1 | property | 200/200, one missing in 10 of 10 (3 runs) | 200/200, none missing | 200/200, none missing |
| Hive 4.2.0 | property | 200/200, one missing in 10 of 10 | 200/200, none missing | 200/200, none missing |
| Hive 4.2.1 | property | 200/200, one missing in 10 of 10 | 200/200, none missing | 200/200, none missing |
| Hive 4.2.1 | guarded snapshot | 200/200, one missing in 10 of 10 | 200/409, none missing | 200/409, none missing |
| Lakekeeper v0.14.2 | property | 200/200, none missing | | |

A direct load of a Hive table after a round showed only the first writer's
property, although both writers got 200.

Earlier runs found the same on the fixture, and on a 1.12.0 fixture
assembled locally from released classes and jars. They found it in two-table
transactions too, and with real appends: two PyIceberg writers each added one
row in its own Parquet file, both got 200, and a scan returned only one row.
An Avro reader confirmed that both data files existed but only one was
reachable from the committed metadata. Their scripts, including a Java probe of
the handler, are in [`bde50d5`][earlier-scripts].

## What happens

The fixture's [`commitTransaction`][adapter] builds a `Transaction` for each
table, applies the request to its in-memory operations through
[`CatalogHandlers.commit`][handler], and then calls `commitTransaction()`.
`CatalogHandlers.commit` checks the requirements and writes the new metadata
straight into the transaction's staged state. It never adds a pending update to
the transaction.

When the second commit reaches the table, the first has already changed it, so
the write fails with `CommitFailedException` and
[`commitSimpleTransaction`][simple-commit] retries. The retry calls
[`applyUpdates`][apply-updates], which refreshes the transaction to the current
table and replays its pending updates. There are none, so the transaction's
change is dropped, and [the table operations][nothing] find nothing to write
and return. The request is answered 200. The requirements were checked once,
against the stale state, and are never checked again, which is why both guarded
snapshots succeed.

Hive's log shows this sequence for every lost change:

```text
CommitFailedException: Cannot commit: Base metadata location '...' is not same as the current table metadata location '...'
  at org.apache.iceberg.BaseTransaction.commitSimpleTransaction(BaseTransaction.java:424)
INFO BaseMetastoreTableOperations: Nothing to commit.
```

## Who is affected

The pattern comes from the fixture's test adapter, and released catalogs copy
it:

- **Apache Hive's REST catalog:** [`HMSCatalogAdapter`][hive-adapter] has the
  same `commitTransaction`, in Hive 4.1.0, 4.2.0 and 4.2.1. Reproduced on
  4.2.0 and 4.2.1 above.
- **Altinity `ice`** and **nimtable** use the same pattern, `ice` with its own
  copy of `CatalogHandlers.commit`. Not run.

[apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552) reports
another transaction problem on the fixture: requirements on tables without
updates are not checked. It mentions acknowledged snapshots going missing as
not yet minimized. This is a different mechanism, and it applies to tables that
do have updates.

## Running Hive's REST catalog

The `apache/hive:standalone-metastore-4.2.1` image serves the REST catalog at
`http://localhost:9001/iceberg` with authentication off. Creating namespaces
also needs an external warehouse directory outside the managed one, which the
image does not set. With this `metastore-site.xml` in a directory mounted as
`HIVE_CUSTOM_CONF_DIR`:

```xml
<configuration>
  <property><name>metastore.warehouse.dir</name><value>/opt/hive/data/warehouse</value></property>
  <property><name>hive.metastore.warehouse.external.dir</name><value>/tmp/hive-external</value></property>
  <property><name>metastore.catalog.servlet.port</name><value>9001</value></property>
  <property><name>metastore.catalog.servlet.auth</name><value>none</value></property>
</configuration>
```

```sh
docker run -d --name hive -p 9001:9001 -e SERVICE_NAME=metastore \
  -e HIVE_CUSTOM_CONF_DIR=/conf -v "$PWD/hive-conf:/conf:ro" \
  apache/hive:standalone-metastore-4.2.1
scripts/repro-transaction-lost-update.sh http://localhost:9001/iceberg 10
```

For the snapshot case, the manifest lists must be readable by the server. A
Linux build of Calve run inside the container writes them:
`calve snapshot --catalog http://localhost:9001/iceberg --clients 2 --rounds 1`.
The `manifest-list` values in its history are the two paths to pass.

Images: `apache/iceberg-rest-fixture:1.10.1`
(`sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f`),
`apache/hive:standalone-metastore-4.2.0`
(`sha256:6ec027489678f06404c8e201010c10e6c67e507b77e41b7e69233d7652453fdf`) and
`apache/hive:standalone-metastore-4.2.1`
(`sha256:f11de992645702a028bf514e3d23400162fff65e7480dad9722b9ef78cdc6142`),
local arm64 image IDs, each with its embedded database.

[spec]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441
[adapter]: https://github.com/apache/iceberg/blob/6951360ec8a01644d3c24824ab635e06a82f8a7c/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L564-L587
[handler]: https://github.com/apache/iceberg/blob/6951360ec8a01644d3c24824ab635e06a82f8a7c/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java#L620-L660
[simple-commit]: https://github.com/apache/iceberg/blob/6951360ec8a01644d3c24824ab635e06a82f8a7c/core/src/main/java/org/apache/iceberg/BaseTransaction.java#L350-L370
[apply-updates]: https://github.com/apache/iceberg/blob/6951360ec8a01644d3c24824ab635e06a82f8a7c/core/src/main/java/org/apache/iceberg/BaseTransaction.java#L439-L455
[nothing]: https://github.com/apache/iceberg/blob/6951360ec8a01644d3c24824ab635e06a82f8a7c/core/src/main/java/org/apache/iceberg/BaseMetastoreTableOperations.java#L118-L123
[hive-adapter]: https://github.com/apache/hive/blob/96b425ef8b4497974671dcdc88c6e3ab196e56ef/standalone-metastore/metastore-rest-catalog/src/main/java/org/apache/iceberg/rest/HMSCatalogAdapter.java#L385-L407
[earlier-scripts]: https://github.com/aiskakov28/calve/tree/bde50d5/scripts

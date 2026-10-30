# Reference fixture: acknowledged multi-table markers go missing

Status: reproduced lead; minimization and upstream triage remain pending.

`calve atomic` updates two fresh tables per transaction with the same unique
property set to `1`, guarded by their baseline UUIDs. It sends each transaction
once. Different transactions use different keys, and no request removes keys.
On the pinned reference fixture 1.10.1, two runs of four clients with ten
transactions each produced:

| Run | Acknowledged | Rejected / unknown | Markers retained on A / B | Lost transaction findings |
| --- | --- | --- | --- | --- |
| first | 40 (HTTP 200) | 0 / 0 | 30 / 29 of 40 | 11 |
| repeat, fresh tables | 40 (HTTP 200) | 0 / 0 | 32 / 32 of 40 | 8 |

Both final loads start after every transaction response and retain the original
table UUIDs. Independent decoding of the saved HTTP responses confirms the
counts. Later direct GETs of the first run's tables still miss 10 and 11
acknowledged markers. Offline checking with networking disabled returns invalid
(exit 3) for the same histories.

The [pinned REST transaction endpoint](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441)
describes an atomic multi-table operation. An acknowledged unique property
missing from a trustworthy later load contradicts the requested effect when
nothing can remove it. Calve records `lost` with basis `catalog-contract`;
upstream confirmation still requires ruling out catalog read freshness and
pinning down server behavior.

The fixture's [transaction adapter](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L545-L573)
explicitly documents its simplified conflict handling and lack of true
transactional atomicity. It stages each table's transaction and commits them
individually. This limitation is relevant context; the runs did not trace the
server or establish the exact cause of these missing properties. The relationship
to [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552) and its
snapshot-loss lead remains to be determined.

## Reproduce and artifacts

Build Calve, start Compose, then use fresh tables on each run:

```sh
calve atomic --catalog http://localhost:8181 --tables 2 --clients 4 \
  --per-client 10 --out history-atomic-reference.jsonl
calve check history-atomic-reference.jsonl
```

Concurrency is not deterministic. The command creates fresh random table names
and records the exact run. A later shrinking/reproduction task should isolate
the minimum failure and investigate the server path before an upstream report.

Local evidence from 2026-10-29 UTC is Git-ignored under `results/issue19/`:

- `reference.jsonl` SHA-256 `c60cf9e6dd1206fe71da6dd130b3faf5d57f236236d3d71d6a88d4c358fb95de`.
- `reference-repeat.jsonl` SHA-256 `f5e7042e1da354514f6ee992bbc8616f4edb96de003e7b4065ee1da94e0e484b`.

Image: `apache/iceberg-rest-fixture:1.10.1`, digest
`sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f`.
The isolated local Compose project was `calve-atomic`. No faults were injected.

The same 40-transaction workload passed on Lakekeeper v0.14.2 (38 acknowledged,
2 rejected), Nessie 0.109.0 (12 / 28), and Polaris 1.8.0 (10 / 30). All had zero
unknown responses and retained every acknowledged marker without rejected
effects. Their offline replays, with networking disabled, returned valid.
These runs establish this workload's result for those executions.

CI runs the workload and offline replay on those three catalogs. The reference
lead stays documented for triage instead of becoming a new expected-failure
gate in this change.

## Follow-up for issue #29

The issue reproduces with **one table and two requests** to the transaction
endpoint. Both requests return success, but one acknowledged update can be
missing after both finish. This occurs for both property updates and guarded
snapshot additions. The [upstream report](https://github.com/apache/iceberg/issues/18552)
already mentions a missing acknowledged snapshot; this work supplies a
reproduction and investigates its mechanism. It does not establish a new
independent discovery.

### Observed results

Every pair used a fresh table. Each request was sent once, and final and later
loads were made after both responses. No test requested snapshot expiration or
property removal.

| Fixture / case | Pairs | Responses | Pairs with an acknowledged effect missing |
| --- | ---: | --- | ---: |
| 1.10.1 transaction, properties | 10 | 200 / 200 | 8 |
| 1.10.1 table endpoint, properties | 10 | 200 / 200 | 0 |
| 1.10.1 transaction, properties, sequential | 5 | 200 / 200 | 0 |
| 1.10.1 transaction, snapshots | 10 | 200 / 200 | 10 |
| 1.10.1 table endpoint, snapshots | 2 | 200 / 409 | 0 |
| 1.10.1 transaction, snapshots, sequential | 5 | 200 / 409 | 0 |
| Assembled 1.12.0 transaction, properties | 10 | 204 / 204 | 10 |
| Assembled 1.12.0 table endpoint, properties | 5 | 200 / 200 | 0 |
| Assembled 1.12.0 transaction, properties, sequential | 5 | 204 / 204 | 0 |
| Assembled 1.12.0 transaction, snapshots | 10 | 204 / 204 | 10 |
| Assembled 1.12.0 table endpoint, snapshots | 3 | 200 / 409 | 0 |
| Assembled 1.12.0 transaction, snapshots, sequential | 5 | 204 / 409 | 0 |

In every concurrent row, request intervals overlapped at the client. The
property writers use different keys and require only the table UUID. Both
snapshot requests require the same UUID and an empty main reference, then add
different snapshots and set main. At most one of those guarded requests should
commit. The check searches the **entire returned snapshot list**, not only the
current snapshot.

The snapshot requests use real empty Avro manifest lists generated by Calve.
Apache Avro 1.11.5 independently decoded both: snapshot IDs 1 and 2, null
parents, sequence 1, and zero records. In one 1.12.0 witness both requests
returned 204; later metadata had the original UUID and only snapshot 1. A
separate copy of the persisted metadata file also contained only snapshot 1.
These initial tests used empty snapshots; the independent recheck below uses
real data files.

One earlier 1.10.1 single-table snapshot control timed out on its third final
read. Its partial history is retained and excluded from the table above; the
two-pair rerun completed. SQLite contention can make fixture runs inconclusive.

The 1.10.1 test used the official fixture image
sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f.
The 1.12.0 comparison was **assembled locally**: the adapter, route, servlet,
and server-adapter classes were compiled from tag apache-iceberg-1.12.0
(commit 212aa894e919ff9fdf89160da4aadcbe6858ac01); Iceberg 1.12.0 libraries
were taken from Maven Central; the Java 17 runtime, HTTP server wrapper, SQLite
driver, and remaining dependencies came from the 1.10.1 fixture image. It is
not an official 1.12.0 fixture image. The relevant paths are also present in
main at be2528185a1d37e6590bce193c2760202d90a0cb.
An independent rebuild using the assembly script reproduced missing
acknowledged effects in 3 of 3 property pairs and 3 of 3 snapshot pairs.

### Independent recheck with real data

An independent PyIceberg 0.11.1 client staged two guarded appends, each with
one distinct row in its own valid Parquet file and a generated manifest list.
The writes and catalog used a fresh native Docker volume. Requests were sent
once. Final scans occurred after both responses, and the SQLite catalog pointer
was checked against the REST response and the persisted metadata file.

| Fixture / request | Concurrent pairs | Result |
| --- | ---: | --- |
| Official 1.10.1, one-table transaction | 4 | Both 200; one acknowledged row and snapshot missing in every pair |
| Assembled 1.12.0, one-table transaction | 4 | Both 204; one acknowledged row and snapshot missing in every pair |
| Official 1.10.1, two-table transaction | 2 | Both 200; one acknowledged row missing from each table in both pairs |
| Assembled 1.12.0, two-table transaction | 2 | Both 204; one acknowledged row missing from each table in both pairs |

For both fixtures, three one-table single-endpoint pairs and three sequential
transaction pairs returned one success and one 409, with the successful row
visible. Two sequential two-table pairs also returned one success and one 409
and retained the successful rows. No response in these completed controls was
unknown.

An independent Apache Avro reader decoded each attempted manifest list and
manifest, and PyArrow read both Parquet files. In every concurrent witness the
files still existed and held the intended rows, but the persisted current
snapshot referenced only one writer's file. The other acknowledged row was
not reachable through the table. The two-table requests also remove any
ambiguity about using a transaction endpoint with only one table.
The local histories and a copy of both stopped fixtures' databases and data
are under results/issue29-independent-400117086/ (Git-ignored). Both sets of
witnesses were independently rechecked from those copies without network
access after the fixtures stopped.

### What happens

The [reference adapter][issue29-adapter] creates a transaction for each table
and applies the request to its in-memory transaction operations through
[CatalogHandlers.commit][issue29-handler]. That changes the staged metadata,
but does not add a pending update to the transaction's replay list.

When the underlying table has changed, [BaseTransaction.applyUpdates][issue29-replay]
refreshes both its base and current metadata, then replays that list. Here the
list is empty, so the requested update is discarded. This can happen at the
first refresh or after a commit conflict triggers a retry. The subsequent
[table operation][issue29-noop] sees identical base and current metadata and
returns without a write. The transaction returns success.

Fixture logs show a commit conflict, a retry through BaseTransaction and
RESTCatalogAdapter, then “Nothing to commit” on the same thread. A separate
deterministic [Java probe](../scripts/Issue29TransactionProbe.java) calls the
actual handler with a controlled in-memory table operation. With raw staged
updates its pending list was empty and the acknowledged update disappeared
after both a refresh and an injected conflict; with the normal Java transaction
API the list contained one pending update and the update survived. The probe
ran inside the fixture's Java 17 runtime with the 1.10.1 library, and again
with the published 1.12.0 core/API/common/bundled-Guava jars ahead of the
fixture's other dependencies. This is a controlled mechanism test alongside
the live HTTP observations, not a full replacement for an upstream regression
test.

### Repeat the experiment

From PowerShell at the repository root, start the pinned fixture and run the
one-table property reproduction:

~~~powershell
docker compose -p calve-reference-issue up -d --wait rest-fixture
docker run --rm --mount "type=bind,source=$PWD,target=/work" -w /work python:3.13-slim python scripts/repro-reference-transaction-issue.py --catalog http://host.docker.internal:8181 --pairs 10 --out results/issue29-properties
~~~

The [script](../scripts/repro-reference-transaction-issue.py) writes HTTP
requests, responses and timings to a new output directory and checks the final
and later metadata. It exits 3 on an acknowledged missing effect, a visible
rejected effect, or two successful snapshots guarded on the same empty main
reference; 4 means an outcome was inconclusive, 1 is an execution error, and
0 means this run found none of those conditions. Pass --endpoint table or
--sequential for the corresponding controls.

To prepare real manifest lists and repeat the snapshot case, build a Linux
Calve binary and run its snapshot workload **inside the fixture**, where the
manifest files are accessible. The standalone script's output directory must
be new:

~~~powershell
New-Item -ItemType Directory -Force results | Out-Null
$fixture = docker compose -p calve-reference-issue ps -q rest-fixture
docker run --rm --mount "type=bind,source=$PWD,target=/work" -w /work -e GOOS=linux -e GOARCH=amd64 -e CGO_ENABLED=0 golang:1.24.7 go build -o results/issue29-calve-linux ./cmd/calve
docker cp results/issue29-calve-linux ($fixture + ':/tmp/calve-issue29')
docker exec $fixture /tmp/calve-issue29 snapshot --catalog http://localhost:8181 --namespace calve_issue29_seed --clients 2 --rounds 1 --out /tmp/issue29-seed.jsonl
docker cp ($fixture + ':/tmp/issue29-seed.jsonl') results/issue29-seed.jsonl
docker run --rm --mount "type=bind,source=$PWD,target=/work" -w /work python:3.13-slim python scripts/repro-reference-transaction-issue.py --catalog http://host.docker.internal:8181 --workload snapshot --seed-history results/issue29-seed.jsonl --pairs 10 --out results/issue29-snapshots
~~~

The seed history must contain both snapshot requests; if setup or a request
failed before they were recorded, repeat the seed on a fresh fixture. The
script reads the two manifest locations from that history. For controls add
--endpoint table or --sequential and choose another output directory.

To repeat the locally assembled 1.12.0 comparison on Windows, leave the 1.10.1
fixture running and use Git, a JDK with javac 17 or newer, curl.exe and Docker.
The [assembly script](../scripts/assemble-reference-1.12.ps1) checks the release
commit and published library checksums, compiles the four 1.12.0 fixture
classes, and records its inputs in assembly.json. If Docker is not on PATH,
pass its executable path with -DockerExe. The command uses a new output
directory, container name, and port 8193:

~~~powershell
.\scripts\assemble-reference-1.12.ps1
docker run --rm --mount "type=bind,source=$PWD,target=/work" -w /work python:3.13-slim python scripts/repro-reference-transaction-issue.py --catalog http://host.docker.internal:8193 --pairs 10 --out results/issue29-release-properties
docker cp results/issue29-calve-linux calve-issue29-release:/tmp/calve-issue29
docker exec calve-issue29-release /tmp/calve-issue29 snapshot --catalog http://localhost:8181 --namespace calve_issue29_release_seed --clients 2 --rounds 1 --out /tmp/issue29-release-seed.jsonl
docker cp calve-issue29-release:/tmp/issue29-release-seed.jsonl results/issue29-release-seed.jsonl
docker run --rm --mount "type=bind,source=$PWD,target=/work" -w /work python:3.13-slim python scripts/repro-reference-transaction-issue.py --catalog http://host.docker.internal:8193 --workload snapshot --seed-history results/issue29-release-seed.jsonl --pairs 10 --out results/issue29-release-snapshots
docker stop calve-issue29-release
~~~

This uses the Linux Calve binary built in the preceding snapshot instructions.
As above, pass --endpoint table or --sequential with a fresh output directory
for controls. The assembly is a comparison setup, not an official 1.12.0 image.

To repeat the one-table real-data recheck on Windows, the following creates a
separate fixture and native Docker volume. Choose unused names, an available
port, and a new output file. The pinned PyIceberg version is needed because
the script obtains staged updates from its transaction object before sending
them to the multi-table REST endpoint.

~~~powershell
$realVolume = 'calve-issue29-real-data'
$realContainer = 'calve-issue29-real-data'
docker volume create $realVolume
docker run --rm --mount "type=volume,source=$realVolume,target=/shared" python:3.13-slim mkdir -p /shared/warehouse
docker run -d --name $realContainer --user 0 -p 8196:8181 --mount "type=volume,source=$realVolume,target=/shared" --env CATALOG_WAREHOUSE=file:///shared/warehouse --env CATALOG_URI=jdbc:sqlite:/shared/warehouse/catalog.sqlite apache/iceberg-rest-fixture:1.10.1
$realReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try {
        if ((Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 http://localhost:8196/v1/config).StatusCode -eq 200) { $realReady = $true; break }
    } catch { Start-Sleep -Seconds 1 }
}
if (-not $realReady) { throw 'Fixture did not become ready' }
docker run --rm --mount "type=volume,source=$realVolume,target=/shared" python:3.13-slim python -m pip install --disable-pip-version-check --no-cache-dir --target /shared/pydeps 'pyiceberg[pyarrow]==0.11.1' 'pyarrow==20.0.0'
docker run --rm --mount "type=volume,source=$realVolume,target=/shared" --mount "type=bind,source=$PWD,target=/work" -w /work --env PYTHONPATH=/shared/pydeps python:3.13-slim python scripts/repro-reference-real-data.py --url http://host.docker.internal:8196 --db /shared/warehouse/catalog.sqlite --mode transaction --pairs 4 --out results/issue29-real-data.jsonl
docker stop $realContainer
~~~

The [real-data script](../scripts/repro-reference-real-data.py) records the
requests, responses, file hashes, SQLite pointer, persisted metadata and table
scan. Exit 3 signals a missing acknowledged effect or two successful requests
guarded on the same empty main reference; exit 4 is inconclusive. To run the
controls, substitute --mode table or --mode sequential and a new output file.
The native volume and its contents remain after stopping the fixture.

To repeat the deterministic Java probe with the fixture's exact 1.10.1 library:

~~~powershell
docker cp ($fixture + ':/usr/lib/iceberg-rest/iceberg-rest-adapter.jar') results/issue29-fixture.jar
javac --release 17 -cp results/issue29-fixture.jar -d results/issue29-classes scripts/Issue29TransactionProbe.java
java -cp "results/issue29-classes;results/issue29-fixture.jar" org.apache.iceberg.rest.Issue29TransactionProbe
~~~

The probe is diagnostic: it asserts the behavior observed in the tested
versions and exits zero when it observes that behavior. Its forced conflict
produces an expected retry warning. A future fix needs an upstream regression
test that expects the requested effect or a definite conflict.

Local histories, server logs, manifest copies, metadata witnesses, Java probe
logs and 1.12.0 assembly inputs are under
results/reference-transaction-issue-20261030/ (Git-ignored). The source-level
mechanism and the snapshot observation are consistent, but the original
upstream 204 observation had no minimized request, so identity of its cause
cannot be proven from that report alone.

[issue29-adapter]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L554-L575
[issue29-handler]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java#L604-L647
[issue29-replay]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/BaseTransaction.java#L438-L453
[issue29-noop]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/BaseMetastoreTableOperations.java#L109-L125

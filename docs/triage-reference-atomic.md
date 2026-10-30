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

Investigation in progress: reproduce the acknowledged missing-update observation
on fresh tables, reduce the request and concurrency needed, and compare the
multi-table transaction endpoint with a single-table commit control. Capture
the requests, responses, final metadata and fixture version. Then investigate
the separately reported missing-snapshot observation with real manifest lists.
The two observations may share a cause, but that remains to be verified.

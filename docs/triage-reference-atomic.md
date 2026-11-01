# Reference fixture: acknowledged multi-table markers go missing

Status: reproduced lead. Not yet minimized or traced in the server, and not
filed upstream.

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

Both final loads started after every response and showed the original table
UUIDs. Decoding the saved responses by hand gives the same counts. Direct GETs
of the first run's tables later still missed 10 and 11 acknowledged markers. The
offline check, with networking disabled, returns invalid for both histories.
Sweeps reproduce it in every run, with and without faults (see the README).

The same 40-transaction workload passed on Lakekeeper v0.14.2 (38 acknowledged,
2 rejected), Nessie 0.109.0 (12 / 28) and Polaris 1.8.0 (10 / 30), with no
unknown responses, every acknowledged marker kept, and no rejected effects.

## Contract

The [REST transaction endpoint](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441)
promises an atomic multi-table operation. A unique, acknowledged property that
is missing from a later load, when nothing could have removed it, contradicts
that. Calve reports it as `lost` with basis `catalog-contract`. Before it can be
reported upstream, read freshness has to be ruled out and the server behavior
pinned down.

## Likely cause

The fixture's
[transaction adapter](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L545-L573)
says plainly that its conflict handling is simplified and not truly
transactional: it stages each table's changes and commits them one by one. That
is relevant, but these runs didn't trace the server, so the exact cause is not
established. How it relates to
[apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552) is also
open.

## Reproduce

Start the Compose stack, then:

```sh
calve atomic --catalog http://localhost:8181 --tables 2 --clients 4 \
  --per-client 10 --out history-atomic-reference.jsonl
calve check history-atomic-reference.jsonl
```

Expect `INVALID` with `lost` findings (exit 3). Each run uses fresh random table
names. The runs above used `apache/iceberg-rest-fixture:1.10.1`, digest
`sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f`, with
no faults.

Next steps: shrink it to a minimal standalone script, then trace the server
path.

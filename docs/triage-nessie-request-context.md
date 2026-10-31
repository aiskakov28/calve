# Nessie request-context failures under concurrent commits

Status: reproduced on Nessie 0.109.0 without injected faults. The server
returns HTTP 500 during some concurrent Iceberg REST commits because a
request-scoped context is inactive. This is an availability issue; the
evidence does not show an acknowledged update disappearing. Whether this
exact issue is already reported upstream and why the context becomes inactive
remain open.

The project had already recorded Nessie metadata-write 500s as an untriaged
observation in [the 409 triage](triage-409-without-requirements.md). This
investigation identifies the exception and supplies a controlled comparison;
it does not claim first discovery of the original 500 symptom.

## Fault-free comparison

The local setup used `ghcr.io/projectnessie/nessie:0.109.0`, PostgreSQL 17.6,
and Silo as an S3-compatible object store. Each run created a fresh table and
sent 200 unique `set-properties` commits with empty requirements. The client
connected directly to Nessie, sent each commit once, and injected no faults.

| Writers | Runs | HTTP 200 | HTTP 409 | HTTP 500 | Offline verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 3 | 600 | 0 | 0 | 3 valid |
| 8 | 5 | 191 | 737 | 72 | 5 inconclusive |

The 409s are concurrency rejections and are not the finding here. All 72 500s
were recorded as unknown outcomes. Sixty-seven responses reported a failed
metadata write; all 67 matching server traces showed an inactive context for
`CatalogProducers.objectIO`. The other five responses explicitly reported an
inactive `SecurityIdentityProxy` context. None of those 72 unknown updates
was visible in the recorded recovery reads; absence alone does not establish
that an unknown commit was rejected.

A representative server trace, with the storage location omitted, is:

```text
HTTP 500: RuntimeException: Failed to write snapshot to: s3://.../metadata/...
  at org.projectnessie.catalog.service.impl.CatalogServiceImpl.storeSnapshot
Caused by: jakarta.enterprise.context.ContextNotActiveException:
  RequestScoped context was not active ... CatalogProducers
  at ...CatalogProducers_ProducerMethod_objectIO...ClientProxy.writeObject
```

The object-I/O proxy raised the exception; the metadata-write message alone
does not establish an S3 service failure. The local histories and server logs
used for this comparison are retained under the ignored
`results/full-fault-sweep-20261030/nessie-controls/` directory.

## Reproduce

With Docker Compose and the Go version in `go.mod`:

```sh
docker compose up -d --wait nessie
mkdir -p bin results
go build -o bin/calve ./cmd/calve
bin/calve append --catalog http://localhost:8183/iceberg \
  --clients 8 --per-client 25 --timeout 5s --out results/nessie-8w-1.jsonl
```

Repeat with a new output filename for each run. For the single-writer control,
use `--clients 1 --per-client 200`. Inspect the Nessie container logs and match
the metadata location from a 500 response to the associated stack trace. The
failure is intermittent, so one run may not reproduce it.

## Related evidence and limits

In an [earlier fault-free sweep](https://github.com/aiskakov28/calve/actions/runs/40992074092),
the five Nessie append histories contain 38 HTTP 500s: 30 metadata-write
messages and eight responses explicitly naming an inactive request context.
In `nessie/append/4.jsonl` from that run's `sweep` artifact, three unique
updates received 500s naming an inactive `QuarkusResultCollectorFactory`
context but were visible in a later successful table read. A 500 means the
commit state is unknown under the [Iceberg REST contract](design.md#responses-and-unknown-outcomes),
so this is not itself evidence of an atomicity violation. It does show that a
client cannot assume every 500 means the update did not commit.

Nessie's [`objectIO` producer](https://github.com/projectnessie/nessie/blob/nessie-0.109.0/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L232)
is request scoped; the [commit path](https://github.com/projectnessie/nessie/blob/nessie-0.109.0/catalog/service/impl/src/main/java/org/projectnessie/catalog/service/impl/CatalogServiceImpl.java#L676-L682)
uses it inside a `CompletionStage` chain. Shared asynchronous task completion
is a possible explanation, not a confirmed root cause. Those source files were
unchanged on main when checked; main itself was not run. A fresh, separate
instance passed two single-writer plain-HTTP controls, but its concurrent
controls were deferred due to memory pressure.

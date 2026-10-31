# Nessie request-context failures under concurrent commits

Status: reproduced on Nessie 0.109.0 without injected faults. The server
returns HTTP 500 during some concurrent Iceberg REST commits because a
request-scoped context is inactive. This is an availability issue; the
HTTP 500 response by itself is not proof of an Iceberg REST contract violation,
and the evidence does not show an acknowledged update disappearing. Whether
this exact issue is already reported upstream and why the context becomes
inactive remain open.

The [stage 1 evidence audit](nessie-evidence-audit.md) checks the saved control
requests, trace correlations, selected witnesses and environment provenance,
and records the limits to address before the upstream report.

The Nessie metadata-write 500s were already recorded as an untriaged
observation in [aiskakov28's 409 triage](triage-409-without-requirements.md).
This investigation identifies the exception and supplies controlled
comparisons; it does not claim first discovery of the original 500 symptom.

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

An independent control used a separate Nessie instance with its own PostgreSQL
and Silo storage, and sent plain HTTP requests from Python rather than Calve.
Two single-writer runs had 400 successes and no 500s. Three eight-writer runs
had 106 successes, 473 conflicts, and 21 HTTP 500s. All 19 metadata-write
errors matched inactive `objectIO` context traces; the other two named an
inactive `QuarkusResultCollectorFactory` context. Both of those two updates
were visible in a later table read. No faults were injected in these controls.
All 21 failed request intervals overlapped requests from the other seven
writers; this is client-side overlap, not proof of simultaneous server execution.
The original Python controls used the default `urllib` redirect handling and
did not record intermediate redirects, if any. The Calve controls disable
redirects; the current Python reproducer does too.
The [checked evidence excerpt](evidence/nessie-request-context.json) contains
one applied unknown update and one metadata-write response with its matching
server trace; the complete histories and logs remain in the local results.

Follow-up independent controls used the Python client with redirects disabled
and no injected faults. Each run sent 200 commits to a fresh table:

| Writers | Runs | HTTP 200 | HTTP 409 | HTTP 500 | Runs with a 500 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 400 | 0 | 0 | 0 |
| 2 | 5 | 675 | 319 | 6 | 4 |
| 4 | 3 | 212 | 367 | 21 | 3 |
| 8 | 3 | 119 | 458 | 23 | 3 |

The issue therefore also reproduces with two writers. All 47 metadata-write
failures in these follow-up controls matched inactive `objectIO` context
traces. Two other responses named an inactive security context, and one named
an inactive result-collector context. The latter update was visible in the
final read. The [two-writer evidence excerpt](evidence/nessie-two-writer-context.json)
contains one failed request, overlapping requests from the other writer,
the later table identity, and its matching server trace. Client-side overlap
does not establish simultaneous server execution.

An independent audit of all 18 saved Python runs found every acknowledged
property in the matching final table state and no rejected 409 property there.
Absent unknown properties remain unresolved. These additional results
strengthen the availability finding without establishing a data-loss or
atomicity violation.

The same inactive `objectIO` context also appeared in a [two-table transaction
witness](evidence/nessie-atomic-context.json) from a reset-fault campaign. That
request started after the last scheduled fault had healed and returned 500;
cleanup later repeated idempotent heals. Its commit outcome remains unknown;
this observation does not establish a partial transaction or an atomicity
violation.

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

The independent Python standard-library client can also exercise the same
requests without Calve or Go:

```sh
python3 scripts/repro-nessie-request-context.py \
  --url http://localhost:8183/iceberg --writers 8 --per-writer 25 \
  --output results/nessie-raw-1.json
```

For its single-writer control, use `--writers 1 --per-writer 200` and a new
output filename. The script saves request bodies, response statuses and error
bodies, including a failed final read, and identifies unknown updates visible
in a successful final read. It omits successful response bodies, which can
contain vended credentials. A missing update remains unknown. The script exits
4 when the final read fails or any unknown update remains unresolved; inspect
the recorded response counts even when it exits 0. It does not follow HTTP
redirects; a redirect response to a commit remains an unknown outcome.
Nanosecond timestamps are decimal strings so JSON readers preserve their full
precision.

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

The fault sweep also has a Nessie maintenance-coverage limitation. In the
30 completed Nessie maintenance campaigns, 34,010 appends were
acknowledged but no snapshot expiration was attempted. The workload expires
non-head snapshots returned by a table load; [Nessie documents](https://projectnessie.org/guides/iceberg-rest/#time-travel-with-iceberg-rest)
that it returns the snapshot corresponding to the selected Nessie commit.
These campaigns therefore do not test expiration racing appends on Nessie.
The missing expiration coverage is not evidence of a catalog issue.

Nessie's [`objectIO` producer](https://github.com/projectnessie/nessie/blob/nessie-0.109.0/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L232)
is request scoped; the [commit path](https://github.com/projectnessie/nessie/blob/nessie-0.109.0/catalog/service/impl/src/main/java/org/projectnessie/catalog/service/impl/CatalogServiceImpl.java#L676-L682)
uses it inside a `CompletionStage` chain. Shared asynchronous task completion
is a possible explanation, not a confirmed root cause. Those source files were
unchanged on main when checked; main itself was not run.

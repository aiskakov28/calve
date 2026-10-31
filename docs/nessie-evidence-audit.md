# Nessie report: stage 1 evidence audit

Status: evidence audit completed on 2026-10-31. The
[stage 2 clean reproduction](nessie-clean-reproduction.md) is also complete;
the current-upstream check and final issue text are later stages. This audit
used saved data and stopped-container metadata; it did not run new live
workloads.

## Confirmed evidence

The audit independently checked all 18 Python controls and eight Calve
controls: 5,200 recorded property-commit requests in total. Every writer's
expected keys and request count were present. Each run used a distinct table
UUID, and no key was submitted twice within a run. The request bodies contain
empty requirements and one `set-properties`
update mapping a unique key to the string `1`. This is permitted by the pinned
[commit request schema][request] and [property-update schema][properties].

The primary independent controls used the redirect-disabled client:

| Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 400 | 400 | 0 | 0 | 0 |
| 2 | 5 | 1,000 | 675 | 319 | 6 | 4 |
| 4 | 3 | 600 | 212 | 367 | 21 | 3 |
| 8 | 3 | 600 | 119 | 458 | 23 | 3 |

Across all 26 controls, 143 responses were HTTP 500. All 133 metadata-write
errors named unique metadata locations and matched a single HTTP/500 server
trace containing `ContextNotActiveException`, the `objectIO` proxy and
`TasksServiceImpl.finalResult`. The other ten error responses explicitly named
inactive security or result-collector contexts. This supports an internal
request-context failure; it does not establish why the context expired.

All 2,703 acknowledged property updates were present in the corresponding
final metadata. None of the 2,354 rejected updates was present. Three updates
with HTTP 500 were visible; the other 140 remain unresolved. Final table UUIDs
matched the originals saved by the clients. Available final-read timings were
after client requests completed. These observations establish neither data
loss nor a REST-contract violation; a 500 has an [unknown commit outcome][outcomes].

The [two-writer witness][two-writer] and [earlier Python witness][earlier]
match the original request bodies, responses and timestamps. Their published
trace excerpts occur in the matching raw server blocks. Older numeric JSON
timestamps were read losslessly for this comparison. The [two-table witness][atomic]
also matches its raw history and trace; its recorded scheduled fault heal
preceded the request, and cleanup repeated heals afterward. That witness is
supplemental fault-campaign evidence with an unknown transaction outcome.

## Environment and isolation

Server startup logs identify Nessie 0.109.0 and Quarkus 3.40.1. The surviving
control containers use the official Nessie image at this immutable reference:

```text
ghcr.io/projectnessie/nessie@sha256:61a1a14e73d76ac3851e7bef627e4ef426f4f0cf71c352843908a5834acd19e1
```

The controls used PostgreSQL 17.6 with the JDBC2 version store and Silo
`RELEASE.2026-09-03T13-18-01Z` for S3-compatible storage. Authentication and
authorization were disabled according to startup logs. The Python client
connected directly to `http://nessie:19120/iceberg` on the independent Compose
network. Its PostgreSQL and Silo volumes differ from the fault-sweep volumes.
No recorded server start/stop falls within a Python control. The Calve controls
used the direct port, and their run intervals overlap no saved Nessie fault
campaign. The control commands themselves inject no faults.

The sanitized container/image snapshot was collected after execution. It
records immutable image IDs, network isolation and storage mounts without
credential values; it is not a manifest captured at the start of each run.

## Limits to carry into the next stages

- Historical Python results do not embed the executed source hash or exact
  Python image/interpreter build. The current script and preserved run copy
  have identical SHA-256 hashes, but this was checked after the runs. Stage 2
  recorded those identities and the Java version before its fresh workloads.
- Five older Python controls used default redirect handling. Keep them as
  supplemental evidence; use the 13 redirect-disabled controls for the primary
  reproduction claims.
- Python intervals use wall-clock timestamps. Client overlap is verified, but
  it does not prove simultaneous server execution. A single-writer control
  with no failures does not establish that sequential use can never fail.
- Python creation response bodies were omitted; the original UUID was saved
  by the client. Successful response bodies were also omitted in the current
  client. Some older files retain them. The known-field scan found no
  unredacted credential fields, and selected public excerpts passed the same
  check; final attachments still need a focused redaction review.
- The shared asynchronous-future explanation, generality across storage
  backends, behavior on current upstream and novelty remain unconfirmed.
  The earlier 500 symptom was already observed by aiskakov28.

Local audit outputs and post-run SHA-256 inventory are preserved under
`results/nessie-report-stage1-20261031/` (Git-ignored). The checks are
`audit-evidence.cjs` and `audit-atomic-witness.cjs`; their JSON reports record
the counts, correlations, witness checks and provenance limits above.

[request]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3476
[properties]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L2896-L2908
[outcomes]: design.md#responses-and-unknown-outcomes
[two-writer]: evidence/nessie-two-writer-context.json
[earlier]: evidence/nessie-request-context.json
[atomic]: evidence/nessie-atomic-context.json

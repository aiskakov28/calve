# Calve design doc

## Goal

Find isolation and durability anomalies in open-source Iceberg REST catalogs under concurrency and faults, with minimal standalone reproductions.

## Non-goals

- Testing managed cloud catalogs.
- Benchmarking performance.
- Testing query engines (Spark, Trino) rather than catalogs.

## What the catalogs promise

The baseline is the [REST OpenAPI shipped with Iceberg 1.10.1][rest-spec], matching our pinned reference catalog. Source links below use release commit `ccb8bc435062171e64bc8b7e5f56e6aed9c5b934`. Record the catalog version and applicable spec revision with each finding; do not silently apply newer protocol features to older catalogs. Quotations are spec text; the checker rules below are Calve's interpretation.

### Single-table commit: requirements and updates

`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (`updateTable`) accepts `requirements` and `updates`. The [endpoint description][single-commit] says:

> Requirements are assertions that will be validated before attempting to make and commit changes.

The [requirement schemas][requirements] define the predicates:

| Requirement | Condition being checked |
| --- | --- |
| `assert-create` | The table does not exist. |
| `assert-table-uuid` | The UUID matches `uuid`. |
| `assert-ref-snapshot-id` | The named branch/tag points to `snapshot-id`; a null ID requires that the ref does not exist. |
| `assert-current-schema-id` | The current schema matches `current-schema-id`. |
| `assert-last-assigned-field-id` | The last assigned column ID matches. |
| `assert-last-assigned-partition-id` | The last assigned partition ID matches. |
| `assert-default-spec-id` | The default partition spec ID matches. |
| `assert-default-sort-order-id` | The default sort order ID matches. |

Both commit endpoints require HTTP 400 for an unrecognized requirement or update. Their HTTP 409 (`CommitFailedException`) descriptions say:

> one or more requirements failed.

The client is allowed to retry after a conflict. Calve records each attempt separately. A retry that refreshes requirements is a new conditional operation, not evidence that the rejected attempt committed.

The [table spec's concurrency model][table-concurrency] supplies the single-table atomicity basis: metadata changes replace the table's metadata pointer atomically. **Calve rule:** a successful commit must fit a sequence of atomic table changes in which its supplied requirements hold. For a deterministic negative test, change a schema from ID `s0` to `s1`, wait for success, then submit a commit requiring `s0`. With no intervening schema changes, success on that stale requirement is a violation.

Two concurrent commits succeeding is not sufficient evidence of a bug. The current property workload sends `requirements: []` and changes distinct keys; both commits may succeed, and both acknowledged keys must survive if no later update removes them. A one-winner test needs a requirement that the winning update actually invalidates; matching only the table UUID does not provide that guard.

### Multi-table commit: atomicity and its limits

`POST /v1/{prefix}/transactions/commit` (`commitTransaction`) is described as an ["atomic operation"][multi-commit]. Its [request schema][commit-requests] contains `table-changes`, each with an identifier, requirements, and updates. An empty `updates` array is permitted, so a table change can contain requirements alone.

**Calve rule:** treat the requested changes as one all-or-nothing commit. After recovery and resolution of in-flight requests, a committed transaction must have applied all its changes; a rejected transaction must have applied none. An unknown outcome permits either complete result, not a permanently committed subset. Orphan metadata/data files alone do not establish a partial commit: inspect the state reachable through the catalog.

Atomicity does not by itself establish serializable isolation. The OpenAPI does not explicitly define a multi-table isolation level, when requirements on unchanged tables must be revalidated against concurrent writes, or a consistent snapshot across separate table loads. The table spec also makes [isolation depend on the validations selected by writers][table-concurrency]. Do not infer a blanket multi-table serializability guarantee from it.

For the workload in [apache/iceberg#18552][skew-report], start from schema IDs `(a0, b0)`: T1 requires both IDs and changes only A; T2 requires both IDs and changes only B. If requirement validation and updates form one serializable operation, at most one can commit: either winner invalidates the other's guard. Both succeeding is an isolation anomaly under that model, but confirmation as a catalog bug needs a cited catalog guarantee or clarification of the OpenAPI's validation semantics. The issue report is a reproduction lead, not a normative guarantee. Separately test a stale requirement on a requirement-only table after the conflicting transaction has already completed; that avoids relying on concurrent isolation semantics.

### Responses and unknown outcomes

These mappings come from the [single-table][single-commit] and [multi-table][multi-commit] response definitions, except where explicitly marked as Calve policy.

| Response | History | Checker interpretation |
| --- | --- | --- |
| Single-table HTTP 200 | `ok` | Commit acknowledged; response contains `metadata-location` and `metadata`. |
| Multi-table HTTP 204 | `ok` | Transaction acknowledged; no response body. |
| HTTP 409 | `fail` | Conditional commit rejected; its changes must not be committed. |
| HTTP 400; documented 401/403/404/419 | `fail` | Request rejected (invalid/unknown input, authentication, authorization, or missing table). |
| HTTP 500/502/504 | `info` | Both endpoints explicitly say the "commit state is unknown." |
| HTTP 503 | `info` | The [shared response][service-unavailable] allows partial request processing; it does not establish rejection. |
| Other 5xx; timeout, reset, or incomplete response | `info` | Conservative Calve policy: insufficient evidence of either success or rejection. |

Keep status, error type, and the raw response available for triage. The [reference catalog report][skew-report] includes HTTP 200 for multi-table success; Calve can accept that acknowledgment while recording the departure from the documented 204 separately. An undocumented status does not itself demonstrate an isolation or durability failure. The 1.10.1 contract does not provide an idempotency key for these operations; do not blindly resubmit an unknown commit and assume exactly-once execution.

### When Calve can call a result a bug

A candidate must identify the endpoint, supplied requirements/updates, observed outcome, and the exact promise it contradicts. Direct candidates include an acknowledged update missing without a later operation that undoes it, a rejected operation's unique effect being present, a successful commit with a demonstrably false requirement, or a permanently partial multi-table commit. Use unique operation markers to establish attribution.

Before confirming a state-based finding, heal faults, stop new writes, let server-side work settle, and reload through the catalog. Rule out intentional overwrites/removals, snapshot expiration, stale client caches, and table recreation. If recovery or a trustworthy final read is unavailable, report the run as inconclusive. A timeout or 5xx alone is never proof of lost data. Likewise, observing old A and new B in two separate reads is not proof of a partial transaction if the transaction could have committed between those reads.

The current property-based append workload can check lost or rejected keys, but a property map cannot expose duplicate occurrences of a key. Its `VALID` result covers that workload's invariants; it does not establish general serializability, exactly-once data-file appends, or behavior under faults that were not exercised.

[rest-spec]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml
[single-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L993-L1109
[requirements]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3093-L3228
[multi-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441
[commit-requests]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3504
[service-unavailable]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L4651-L4667
[table-concurrency]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L90
[skew-report]: https://github.com/apache/iceberg/issues/18552

## History format

One JSON record per line (`internal/history`):

| Field | Meaning |
| --- | --- |
| index | position in the history |
| process | logical client |
| type | invoke, ok, fail, info (unknown) |
| f | operation name |
| value | workload-specific payload |
| time | nanoseconds since run start |
| status | HTTP status, 0 if no response |
| error | error text for fail and info |

## Workloads and checkers

| Workload | Checker | Anomalies |
| --- | --- | --- |
| append | `checker.CheckAppend` | lost, phantom-failed, duplicate, unexpected |
| write skew | TODO | both conflicting transactions committed |
| compare-and-set | TODO (Porcupine) | non-linearizable history |
| multi-table general | TODO (dependency graph + cycle search) | G0, G1c, G2 cycles |

## Faults

TODO: Toxiproxy (latency, reset, partition) between client and catalog and between catalog and Postgres; kill, pause, restart of the catalog process; every schedule derived from one seed.

## Open questions

- TODO

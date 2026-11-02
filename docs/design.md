# Calve design

## Goal

Find isolation and durability anomalies in open-source Iceberg REST catalogs
under concurrency and faults, and turn each one into a minimal standalone
reproduction.

Out of scope: managed cloud catalogs, performance benchmarking, and query
engines such as Spark or Trino.

## What the catalogs promise

The baseline is the [REST OpenAPI shipped with Iceberg 1.10.1][rest-spec], the
version of the pinned reference catalog. Source links here use release commit
`ccb8bc43`. Every finding records the catalog version and the spec revision it
was judged against; newer protocol features are never applied to older
catalogs. Quotations below are spec text. The rules marked **Calve rule** are
our interpretation.

### Single-table commits

`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (`updateTable`) takes
`requirements` and `updates`. The [endpoint description][single-commit] says:

> Requirements are assertions that will be validated before attempting to make and commit changes.

The [requirement schemas][requirements] define what each one checks:

| Requirement | Checks that |
| --- | --- |
| `assert-create` | the table does not exist |
| `assert-table-uuid` | the UUID matches |
| `assert-ref-snapshot-id` | the named branch or tag points at `snapshot-id`; null means the ref must not exist |
| `assert-current-schema-id` | the current schema ID matches |
| `assert-last-assigned-field-id` | the last assigned column ID matches |
| `assert-last-assigned-partition-id` | the last assigned partition ID matches |
| `assert-default-spec-id` | the default partition spec ID matches |
| `assert-default-sort-order-id` | the default sort order ID matches |

An unrecognized requirement or update must get HTTP 400. A 409
(`CommitFailedException`) means "one or more requirements failed", and the
client may retry. Calve records every attempt separately; a retry with
refreshed requirements is a new operation, not evidence about the old one.

The [table spec's concurrency model][table-concurrency] says a metadata change
swaps the table's metadata pointer atomically. **Calve rule:** every successful
commit must fit into a sequence of atomic changes in which its requirements
held when it applied.

Two concurrent successes are not a bug on their own. The append workload sends
`requirements: []` and changes different keys, so both may succeed, and both
keys must then survive. A one-winner test needs a requirement that the winner's
update actually breaks; a matching table UUID alone is not enough.

### Multi-table commits

`POST /v1/{prefix}/transactions/commit` (`commitTransaction`) is described as an
["atomic operation"][multi-commit]. Each entry in its
[`table-changes`][commit-requests] has an identifier, requirements and updates,
and `updates` may be empty, so a table can appear only to be asserted on.

**Calve rule:** a transaction is all-or-nothing. Once in-flight requests have
been resolved, a committed transaction has applied every change and a rejected
one has applied none. An unknown outcome allows either complete result, never a
lasting subset. Orphaned files alone don't prove a partial commit; only state
visible through the catalog counts.

Atomicity is not serializability. The spec does not define a multi-table
isolation level, say when requirements on unchanged tables are rechecked against
concurrent writes, or promise a consistent snapshot across separate table loads.
The table spec also makes [isolation depend on the validations writers
choose][table-concurrency]. So Calve never assumes multi-table serializability.

The skew workload follows [apache/iceberg#18552][skew-report]. Both
transactions start from schema IDs `(a0, b0)` and require both. T1 changes only
A and T2 changes only B. If validation and updates form one serializable step,
at most one can commit, because either winner breaks the other's requirement.
Both committing is a `write-skew` **lead** under that model: confirming it as a
catalog bug needs a cited guarantee or clarification of the spec. The
sequential control is different. It sends T2 after T1 has visibly committed, so
accepting it violates the requirement directly, with no assumption about
concurrency.

### Responses and outcomes

Mappings come from the [single-table][single-commit] and
[multi-table][multi-commit] response definitions, except the last row.

| Response | Recorded as | Meaning |
| --- | --- | --- |
| Single-table 200 | `ok` | Commit acknowledged; the body has `metadata-location` and `metadata` |
| Multi-table 204 | `ok` | Transaction acknowledged, no body |
| 409 | `fail` | Conditional commit rejected; its changes must not appear |
| 400; 401, 403, 404, 419 | `fail` | Request rejected: bad input, authentication, authorization or missing table |
| 500, 502, 504 | `info` | Both endpoints say the "commit state is unknown" |
| 503 | `info` | The [shared response][service-unavailable] allows partial processing |
| Any other status (201, 202, 408, 422, 429, ...), timeout, reset, incomplete response | `info` | Calve policy: no evidence either way |

The same mapping applies to every call Calve makes (`history.Outcome`), so 406,
which namespace creation documents as an unsupported operation, is also `fail`.
Checkers refuse a history whose `ok` or `fail` is not backed by one of these
statuses. Status, error type and the raw body are kept for triage. The reference catalog
answers a successful transaction with 200 instead of the documented 204; Calve
accepts it as an acknowledgment. An undocumented status is never an isolation
or durability finding by itself. Iceberg 1.10.1 has no idempotency key, so
Calve never resubmits an unknown commit.

### When a result is a bug

A finding names the endpoint, the requirements and updates sent, what came
back, and the exact promise it breaks. Direct findings are:

- an acknowledged change that is missing, with nothing later that could undo it;
- a rejected operation's unique effect showing up;
- a commit that succeeded with a requirement that was demonstrably false;
- a multi-table commit that stays partial.

Every operation carries a unique marker, so each effect can be traced to the
request that made it. Before a state-based finding counts, faults are healed,
writes stop and the table is reloaded through the catalog. Overwrites,
snapshot expiration, stale client caches and table recreation must be ruled
out. Without a trustworthy final read the run is inconclusive. A timeout or 5xx
is never proof of lost data, and seeing old A and new B in two separate reads
is not proof of a partial transaction, since it may have committed between the
reads.

## Verdicts and unknown outcomes

Every checker returns `valid`, `invalid` or `inconclusive`:

- **Valid** means the named invariant held in every round that could be fully
  checked. It is never printed for a run with unresolved rounds.
- **Invalid** means at least one proven finding. Proven findings win over
  incomplete checks, whose reasons are still reported.
- **Inconclusive** means the evidence can't decide.

Each finding has a kind, the operation IDs that prove it, and a basis.
`catalog-contract` findings (`lost`, `phantom-failed`, `stale-requirement`,
`unexpected` and the workload-specific kinds) break a stated promise.
Model-based findings (`write-skew`, `non-atomic-visibility`,
`single-table-regression`) are leads that depend on a stated model, such as
serializability or fresh table loads.

An `info` outcome stays `info` in the history. A complete, uniquely
attributable effect seen through the catalog proves it committed; the checker
records that resolution separately, with the observations that support it.
Absence never proves rejection, because Calve has no way to know that the
server has finished with a request. See [unknown outcomes](unknown-outcomes.md).

Checkers reject malformed evidence, such as a completion without an invocation
or requests that don't match their recorded baseline, as history errors rather
than guessing.

## History format

Histories are version 1 JSON Lines, written by `internal/history`. The first
record is `type: "meta", f: "run"`, describing the run: run ID, workload,
catalog and warehouse, tables, client counts, timeouts and read-back settings.
Bearer tokens are never written. Paired invocation and completion records
follow for every HTTP call: configuration, setup, baseline reads, the workload,
final reads and any read-back.

| Field | Meaning |
| --- | --- |
| `version` | format version, 1 |
| `id` | operation ID shared by an invocation and its completion |
| `index` | position in the history |
| `process` | logical client; -1 is the coordinator, -2 the fault controller |
| `type` | `meta`, `invoke`, `ok`, `fail` or `info` |
| `f` | operation name |
| `phase` | `setup`, `baseline`, `workload`, `final` or `recovery`; skew adds `control-verify` |
| `round` | workload round, where the workload has rounds |
| `value` | workload-specific payload, such as the operation's marker |
| `time` | nanoseconds since the run started |
| `request` | method, URL, headers and body, on the invocation |
| `response` | status, headers and body, on the completion |
| `status` | HTTP status, 0 if no response arrived |
| `error` | error text for `fail` and `info` |

Bodies are base64-encoded bytes, so HTML error pages, invalid UTF-8, partial
responses and 64-bit numbers survive exactly. If reading a body fails, the
status and partial bytes are kept and the outcome is `info`, even for a 200. A
missing completion is unknown.

The recorder works at the level of `http.Client.Do`: it sees application
requests and responses, not packets or transport retries. Redirects are
disabled so their statuses stay visible. Credentials are redacted from known
headers, URL user info and query fields, known credential keys in JSON, and any
occurrence of the configured token; redacted records carry `redacted: true`.
This is targeted redaction, not a scan for arbitrary secrets in server output.

Histories are saved when a run finishes or returns an error, including when a
final read fails. Ctrl+C or SIGTERM during `calve run` counts as an error
return. They are not written incrementally, so a hard kill of Calve itself
loses the run. Each history goes to a new file only its owner can read (mode
0600); Calve never replaces an existing one, and a workload whose `--out`
already exists stops before sending any request.

## Workloads and checkers

| Workload | Checker | Findings |
| --- | --- | --- |
| property append | `checker.CheckAppendHistory` | `lost`, `phantom-failed`, `unexpected` |
| write skew | `checker.CheckSkew` | `write-skew` (lead), `stale-requirement`, `lost`, `phantom-failed` |
| multi-table markers ([details](multi-table-checker.md)) | `checker.CheckAtomic` | `lost`, `phantom-failed`, `unexpected`, `non-atomic-visibility` and `single-table-regression` (leads) |
| snapshot compare-and-set ([details](single-table-checker.md)) | `checker.CheckSnapshotHistory`, using Porcupine | non-linearizable history |
| schema compare-and-set ([details](schema-workload.md)) | `checker.CheckSchema` | `multiple-winners`, `lost`, `phantom-failed`, `unexpected` |
| maintenance race ([details](maintenance-workload.md)) | `checker.CheckMaintenance` | `lost-snapshot`, `dangling-main`, `main-regressed`, `phantom-failed`, `unexpected` |

A property map can't show a key written twice, so the append workload doesn't
detect duplicates. General multi-table cycle detection (G0, G1c, G2, as in
[Elle](checker-background.md)) is not implemented; the multi-table checks are
specific to their workloads.

### The skew workload in detail

Each round:

1. Creates fresh tables A and B and, with no writers active, loads their UUIDs,
   schemas and current schema IDs `(a0, b0)`.
2. Builds both transactions from that baseline. Every table change asserts its
   UUID and current schema ID. T1 adds a schema to A and sends a
   requirement-only change for B (`updates: []`); T2 does the reverse.
3. Gives each added schema a unique column and proposes the schema ID
   `max(existing IDs) + 1`, as the [Java client does][java-schema-ids]. Nessie
   rejects an omitted ID ([triage](triage-nessie-schema-id.md)).
4. Releases both clients through a barrier. Each sends one transaction, with no
   retries. A barrier encourages overlap but can't guarantee it, so the result
   reports whether the recorded intervals actually overlapped.
5. Reloads both tables.

After the concurrent rounds come two sequential controls, one in each
direction. T1 commits, its effect is verified, then the original T2 is sent.
The catalog must answer 409 and leave B untouched. If the first commit or its
verification is uncertain, the control is skipped and the run can't be valid.

For the at-most-one rule:

| Evidence for the pair | Result |
| --- | --- |
| Both acknowledged, or proven committed | `write-skew` |
| One acknowledged, the other rejected | No violation |
| One acknowledged, the other unknown but proven not committed | No violation |
| One acknowledged, the other unknown and unresolved | Inconclusive |
| Both unknown | Resolved from evidence where possible, otherwise inconclusive |

A complete expected schema seen on the original table UUID can prove an
unknown committed. The checker compares full schema contents and allows
catalog-assigned schema IDs.

To re-check a saved skew history from Go, use `history.ReadJSONL` followed by
`checker.CheckSkew`, or run:

```sh
CALVE_SKEW_RECHECK="$PWD/history-skew.jsonl" \
  go test -v ./internal/checker -run '^TestCheckSkewSavedHistory$' -count=1
```

Set `CALVE_SKEW_EXPECT=valid`, `invalid` or `inconclusive` to assert a result.

## Faults

Fault campaigns are described in [fault campaigns](fault-campaigns.md) and the
[seeded scheduler](fault-scheduler.md). The model in short:

- **Network faults** go through one Toxiproxy proxy per catalog, between Calve
  and the catalog. Every workload request uses the proxy; the Toxiproxy API and
  the Docker CLI stay outside the faulted path, so the controller can always
  heal. Faults between the catalog and its database or object store are not
  modeled.
- **Process faults** act on the catalog container only. Its database and object
  store keep running.

| Fault | Mechanism | Healing |
| --- | --- | --- |
| latency | Toxiproxy `latency` toxic, fixed delay, no jitter, on one direction | Remove the toxic |
| reset | `reset_peer` toxic on one direction | Remove the toxic |
| partition | `timeout` toxics with `timeout: 0` on both directions | Remove both toxics |
| kill | [`docker kill`][docker-kill] with `SIGKILL` | Start the same container |
| pause | [`docker pause`][docker-pause] | Unpause it |
| restart | Stop with `SIGTERM` and a grace period | Start the same container |

Controllers verify each fault took effect and each heal worked, rather than
trusting the command's exit status. Automatic restart policies are refused,
because they would end an outage early. Recreating a container, deleting a
volume or using an in-memory backend is not a durability test, so process
faults keep the same container and storage.

After a campaign: no new faults or writes, every fault healed, a wait until the
catalog answers, then final reads and checking. A healthy endpoint shows the
catalog is reachable, not that earlier commits have finished, so absence-based
conclusions stay inconclusive.

## Open questions

**What multi-table isolation does each catalog promise?** Calve reports a
conflicting concurrent pair as a lead until a cited guarantee settles the
model, and keeps the sequential control as a direct contract check. Settling it
takes version-specific catalog documentation or an answer upstream; a
reproduction alone doesn't.

**When can absence prove a commit didn't happen?** Calve has no per-catalog way
to know that a timed-out request can no longer apply. Until one exists and is
tested, including a request that commits after its client timed out, an absent
effect leaves the commit unknown.

**Data files.** The snapshot and maintenance workloads write real, empty
manifest lists, so snapshot publication is tested. No workload writes data
files, so duplicate or lost data-file appends are not covered.

[rest-spec]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml
[single-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L993-L1109
[requirements]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3093-L3228
[multi-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441
[commit-requests]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3504
[service-unavailable]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L4651-L4667
[table-concurrency]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L90
[skew-report]: https://github.com/apache/iceberg/issues/18552
[java-schema-ids]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651
[docker-kill]: https://docs.docker.com/reference/cli/docker/container/kill/
[docker-pause]: https://docs.docker.com/reference/cli/docker/container/pause/

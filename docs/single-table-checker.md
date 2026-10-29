# Single-table snapshot linearizability (issue #18)

`checker.CheckSnapshotHistory(ops, timeout)` runs Porcupine v1.3.1 against the
`calve-snapshot-v1` sequential model. It checks recorded HTTP evidence offline.
The CLI accepts the same history:

```sh
go run ./cmd/calve check-table --timeout 5s snapshot-history.jsonl
```

Exit codes are 0 valid, 3 invalid, 4 inconclusive, and 1 for malformed or
unsupported evidence. The search budget must be positive; Porcupine exhaustion
is inconclusive. An invalid result identifies the entire history's operation
IDs, not a minimized counterexample.

## Sequential model

The state is one v2 table's UUID, last sequence number, current main snapshot,
and the retained snapshots' complete descriptors. Each successful commit
atomically:

1. Checks `assert-table-uuid` and `assert-ref-snapshot-id` on `main`.
2. Adds one previously unused snapshot with a strictly larger sequence number.
3. Moves `main` and `current-snapshot-id` to that snapshot.

Supported requests contain exactly those two requirements and, in that order,
`add-snapshot` and `set-snapshot-ref` updates. The new snapshot's parent must
match the expected main snapshot; null means that main must not exist.
Snapshot IDs must be unique across all attempts and absent from the baseline.
New descriptors explicitly include `snapshot-id`, `sequence-number`,
`timestamp-ms`, `manifest-list`, `schema-id`, and an append summary. Explicit
schema IDs avoid confusing server-supplied defaults with changed effects.

Reads return the modeled projection of metadata. A successful commit's returned
metadata must match its resulting state too. Retained snapshot order is
irrelevant; descriptor contents and 64-bit IDs are preserved. Missing/null/-1
current snapshot IDs represent an empty table. Main is implicit when `refs` is
absent/null (an empty map is also normalized); explicit main references must
match the current snapshot. Reference retention defaults do not affect this
projection.

Sequence numbers need not be consecutive. The pinned Java metadata builder
accepts an increase larger than one; Calve must not label that accepted input
as a catalog violation.

A rejection makes no changes. The model permits rejection under internal
contention even when the abstract guard holds; it does not test progress or
require every possible commit to succeed.

The model is pure: each transition copies snapshot storage, and all future
states leave earlier search states intact.

## Evidence contract

The caller must exclusively own the table, finish setup and any earlier server
work before recording, and record **every** request during the checked window.
The checker cannot discover unrecorded outside writers or establish that setup
has drained. Begin a new recorder after setup. Do not slice an existing history
or rewrite its outcome/timestamp fields to make it fit.

The first version-1 record is `type: "meta", f: "run"`. Its value is a
`checker.SnapshotManifest`, for example:

```json
{
  "run_id": "snapshot-example",
  "workload": "snapshot-cas",
  "model": "calve-snapshot-v1",
  "catalog": "http://localhost:8181",
  "namespace": "calve",
  "table": "snapshot_example",
  "commits": 2
}
```

Use `rest.Client.WithRecorder` and `history.WithOperation` to record calls.
The first REST call is `load-table` with phase `baseline`. It must complete
successfully before any workload call. Later operations are:

| Operation | Method | Phase |
| --- | --- | --- |
| `commit-table` | POST | `workload` |
| `load-table` | GET | `workload`, `final`, or `recovery` |

They all target exactly the same table URL, including the negotiated prefix,
under the manifest's catalog root. Setup/config calls are outside this window;
configure the client before recording. No extra round or derived value labels
are used. Operation IDs pair invocations and completions; labels and statuses
must agree, record indexes are contiguous from zero, and the recorder clock
never moves backward. Successful operations require HTTP 200 metadata.

The baseline can be empty or contain an existing main snapshot and retained
snapshots. A final/recovery GET must succeed after the recorded attempts;
missing attempts, missing final evidence or failed final reads prevent an
unqualified pass until usable recovery evidence is available. Failed/pending
reads supply no observable value and no state constraint; a successful later
recovery read can restore coverage. Redaction in storage credentials, properties
or other fields outside the model is permitted. Redaction of a UUID, snapshot
descriptor or modeled reference makes that evidence unusable.

A changed UUID ends the original table's observation window. Search uses only
records before the invocation that first observes a different UUID; commits
crossing that boundary remain pending in the prefix even if they later finish.
Later table incarnations cannot create findings against the old state machine.
The whole run stays inconclusive unless the earlier prefix already proves a
violation. Finding evidence IDs name only that checked prefix; raw commit
outcomes/counts still describe the full recorded run.

The adapter uses the recorder's call/return **event order**. This preserves
real-time precedence even when monotonic timestamps tie, while overlapping
requests may be linearized in either order. A multi-table transaction is
refused, never split into single-table histories.

## Unknown outcomes

Timeouts, incomplete responses, 5xx and missing commit completions preserve
their original `info` outcome. Their possible execution intervals extend
through the end of the recorded history. Porcupine explores either no effect
or the complete atomic update at any point after invocation; it does not force
publication to precede a client's timeout. This permits a read of old metadata
after a timeout followed by the newly committed snapshot in a later read.

An exact complete observation of an attempt's uniquely identified snapshot at
main can resolve it as committed. The result retains its raw outcome and lists
supporting operation IDs. A historical descriptor alone does not resolve the
main-ref update. Absence never proves rejection: an unresolved unknown makes
an otherwise legal history inconclusive, including at the end of a read-back
budget. A lack of any legal ordering is invalid even with unresolved peers.

These are model-relative findings, requiring the evidence assumptions above and
spec/catalog triage before an upstream bug claim. A search timeout is not a
pass, and a successful bounded run is not a proof for all executions.

## Validation and scope

The correct fake catalog runs two rounds of four concurrent CAS attempts with
one winner each. Planted modes cover accepted stale guards, lost acknowledged
updates, partial publication, rejected effects, and stale reads. Additional
histories cover two overlapping winners, forgotten ancestors, real-time ordering
versus overlap, large IDs, implicit main, sequence gaps, recovered reads, scoped
credential redaction, table-identity boundaries, malformed evidence, and a search
timeout. A real HTTP test lets server work outlive a client timeout
and an early final GET, then records the later complete effect. Offline replay
continues after all fake servers stop, without resending commits.

Try the **synthetic** examples (these are not live-catalog findings):

```sh
go run ./cmd/calve check-table internal/checker/testdata/snapshot-valid.jsonl
go run ./cmd/calve check-table internal/checker/testdata/snapshot-invalid.jsonl
go run ./cmd/calve check-table internal/checker/testdata/snapshot-inconclusive.jsonl
```

This issue implements the checker and its evidence contract. The existing
`append` and `run` commands still write properties; they do not emit this
snapshot history. Generating valid manifests/data files and a live snapshot
workload against Compose catalogs remains separate workload work. There is no
claim of actual data-file correctness from metadata-only fake tests.

Other branches/tags, expiration, rollback, schema changes, v1/v3 metadata,
property updates, multi-table transactions and fault-controller histories are
outside this model. Unsupported operations are errors, not silently projected
away. This checker does not implement the schema workload in #20 or replace
the multi-table checker in #19.

Sources: the pinned Iceberg [single-table concurrency and sequence rules](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L98),
[snapshot fields](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L742-L765),
[table metadata and implicit main](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L926-L956),
[REST commit and update schemas](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml),
and [Porcupine's model and event API at v1.3.1](https://github.com/anishathalye/porcupine/blob/v1.3.1/model.go).
The pinned Java [metadata builder's sequence validation](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1232-L1261)
permits sequence-number gaps.

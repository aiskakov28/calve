# Multi-table invariant checks (issue #19)

The write-skew checker already checks the opposing-guard invariant, including
the sequential stale-requirement controls. Each of those transactions updates
only one table. The atomic workload and checker cover transactions
that update two or more tables.

Each transaction sets one unique property marker to `1` on every table, guarded
by the baseline table UUIDs. Requests run once, without retries. Fresh tables,
recorded baselines, exact requests/outcomes, and ordered metadata observations
make each effect attributable. No operation removes or overwrites a marker.

Check acknowledged effects for loss, rejected effects for forbidden publication,
and unknown outcomes for complete positive effects across all target tables.
Absent unknown effects stay unresolved. Reads of old A followed by new B can
straddle a correct atomic commit and must not be reported as a partial commit.
A positive observation completed before a later missing-effect read can support
an atomic-visibility lead, assuming fresh table loads. Keep that model-dependent
claim separate from acknowledged-loss/rejected-effect contract candidates.

Failed reads are not empty tables. Later usable recovery observations may restore
coverage. A changed UUID ends the trusted observation window; preserve earlier
independent evidence without modeling a new table incarnation as the old one.
Save evidence on errors, and make replay entirely offline.

Implementation includes the live `calve atomic` workload, offline checking, a
correct fake catalog, planted lost/rejected/partial multi-table effects, and a
real timeout test whose separate table reads straddle a late atomic commit.
The existing skew checker remains in place. General G0/G1c/G2 cycle search,
data-file/snapshot generation, and fault-campaign wiring remain separate work.

The normative baseline is the [Iceberg 1.10.1 REST transaction endpoint](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441).
Its atomic-operation promise does not establish general multi-table serializable
isolation or freshness for arbitrary catalog caches. Findings still need triage.

## Run and replay

```sh
calve atomic --catalog http://localhost:8182/catalog --warehouse calve \
  --tables 2 --clients 4 --per-client 10 --readers 1 --read-interval 10ms \
  --out history-atomic.jsonl
calve check history-atomic.jsonl
```

Tables default to 2 (range 2..32), clients to 4, and attempts per client to 10.
An acknowledged transaction need not exclude another transaction: their property
keys differ, and the UUID guards do not change. Conflicts are allowed and do not
impose a progress guarantee. A run with no proven committed transaction remains
inconclusive for lack of successful coverage.

The CLI defaults to one concurrent reader. Each reader loads every table in
order, then pauses for `--read-interval` (default 10 ms). `--readers 0` disables
workload reads; enabled readers require a positive interval. Readers share the
writers' start gate, then are canceled and joined before final observations.
An interrupted GET stays unknown. Recorded request intervals show actual
reader/writer overlap; separate loads are never one cross-table snapshot.

Output counts workload GET attempts and usable GETs overlapping recorded
transaction-call intervals. A usable read has successful, matching-UUID metadata
inside the trusted evidence window. With readers enabled, no such overlap leaves
the run inconclusive; a catalog finding still makes it invalid. Reads before
transactions, after they finish, or in recovery do not supply concurrent coverage.
Recovery still observes unresolved commits and unavailable final state, but skips
extra GETs when missing concurrent coverage is the only remaining limitation.

The Go API enables readers with `AtomicConfig.Readers` and `ReadInterval`;
zero readers preserves post-run-only sampling for existing callers. The run
manifest records reader count and interval. Older version-1 histories remain
readable.

Exit status is 0 valid, 3 invalid, 4 inconclusive, and 1 on execution/history
errors. Both commands print raw outcome counts separately from positive unknown
resolutions and unresolved states. Run-directory `calve check` and snapshot
`calve check-table` continue to work as before.

The workload uses random run/table names, creates each table without dropping
anything, and records config, namespace/table creation, baseline loads, concurrent
transaction attempts, concurrent workload reads, and final loads. Tables remain
available for inspection.
Setup, cancellation, and save errors retain the available history. A new run
requires a new recorder and exclusive ownership of its tables.

Invalid workload/read-back arguments are rejected before writing history, so
they preserve an existing evidence file. Setup diagnostics redact the configured
token before truncating response bodies, as does the saved HTTP evidence.

Both checker APIs and CLI replay read the exact version-1 HTTP bytes. The atomic
run manifest names the tables, catalog/warehouse, namespace, clients, and attempts.
Every transaction labels its process/attempt marker; its request must update
every listed table with exactly that property and baseline UUID guard. The
checker rejects unsupported operations, duplicate markers, incomplete/mismatched
requests, changed routing, and inconsistent labels/statuses. No summary can
override request or observation evidence.

## Findings and outcome rules

| Kind | Basis | Required evidence |
| --- | --- | --- |
| `lost` | `catalog-contract` | acknowledgment, or complete positive effects, followed by a matching-UUID load missing an expected effect |
| `phantom-failed` | `catalog-contract` | a load started after definite rejection observes any value of the attempt's unique property |
| `unexpected` | `catalog-contract` | a run-scoped marker with no preceding or overlapping recorded attempt |
| `single-table-regression` | `fresh-read-model` | an exact positive effect observed before a later load of the same table starts and lacks that effect |
| `non-atomic-visibility` | `atomic-visibility-model` | an exact positive effect observed before a later missing-effect load of a different table starts, including while an attempt is pending |

The last two findings are model-dependent leads assuming fresh table loads.
Prefer a same-table regression when both witness types exist for an attempt.
Stronger `lost` and `phantom-failed` evidence retains precedence. The checker does
not require a blanket serializable isolation level or treat separate loads as
one snapshot. An old-A/new-B pair and overlapping read intervals cannot alone
prove a visibility violation.

Visibility is checked even if the attempt later succeeds or rejects. Effects
observed only before rejection are model evidence; they do not establish that
effects persisted after the rejection. A definite rejected outcome stays
rejected, with forbidden observations reported separately.

An unknown or missing completion stays `info`. Exact `1` values on every
original table can prove commitment, retaining the source outcome and supporting
observation IDs. Absence, a partial effect, a failed read, or expiration of the
read-back budget never proves rejection. A lost finding about a positively
resolved unknown includes both the complete-effect proofs and the later negative
observation IDs.

No request removes or overwrites another attempt's property, so unresolved peers
cannot excuse a lost acknowledged marker. Positive/rejected/unexpected findings
remain evidence after later reads fail. Only observations completed before the
earliest changed-UUID observation's invocation support findings; attempts whose
outcomes cross that boundary remain uncertain in the checked prefix.

Incomplete setup/baselines and missing planned attempts prevent a pass.
Failed final reads supply no empty-table evidence; later usable recovery reads
can restore coverage. The CLI uses the existing independent read-back budget
(default 5 seconds, interval 100 ms; `--readback-timeout 0` disables it), repeating
only table GETs using their recorded negotiated routing. It never resubmits a
transaction or treats a deadline as a server completion barrier.

## Validation and current coverage

Tests use an independent correct fake plus planted lost, rejected, partial, and
unknown effects. A real client timeout test holds server work past the first
final load, publishes the whole transaction before the second load, and verifies
that recovery resolves commitment without a false partial-commit report. Tests
also cover missing/incomplete completions, wrong request shapes, unavailable
observations, UUID changes, unknown peers beside definite loss, evidence IDs,
credential redaction, one attempt per marker, subprocess exit codes, and offline
replay after the fake servers stop.

A controlled HTTP frontend publishes the first table, waits for a concurrent
reader to see the second table's old state, then publishes the second before
acknowledging. This exercises the actual reader workload and detects visibility
evidence even with complete final state. Reader shutdown tests cover blocked
GETs, parent cancellation, and long pacing waits. Same-table tests keep old/new
and overlapping reads legal while classifying an ordered new/missing pair as
a single-table regression.

Initial live runs of 40 two-table transactions passed on Lakekeeper v0.14.2,
Nessie 0.109.0, and Polaris 1.8.0, with the same verdicts replayed with networking
disabled. The reference fixture produced a repeatable
[missing-marker lead](triage-reference-atomic.md). This records observed
executions, not correctness for all schedules.

CI runs a small workload and matching offline replay on Lakekeeper, Nessie, and
Polaris. A finding or harness error fails; an unresolved unknown may remain
inconclusive and its history is uploaded. CI requires at least one acknowledged
or positively resolved commit; all-rejected and wholly unresolved runs do not
supply commit coverage. It also requires a usable concurrent reader observation
on each catalog. Existing reference/skew gates remain
in place. General G0/G1c/G2 graph analysis remains deferred; this issue implements
the workload-specific checks first.

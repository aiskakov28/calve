# Multi-table invariant checks (issue #19)

The write-skew checker already checks the opposing-guard invariant, including
the sequential stale-requirement controls. Each of those transactions updates
only one table. This draft adds a separate workload and checker for transactions
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

Implementation includes a live `calve atomic` workload, offline checking, a
correct fake catalog, planted lost/rejected/partial multi-table effects, and a
real timeout test whose separate table reads straddle a late atomic commit.
The existing skew checker remains in place. General G0/G1c/G2 cycle search,
data-file/snapshot generation, and fault-campaign wiring remain separate work.

The normative baseline is the [Iceberg 1.10.1 REST transaction endpoint](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441).
Its atomic-operation promise does not establish general multi-table serializable
isolation or freshness for arbitrary catalog caches. Findings still need triage.

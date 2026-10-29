# Single-table snapshot linearizability (issue #18)

This draft defines the scope of the Porcupine checker before implementation.

The sequential state is one table's UUID, current main snapshot, last sequence
number, and retained snapshot metadata. Supported commits atomically add one
snapshot and move the main branch to it, guarded by the table UUID and main's
previous snapshot ID. Reads observe that same state. Multi-table transactions,
other branches/tags, expiration, schema changes, and property appends are outside
this model and must not be silently projected into it.

The checker will read version-1 HTTP histories with an explicit snapshot model
manifest and an established baseline, pair requests and outcomes, and preserve
real-time ordering. It will check successful commit response metadata as well as
GET observations. Rejections are no-ops; the model does not require a rejected
attempt to have been able to succeed.

An unknown or missing commit completion can either apply atomically or have no
effect. Its possible execution interval extends through the end of the recorded
history, because the client timeout does not stop server work. Positive complete
observations may establish commitment, but absence cannot establish rejection.
Unresolved outcomes and search exhaustion are inconclusive, never an unqualified
pass. A model violation remains a finding even alongside unknown operations.

Implementation includes an offline CLI entry point, a pinned Porcupine dependency,
and tests against a correct fake catalog and planted snapshot faults. Tests must
cover real-time precedence, overlapping CAS attempts, rejected effects, lost or
partial effects, and a commit that applies after its client timeout. Saved test
histories must re-check after the fake server stops.

This issue implements a checker and its evidence contract. The current append
workload writes properties; generating valid snapshot manifests/data files and
running a live snapshot workload against the Compose catalogs is separate work.
No snapshot or data-file coverage is claimed for current property campaigns.

Sources: the [single-table concurrency model](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L90),
the [pinned REST spec](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml),
and [Porcupine's executable models and history API](https://github.com/anishathalye/porcupine).

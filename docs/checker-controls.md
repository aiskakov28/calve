# Checker controls

Every checker has a correct `internal/fakecatalog` control that must return
`valid` without findings, and faulty server modes that must produce the expected
finding. These tests exercise real HTTP requests, serialize their evidence
through JSONL, and check it offline after the fake server stops.
The planted controls leave responses, recorded outcomes and event ordering
intact. Separate tests that mutate evidence cover malformed histories and
additional observation orderings; they are not counted as planted server bugs.

## Coverage matrix

| Checker | Planted modes and expected findings | HTTP control tests |
| --- | --- | --- |
| Property append | `LoseEvery` → `lost`; `RejectAppliedProperties` → `phantom-failed`; `UnexpectedProperties` → `unexpected` | [`TestPlantedPropertyAppendControls`](../internal/checker/planted_test.go) includes the correct server and applied/unapplied unknowns. |
| Two-table write skew | `WriteSkew` → `write-skew`; `IgnoreReadOnlyRequirements` → `stale-requirement` and `write-skew`; `LoseTransactions` → `lost`; `RejectAppliedTransactions` → `phantom-failed` | [`TestCheckSkewCorrectAndPlantedCatalogs`](../internal/checker/skew_test.go) includes correct and unknown controls; its validation hook coordinates the race. |
| Multi-table markers | `LoseTransactions` or `PartialTransactions` → `lost`; `RejectAppliedTransactions` → `phantom-failed`; `UnknownAppliedTransactions` + `PartialTransactions` → `non-atomic-visibility`; `UnexpectedProperties` → `unexpected`; `ForgetTransactionProperties` + `UnknownAppliedTransactions` → `single-table-regression` | [`TestAtomicCheckerPlantedEffects`](../internal/checker/atomic_test.go) includes correct and unknown controls; [`TestPlantedAtomicUnexpectedProperties` and `TestPlantedAtomicPropertyRegression`](../internal/checker/planted_test.go) cover the extra marker and same-table disappearance. |
| Snapshot CAS | `LoseEvery`, `PartialSnapshots`, `RejectAppliedSnapshots`, `StaleSnapshotReads`, `UnexpectedSnapshots` or `IgnoreSnapshotRequirements` → `non-linearizable` | [`TestSnapshotCheckerAgainstFakeCatalog`](../internal/checker/snapshot_test.go) includes correct, applied-unknown and absent-unknown controls. The checker searches for a legal sequential explanation rather than assigning each server fault a separate anomaly kind. |
| Schema CAS | `IgnoreSchemaRequirements` or `LoseEvery` → `multiple-winners`; `RejectAppliedSchemas` → `phantom-failed`; `StaleSchemaReads` → `lost`; `UnexpectedSchemaFields` → `unexpected` | [`TestSchemaCheckerCorrectCatalog`, `TestSchemaCheckerPlantedBugs`, `TestSchemaCheckerPlantedMissingAndUnexpectedEffects` and `TestSchemaCheckerResolvesUnknownsOnlyFromEffects`](../internal/checker/schema_test.go) cover correct, rejected, missing, unexpected and positively resolved unknown effects. |
| Maintenance race | `ExpireFromStaleMetadata` → `lost-snapshot` and `main-regressed`; `ExpireRemovesHead` → `lost-snapshot` and `dangling-main`; `RejectAppliedExpiration` or `RejectAppliedSnapshots` → `phantom-failed`; `UnexpectedSnapshots` → `unexpected` | [`TestMaintenanceCheckerCorrectCatalog` and `TestMaintenanceCheckerPlantedBugs`](../internal/checker/maintenance_controls_test.go) run the same forced race with correct and faulty servers under both models; [`TestMaintenanceCheckerUnknownAppends`](../internal/checker/maintenance_test.go) covers unknowns. |

The maintenance controls hold an expiration after its guard check, acknowledge a
new append, publish the expiration, then record a GET before another append can
hide the result. Channels establish that order; retrying until a lucky race
occurs is not required. The correct server removes only the requested old
snapshots from the latest metadata. The stale-metadata plant instead publishes
from the metadata it read before the racing append.

The correct expiration path reloads the live table after the hook and rechecks
its UUID guard. [`TestExpirationRechecksTableAfterHook`](../internal/fakecatalog/expiration_test.go)
covers concurrent property commits, multi-table transactions, deletion and
replacement of the target table with all planted bugs disabled.

## Interpretation and limits

- Findings keep their existing basis. `write-skew` is a lead under the
  serializable model; `non-atomic-visibility` and `single-table-regression`
  assume fresh table loads. Definite lost/rejected effects and violated guards
  are reported separately as catalog-contract findings.
- Unknown outcomes stay unknown in the recorded history. Complete positive
  evidence may resolve commitment; absent effects do not prove rejection.
  Applied and unapplied controls assert that distinction. A retained but
  non-current schema can prove an unknown committed while still exposing a
  missing-current-schema finding.
- Property maps cannot represent repeated occurrences of one key. The
  lower-level list-based `CheckAppend` has a synthetic duplicate test in
  [`TestCheckAppendAnomalies`](../internal/checker/append_test.go); the HTTP
  property-append workload does not claim duplicate detection.
- Snapshot CAS controls above use the retained-history model. The explicit
  current-snapshot profile ignores non-current descriptors. Maintenance tests
  also run its current-snapshot profile, which deliberately omits historical
  `lost-snapshot` checks. A rejected expiration that only removed old snapshots
  remains inconclusive under that profile, without an acknowledged race or a
  visible current-state violation.
- `valid` means the recorded history passed the selected model. These controls
  do not establish data-file correctness, availability or deadlock freedom.

Run the controls and the rest of the suite with:

```sh
go vet ./...
go test -race -count=1 ./...
```

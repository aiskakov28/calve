# Nessie rejects an omitted schema ID in the skew workload

Status: locally reproduced interoperability difference; Calve supplies explicit IDs as a compatibility fix. This is not an isolation finding or a confirmed upstream bug.

The original issue #8 workload at `95fb919` omitted `schema-id` inside the `add-schema` update. On fresh tables, the existing schema has ID 0. Against the pinned `ghcr.io/projectnessie/nessie:0.109.0` Compose service, every attempted transaction returned HTTP 400 with `IllegalArgumentException` and message `A schema with ID 0 already exists`. This reproduced the collaborator's report: 20 concurrent attempts and the first attempt in each control were rejected; the two stale control attempts were skipped because their first commits failed. These rounds supplied no useful isolation coverage.

The fix includes `schema-id: max(existing schema IDs) + 1` in each target table's added schema. It uses the full baseline `schemas` list, including inactive schemas, and preserves the original UUID/schema requirements, field metadata, `updates: []` on the other table, and `set-current-schema` with ID -1. Allocation errors stop the round before either transaction is sent. Iceberg's pinned [Java metadata builder](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651) similarly chooses an ID above the highest existing ID for a new schema; Calve's unique marker makes the schema new.

After the fix, a local run of ten concurrent rounds plus both controls on each pinned Compose catalog recorded:

| Catalog | Concurrent rounds | Sequential controls |
| --- | --- | --- |
| Reference fixture 1.10.1 | Both transactions acknowledged in 10/10 (HTTP 200) | First acknowledged, stale second rejected with 409 in both directions |
| Lakekeeper v0.14.2 | Exactly one acknowledgment and one 409 in 10/10 | First acknowledged, stale second rejected with 409 in both directions |
| Nessie 0.109.0 | Exactly one acknowledgment (204) and one 409 in 10/10 | First acknowledged, stale second rejected with 409 in both directions |

All ten concurrent pairs overlapped on each catalog, with no unknown responses. Subsequent offline replay with the issue #9 checker found ten isolation leads on reference and passing controls, and a full pass on Lakekeeper and fixed Nessie. The pre-fix Nessie history was inconclusive because the transactions were rejected with 400. The reference result remains an isolation lead under the model in [design.md](design.md#multi-table-commit-atomicity-and-its-limits).

Local artifacts (Git-ignored) are `results/skew-nessie-schema-id-before.jsonl` and `results/skew-{nessie,reference,lakekeeper}-schema-id-after.jsonl`. The before run used a test binary built from `95fb919`; the after runs used the fixed workload. Each artifact preserves the exact requests, responses, baselines, and final observations. The opt-in command in the README reproduces these runs with the respective endpoint and warehouse.

Before filing an upstream report, clarify the contract for an omitted `add-schema.schema.schema-id`: does the server assign an unused ID, or preserve the parser's default? The [REST schema definition](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml) and the Java client's behavior are starting points for that investigation. No upstream report has been filed.

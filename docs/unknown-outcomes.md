# Unknown commit outcomes (issue #16)

A timeout, connection reset, incomplete response, or HTTP 5xx leaves a commit's
outcome unknown. Preserve that original outcome in the history. Later table
observations provide additional evidence; they do not change a timeout into an
acknowledgment or a rejection.

Both checkers resolve commitment from attributable, complete effects. Append
requires the exact unique property value; skew requires the full expected current
schema on the original table UUID. Each resolution references recorded table
GETs and preserves the source outcome, including an invocation with no completion.
An earlier positive observation remains evidence if a later read fails or the
effect disappears; failed reads cannot invent absence-based findings.

The append and skew CLIs perform bounded read-back for unresolved commits using
`--readback-timeout` (default 5 seconds, 0 disables extra reads) and
`--readback-interval` (default 100 milliseconds, must be positive when enabled).
The budget is independent of workload cancellation and applies to GETs, never
commit resubmission. Recorded baseline URLs retain negotiated catalog prefixes.
Append run metadata records both settings; skew records `readback-config`
metadata before extra reads. Requests have phase `recovery`, with the original
round label for skew. These settings are provenance, not completion evidence.

`checker.CheckAppendHistory` and `checker.CheckSkew` replay the saved HTTP evidence
without a live catalog. Results separate raw unknown counts, resolved commitments,
and unresolved outcomes. An unresolved unknown prevents an unqualified `VALID`;
both commands return exit 4 when the result is inconclusive. An independently
proven violation retains `INVALID` even when other operations are unresolved.

Absence cannot establish noncommitment while a request could still apply. Calve
currently has no validated per-catalog drain/completion mechanism. Healthy
endpoints, healed controllers, fixed waits, and repeated identical reads are not
such a mechanism. Negative resolution stays unavailable until that contract is
established and tested.

Regression tests cover lost responses and 5xx after application, unapplied
requests, incomplete successful responses, missing completions, wrong table
identity/value/request evidence, and a real HTTP timeout whose server work is
held until after an early final read. Both append and skew histories are replayed
after their servers stop. Subprocess tests verify append's valid/inconclusive
exit codes. A separate test verifies that cancellation does not cancel the
bounded read-back request.

This implementation does not heal controllers or accept faulted campaign
histories. Existing fault-free catalog gates remain applicable. Wiring full
fault campaigns into `calve run`, reconciling late controller actions, and proving
per-catalog completion/freshness boundaries remain required integration work.

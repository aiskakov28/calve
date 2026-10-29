# Planted checker controls (#22)

This change audits the property-append, write-skew, multi-table marker,
snapshot CAS, schema CAS and maintenance checkers against `internal/fakecatalog`.

For each checker, record the supported findings and the fake-server mode that
exposes them. Fill gaps with reproducible HTTP histories and require a correct
server control with no false alarms. Preserve unknown outcomes and distinguish
unresolved evidence from a planted violation. Check saved evidence offline after
the fake server stops.

Implementation will build on the existing planted modes and tests. Add fake
behavior only where a supported finding lacks a live fake-server control;
document behavior that the current workloads cannot represent.

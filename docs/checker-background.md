# Background: Elle and Porcupine

Calve's checkers borrow from two existing tools.

**Elle.** Kingsbury and Alvaro's [paper][elle] checks transactional isolation by
inferring dependencies between transactions from what clients read, wrote and
were told. It finds aborted and intermediate reads and classifies dependency
cycles: G0 uses write-write edges, G1c adds write-read edges, and G2 includes
read-write anti-dependencies. Its strongest inference needs every observed
value to identify its writer (recoverability) and every read to expose version
history (traceability), as with lists of uniquely appended values. Missing
observations and undecided transactions can hide anomalies, so finding no
cycle doesn't prove serializability. Predicate anomalies are out of its scope.
See sections 2 to 4 and 6 of the paper.

**Porcupine.** [Porcupine][porcupine] is a Go linearizability checker. You give
it a sequential model of the system (`Init`, `Step`) and a concurrent history
of timed operations, and it searches for a legal sequential order that respects
real time: an operation that finished before another started must come first.
The search can blow up on large histories, and inaccurate timestamps can
produce false violations. A verdict only holds for the model and history it was
given.

## What Calve takes from them

- **Porcupine** checks the [snapshot workload](single-table-checker.md): guarded
  snapshot commits and reads against an explicit model, with unknown commits
  allowed to apply at any point after they were sent.
- **From Elle**, unique operation markers that tie every effect to one request,
  findings backed by recorded evidence, and short witnesses. The multi-table,
  schema and maintenance checkers check specific invariants, not general
  dependency cycles. General G0/G1c/G2 cycle detection isn't implemented.
- The append workload writes property maps, not lists, so it lacks Elle's
  traceability and can't see a key written twice.
- A multi-table transaction is never split into separate single-table
  histories.

[elle]: https://arxiv.org/html/2003.10554v1
[porcupine]: https://github.com/anishathalye/porcupine/blob/97cd067defd99df172053ba7f4ca5bbae57fe446/README.md

package checker

// Verdict describes whether recorded evidence proves a violation, establishes
// a complete pass, or leaves an operation/observation undecided.
type Verdict string

const (
	Valid        Verdict = "valid"
	Invalid      Verdict = "invalid"
	Inconclusive Verdict = "inconclusive"
)

package checker

import (
	"fmt"
	"testing"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
)

// Every history parser takes "ok" and "fail" only from statuses the REST spec
// documents as proving the call's effect. Any other status may be recorded,
// but only as unknown.
func TestParsersAcceptDefiniteOutcomesOnlyFromDocumentedStatuses(t *testing.T) {
	parsers := []struct {
		name  string
		ops   func(*testing.T) []history.Op
		check func([]history.Op) error
	}{
		{"append", func(t *testing.T) []history.Op { return propertyControlHistory(t, fakecatalog.Bugs{}) },
			func(ops []history.Op) error { _, err := CheckAppendHistory(ops); return err }},
		{"atomic", func(t *testing.T) []history.Op { return atomicHistory(t, fakecatalog.Bugs{}, 2, 1) },
			func(ops []history.Op) error { _, err := CheckAtomic(ops); return err }},
		{"schema", func(t *testing.T) []history.Op { return schemaHistory(t, fakecatalog.Bugs{}, 2, 1) },
			func(ops []history.Op) error { _, err := CheckSchema(ops); return err }},
		{"maintenance", func(t *testing.T) []history.Op { return maintenanceHistory(t, fakecatalog.Bugs{}, 2) },
			func(ops []history.Op) error { _, err := CheckMaintenance(ops); return err }},
	}
	type change struct {
		outcome history.Type
		status  int
	}
	cases := []struct {
		change
		accepted bool
	}{
		{change{history.OK, 201}, false},
		{change{history.OK, 202}, false},
		{change{history.Fail, 408}, false},
		{change{history.Fail, 418}, false},
		{change{history.Fail, 422}, false},
		{change{history.Fail, 429}, false},
		{change{history.Fail, 500}, false},
		{change{history.Info, 202}, true},
		{change{history.Info, 408}, true},
		{change{history.Info, 429}, true},
	}
	for _, p := range parsers {
		t.Run(p.name, func(t *testing.T) {
			base := p.ops(t)
			if err := p.check(base); err != nil {
				t.Fatalf("unchanged history rejected: %v", err)
			}
			// Rewrite one workload client's acknowledged call.
			target := -1
			for i, op := range base {
				if op.Type == history.OK && op.Process >= 0 {
					target = i
					break
				}
			}
			if target < 0 {
				t.Fatal("no acknowledged workload call")
			}
			for _, c := range cases {
				t.Run(fmt.Sprintf("%s-%d", c.outcome, c.status), func(t *testing.T) {
					ops := append([]history.Op(nil), base...)
					op := &ops[target]
					op.Type, op.Status = c.outcome, c.status
					op.Response = &history.Response{Status: c.status, Headers: op.Response.Headers, Body: op.Response.Body}
					if c.outcome != history.OK {
						op.Error = fmt.Sprintf("HTTP %d", c.status)
					}
					if err := p.check(ops); (err == nil) != c.accepted {
						t.Fatalf("accepted=%t, error %v", err == nil, err)
					}
				})
			}
		})
	}
}

package checker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

const atomicPrefix = "calve.atomic."

type atomicManifest struct {
	RunID          string            `json:"run_id"`
	Workload       string            `json:"workload"`
	Catalog        string            `json:"catalog"`
	Warehouse      string            `json:"warehouse"`
	Namespace      string            `json:"namespace"`
	Tables         []rest.Identifier `json:"tables"`
	Clients        int               `json:"clients"`
	PerClient      int               `json:"per_client"`
	Readers        int               `json:"readers,omitempty"`
	ReadIntervalNS int64             `json:"read_interval_ns,omitempty"`
	// Set when the run is part of a fault campaign (calve run): PerClient then
	// only caps each client, and the run ends at the plan's horizon.
	FaultRunID   string `json:"fault_run_id,omitempty"`
	FaultSeed    int64  `json:"fault_seed,omitempty"`
	FaultPlan    string `json:"fault_plan,omitempty"`
	FaultHistory string `json:"fault_history,omitempty"`
}

type atomicCall struct {
	invoke history.Op
	done   *history.Op
	table  string
	key    string
}

type atomicView struct {
	call  *atomicCall
	uuid  string
	props map[string]string
}

func atomicJSON(raw []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func parseAtomicHistory(ops []history.Op) (m atomicManifest, calls []*atomicCall, err error) {
	bad := func(message string) (atomicManifest, []*atomicCall, error) {
		return m, nil, fmt.Errorf("atomic history: %s", message)
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" ||
		atomicJSON(ops[0].Value, &m) != nil || m.Workload != "atomic" || m.Namespace == "" ||
		!strings.HasPrefix(m.RunID, "atomic_") || m.Clients <= 0 || m.PerClient <= 0 ||
		m.Clients > int(^uint(0)>>1)/m.PerClient || len(m.Tables) < 2 || len(m.Tables) > 32 {
		return bad("missing or invalid atomic run manifest")
	}
	if m.Readers < 0 || m.Readers > int(^uint(0)>>1)-m.Clients || m.ReadIntervalNS < 0 || m.Readers > 0 && m.ReadIntervalNS == 0 {
		return bad("invalid atomic reader settings")
	}
	for _, level := range strings.Split(m.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return bad("empty namespace level")
		}
	}
	tables := map[string]bool{}
	for i, table := range m.Tables {
		if table.Name != fmt.Sprintf("%s_t%d", m.RunID, i) || !reflect.DeepEqual(table.Namespace, strings.Split(m.Namespace, ".")) {
			return bad("table list does not describe fresh run tables")
		}
		tables[skewTableKey(table)] = true
	}
	origin, e := url.Parse(m.Catalog)
	if e != nil || origin.Host == "" || origin.Scheme != "http" && origin.Scheme != "https" ||
		origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || strings.Contains(m.Catalog, "[REDACTED]") {
		return bad("invalid catalog root")
	}
	byID := map[int]*atomicCall{}
	for i, op := range ops {
		if op.Version != history.Version || op.Index != i || op.Time < 0 || i > 0 && op.Time < ops[i-1].Time {
			return bad("invalid record version, index or clock order")
		}
		if i == 0 {
			continue
		}
		if op.Type == history.Meta {
			if op.F != "readback-config" || op.Phase != "recovery" || op.Request != nil || op.Response != nil {
				return bad("unsupported metadata")
			}
			continue // an observation budget does not establish completion
		}
		if op.ID <= 0 || op.Round != "" {
			return bad("missing operation ID or unsupported round")
		}
		if op.Type == history.Invoke {
			if byID[op.ID] != nil || op.Request == nil || op.Response != nil {
				return bad("duplicate invocation or missing request")
			}
			u, e := url.Parse(op.Request.URL)
			if e != nil || u.Scheme != origin.Scheme || u.Host != origin.Host || u.User != nil || u.Fragment != "" ||
				!strings.HasPrefix(u.EscapedPath(), strings.TrimRight(origin.EscapedPath(), "/")+"/v1/") ||
				strings.Contains(op.Request.URL, "[REDACTED]") {
				return bad("request targets a different catalog or has redacted routing")
			}
			call := &atomicCall{invoke: op}
			method := http.MethodPost
			switch op.F {
			case "config":
				method = http.MethodGet
				query := ""
				if m.Warehouse != "" {
					query = "warehouse=" + url.QueryEscape(m.Warehouse)
				}
				if u.EscapedPath() != strings.TrimRight(origin.EscapedPath(), "/")+"/v1/config" || u.RawQuery != query {
					return bad("config request disagrees with run settings")
				}
				fallthrough
			case "create-namespace", "create-table":
				if op.Phase != "setup" || op.Process != -1 || op.Value != nil {
					return bad("invalid setup labels")
				}
			case "load-table":
				method = http.MethodGet
				if op.Process != -1 || op.Value != nil ||
					op.Phase != "baseline" && op.Phase != "workload" && op.Phase != "final" && op.Phase != "recovery" {
					return bad("invalid observation labels")
				}
				key, e := skewReadKey(op.Request.URL)
				if e != nil || !tables[key] {
					return bad("observation targets an unplanned table")
				}
				call.table = key
			case "atomic-txn":
				if op.Phase != "workload" || op.Process < 0 || op.Process >= m.Clients ||
					!strings.HasSuffix(u.EscapedPath(), "/transactions/commit") {
					return bad("invalid transaction labels or endpoint")
				}
				var label struct {
					Key string `json:"key"`
				}
				if atomicJSON(op.Value, &label) != nil {
					return bad("missing transaction key label")
				}
				prefix := atomicPrefix + m.RunID + "."
				suffix := strings.TrimPrefix(label.Key, prefix)
				parts := strings.Split(suffix, "-")
				if suffix == label.Key || len(parts) != 2 {
					return bad("key is outside the run's marker space")
				}
				client, e1 := strconv.Atoi(parts[0])
				attempt, e2 := strconv.Atoi(parts[1])
				if e1 != nil || e2 != nil || client != op.Process || attempt < 0 || attempt >= m.PerClient ||
					suffix != fmt.Sprintf("%d-%d", client, attempt) {
					return bad("key does not identify a planned client attempt")
				}
				call.key = label.Key
			default:
				return bad("operation is outside the closed atomic workload")
			}
			if op.Request.Method != method || op.F != "config" && u.RawQuery != "" ||
				method == http.MethodGet && len(op.Request.Body) != 0 {
				return bad("unexpected HTTP method, query or GET body")
			}
			calls, byID[op.ID] = append(calls, call), call
			continue
		}
		call := byID[op.ID]
		if call == nil || call.done != nil {
			return bad("completion without a unique invocation")
		}
		inv := call.invoke
		if op.F != inv.F || op.Phase != inv.Phase || op.Process != inv.Process || !bytes.Equal(op.Value, inv.Value) ||
			op.Request != nil || op.Status == 0 && op.Response != nil ||
			op.Status != 0 && (op.Response == nil || op.Response.Status != op.Status) {
			return bad("completion labels or HTTP evidence disagree")
		}
		switch op.Type {
		case history.OK:
			allowed := op.Status == 200
			if inv.F == "atomic-txn" {
				allowed = allowed || op.Status == 204
			}
			if inv.F == "create-table" || inv.F == "create-namespace" {
				allowed = allowed || op.Status == 201
			}
			if !allowed || op.Error != "" {
				return bad("unsupported successful response")
			}
		case history.Fail:
			if op.Status < 400 || op.Status >= 500 {
				return bad("rejection requires a 4xx response")
			}
		case history.Info:
			if op.Status != 0 && op.Status < 500 && op.Error == "" {
				return bad("unknown without uncertain HTTP evidence")
			}
		default:
			return bad("invalid completion type")
		}
		copy := op
		call.done = &copy
	}
	return m, calls, nil
}

func atomicMetadata(call *atomicCall, prefix string) (atomicView, error) {
	view := atomicView{call: call, props: map[string]string{}}
	if call == nil || call.done == nil || call.done.Type != history.OK {
		return view, fmt.Errorf("successful observation unavailable")
	}
	var body struct {
		Metadata struct {
			UUID       string                     `json:"table-uuid"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(call.done.Response.Body, &body); err != nil {
		return view, err
	}
	view.uuid = body.Metadata.UUID
	if view.uuid == "" || strings.Contains(view.uuid, "[REDACTED]") {
		view.uuid = ""
		return view, fmt.Errorf("usable table UUID unavailable")
	}
	for key, raw := range body.Metadata.Properties {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil || strings.Contains(value, "[REDACTED]") || strings.Contains(key, "[REDACTED]") {
			return view, fmt.Errorf("modeled property is malformed or redacted")
		}
		view.props[key] = value
	}
	return view, nil
}

func validateAtomicRequest(call *atomicCall, m atomicManifest, uuids map[string]string, route string) error {
	var body struct {
		Changes []struct {
			Identifier   *rest.Identifier `json:"identifier"`
			Requirements []struct {
				Type string `json:"type"`
				UUID string `json:"uuid"`
			} `json:"requirements"`
			Updates []struct {
				Action     string            `json:"action"`
				Properties map[string]string `json:"updates"`
			} `json:"updates"`
		} `json:"table-changes"`
	}
	u, _ := url.Parse(call.invoke.Request.URL)
	bad := func() error {
		return fmt.Errorf("atomic transaction %d: request is outside the all-table unique-marker model", call.invoke.ID)
	}
	if u.EscapedPath() != route+"/transactions/commit" ||
		bytes.Contains(call.invoke.Request.Body, []byte("[REDACTED]")) ||
		atomicJSON(call.invoke.Request.Body, &body) != nil || len(body.Changes) != len(m.Tables) {
		return bad()
	}
	seen := map[string]bool{}
	for _, change := range body.Changes {
		if change.Identifier == nil {
			return bad()
		}
		table := skewTableKey(*change.Identifier)
		if uuids[table] == "" || seen[table] || len(change.Requirements) != 1 ||
			change.Requirements[0].Type != "assert-table-uuid" || change.Requirements[0].UUID != uuids[table] ||
			len(change.Updates) != 1 || change.Updates[0].Action != "set-properties" ||
			len(change.Updates[0].Properties) != 1 || change.Updates[0].Properties[call.key] != "1" {
			return bad()
		}
		seen[table] = true
	}
	return nil
}

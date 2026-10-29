package checker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/anishathalye/porcupine"
)

// SnapshotModelVersion identifies the supported, deliberately small REST model.
const SnapshotModelVersion = "calve-snapshot-v1"

// SnapshotManifest starts a history taken under exclusive ownership of one
// table. Setup is completed before recording; baseline is the first REST call.
// Commits is the number of planned single-attempt snapshot CAS requests.
type SnapshotManifest struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"` // snapshot-cas
	Model     string `json:"model"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
	Commits   int    `json:"commits"`
}

// Keep int64 fields typed throughout: converting snapshot IDs through float64
// would merge distinct IDs above 2^53.
type snapshotRecord struct {
	ID       *int64            `json:"snapshot-id"`
	Parent   *int64            `json:"parent-snapshot-id,omitempty"`
	Sequence *int64            `json:"sequence-number"`
	Time     *int64            `json:"timestamp-ms"`
	Manifest string            `json:"manifest-list"`
	Schema   *int              `json:"schema-id,omitempty"`
	Summary  map[string]string `json:"summary"`
}

type snapshotRef struct {
	ID   *int64 `json:"snapshot-id"`
	Type string `json:"type"`
}

// The modeled projection includes the retained snapshot descriptors, not just
// the head. This makes partial publication and forgotten ancestors observable.
type snapshotState struct {
	UUID      string
	Sequence  int64
	Current   *int64
	Main      *snapshotRef
	Snapshots map[int64]string // canonical complete snapshot descriptors
}

type snapshotInput struct {
	read     bool
	uuid     string
	expected *int64
	added    snapshotRecord
	encoded  string
}

type snapshotOutput struct {
	outcome history.Type
	view    *snapshotState
}

func snapshotModel(initial snapshotState) porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init:  func() []any { return []any{initial} },
		Equal: reflect.DeepEqual,
		Step: func(state, input, output any) []any {
			s, in, out := state.(snapshotState), input.(snapshotInput), output.(snapshotOutput)
			if in.read {
				if out.view != nil && reflect.DeepEqual(s, *out.view) {
					return []any{s}
				}
				return nil
			}
			if out.outcome == history.Fail {
				// Rejections make no changes. Internal contention may reject a
				// request even when its guard fits this abstract state.
				return []any{s}
			}
			var next []any
			if out.outcome == history.Info {
				next = append(next, s) // may never have applied
			}
			if in.uuid != s.UUID || !sameSnapshotID(in.expected, s.Current) ||
				s.Sequence == int64(^uint64(0)>>1) || *in.added.Sequence != s.Sequence+1 {
				return next
			}
			if _, exists := s.Snapshots[*in.added.ID]; exists {
				return next
			}
			updated := s // Step must not mutate any earlier search state.
			updated.Snapshots = make(map[int64]string, len(s.Snapshots)+1)
			for id, descriptor := range s.Snapshots {
				updated.Snapshots[id] = descriptor
			}
			updated.Snapshots[*in.added.ID] = in.encoded
			updated.Sequence = *in.added.Sequence
			updated.Current = in.added.ID
			updated.Main = &snapshotRef{ID: in.added.ID, Type: "branch"}
			if out.view == nil || reflect.DeepEqual(updated, *out.view) {
				next = append(next, updated)
			}
			return next
		},
	}
	return nm.ToModel()
}

func sameSnapshotID(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// Strict decoding is used for requests and snapshot descriptors; ignoring an
// unmodeled update or requirement would change the meaning of the experiment.
func snapshotJSON(raw []byte, target any) error {
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

func parseSnapshot(raw json.RawMessage) (snapshotRecord, string, error) {
	var s snapshotRecord
	if err := snapshotJSON(raw, &s); err != nil {
		return s, "", err
	}
	if s.ID == nil || *s.ID < 0 || s.Sequence == nil || *s.Sequence < 0 ||
		s.Time == nil || *s.Time < 0 || s.Manifest == "" || s.Summary["operation"] == "" ||
		s.Parent != nil && (*s.Parent < 0 || *s.Parent == *s.ID) {
		return s, "", fmt.Errorf("incomplete snapshot descriptor")
	}
	encoded, err := json.Marshal(s)
	return s, string(encoded), err
}

func snapshotMetadata(raw []byte) (snapshotState, error) {
	// Other table metadata (schemas, properties, specs) is outside the snapshot
	// projection. Supported requests cannot change it.
	var body struct {
		Metadata struct {
			Format    int                        `json:"format-version"`
			UUID      string                     `json:"table-uuid"`
			Sequence  *int64                     `json:"last-sequence-number"`
			Current   *int64                     `json:"current-snapshot-id"`
			Refs      map[string]json.RawMessage `json:"refs"`
			Snapshots []json.RawMessage          `json:"snapshots"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return snapshotState{}, err
	}
	m := body.Metadata
	if m.Format != 2 || m.UUID == "" || m.Sequence == nil || *m.Sequence < 0 {
		return snapshotState{}, fmt.Errorf("requires v2 metadata, UUID and last-sequence-number")
	}
	s := snapshotState{UUID: m.UUID, Sequence: *m.Sequence, Current: m.Current, Snapshots: map[int64]string{}}
	// -1 and missing/null are the spec's empty-table representations.
	if s.Current != nil && *s.Current == -1 {
		s.Current = nil
	}
	for name, raw := range m.Refs {
		if name != "main" {
			return s, fmt.Errorf("branches/tags other than main are outside this model")
		}
		var ref snapshotRef
		// Retention defaults on metadata references do not affect snapshot CAS.
		if err := json.Unmarshal(raw, &ref); err != nil || ref.ID == nil {
			return s, fmt.Errorf("incomplete main reference")
		}
		s.Main = &ref
	}
	// The table spec defines an implicit main reference when refs is absent
	// or null. Treat an empty map the same way rather than inventing a stale
	// reference violation from an optional metadata representation.
	if len(m.Refs) == 0 && s.Current != nil {
		s.Main = &snapshotRef{ID: s.Current, Type: "branch"}
	}
	for _, raw := range m.Snapshots {
		record, encoded, err := parseSnapshot(raw)
		if err != nil {
			return s, err
		}
		if _, exists := s.Snapshots[*record.ID]; exists {
			return s, fmt.Errorf("duplicate snapshot ID")
		}
		s.Snapshots[*record.ID] = encoded
	}
	return s, nil
}

func snapshotBaselineValid(s snapshotState) bool {
	if s.Current == nil {
		return s.Main == nil && len(s.Snapshots) == 0 && s.Sequence == 0
	}
	if s.Main == nil || s.Main.Type != "branch" || !sameSnapshotID(s.Current, s.Main.ID) {
		return false
	}
	if _, ok := s.Snapshots[*s.Current]; !ok {
		return false
	}
	for _, raw := range s.Snapshots {
		record, _, _ := parseSnapshot(json.RawMessage(raw))
		if *record.Sequence > s.Sequence {
			return false
		}
	}
	return true
}

func snapshotRequest(raw []byte, manifest SnapshotManifest) (snapshotInput, error) {
	var request struct {
		Identifier *struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifier,omitempty"`
		Requirements []json.RawMessage `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	bad := func() (snapshotInput, error) {
		return snapshotInput{}, fmt.Errorf("request is outside the guarded add-snapshot/main-ref model")
	}
	if snapshotJSON(raw, &request) != nil || len(request.Requirements) != 2 || len(request.Updates) != 2 {
		return bad()
	}
	if request.Identifier != nil && (request.Identifier.Name != manifest.Table ||
		!reflect.DeepEqual(request.Identifier.Namespace, splitSnapshotNamespace(manifest.Namespace))) {
		return bad()
	}
	in := snapshotInput{}
	uuidSeen, refSeen := false, false
	for _, raw := range request.Requirements {
		var kind struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &kind) != nil {
			return bad()
		}
		switch kind.Type {
		case "assert-table-uuid":
			var req struct {
				Type string `json:"type"`
				UUID string `json:"uuid"`
			}
			if snapshotJSON(raw, &req) != nil || uuidSeen || req.UUID == "" {
				return bad()
			}
			in.uuid, uuidSeen = req.UUID, true
		case "assert-ref-snapshot-id":
			var req struct {
				Type string          `json:"type"`
				Ref  string          `json:"ref"`
				ID   json.RawMessage `json:"snapshot-id"`
			}
			if snapshotJSON(raw, &req) != nil || refSeen || req.Ref != "main" || len(req.ID) == 0 ||
				json.Unmarshal(req.ID, &in.expected) != nil || in.expected != nil && *in.expected < 0 {
				return bad()
			}
			refSeen = true
		default:
			return bad()
		}
	}
	var add struct {
		Action   string          `json:"action"`
		Snapshot json.RawMessage `json:"snapshot"`
	}
	var set struct {
		Action string `json:"action"`
		Name   string `json:"ref-name"`
		Type   string `json:"type"`
		ID     *int64 `json:"snapshot-id"`
	}
	if !uuidSeen || !refSeen || snapshotJSON(request.Updates[0], &add) != nil || add.Action != "add-snapshot" ||
		snapshotJSON(request.Updates[1], &set) != nil || set.Action != "set-snapshot-ref" || set.Name != "main" || set.Type != "branch" || set.ID == nil {
		return bad()
	}
	var err error
	in.added, in.encoded, err = parseSnapshot(add.Snapshot)
	if err != nil || *set.ID != *in.added.ID || !sameSnapshotID(in.expected, in.added.Parent) ||
		in.added.Summary["operation"] != "append" || *in.added.Sequence == 0 ||
		in.added.Schema == nil || *in.added.Schema < 0 {
		return bad()
	}
	return in, nil
}

// snapshotIDs is stable evidence ordering, not a minimal counterexample.
func snapshotIDs(calls []*snapshotCall) []int {
	ids := make([]int, 0, len(calls))
	for _, call := range calls {
		ids = append(ids, call.invoke.ID)
	}
	sort.Ints(ids)
	return ids
}

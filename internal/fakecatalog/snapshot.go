package fakecatalog

import (
	"encoding/json"
	"net/http"
	"sort"
)

// This implementation is independent of the checker's sequential model. The
// caller holds the catalog lock across validation and atomic publication.
func (c *Catalog) commitSnapshot(w http.ResponseWriter, raw []byte, t *table) {
	var change struct {
		Requirements []struct {
			Type string `json:"type"`
			UUID string `json:"uuid"`
			Ref  string `json:"ref"`
			ID   *int64 `json:"snapshot-id"`
		} `json:"requirements"`
		Updates []struct {
			Action   string          `json:"action"`
			Snapshot json.RawMessage `json:"snapshot"`
			Name     string          `json:"ref-name"`
			Type     string          `json:"type"`
			ID       *int64          `json:"snapshot-id"`
		} `json:"updates"`
	}
	if json.Unmarshal(raw, &change) != nil || len(change.Updates) != 2 ||
		change.Updates[0].Action != "add-snapshot" || change.Updates[1].Action != "set-snapshot-ref" {
		writeJSON(w, 400, errBody(400, "BadRequestException", "unsupported snapshot updates"))
		return
	}
	for _, req := range change.Requirements {
		valid := false
		switch req.Type {
		case "assert-table-uuid":
			valid = req.UUID == t.uuid
		case "assert-ref-snapshot-id":
			valid = req.Ref == "main" && (c.Bugs.IgnoreSnapshotRequirements ||
				req.ID == nil && t.current == nil || req.ID != nil && t.current != nil && *req.ID == *t.current)
		}
		if !valid {
			writeJSON(w, 409, errBody(409, "CommitFailedException", "snapshot requirement failed"))
			return
		}
	}
	var snapshot struct {
		ID       *int64 `json:"snapshot-id"`
		Sequence *int64 `json:"sequence-number"`
	}
	set := change.Updates[1]
	if json.Unmarshal(change.Updates[0].Snapshot, &snapshot) != nil || snapshot.ID == nil || snapshot.Sequence == nil ||
		*snapshot.Sequence != t.sequence+1 || t.snapshots[*snapshot.ID] != nil ||
		set.Name != "main" || set.Type != "branch" || set.ID == nil || *set.ID != *snapshot.ID {
		writeJSON(w, 400, errBody(400, "BadRequestException", "invalid snapshot"))
		return
	}
	if c.Bugs.UnknownUnappliedSnapshots {
		writeJSON(w, 503, errBody(503, "ServiceUnavailableException", "not applied"))
		return
	}
	c.commits++
	if c.Bugs.LoseEvery > 0 && c.commits%c.Bugs.LoseEvery == 0 {
		writeJSON(w, 200, c.loadBody(t))
		return
	}
	next := cloneTable(t)
	next.snapshots[*snapshot.ID] = append(json.RawMessage(nil), change.Updates[0].Snapshot...)
	next.sequence = *snapshot.Sequence
	if !c.Bugs.PartialSnapshots {
		next.current = snapshot.ID
	}
	*t = *next
	switch {
	case c.Bugs.RejectAppliedSnapshots:
		writeJSON(w, 409, errBody(409, "CommitFailedException", "rejected after application"))
	case c.Bugs.UnknownAppliedSnapshots:
		writeJSON(w, 503, errBody(503, "ServiceUnavailableException", "applied but response lost"))
	default:
		writeJSON(w, 200, c.loadBody(t))
	}
}

func snapshotList(t *table) []json.RawMessage {
	ids := make([]int64, 0, len(t.snapshots))
	for id := range t.snapshots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		out = append(out, t.snapshots[id])
	}
	return out
}

func snapshotRefs(t *table) map[string]any {
	refs := map[string]any{}
	if t.current != nil {
		refs["main"] = map[string]any{"type": "branch", "snapshot-id": *t.current}
	}
	return refs
}

package fakecatalog

import (
	"context"
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
		*snapshot.Sequence <= t.sequence || t.snapshots[*snapshot.ID] != nil ||
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
	if c.Bugs.UnexpectedSnapshots {
		var extra map[string]json.RawMessage
		json.Unmarshal(change.Updates[0].Snapshot, &extra)
		id := int64(9223372036854775807)
		for next.snapshots[id] != nil {
			id--
		}
		extra["snapshot-id"], _ = json.Marshal(id)
		next.snapshots[id], _ = json.Marshal(extra)
	}
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

// commitExpire removes the listed snapshots as Iceberg's metadata builder
// does: unknown IDs are ignored, and a ref left pointing at a removed snapshot
// is dropped.
func (c *Catalog) commitExpire(ctx context.Context, w http.ResponseWriter, raw []byte, key string, t *table) {
	var change struct {
		Requirements []struct {
			Type string `json:"type"`
			UUID string `json:"uuid"`
		} `json:"requirements"`
		Updates []struct {
			Action string  `json:"action"`
			IDs    []int64 `json:"snapshot-ids"`
		} `json:"updates"`
	}
	if json.Unmarshal(raw, &change) != nil || len(change.Updates) != 1 || change.Updates[0].Action != "remove-snapshots" {
		writeJSON(w, 400, errBody(400, "BadRequestException", "unsupported expiration updates"))
		return
	}
	validate := func(t *table) bool {
		for _, req := range change.Requirements {
			if req.Type != "assert-table-uuid" || req.UUID != t.uuid {
				writeJSON(w, 409, errBody(409, "CommitFailedException", "expiration requirement failed"))
				return false
			}
		}
		return true
	}
	if !validate(t) {
		return
	}
	if hook := c.beforeExpiration; hook != nil {
		func() {
			c.mu.Unlock()
			defer c.mu.Lock()
			hook(ctx)
		}()
		if ctx.Err() != nil {
			return
		}
		// A transaction may replace the table object while the hook runs.
		// Publish expiration on the live entry and recheck its UUID guard.
		t = c.tables[key]
		if t == nil {
			writeJSON(w, 404, errBody(404, "NoSuchTableException", "no such table"))
			return
		}
		if !validate(t) {
			return
		}
	}
	next := cloneTable(t)
	if stale := c.reads[t.uuid]; c.Bugs.ExpireFromStaleMetadata && stale != nil {
		// Planted bug: rebuild from the last read, losing later commits.
		next = cloneTable(stale)
	}
	for _, id := range change.Updates[0].IDs {
		delete(next.snapshots, id)
	}
	if c.Bugs.ExpireRemovesHead && next.current != nil {
		delete(next.snapshots, *next.current)
	}
	if next.current != nil && next.snapshots[*next.current] == nil {
		next.current = nil
	}
	*t = *next
	if c.Bugs.RejectAppliedExpiration {
		writeJSON(w, 409, errBody(409, "CommitFailedException", "rejected after application"))
		return
	}
	writeJSON(w, 200, c.loadBody(t))
}

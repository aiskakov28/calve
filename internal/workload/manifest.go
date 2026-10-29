package workload

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"strconv"
)

// emptyManifestList emits a v2 Avro manifest_file OCF containing zero entries.
// Field IDs/types follow Iceberg V2Metadata.MANIFEST_LIST_SCHEMA. An explicit
// zero-record block retains the standard block framing and sync marker.
func emptyManifestList(id int64, parent *int64, sequence int64) ([]byte, error) {
	field := func(id int, name string, kind any) map[string]any {
		return map[string]any{"name": name, "type": kind, "field-id": id}
	}
	optional := func(id int, name string, kind any) map[string]any {
		f := field(id, name, []any{"null", kind})
		f["default"] = nil
		return f
	}
	partitions := map[string]any{"type": "array", "element-id": 508, "items": map[string]any{
		"type": "record", "name": "r508", "fields": []any{
			field(509, "contains_null", "boolean"), optional(518, "contains_nan", "boolean"),
			optional(510, "lower_bound", "bytes"), optional(511, "upper_bound", "bytes"),
		},
	}}
	schema, err := json.Marshal(map[string]any{"type": "record", "name": "manifest_file", "fields": []any{
		field(500, "manifest_path", "string"), field(501, "manifest_length", "long"), field(502, "partition_spec_id", "int"),
		field(517, "content", "int"), field(515, "sequence_number", "long"), field(516, "min_sequence_number", "long"),
		field(503, "added_snapshot_id", "long"), field(504, "added_files_count", "int"), field(505, "existing_files_count", "int"),
		field(506, "deleted_files_count", "int"), field(512, "added_rows_count", "long"), field(513, "existing_rows_count", "long"),
		field(514, "deleted_rows_count", "long"), optional(507, "partitions", partitions), optional(519, "key_metadata", "bytes"),
	}})
	if err != nil {
		return nil, err
	}
	parentText := "null"
	if parent != nil {
		parentText = strconv.FormatInt(*parent, 10)
	}
	meta := [][2]string{
		{"avro.schema", string(schema)}, {"avro.codec", "null"}, {"format-version", "2"},
		{"snapshot-id", strconv.FormatInt(id, 10)}, {"parent-snapshot-id", parentText}, {"sequence-number", strconv.FormatInt(sequence, 10)},
	}
	var sync [16]byte
	if _, err := rand.Read(sync[:]); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteString("Obj\x01")
	long := func(n int64) { var b [10]byte; length := binary.PutVarint(b[:], n); out.Write(b[:length]) }
	text := func(s string) { long(int64(len(s))); out.WriteString(s) }
	long(int64(len(meta)))
	for _, pair := range meta {
		text(pair[0])
		text(pair[1])
	}
	long(0)
	out.Write(sync[:])
	long(0)
	long(0)
	out.Write(sync[:])
	return out.Bytes(), nil
}

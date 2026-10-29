package workload

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"testing"
)

func TestEmptyManifestListFramingAndSchema(t *testing.T) {
	parent := int64(17)
	body, err := emptyManifestList(18, &parent, 4)
	if err != nil || !bytes.HasPrefix(body, []byte("Obj\x01")) {
		t.Fatal(err)
	}
	reader := bytes.NewReader(body[4:])
	long := func() int64 {
		v, e := binary.ReadVarint(reader)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	text := func() string {
		b := make([]byte, long())
		if _, e := io.ReadFull(reader, b); e != nil {
			t.Fatal(e)
		}
		return string(b)
	}
	count := long()
	meta := map[string]string{}
	for i := int64(0); i < count; i++ {
		key := text()
		meta[key] = text()
	}
	if long() != 0 || meta["format-version"] != "2" || meta["snapshot-id"] != "18" || meta["parent-snapshot-id"] != "17" || meta["sequence-number"] != "4" {
		t.Fatal(meta)
	}
	var schema struct {
		Name   string
		Fields []struct {
			Name string
			ID   int `json:"field-id"`
		}
	}
	if json.Unmarshal([]byte(meta["avro.schema"]), &schema) != nil || schema.Name != "manifest_file" || len(schema.Fields) != 15 {
		t.Fatal(meta["avro.schema"])
	}
	ids := []int{500, 501, 502, 517, 515, 516, 503, 504, 505, 506, 512, 513, 514, 507, 519}
	for i, id := range ids {
		if schema.Fields[i].ID != id {
			t.Fatal(schema.Fields)
		}
	}
	sync := make([]byte, 16)
	io.ReadFull(reader, sync)
	if long() != 0 || long() != 0 {
		t.Fatal("nonempty data block")
	}
	tail := make([]byte, 16)
	io.ReadFull(reader, tail)
	if !bytes.Equal(sync, tail) || reader.Len() != 0 {
		t.Fatal("bad sync framing")
	}
}

package builtin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
)

func newCodec(t *testing.T, name string, cfg map[string]any, dir string) registry.Codec {
	t.Helper()
	reg := newReg(t)
	c, err := reg.NewCodec(name, cfg, dir)
	if err != nil {
		t.Fatalf("new %s codec: %v", name, err)
	}
	return c
}

// newCodecErr expects the instantiation to fail and returns the error.
func newCodecErr(t *testing.T, name string, cfg map[string]any, dir string) error {
	t.Helper()
	reg := newReg(t)
	_, err := reg.NewCodec(name, cfg, dir)
	if err == nil {
		t.Fatalf("new %s codec: expected an error", name)
	}
	return err
}

// --- csv ---

func TestCSVCodecExplicitColumnsRoundTrip(t *testing.T) {
	c := newCodec(t, "csv", map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "int"},
			map[string]any{"name": "amount", "type": "float"},
			map[string]any{"name": "active", "type": "bool"},
			map[string]any{"name": "sku"},
		},
	}, "")
	v, err := c.Decode([]byte(`42,3.5,true,"A,001"`))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("decode = %T", v)
	}
	if m["id"] != int64(42) || m["amount"] != 3.5 || m["active"] != true || m["sku"] != "A,001" {
		t.Fatalf("decoded = %v", m)
	}
	out, err := c.Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `42,3.5,true,"A,001"` {
		t.Fatalf("encode = %s", out)
	}
}

func TestCSVCodecHeaderMode(t *testing.T) {
	c := newCodec(t, "csv", map[string]any{"header": true}, "")
	// First decoded record defines the columns and carries no data.
	v, err := c.Decode([]byte("name,score"))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.(map[string]any)) != 0 {
		t.Fatalf("header row produced data: %v", v)
	}
	v, err = c.Decode([]byte("alice,9"))
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["name"] != "alice" || m["score"] != "9" { // header mode: strings
		t.Fatalf("decoded = %v", m)
	}
	// Encode uses the remembered column order.
	out, err := c.Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "alice,9" {
		t.Fatalf("encode = %s", out)
	}
}

func TestCSVCodecErrorPaths(t *testing.T) {
	// Factory guards.
	newCodecErr(t, "csv", map[string]any{}, "")
	newCodecErr(t, "csv", map[string]any{"header": true, "columns": []any{map[string]any{"name": "x"}}}, "")
	c := newCodec(t, "csv", map[string]any{"columns": []any{map[string]any{"name": "n", "type": "int"}}}, "")
	if _, err := c.Decode([]byte("not-an-int")); err == nil {
		t.Fatal("bad int accepted")
	}
	if _, err := c.Decode([]byte("1,2")); err == nil {
		t.Fatal("field count mismatch accepted")
	}
	if _, err := c.Encode(map[string]any{}); err == nil {
		t.Fatal("encode with missing column accepted")
	}
	if _, err := c.Encode([]any{1}); err == nil {
		t.Fatal("encode of non-mapping accepted")
	}
	// Encode without any known order.
	c2 := newCodec(t, "csv", map[string]any{"header": true}, "")
	if _, err := c2.Encode(map[string]any{"a": "1"}); err == nil {
		t.Fatal("encode before header decode accepted")
	}
}

// --- avro ---

const avroTestSchema = `{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "total", "type": "double"},
    {"name": "count", "type": "long"},
    {"name": "vip", "type": "boolean"},
    {"name": "tags", "type": {"type": "array", "items": "string"}}
  ]
}`

func TestAvroCodecRoundTrip(t *testing.T) {
	c := newCodec(t, "avro", map[string]any{"schema": avroTestSchema}, "")
	in := map[string]any{
		"id": "ORD-1", "total": 99.5, "count": int64(3), "vip": true,
		"tags": []any{"a", "b"},
	}
	b, err := c.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("decode = %T", v)
	}
	if m["id"] != "ORD-1" || m["vip"] != true {
		t.Fatalf("decoded = %v", m)
	}
	// Numeric widths are the CEL-relevant part: long → int64, double →
	// float64 (docs/codecs.md mapping table).
	if m["count"] != int64(3) {
		t.Fatalf("count = %T %v", m["count"], m["count"])
	}
	if f, ok := m["total"].(float64); !ok || f != 99.5 {
		t.Fatalf("total = %T %v", m["total"], m["total"])
	}
}

func TestAvroCodecErrorPaths(t *testing.T) {
	newCodecErr(t, "avro", map[string]any{"schema": "not json {"}, "")
	newCodecErr(t, "avro", map[string]any{}, "")
	c := newCodec(t, "avro", map[string]any{"schema": avroTestSchema}, "")
	if _, err := c.Decode([]byte{0x01, 0x02, 0x03}); err == nil {
		t.Fatal("garbage bytes accepted")
	}
	if _, err := c.Encode(map[string]any{"id": 42}); err == nil {
		t.Fatal("type-mismatched encode accepted")
	}
}

// --- protobuf ---

// The descriptor set for these tests is built by the Go generator in
// descr_test.go (byte-deterministic; CI needs no protoc). The schema:
//
//	syntax = "proto3"; package eventboat.example;
//	message Metric { string name = 1; int64 value = 2; repeated string tags = 3; }
func TestProtobufCodecRoundTrip(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	setPath := filepath.Join(dir, "testdata", "example.descr")
	c := newCodec(t, "protobuf", map[string]any{
		"descriptor_set": setPath,
		"message":        "eventboat.example.Metric",
	}, "")
	in := map[string]any{"name": "cpu", "value": int64(42), "tags": []any{"a", "b"}}
	b, err := c.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("decode = %T", v)
	}
	if m["name"] != "cpu" || m["value"] != "42" {
		t.Fatalf("decoded = %v (int64 surfaces as protojson string form)", m)
	}
}

func TestProtobufCodecErrorPaths(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	setPath := filepath.Join(dir, "testdata", "example.descr")
	newCodecErr(t, "protobuf", map[string]any{"descriptor_set": "missing.descr", "message": "x.Y"}, dir)
	newCodecErr(t, "protobuf", map[string]any{"descriptor_set": setPath, "message": "no.Such"}, dir)
	c := newCodec(t, "protobuf", map[string]any{"descriptor_set": setPath, "message": "eventboat.example.Metric"}, dir)
	if _, err := c.Decode([]byte{0xff, 0xff}); err == nil {
		t.Fatal("garbage bytes accepted")
	}
	if _, err := c.Encode(map[string]any{"value": "not-a-number"}); err == nil {
		t.Fatal("bad field encode accepted")
	}
}

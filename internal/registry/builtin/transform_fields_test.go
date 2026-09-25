package builtin

import (
	"errors"
	"reflect"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
)

func newFieldsTransform(t *testing.T, cfg map[string]any) registry.Transform {
	t.Helper()
	reg := newReg(t)
	tr, err := reg.NewTransform("fields", cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Init(&registry.TransformEnv{}); err != nil {
		t.Fatal(err)
	}
	return tr
}

// Static fields and meta passthrough, with explicit fields overriding both
// the payload and from_meta, and a missing meta key skipped. The payload map
// is replaced (copy-on-write), never mutated in place.
func TestFieldsTransformStaticMetaAndOverride(t *testing.T) {
	tr := newFieldsTransform(t, map[string]any{
		"fields":    map[string]any{"host": "web-01", "app": "nginx"},
		"from_meta": []any{"file_path", "ingest_time", "absent"},
	})
	original := map[string]any{"msg": "hi", "host": "stale"}
	msg := &registry.Message{
		Decoded: original,
		Meta: map[string]any{
			"file_path":   "/var/log/app.jsonl",
			"ingest_time": "2026-09-25T00:00:00Z",
			"host":        "meta-host",
		},
	}
	out, err := tr.Apply(msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("outputs = %d, want 1", len(out))
	}
	got, ok := out[0].Decoded.(map[string]any)
	if !ok {
		t.Fatalf("Decoded = %T, want map", out[0].Decoded)
	}
	want := map[string]any{
		"msg":         "hi",
		"host":        "web-01", // static fields win over payload and meta
		"app":         "nginx",
		"file_path":   "/var/log/app.jsonl",
		"ingest_time": "2026-09-25T00:00:00Z",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload = %#v, want %#v", got, want)
	}
	if _, exists := got["absent"]; exists {
		t.Fatal("missing meta key was written")
	}
	// Copy-on-write: the shared input map is untouched.
	if !reflect.DeepEqual(original, map[string]any{"msg": "hi", "host": "stale"}) {
		t.Fatalf("input payload mutated: %#v", original)
	}
}

// from_meta alone: present keys are copied under the same name, absent ones
// are skipped silently.
func TestFieldsTransformMetaPassthroughSkipped(t *testing.T) {
	tr := newFieldsTransform(t, map[string]any{"from_meta": []any{"file_path", "nope"}})
	msg := &registry.Message{Decoded: map[string]any{}, Meta: map[string]any{"file_path": "/tmp/a.log"}}
	out, err := tr.Apply(msg)
	if err != nil {
		t.Fatal(err)
	}
	got := out[0].Decoded.(map[string]any)
	if got["file_path"] != "/tmp/a.log" || len(got) != 1 {
		t.Fatalf("payload = %#v", got)
	}
}

// A non-map payload is a loud, typed failure (retry per edge policy, then
// dead-letter); it is never silently skipped.
func TestFieldsTransformNonMapPayloadErrors(t *testing.T) {
	tr := newFieldsTransform(t, map[string]any{"fields": map[string]any{"x": 1}})
	for _, payload := range []any{"raw string", nil, []any{1, 2}, 42} {
		_, err := tr.Apply(&registry.Message{Decoded: payload, Meta: map[string]any{}})
		var te *registry.TransformError
		if !errors.As(err, &te) {
			t.Fatalf("payload %T: error = %v, want *TransformError", payload, err)
		}
		if te.Kind != registry.FailureOther {
			t.Fatalf("payload %T: kind = %q, want %q", payload, te.Kind, registry.FailureOther)
		}
	}
}

// An empty config is a valid no-op and still replaces the map.
func TestFieldsTransformEmptyConfigNoop(t *testing.T) {
	tr := newFieldsTransform(t, map[string]any{})
	msg := &registry.Message{Decoded: map[string]any{"a": 1}, Meta: map[string]any{}}
	out, err := tr.Apply(msg)
	if err != nil {
		t.Fatal(err)
	}
	if got := out[0].Decoded.(map[string]any); !reflect.DeepEqual(got, map[string]any{"a": 1}) {
		t.Fatalf("payload = %#v", got)
	}
}

// The plugin is visible in the catalog with its schema and capability, and
// the strict schema rejects unknown keys and mistyped values.
func TestFieldsTransformCatalogAndSchema(t *testing.T) {
	reg := newReg(t)
	meta, ok := reg.LookupTransform("fields")
	if !ok {
		t.Fatal("fields transform not registered")
	}
	if meta.Version != 1 || len(meta.Capabilities) != 1 || meta.Capabilities[0] != "explain-safe" {
		t.Fatalf("meta = %+v, want version 1 and [explain-safe]", meta)
	}
	if _, err := reg.NewTransform("fields", map[string]any{"bogus": 1}, ""); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := reg.NewTransform("fields", map[string]any{"from_meta": "file_path"}, ""); err == nil {
		t.Fatal("non-array from_meta accepted")
	}
	if _, err := reg.NewTransform("fields", map[string]any{"fields": "nope"}, ""); err == nil {
		t.Fatal("non-object fields accepted")
	}
}

package registry

import (
	"strings"
	"testing"
)

const okSchema = `{
  "type": "object",
  "required": ["path"],
  "properties": { "path": { "type": "string" } },
  "additionalProperties": false
}`

func TestRegisterReservedNameRejected(t *testing.T) {
	// review R5: plugin names colliding with framework fields are rejected at
	// registration, not discovered at verify time. script/split/wasm are NOT
	// reserved — they are the built-in transform plugin names (spec v1.19).
	for _, name := range []string{"from", "when", "batch", "delivery"} {
		r := New()
		if err := r.RegisterSource(name, 1, okSchema, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
			t.Errorf("source %q: reserved name accepted", name)
		}
		if err := r.RegisterSink(name, 1, okSchema, func(map[string]any) (Sink, error) { return nil, nil }); err == nil {
			t.Errorf("sink %q: reserved name accepted", name)
		}
		if err := r.RegisterCodec(name, 1, okSchema, func(map[string]any, string) (Codec, error) { return nil, nil }); err == nil {
			t.Errorf("codec %q: reserved name accepted", name)
		}
		if err := r.RegisterTransform(name, 1, okSchema, nil, func(any, string) (Transform, error) { return nil, nil }); err == nil {
			t.Errorf("transform %q: reserved name accepted", name)
		}
	}
}

func TestRegisterDuplicateRejected(t *testing.T) {
	r := New()
	factory := func(map[string]any) (Source, error) { return nil, nil }
	if err := r.RegisterSource("dup", 1, okSchema, nil, factory); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource("dup", 1, okSchema, nil, factory); err == nil {
		t.Error("duplicate source registration accepted")
	}
	// Sections are separate namespaces: the "file" plugin legitimately
	// exists as both a source and a sink.
	noopSnk := func(map[string]any) (Sink, error) { return nil, nil }
	if err := r.RegisterSink("dup", 1, okSchema, noopSnk); err != nil {
		t.Errorf("sink name sharing a source name rejected: %v", err)
	}
	if err := r.RegisterSink("dup", 1, okSchema, noopSnk); err == nil {
		t.Error("duplicate sink registration accepted")
	}
}

func TestRegisterNilFactoryRejected(t *testing.T) {
	r := New()
	if err := r.RegisterSource("x", 1, okSchema, nil, nil); err == nil {
		t.Error("nil source factory accepted")
	}
	if err := r.RegisterSink("x", 1, okSchema, nil); err == nil {
		t.Error("nil sink factory accepted")
	}
}

func TestRegisterVersionValidated(t *testing.T) {
	r := New()
	// ABI versions start at 1 (redesign-v3.md §6.5: catalog carries versions;
	// a config-referenced version mismatch is a verify error).
	noopSrc := func(map[string]any) (Source, error) { return nil, nil }
	noopSnk := func(map[string]any) (Sink, error) { return nil, nil }
	if err := r.RegisterSource("v", 0, okSchema, nil, noopSrc); err == nil {
		t.Error("version 0 source accepted")
	}
	if err := r.RegisterSink("v", -1, okSchema, noopSnk); err == nil {
		t.Error("negative sink version accepted")
	}
	if err := r.RegisterSource("v2", 2, okSchema, nil, noopSrc); err != nil {
		t.Fatal(err)
	}
	if meta, ok := r.LookupSource("v2"); !ok || meta.Version != 2 {
		t.Errorf("LookupSource version = %v, %v; want 2, true", meta, ok)
	}
	if c := r.Catalog(); len(c.Sources) == 0 || c.Sources[0].Version != 2 {
		t.Errorf("catalog does not carry source version: %+v", c.Sources)
	}
}

func TestRegisterInvalidSchemaRejected(t *testing.T) {
	r := New()
	if err := r.RegisterSource("bad", 1, `{not json`, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
		t.Error("invalid schema JSON accepted")
	}
	// A schema that does not compile (unknown $ref) is also rejected.
	badRef := `{ "$ref": "https://eventboat.dev/nope.json" }`
	if err := r.RegisterSource("badref", 1, badRef, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
		t.Error("unresolvable $ref accepted")
	}
}

func TestNewSourceUnknownAndSchemaValidation(t *testing.T) {
	r := New()
	if _, err := r.NewSource("ghost", nil); err == nil || !strings.Contains(err.Error(), "unknown source plugin") {
		t.Errorf("unknown plugin error = %v", err)
	}
	if _, err := r.NewSink("ghost", nil); err == nil {
		t.Error("unknown sink accepted")
	}
	if _, err := r.NewCodec("ghost", nil, ""); err == nil {
		t.Error("unknown codec accepted")
	}

	if err := r.RegisterSource("s", 1, okSchema, []string{"pull"}, func(map[string]any) (Source, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	// Missing required field fails validation before the factory runs.
	_, err := r.NewSource("s", map[string]any{"nope": 1})
	if err == nil {
		t.Fatal("schema violation accepted")
	}
	var se *SchemaError
	if !asSchemaError(err, &se) {
		t.Fatalf("error type = %T, want *SchemaError", err)
	}
	if len(se.Errors) == 0 {
		t.Error("SchemaError carries no issues")
	}
	joined := se.Error()
	if !strings.Contains(joined, "s") || !strings.Contains(joined, "schema validation") {
		t.Errorf("SchemaError message = %q", joined)
	}
}

func asSchemaError(err error, target **SchemaError) bool {
	se, ok := err.(*SchemaError)
	if ok {
		*target = se
	}
	return ok
}

func TestCatalogListsSorted(t *testing.T) {
	r := New()
	noopSrc := func(map[string]any) (Source, error) { return nil, nil }
	noopSnk := func(map[string]any) (Sink, error) { return nil, nil }
	for _, n := range []string{"kafka", "cron", "file"} {
		if err := r.RegisterSource(n, 1, okSchema, nil, noopSrc); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.RegisterSink("drop", 1, okSchema, noopSnk); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterCodec("json", 1, okSchema, func(map[string]any, string) (Codec, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	c := r.Catalog()
	if len(c.Sources) != 3 || c.Sources[0].Name != "cron" || c.Sources[1].Name != "file" || c.Sources[2].Name != "kafka" {
		t.Errorf("sources not sorted: %+v", c.Sources)
	}
	if len(c.Sinks) != 1 || c.Sinks[0].Name != "drop" {
		t.Errorf("sinks = %+v", c.Sinks)
	}
	if len(c.Codecs) != 1 || c.Codecs[0].Name != "json" || c.Codecs[0].Version != 1 {
		t.Errorf("codecs = %+v", c.Codecs)
	}
}

func TestLookupSourceCapabilities(t *testing.T) {
	r := New()
	noopSrc := func(map[string]any) (Source, error) { return nil, nil }
	if err := r.RegisterSource("sql", 1, okSchema, []string{"pull"}, noopSrc); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource("cron", 1, okSchema, nil, noopSrc); err != nil {
		t.Fatal(err)
	}
	m, ok := r.LookupSource("sql")
	if !ok || len(m.Capabilities) != 1 || m.Capabilities[0] != "pull" {
		t.Errorf("sql capabilities = %+v", m)
	}
	if m, ok := r.LookupSource("cron"); !ok || len(m.Capabilities) != 0 {
		t.Errorf("cron capabilities = %+v", m)
	}
	if _, ok := r.LookupSource("nope"); ok {
		t.Error("unknown source found")
	}
}

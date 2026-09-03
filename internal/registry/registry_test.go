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
	// registration, not discovered at verify time.
	for _, name := range []string{"from", "when", "batch", "script", "delivery"} {
		r := New()
		if err := r.RegisterSource(name, okSchema, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
			t.Errorf("source %q: reserved name accepted", name)
		}
		if err := r.RegisterSink(name, okSchema, func(map[string]any) (Sink, error) { return nil, nil }); err == nil {
			t.Errorf("sink %q: reserved name accepted", name)
		}
		if err := r.RegisterCodec(name, func(map[string]any) (Codec, error) { return nil, nil }); err == nil {
			t.Errorf("codec %q: reserved name accepted", name)
		}
	}
}

func TestRegisterDuplicateRejected(t *testing.T) {
	r := New()
	factory := func(map[string]any) (Source, error) { return nil, nil }
	if err := r.RegisterSource("dup", okSchema, nil, factory); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource("dup", okSchema, nil, factory); err == nil {
		t.Error("duplicate source registration accepted")
	}
	// Sections are separate namespaces: the "file" plugin legitimately
	// exists as both a source and a sink.
	noopSnk := func(map[string]any) (Sink, error) { return nil, nil }
	if err := r.RegisterSink("dup", okSchema, noopSnk); err != nil {
		t.Errorf("sink name sharing a source name rejected: %v", err)
	}
	if err := r.RegisterSink("dup", okSchema, noopSnk); err == nil {
		t.Error("duplicate sink registration accepted")
	}
}

func TestRegisterNilFactoryRejected(t *testing.T) {
	r := New()
	if err := r.RegisterSource("x", okSchema, nil, nil); err == nil {
		t.Error("nil source factory accepted")
	}
	if err := r.RegisterSink("x", okSchema, nil); err == nil {
		t.Error("nil sink factory accepted")
	}
}

func TestRegisterInvalidSchemaRejected(t *testing.T) {
	r := New()
	if err := r.RegisterSource("bad", `{not json`, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
		t.Error("invalid schema JSON accepted")
	}
	// A schema that does not compile (unknown $ref) is also rejected.
	badRef := `{ "$ref": "https://eventboat.dev/nope.json" }`
	if err := r.RegisterSource("badref", badRef, nil, func(map[string]any) (Source, error) { return nil, nil }); err == nil {
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
	if _, err := r.NewCodec("ghost", nil); err == nil {
		t.Error("unknown codec accepted")
	}

	if err := r.RegisterSource("s", okSchema, []string{"pull"}, func(map[string]any) (Source, error) { return nil, nil }); err != nil {
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
		if err := r.RegisterSource(n, okSchema, nil, noopSrc); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.RegisterSink("drop", okSchema, noopSnk); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterCodec("json", func(map[string]any) (Codec, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	c := r.Catalog()
	if len(c.Sources) != 3 || c.Sources[0].Name != "cron" || c.Sources[1].Name != "file" || c.Sources[2].Name != "kafka" {
		t.Errorf("sources not sorted: %+v", c.Sources)
	}
	if len(c.Sinks) != 1 || c.Sinks[0].Name != "drop" {
		t.Errorf("sinks = %+v", c.Sinks)
	}
	if len(c.Codecs) != 1 || c.Codecs[0] != "json" {
		t.Errorf("codecs = %+v", c.Codecs)
	}
}

func TestLookupSourceCapabilities(t *testing.T) {
	r := New()
	noopSrc := func(map[string]any) (Source, error) { return nil, nil }
	if err := r.RegisterSource("sql", okSchema, []string{"pull"}, noopSrc); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource("cron", okSchema, nil, noopSrc); err != nil {
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

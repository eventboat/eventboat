package registry

import (
	"strings"
	"testing"
)

type fakeTransform struct{}

func (fakeTransform) Init(env *TransformEnv) error           { return nil }
func (fakeTransform) Apply(msg *Message) ([]*Message, error) { return nil, nil }
func (fakeTransform) Close() error                           { return nil }

func TestTransformRegistrationLifecycle(t *testing.T) {
	r := New()
	factory := func(cfg any, dir string) (Transform, error) { return fakeTransform{}, nil }
	if err := r.RegisterTransform("mapper", 0, okSchema, nil, factory); err == nil {
		t.Error("version 0 accepted")
	}
	if err := r.RegisterTransform("mapper", 1, okSchema, nil, nil); err == nil {
		t.Error("nil factory accepted")
	}
	if err := r.RegisterTransform("mapper", 1, okSchema, []string{"explain-safe"}, factory); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterTransform("mapper", 1, okSchema, nil, factory); err == nil {
		t.Error("duplicate accepted")
	}
	if m, ok := r.LookupTransform("mapper"); !ok || m.Version != 1 || m.Schema != okSchema || len(m.Capabilities) != 1 {
		t.Errorf("meta = %+v ok=%v", m, ok)
	}
	if _, err := r.NewTransform("nope", nil, ""); err == nil || !strings.Contains(err.Error(), "unknown transform") {
		t.Errorf("unknown error = %v", err)
	}
	if _, err := r.NewTransform("mapper", map[string]any{}, ""); err == nil {
		t.Error("schema violation accepted (path required)")
	}
	if _, err := r.NewTransform("mapper", map[string]any{"path": "x"}, ""); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
	if cat := r.Catalog(); len(cat.Transforms) != 1 || cat.Transforms[0].Name != "mapper" || cat.Transforms[0].Capabilities[0] != "explain-safe" {
		t.Errorf("catalog transforms = %+v", cat.Transforms)
	}
}

// The script plugin's contract: scalar-root schemas validate non-mapping
// configs (the plugin block's value is the config itself, spec v1.19).
func TestTransformScalarRootSchema(t *testing.T) {
	r := New()
	strSchema := `{"type":"string","minLength":1}`
	var got any
	if err := r.RegisterTransform("sayer", 1, strSchema, nil, func(cfg any, dir string) (Transform, error) {
		got = cfg
		return fakeTransform{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.NewTransform("sayer", 123, ""); err == nil {
		t.Error("non-string config accepted")
	}
	if _, err := r.NewTransform("sayer", "hello", ""); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("factory cfg = %#v", got)
	}
}

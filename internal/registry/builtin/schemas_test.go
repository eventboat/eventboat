package builtin

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
)

// Every builtin's generated JSON Schema is pinned to a committed golden file:
// the struct is the single source of truth, and the goldens make schema
// changes reviewable as diffs (the same contract testdata/example.descr uses
// with -update-descr). Run -update-schemas after changing a config struct.
var updateSchemas = flag.Bool("update-schemas", false, "regenerate testdata/schemas from the registered plugins")

func writeSchemaGolden(t *testing.T, prefix, name, schema string) {
	t.Helper()
	path := filepath.Join("testdata", "schemas", prefix+"-"+name+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestSchemaGoldens pins the generated schema of every registered plugin.
func TestSchemaGoldens(t *testing.T) {
	reg := registry.New()
	if err := RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	cat := reg.Catalog()
	compare := func(kind, name, schema string) {
		path := filepath.Join("testdata", "schemas", kind+"-"+name+".json")
		if *updateSchemas {
			writeSchemaGolden(t, kind, name, schema)
			return
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s %q: golden missing: %v (run: go test ./internal/registry/builtin -update-schemas)", kind, name, err)
		}
		if string(want) != schema {
			t.Errorf("%s %q: generated schema drifted from the golden — review the diff and regenerate with -update-schemas", kind, name)
		}
	}
	for _, m := range cat.Sources {
		compare("source", m.Name, m.Schema)
	}
	for _, m := range cat.Transforms {
		compare("transform", m.Name, m.Schema)
	}
	for _, m := range cat.Sinks {
		compare("sink", m.Name, m.Schema)
	}
	for _, m := range cat.Codecs {
		compare("codec", m.Name, m.Schema)
	}
}

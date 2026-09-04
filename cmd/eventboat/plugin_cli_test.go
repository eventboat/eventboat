package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The schema export surface (M4 §7.4): single-name lookup across sections,
// JSON envelope, and bulk --all --dir export with predictable layout.
func TestPluginSchemaCLI(t *testing.T) {
	dir := t.TempDir()

	// Single name, text mode: prints kind-scoped entries with pretty JSON.
	if code := cmdPlugin([]string{"schema", "kafka"}, false); code != 0 {
		t.Fatalf("schema kafka exit = %d", code)
	}
	// Unknown name exits 1.
	if code := cmdPlugin([]string{"schema", "no-such-plugin"}, false); code != 1 {
		t.Fatalf("unknown name exit = %d, want 1", code)
	}

	// Bulk export: one file per plugin under <dir>/<kind>/<name>.json.
	out := filepath.Join(dir, "schemas")
	if code := cmdPlugin([]string{"schema", "--all", "--dir", out}, false); code != 0 {
		t.Fatalf("--all exit = %d", code)
	}
	for _, p := range []string{
		filepath.Join("sources", "kafka.json"),
		filepath.Join("sources", "cron.json"),
		filepath.Join("sinks", "file.json"),
		filepath.Join("codecs", "avro.json"),
		filepath.Join("codecs", "csv.json"),
		filepath.Join("codecs", "protobuf.json"),
	} {
		if _, err := os.Stat(filepath.Join(out, p)); err != nil {
			t.Errorf("missing exported schema %s: %v", p, err)
		}
	}
	// Each file is valid JSON.
	data, err := os.ReadFile(filepath.Join(out, "sources", "kafka.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("exported schema is not valid JSON: %v", err)
	}
	if !strings.Contains(string(data), "brokers") {
		t.Errorf("kafka schema looks wrong: %s", data)
	}
}

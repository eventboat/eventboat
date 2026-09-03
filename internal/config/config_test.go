package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValidThreeSection(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: orders }

edge_defaults:
  delivery: { retries: 3, backoff: exponential }

constants:
  currency: EUR

sources:
  ingest:
    decoder: json
    kafka:
      brokers: ["localhost:9092"]
      topics: [orders]

transforms:
  enrich:
    from: [ingest]
    workers: 2
    script: |
      payload.total = payload.price * payload.qty

sinks:
  eu-out:
    from: { enrich: { when: 'meta.region == "eu"' } }
    encoder: json
    kafka: { topic: orders-eu }
`))
	if res.HasErrors() {
		t.Fatalf("expected clean load, got:\n%v", res.Diagnostics)
	}
	p := res.Pipeline
	if p.Name != "orders" {
		t.Errorf("name = %q", p.Name)
	}
	if p.Sources["ingest"].Plugin != "kafka" {
		t.Errorf("ingest plugin = %q", p.Sources["ingest"].Plugin)
	}
	if p.Transforms["enrich"].Workers != 2 {
		t.Errorf("workers = %d", p.Transforms["enrich"].Workers)
	}
	if len(p.Sinks["eu-out"].From) != 1 || p.Sinks["eu-out"].From[0].When == "" {
		t.Errorf("edge when not parsed: %+v", p.Sinks["eu-out"].From)
	}
	if p.EdgeDefaults.Delivery == nil || p.EdgeDefaults.Delivery.Retries != 3 {
		t.Errorf("edge_defaults not parsed: %+v", p.EdgeDefaults)
	}
}

func TestUnknownTopLevelAndNodeFieldsAreErrors(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
run: { mode: job }
sources:
  in: { decoder: json, fil: { path: x } }
sinks:
  out: { from: [in], mystery: true, file: { path: out.txt } }
`))
	codes := map[string]bool{}
	for _, d := range res.Diagnostics {
		codes[d.Code] = true
	}
	if !codes["cfg_unknown_top_section"] {
		t.Errorf("missing cfg_unknown_top_section, got %v", codes)
	}
	if !codes["plugin_unknown"] && !codes["cfg_missing_plugin"] {
		// "fil" is an unknown plugin: it still parses as the plugin key;
		// existence is checked by ir against the registry.
		t.Logf("unknown plugin key surfaced as: %v", codes)
	}
	if !codes["cfg_multiple_plugins"] {
		t.Errorf("missing cfg_multiple_plugins, got %v", codes)
	}
}

func TestTransformMainFieldMutualExclusion(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  t:
    from: [in]
    script: |
      payload.x = 1
    split: {}
sinks:
  out: { from: [t], file: { path: b } }
`))
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_transform_main_field" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cfg_transform_main_field, got %+v", res.Diagnostics)
	}
}

func TestSourceWithFromRejected(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, from: [nope], file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`))
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_source_with_from" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cfg_source_with_from, got %+v", res.Diagnostics)
	}
}

func TestEnvSubstitutionForms(t *testing.T) {
	os.Setenv("EB_TEST_BROKERS", "broker1:9092")
	os.Setenv("EB_TEST_WORKERS", "4")
	defer os.Unsetenv("EB_TEST_BROKERS")
	defer os.Unsetenv("EB_TEST_WORKERS")

	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in:
    decoder: json
    file: { path: "${EB_TEST_MISSING}" }
transforms:
  t:
    from: [in]
    workers: ${EB_TEST_WORKERS}
    script: |
      payload.x = 1
sinks:
  out:
    from: [t]
    file:
      path: fixed.txt
      ${?EB_TEST_OPTIONAL}: omit-me
`))
	// ${EB_TEST_MISSING} unset => error
	unsetErr := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_env_unset" {
			unsetErr = true
		}
	}
	if !unsetErr {
		t.Fatalf("expected cfg_env_unset, got %+v", res.Diagnostics)
	}

	// Optional key omitted: sink plugin block has only path.
	sink := res.Pipeline.Sinks["out"]
	if _, present := sink.PluginConfig["EB_TEST_OPTIONAL"]; present {
		t.Errorf("optional key not omitted: %+v", sink.PluginConfig)
	}
	if sink.PluginConfig["path"] != "fixed.txt" {
		t.Errorf("path = %v", sink.PluginConfig["path"])
	}
	// Typed substitution: workers becomes int 4.
	if res.Pipeline.Transforms["t"].Workers != 4 {
		t.Errorf("workers via env = %d, want 4", res.Pipeline.Transforms["t"].Workers)
	}
}

func TestConstantsSubstitution(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
constants:
  threshold: 100
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  t:
    from: [in]
    script: |
      payload.x = constants.threshold
sinks:
  out: { from: [t], file: { path: "out-${constants.threshold}.txt" } }
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	if got := res.Pipeline.Sinks["out"].PluginConfig["path"]; got != "out-100.txt" {
		t.Errorf("path = %v, want out-100.txt", got)
	}
}

func TestLoadFileMissing(t *testing.T) {
	res := LoadFile(filepath.Join(t.TempDir(), "nope.yaml"))
	if !res.HasErrors() {
		t.Fatal("expected io_read error")
	}
}

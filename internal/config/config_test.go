package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
bogus_section: { mode: job }
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

func TestWasmFieldParsing(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  good:
    from: [in]
    wasm:
      module: guests/heavy.wasm
      entrypoint: transform
      timeout_ms: 500
      max_memory_pages: 256
      allow: [log]
  bad:
    from: [in]
    wasm:
      module: guests/heavy.wasm
      timeout_ms: -1
      max_memory_pages: 99999
      allow: [net]
  both:
    from: [in]
    wasm: { module: guests/heavy.wasm }
    script: |
      payload.x = 1
sinks:
  out: { from: [good, bad, both], file: { path: b } }
`))
	var good *Node
	codes := map[string]bool{}
	for _, d := range res.Diagnostics {
		codes[d.Code] = true
	}
	for _, code := range []string{"cfg_transform_main_field", "cfg_wasm_range", "cfg_wasm_allow"} {
		if !codes[code] {
			t.Errorf("missing %s in %+v", code, res.Diagnostics)
		}
	}
	for name, n := range res.Pipeline.Transforms {
		if name == "good" {
			good = n
		}
	}
	if good == nil || good.Wasm == nil {
		t.Fatalf("good wasm node not parsed: %+v", good)
	}
	w := good.Wasm
	if w.Module != "guests/heavy.wasm" || w.Entrypoint != "transform" || w.TimeoutMs != 500 || w.MaxMemoryPages != 256 || len(w.Allow) != 1 || w.Allow[0] != "log" {
		t.Fatalf("wasm config parsed wrong: %+v", w)
	}
}

func TestWasmTimeoutDefaults(t *testing.T) {
	// M3-audit J2: unset timeout_ms parses to the -1 sentinel (fast mode +
	// verify lint warning); explicit 0 is a deliberate fast-mode choice and
	// must not warn.
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  unset:
    from: [in]
    wasm: { module: guests/heavy.wasm }
  fast:
    from: [unset]
    wasm: { module: guests/heavy.wasm, timeout_ms: 0 }
sinks:
  out: { from: [fast], file: { path: b } }
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	if got := res.Pipeline.Transforms["unset"].Wasm.TimeoutMs; got != -1 {
		t.Errorf("unset timeout_ms = %d, want -1 sentinel", got)
	}
	if got := res.Pipeline.Transforms["fast"].Wasm.TimeoutMs; got != 0 {
		t.Errorf("explicit timeout_ms: 0 = %d, want 0", got)
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

// Unknown scoped substitutions must be errors ("unknown means error",
// redesign-v3.md §5.5): only `constants` is a legal scope in the POC.
func TestScopedSubstitutionUnknownScopeErrors(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: "${bogus.value}.txt" } }
`))
	var diag *Diagnostic
	for i := range res.Diagnostics {
		if res.Diagnostics[i].Code == "cfg_scope_unknown" {
			diag = &res.Diagnostics[i]
		}
	}
	if diag == nil {
		t.Fatalf("expected cfg_scope_unknown, got %+v", res.Diagnostics)
	}
	if !strings.Contains(diag.Message, "bogus") {
		t.Errorf("message should name the offending scope: %q", diag.Message)
	}
}

// ${parameters.*} in a continuous pipeline errors with explicit guidance;
// in a job pipeline the token passes through unresolved (resolved per run).
func TestScopedSubstitutionParametersGuided(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: "run-${parameters.from}.txt" } }
`))
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_scope_unknown" && strings.Contains(d.Message, "parameters.from") {
			found = true
			if !strings.Contains(d.Message, "parameters are only available in job pipelines") {
				t.Errorf("missing job-pipeline guidance: %q", d.Message)
			}
		}
	}
	if !found {
		t.Fatalf("expected cfg_scope_unknown for parameters with guidance, got %+v", res.Diagnostics)
	}

	// Job pipeline: the token survives for the jobs runner.
	res = LoadBytes("job.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
run:
  mode: job
sources:
  in:
    decoder: json
    file: { path: a }
sinks:
  out: { from: [in], file: { path: "run-${parameters.from}.txt" } }
`))
	if res.HasErrors() {
		t.Fatalf("job pipeline with ${parameters.x} rejected at load: %+v", res.Diagnostics)
	}
	got := res.Pipeline.Sinks["out"].PluginConfig["path"]
	if got != "run-${parameters.from}.txt" {
		t.Errorf("parameters token did not pass through: %v", got)
	}
}

// constants and plain env vars keep working; dotted references are not
// mistaken for env vars.
func TestScopedSubstitutionConstantsAndEnvStillWork(t *testing.T) {
	os.Setenv("EB_TEST_OK_VAR", "prod")
	defer os.Unsetenv("EB_TEST_OK_VAR")
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
constants:
  tier: gold
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: "${constants.tier}-${EB_TEST_OK_VAR}.txt" } }
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	if got := res.Pipeline.Sinks["out"].PluginConfig["path"]; got != "gold-prod.txt" {
		t.Errorf("path = %v, want gold-prod.txt", got)
	}
}

// The optional marker is only meaningful for environment variables: any
// dotted scope reference combined with `?` is an error (round-2 review #1).
func TestOptionalScopedReferencesAreErrors(t *testing.T) {
	cases := []struct {
		ref      string
		wantPart string
	}{
		{"${?parameters.from}", "parameters are only available in job pipelines"},
		{"${?bogus.v}", "bogus"},
		{"${?constants.tier}", "optional marker"},
	}
	for _, tc := range cases {
		res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
constants:
  tier: gold
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: "out-`+tc.ref+`.txt" } }
`))
		found := false
		for _, d := range res.Diagnostics {
			if d.Code == "cfg_scope_unknown" && strings.Contains(d.Message, tc.wantPart) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: expected cfg_scope_unknown mentioning %q, got %+v", tc.ref, tc.wantPart, res.Diagnostics)
		}
	}
}

// Regression guard: ${?SOME_ENV} (no dot, optional, unset) still omits the
// key silently — optionality remains valid for plain environment variables.
func TestOptionalEnvStillOmitsKey(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out:
    from: [in]
    file:
      path: fixed.txt
      ${?EB_TEST_OPTIONAL_UNSET}: omit-me
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	if _, present := res.Pipeline.Sinks["out"].PluginConfig["EB_TEST_OPTIONAL_UNSET"]; present {
		t.Errorf("optional env key not omitted: %+v", res.Pipeline.Sinks["out"].PluginConfig)
	}
}

// One unset ${VAR} reference in a nested value must produce exactly one
// cfg_env_unset diagnostic (round-2 review #4: it used to be reported
// twice — once per traversal pass over the same scalar).
func TestEnvUnsetDiagnosticReportedOnce(t *testing.T) {
	for _, placement := range []string{
		`file: { path: "${EB_TEST_MISSING_ONCE}" }`,               // mapping value
		`file: { brokers: ["${EB_TEST_MISSING_ONCE}"], path: a }`, // sequence element (kafka-style)
	} {
		res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in:
    decoder: json
    `+placement+`
sinks:
  out: { from: [in], file: { path: b } }
`))
		count := 0
		for _, d := range res.Diagnostics {
			if d.Code == "cfg_env_unset" {
				count++
			}
		}
		if count != 1 {
			t.Errorf("placement %q: cfg_env_unset reported %d times, want 1 (diags: %+v)", placement, count, res.Diagnostics)
		}
	}
}

// The limits section parses into typed values (M1 debt: wiring into engine
// Options happens via engine.Options.WithLimits).
func TestLimitsSection(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
limits:
  max_in_flight: 500
  drain_timeout: 45s
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: o } }
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	l := res.Pipeline.Limits
	if l == nil || l.MaxInFlight != 500 || l.DrainTimeout != 45*time.Second {
		t.Errorf("limits = %+v", l)
	}

	// Unknown fields, bad ranges and bad durations are errors.
	for _, tc := range []struct {
		name string
		body string
		code string
	}{
		{"unknown field", "limits: { max_in_flight: 5, workers: 2 }", "cfg_unknown_field"},
		{"zero range", "limits: { max_in_flight: 0 }", "cfg_limits_range"},
		{"bad duration", "limits: { drain_timeout: soon }", "cfg_limits_range"},
		{"non-string duration", "limits: { drain_timeout: 10 }", "cfg_limits_type"},
		{"not a mapping", "limits: 10", "cfg_limits_type"},
	} {
		res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
`+tc.body+`
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: o } }
`))
		found := false
		for _, d := range res.Diagnostics {
			if d.Code == tc.code {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: expected %s, got %+v", tc.name, tc.code, res.Diagnostics)
		}
	}
}

func TestParseDurationWithDays(t *testing.T) {
	cases := map[string]time.Duration{
		"90d":   90 * 24 * time.Hour,
		"1d":    24 * time.Hour,
		"2h":    2 * time.Hour,
		"1d12h": 36 * time.Hour,
		"500ms": 500 * time.Millisecond,
	}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "xd", "d", "1x"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("ParseDuration(%q) unexpectedly ok", bad)
		}
	}
}

package convert

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderScript(t *testing.T) {
	cases := []struct {
		name string
		dsl  string
		want string
	}{
		{"assignment same-shape", "payload.total = payload.price * payload.quantity", "payload.total = payload.price * payload.quantity\n"},
		{"metadata root rewrite", `metadata.enriched_at = "riverpod"`, `meta.enriched_at = "riverpod"` + "\n"},
		{"bool literals python-style", "payload.ok = true\npayload.off = false", "payload.ok = True\npayload.off = False\n"},
		{"null literal", "payload.x = null", "payload.x = None\n"},
		{"ternary flattens to if/else", "payload.tier = payload.total > 100 ? \"vip\" : \"basic\"",
			"if payload.total > 100:\n    payload.tier = \"vip\"\nelse:\n    payload.tier = \"basic\"\n"},
		{"ternary chain flattens to elif", "payload.t = payload.a > 3 ? \"x\" : payload.a > 2 ? \"y\" : \"z\"",
			"if payload.a > 3:\n    payload.t = \"x\"\nelif payload.a > 2:\n    payload.t = \"y\"\nelse:\n    payload.t = \"z\"\n"},
		{"nested ternary in condition flattens with expression condition", "payload.t = (payload.a > 3 ? \"x\" : \"y\") == \"x\" ? 1 : 2",
			"if ((\"x\" if payload.a > 3 else \"y\")) == \"x\":\n    payload.t = 1\nelse:\n    payload.t = 2\n"},
		{"del to remove", "del(payload.a.b)", "remove(payload.a, \"b\")\n"},
		{"del metadata root", `del(metadata.tag)`, "remove(meta, \"tag\")\n"},
		{"del quoted key", `del(payload["a b"])`, "remove(payload, \"a b\")\n"},
		{"format via percent", `payload.label = "order-%s-%s"`, "payload.label = \"order-%s-%s\"\n"},
		{"string concat", `payload.m = payload.message + " suffix"`, "payload.m = payload.message + \" suffix\"\n"},
		{"size to len", "payload.n = size(payload.items)", "payload.n = len(payload[\"items\"])\n"},
		{"member startsWith", `payload.ok = payload.s.startsWith("a")`, "payload.ok = payload.s.startswith(\"a\")\n"},
		{"member contains via in", `payload.ok = payload.s.contains("x")`, "payload.ok = (\"x\" in payload.s)\n"},
		{"index access", "payload.first = payload.items[0]", "payload.first = payload[\"items\"][0]\n"}, // items shadows a dict method
		{"list literal", "payload.tags = [\"a\", \"b\"]", "payload.tags = [\"a\", \"b\"]\n"},
		{"map literal", `payload.m = {"k": 1}`, "payload.m = {\"k\": 1}\n"},
		{"and or parens", "payload.go = payload.a > 1 && (payload.b > 2 || payload.c > 3)", "payload.go = (payload.a > 1) and ((payload.b > 2) or (payload.c > 3))\n"},
		{"in operator", "payload.hot = payload.tag in [\"a\", \"b\"]", "payload.hot = (payload.tag in [\"a\", \"b\"])\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script, rows, manuals, _ := renderScript("test", tc.dsl)
			if len(manuals) > 0 {
				t.Fatalf("unexpected manual items: %+v", manuals)
			}
			for _, r := range rows {
				if r.Status != "auto" {
					t.Fatalf("row not auto: %+v", r)
				}
			}
			if script != tc.want {
				t.Fatalf("renderScript mismatch:\n got: %q\nwant: %q", script, tc.want)
			}
			// Compile gate: the generated script must be valid Starlark
			// under the real host (payload/meta are legal predeclareds).
			if _, err := compileForTest(script); err != nil {
				t.Fatalf("generated script does not compile: %v\nscript:\n%s", err, script)
			}
		})
	}
}

func TestRenderScriptManuals(t *testing.T) {
	cases := []struct {
		name string
		dsl  string
	}{
		{"now() has no equivalent", "payload.t = now()"},
		{"whole payload replace", "payload = {a: 1}"},
		{"whole metadata replace", "metadata = {a: 1}"},
		{"unsupported statement form", "if payload.x > 1 { }"},
		{"unknown function", "payload.u = frobnicate(payload.x)"},
		{"matches has no form", `payload.m = payload.s.matches("^a")`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rows, manuals, _ := renderScript("test", tc.dsl)
			if len(manuals) == 0 {
				t.Fatalf("expected a manual item for %q", tc.dsl)
			}
			if rows[0].Status != "manual" {
				t.Fatalf("expected manual status, got %+v", rows[0])
			}
		})
	}
}

func TestRewritePredicate(t *testing.T) {
	cases := []struct{ in, want string }{
		{`metadata.region == "eu"`, `meta.region == "eu"`},
		{`metadata["er-route"] == "us"`, `meta["er-route"] == "us"`},
		{`payload.x == "metadata.region"`, `payload.x == "metadata.region"`}, // literals untouched
		{`metadatax == 1`, `metadatax == 1`},                                 // whole-name boundary
		{`payload.metadata.deep == 1`, `payload.metadata.deep == 1`},         // non-root occurrence untouched
		{`payload.total > 20`, `payload.total > 20`},
	}
	for _, tc := range cases {
		if got := rewritePredicate(tc.in); got != tc.want {
			t.Errorf("rewritePredicate(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestGuardComposition(t *testing.T) {
	// Route order [us, eu, _default] with disjoint predicates.
	preds := []string{`meta.region == 'us'`, `meta.region == 'eu'`, "true"}
	if got := guardFor(0, preds); got != `meta.region == 'us'` {
		t.Errorf("guard0 = %q", got)
	}
	want := `meta.region == 'eu' && !(meta.region == 'us')`
	if got := guardFor(1, preds); got != want {
		t.Errorf("guard1 = %q, want %q", got, want)
	}
	want = `!(meta.region == 'us' || meta.region == 'eu')`
	if got := guardFor(2, preds); got != want {
		t.Errorf("guard2 = %q, want %q", got, want)
	}
}

// fixtures are every archived v2 example plus the two v2 testdata pipelines:
// three writing styles, YAML and HOCON, all v2 feature areas (route, fan-in,
// combined steps, edge delivery, dlq, flat pipeline). Names disambiguate the
// two linear.* files from different directories.
var fixtures = []struct{ path, name string }{
	{"../../legacy/_examples/01-linear-etl.yaml", "01-linear-etl"},
	{"../../legacy/_examples/02-route-branching.yaml", "02-route-branching"},
	{"../../legacy/_examples/03-fan-in.yaml", "03-fan-in"},
	{"../../legacy/_examples/04-http-webhook.yaml", "04-http-webhook"},
	{"../../legacy/_examples/05-transform-sink-combined.yaml", "05-transform-sink-combined"},
	{"../../legacy/_examples/06-edge-delivery.yaml", "06-edge-delivery"},
	{"../../legacy/_examples/07-flat-pipeline.yaml", "07-flat-pipeline"},
	{"../../legacy/_examples/08-hocon-linear.conf", "08-hocon-linear"},
	{"../../legacy/_examples/multi-pipeline/heartbeat.yaml", "mp-heartbeat"},
	{"../../legacy/_examples/multi-pipeline/metrics-ingest.yaml", "mp-metrics-ingest"},
	{"../../legacy/testdata/pipelines/linear.yaml", "testdata-linear-yaml"},
	{"../../legacy/testdata/pipelines/linear.conf", "testdata-linear-conf"},
}

// TestAcceptanceConvertedExamplesVerify: the M4 acceptance anchor — every
// fixture converts and the generated v3 config passes the real verify
// pipeline with the required env provided (§7.4 M4).
func TestAcceptanceConvertedExamplesVerify(t *testing.T) {
	t.Setenv("WEBHOOK_TARGET_URL", "https://httpbin.example/post")
	t.Setenv("PRIMARY_SINK_URL", "https://primary.example/api")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	for _, fx := range fixtures {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			data, err := os.ReadFile(fx.path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			res, err := Convert(fx.path, data)
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			if !res.Report.VerifyOK {
				for _, d := range res.Report.VerifyDiags {
					if d.Severity == "error" {
						t.Errorf("verify error: %s", d.Error())
					}
				}
			}
			// Determinism: same input, same output.
			again, err := Convert(fx.path, data)
			if err != nil {
				t.Fatalf("convert again: %v", err)
			}
			if string(again.YAML) != string(res.YAML) {
				t.Errorf("convert is not deterministic for %s", fx.path)
			}
		})
	}
}

// TestSnapshots pins the converter output (config + report) — run with
// -update to regenerate after an intentional change.
func TestSnapshots(t *testing.T) {
	update := *updateGoldens
	t.Setenv("WEBHOOK_TARGET_URL", "https://httpbin.example/post")
	t.Setenv("PRIMARY_SINK_URL", "https://primary.example/api")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	if err := os.MkdirAll(filepath.Join("testdata", "golden"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, fx := range fixtures {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			data, err := os.ReadFile(fx.path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			res, err := Convert(fx.path, data)
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			yamlWant := filepath.Join("testdata", "golden", fx.name+".yaml")
			reportWant := filepath.Join("testdata", "golden", fx.name+".report.md")
			if update {
				if err := os.WriteFile(yamlWant, res.YAML, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(reportWant, []byte(res.Report.Markdown()), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			wantYAML, err := os.ReadFile(yamlWant)
			if err != nil {
				t.Fatalf("golden missing (run go test ./internal/convert -update): %v", err)
			}
			if string(wantYAML) != string(res.YAML) {
				t.Errorf("yaml snapshot drift for %s:\n--- got ---\n%s\n--- want ---\n%s", fx.path, res.YAML, wantYAML)
			}
			wantReport, err := os.ReadFile(reportWant)
			if err != nil {
				t.Fatalf("golden report missing: %v", err)
			}
			if string(wantReport) != res.Report.Markdown() {
				t.Errorf("report snapshot drift for %s (run -update after review)", fx.path)
			}
		})
	}
}

func TestConvertRejectsBroken(t *testing.T) {
	// steps and pipeline are mutually exclusive in v2 (legacy loader rule).
	both := []byte("apiVersion: riverpod/v1\nkind: Pipeline\nmetadata: {name: x}\nsteps:\n  a:\n    source: {type: cron, config: {schedule: \"0 0 * * * *\"}}\npipeline:\n  - id: b\n    kind: sink\n    type: drop\n")
	if _, err := Convert("both.yaml", both); err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
	// Unknown fields are strict-decoded, like the v2 loader.
	unknown := []byte("apiVersion: riverpod/v1\nkind: Pipeline\nmetadata: {name: x}\nbogus_section: {}\nsteps:\n  a:\n    source: {type: cron, config: {schedule: \"0 0 * * * *\"}}\n")
	if _, err := Convert("unknown.yaml", unknown); err == nil {
		t.Fatal("expected unknown-field error")
	}
}

// The v2 codecs list + ref/inline codec references become v3 named
// declarations (§5.10): refs keep their name, inline configs synthesize one.
func TestConvertV2Codecs(t *testing.T) {
	src := []byte(`apiVersion: riverpod/v1
kind: Pipeline
metadata: {name: codecs}
codecs:
  - name: my-json
    type: json
    config: {}
steps:
  in:
    source:
      type: cron
      decoder: {ref: my-json}
      config: {schedule: "0 0 * * * *"}
  mid:
    depends_on: [in]
    transform:
      type: map
      config: {dsl: "payload.x = 1"}
  out:
    depends_on: [mid]
    sink:
      type: drop
      encoder: {type: json, config: {pretty: true}}
`)
	res, err := Convert("codecs.yaml", src)
	if err != nil {
		t.Fatal(err)
	}
	out := string(res.YAML)
	for _, want := range []string{
		"codecs:", "my-json:", "type: json", "out-encoder-codec:", "pretty: true",
		"decoder: my-json", "encoder: out-encoder-codec",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if !res.Report.VerifyOK {
		for _, d := range res.Report.VerifyDiags {
			if d.Severity == "error" {
				t.Errorf("verify error: %s", d.Error())
			}
		}
	}
}

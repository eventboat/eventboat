package ir

import (
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

func buildStr(t *testing.T, yaml string) (*Pipeline, []config.Diagnostic) {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	lr := config.LoadBytes("test.yaml", []byte(yaml))
	if lr.HasErrors() {
		return nil, lr.Diagnostics
	}
	return Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
}

func errCodes(diags []config.Diagnostic) []string {
	var out []string
	for _, d := range diags {
		if d.Severity == "error" {
			out = append(out, d.Code)
		}
	}
	return out
}

const csvPipeline = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: codecs-test}
codecs:
  events-csv:
    type: csv
    columns:
      - {name: id, type: int}
      - {name: amount, type: float}
sources:
  ingest:
    decoder: events-csv
    cron: {expression: "* * * * *", payload: "1,2.5"}
transforms:
  double:
    from: [ingest]
    script: |
      payload.amount = payload.amount * 2
sinks:
  out:
    from: [double]
    encoder: events-csv
    file: {path: out/out.csvl}
`

func TestCodecsSectionResolves(t *testing.T) {
	p, diags := buildStr(t, csvPipeline)
	for _, d := range diags {
		if d.Severity == "error" {
			t.Fatalf("unexpected error: %s", d.Error())
		}
	}
	if p == nil || p.Codecs["events-csv"] == nil {
		t.Fatal("named codec not resolved on the IR")
	}
}

func TestCodecsSectionShadowRejected(t *testing.T) {
	yaml := strings.Replace(csvPipeline, "  events-csv:\n    type: csv", "  json:\n    type: csv", 1)
	yaml = strings.ReplaceAll(yaml, "events-csv", "json")
	_, diags := buildStr(t, yaml)
	codes := errCodes(diags)
	found := false
	for _, c := range codes {
		if c == "cfg_codec_shadow" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cfg_codec_shadow, got %v (diags: %v)", codes, diags)
	}
}

func TestCodecsSectionUnknownType(t *testing.T) {
	yaml := strings.Replace(csvPipeline, "type: csv", "type: nope", 1)
	_, diags := buildStr(t, yaml)
	codes := errCodes(diags)
	found := false
	for _, c := range codes {
		if c == "plugin_schema" { // unknown type surfaces through the schema-diag path
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a schema diagnostic for the unknown codec type, got %v", codes)
	}
}

func TestCodecsSectionConfigSchemaError(t *testing.T) {
	// csv requires columns or header — an empty config fails validation.
	yaml := strings.Replace(csvPipeline,
		"    columns:\n      - {name: id, type: int}\n      - {name: amount, type: float}", "", 1)
	_, diags := buildStr(t, yaml)
	codes := errCodes(diags)
	found := false
	for _, c := range codes {
		if c == "plugin_schema" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected plugin_schema for invalid codec config, got %v", codes)
	}
}

func TestDecoderBareConfigCodecRejected(t *testing.T) {
	// `decoder: csv` bare: csv needs configuration — codec_config, not
	// codec_unknown.
	yaml := strings.ReplaceAll(csvPipeline, "decoder: events-csv", "decoder: csv")
	_, diags := buildStr(t, yaml)
	codes := errCodes(diags)
	found := false
	for _, c := range codes {
		if c == "codec_config" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected codec_config for bare csv decoder, got %v", codes)
	}
}

func TestUnknownDecoderStillReported(t *testing.T) {
	yaml := strings.ReplaceAll(csvPipeline, "decoder: events-csv", "decoder: ghost-codec")
	_, diags := buildStr(t, yaml)
	codes := errCodes(diags)
	found := false
	for _, c := range codes {
		if c == "codec_unknown" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected codec_unknown, got %v", codes)
	}
}

package config

import "testing"

// The telemetry section (§5.10, beta round): redact globs + span sampling
// rate, strict whitelist, nil when absent.
func TestTelemetrySectionParsing(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
telemetry:
  redact:
    - payload.user.email
    - meta.authorization
  span_sample_rate: 0.05
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	tel := res.Pipeline.Telemetry
	if tel == nil {
		t.Fatal("telemetry section not parsed")
	}
	if len(tel.Redact) != 2 || tel.Redact[0] != "payload.user.email" {
		t.Fatalf("redact = %v", tel.Redact)
	}
	if tel.SpanSampleRate != 0.05 {
		t.Fatalf("span_sample_rate = %v", tel.SpanSampleRate)
	}
}

func TestTelemetrySectionErrors(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
telemetry:
  redact: payload.user.email
  span_sample_rate: 1.5
  bogus: true
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`))
	codes := map[string]bool{}
	for _, d := range res.Diagnostics {
		codes[d.Code] = true
	}
	if !codes["cfg_telemetry_redact"] {
		t.Errorf("missing cfg_telemetry_redact (non-list), got %v", codes)
	}
	if !codes["cfg_telemetry_span_rate"] {
		t.Errorf("missing cfg_telemetry_span_rate (out of range), got %v", codes)
	}
	if !codes["cfg_unknown_field"] {
		t.Errorf("missing cfg_unknown_field (bogus key), got %v", codes)
	}
}

func TestTelemetryAbsentMeansNil(t *testing.T) {
	res := LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`))
	if res.HasErrors() {
		t.Fatal(res.Diagnostics)
	}
	if res.Pipeline.Telemetry != nil {
		t.Fatalf("telemetry = %+v, want nil default", res.Pipeline.Telemetry)
	}
}

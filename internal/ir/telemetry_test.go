package ir

import "testing"

// telemetry.redact patterns are verified: a bad glob would silently never
// match, so it is a verify error (telemetry_redact_pattern).
func TestTelemetryRedactPatternVerified(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
telemetry:
  redact: [payload.user.email, "payload.a[bad"]
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`)
	if !hasCode(diags, "telemetry_redact_pattern") {
		t.Fatalf("expected telemetry_redact_pattern, got %+v", diags)
	}
	if pip, _ := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
telemetry:
  redact: [payload.user.email, "payload.card*", "payload.items.*.sku"]
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: b } }
`); pip == nil {
		t.Fatal("valid redact patterns must verify clean")
	}
}

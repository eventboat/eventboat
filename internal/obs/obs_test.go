package obs

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The Prometheus reader exposes the recorded metrics at the exposition
// endpoint in text format, with the eventboat_ prefix intact.
func TestPrometheusExposition(t *testing.T) {
	o, err := Setup(context.Background(), Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()

	o.RecordMessageIn("p1", "in")
	o.RecordSettled("p1", 0)
	o.RecordDeadLetter("p1", "out", ReasonClass("script: boom"))
	o.RecordScript("p1", "t", time.Millisecond, true)
	o.RecordJobStart("p1", "schedule")
	o.RecordJobEnd("p1", "success", 0, 3, 3, 0)
	o.RecordOverlapSkip("p1")
	o.SetGauges("p1", 2, 4, false)

	h := o.Handler()
	if h == nil {
		t.Fatal("prometheus handler is nil")
	}
	// Force a collection cycle (the reader serves on scrape).
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"eventboat_messages_in_total",
		"eventboat_messages_settled_total",
		"eventboat_dead_letter_total",
		`reason_class="script"`,
		"eventboat_script_step_budget_exhausted_total",
		"eventboat_jobs_started_total",
		"eventboat_jobs_completed_total",
		"eventboat_job_rows_read_total",
		"eventboat_jobs_overlap_skipped_total",
		"eventboat_in_flight_messages",
		"eventboat_spool_depth",
		`pipeline="p1"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %s", want)
		}
	}
}

// A nil *Obs is inert: every helper must be safe (telemetry disabled).
func TestNilObsIsInert(t *testing.T) {
	var o *Obs
	o.RecordMessageIn("p", "s")
	o.RecordSettled("p", 0)
	o.RecordDeadLetter("p", "n", "script")
	o.RecordScript("p", "n", 0, false)
	o.RecordSinkWrite("p", "n", 0)
	o.RecordJobStart("p", "manual")
	o.RecordJobEnd("p", "success", 0, 0, 0, 0)
	o.RecordOverlapSkip("p")
	o.RecordCatchupSkip("p")
	o.SetGauges("p", 0, 0, false)
	o.RecordCelError("p", "e", "cesql")
	o.RecordNoMatch("p", "n")
	o.RecordRetry("p", "n")
	o.RecordOptionalDrop("p", "e")
	o.RecordDecodeError("p", "s")
	o.RecordSpoolFailure("p")
	o.RecordBackpressure("p", "s")
	o.RecordDlqFailure("p")
	if o.Handler() != nil {
		t.Error("nil obs must not expose a handler")
	}
	_ = o.Shutdown(context.Background())
	if o.Tracer() == nil {
		t.Error("nil obs must still provide a (noop) tracer")
	}
}

// Fully disabled telemetry yields a noop provider set without error.
func TestDisabledSetup(t *testing.T) {
	o, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if o.provider != nil {
		t.Error("disabled telemetry must not build a real provider")
	}
	// Recording is harmless.
	o.RecordMessageIn("p", "s")
	if o.Handler() != nil {
		t.Error("disabled telemetry must not expose /metrics")
	}
}

// OTLP push export constructs without contacting the endpoint at setup
// (export happens in the background and fails soft).
func TestOTLPSetupLazy(t *testing.T) {
	o, err := Setup(context.Background(), Config{Prometheus: true, OTLPEndpoint: "http://127.0.0.1:1/otlp/v1/metrics", SampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()
	o.RecordMessageIn("p", "s")
	_, _ = io.Discard, o.Handler()
}

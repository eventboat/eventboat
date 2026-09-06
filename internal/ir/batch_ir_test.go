package ir

import "testing"

// run.mode: batch warns when no source can exhaust: the run would hang until
// cancelled. A file source with on_eof: stop is the canonical finite source.
func TestBatchNoFiniteSource(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: hang }
run: { mode: batch }
sources:
  in:
    decoder: json
    kafka: { brokers: [ "b:9092" ], topics: [ t ] }
sinks:
  out: { depends_on: [in], file: { path: out.jsonl } }
`)
	if !hasCode(diags, "batch_no_finite_source") {
		t.Fatalf("expected batch_no_finite_source, got %+v", diags)
	}
}

func TestBatchFiniteSourceClean(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: drain }
run: { mode: batch }
sources:
  in: { decoder: json, file: { path: in.jsonl, on_eof: stop } }
sinks:
  out: { depends_on: [in], file: { path: out.jsonl } }
`)
	for _, d := range diags {
		if d.Code == "batch_no_finite_source" {
			t.Fatalf("finite batch pipeline warned: %+v", diags)
		}
	}
}

// A file source serving a job run must actually finish: without on_eof:stop
// the run tails forever and the job never completes.
func TestJobFileSourceNoEof(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: tailjob }
run: { mode: job }
sources:
  in: { decoder: json, file: { path: in.jsonl } }
sinks:
  out: { depends_on: [in], file: { path: out.jsonl } }
`)
	if !hasCode(diags, "job_file_source_no_eof") {
		t.Fatalf("expected job_file_source_no_eof, got %+v", diags)
	}

	_, diags = build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: drainjob }
run: { mode: job }
sources:
  in: { decoder: json, file: { path: in.jsonl, on_eof: stop } }
sinks:
  out: { depends_on: [in], file: { path: out.jsonl } }
`)
	for _, d := range diags {
		if d.Code == "job_file_source_no_eof" {
			t.Fatalf("on_eof: stop still warned: %+v", diags)
		}
	}
}

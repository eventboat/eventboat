package ir

import (
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/starhost"
)

func loadBytes(t *testing.T, yamlText string) *config.Pipeline {
	t.Helper()
	lr := config.LoadBytes("p.yaml", []byte(yamlText))
	if lr.HasErrors() {
		t.Fatalf("config errors:\n%+v", lr.Diagnostics)
	}
	return lr.Pipeline
}

func defaultStarOpts() starhost.Options { return starhost.DefaultOptions() }

// A job pipeline whose source lacks pull capability is a verify error.
func TestJobSourceMustBePull(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: badjob }
run: { mode: job, schedule: "0 1 * * *" }
sources:
  pull:
    decoder: json
    file: { path: in.jsonl }
sinks:
  out: { from: [pull], file: { path: out.jsonl } }
`)
	if !hasCode(diags, "job_source_not_pull") {
		t.Fatalf("expected job_source_not_pull, got %+v", diags)
	}
}

// Bad cron is caught at verify (§3.1 item 4). The pull capability check
// cannot use file, so only the schedule error is exercised here — the
// capability path is covered by TestJobSourceMustBePull and the sql tests.
func TestJobBadScheduleRejected(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: badcron }
run: { mode: job, schedule: "not cron" }
sources:
  pull:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", cursor: { column: id } }
sinks:
  out: { from: [pull], file: { path: out.jsonl } }
`)
	if !hasCode(diags, "job_bad_schedule") {
		t.Fatalf("expected job_bad_schedule, got %+v", diags)
	}
}

// Parameters references in continuous pipelines are errors; in job
// pipelines they must name declared parameters.
func TestParametersReferenceLegality(t *testing.T) {
	// Continuous pipeline referencing parameters binding in a script.
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: cont }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  t:
    from: [in]
    script: |
      payload.x = parameters.threshold
sinks:
  out: { from: [t], file: { path: o } }
`)
	if !hasCode(diags, "job_parameters_in_continuous") {
		t.Fatalf("expected job_parameters_in_continuous, got %+v", diags)
	}

	// Continuous pipeline referencing parameters in a when.
	_, diags = build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: cont2 }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: { in: { when: 'payload.x > parameters.threshold' } }, file: { path: o } }
`)
	if !hasCode(diags, "job_parameters_in_continuous") {
		t.Fatalf("expected job_parameters_in_continuous for when, got %+v", diags)
	}

	// Job pipeline: undeclared ${parameters.x} token.
	_, diags = build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: job1 }
run: { mode: job }
parameters:
  from: { type: string, default: cursor }
sources:
  pull:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", args: { a: "${parameters.nope}" }, cursor: { column: id } }
sinks:
  out: { from: [pull], file: { path: o } }
`)
	if !hasCode(diags, "job_parameter_unknown") {
		t.Fatalf("expected job_parameter_unknown, got %+v", diags)
	}
}

// Hooks are validated against sink plugin schemas.
func TestHookSinkSchemaValidated(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: jobhooks }
run: { mode: job }
hooks:
  failure: { http: { not_a_field: 1 } }
sources:
  pull:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", cursor: { column: id } }
sinks:
  out: { from: [pull], file: { path: o } }
`)
	if !hasCode(diags, "plugin_schema") {
		t.Fatalf("expected plugin_schema for hook sink, got %+v", diags)
	}

	_, diags = build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: jobhooks2 }
run: { mode: job }
hooks:
  failure: { nosuchplugin: {} }
sources:
  pull:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", cursor: { column: id } }
sinks:
  out: { from: [pull], file: { path: o } }
`)
	if !hasCode(diags, "plugin_unknown") {
		t.Fatalf("expected plugin_unknown for hook sink, got %+v", diags)
	}
}

// sql (pull) source in a continuous pipeline is a warning, not an error.
func TestSqlInContinuousIsWarning(t *testing.T) {
	pip, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: contsql }
sources:
  db:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", cursor: { column: id } }
sinks:
  out: { from: [db], file: { path: o } }
`)
	if pip == nil {
		t.Fatalf("continuous sql pipeline must verify: %+v", diags)
	}
	found := false
	for _, d := range diags {
		if d.Code == "lint_sql_continuous" && d.Severity == "warning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected lint_sql_continuous warning, got %+v", diags)
	}
}

// A job pipeline with declared parameters binds them into scripts and
// predicates; resolved actuals override defaults when provided.
func TestJobPipelineBuildsWithParameters(t *testing.T) {
	lr := loadBytes(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: jobok }
run: { mode: job }
parameters:
  threshold: { type: integer, default: 100 }
sources:
  pull:
    decoder: json
    sql: { driver: sqlite, dsn: file:x.db, query: "SELECT 1 AS id", cursor: { column: id } }
transforms:
  t:
    from: [pull]
    script: |
      payload.big = payload.v > parameters.threshold
sinks:
  out:
    from: { t: { when: 'payload.big' } }
    file: { path: o }
`)
	pip, diags := Build(lr, testReg(t), defaultStarOpts(), nil)
	if pip == nil {
		t.Fatalf("build failed: %+v", diags)
	}
	if pip.Parameters["threshold"] != 100 {
		t.Errorf("default parameter not bound: %+v", pip.Parameters)
	}

	// Trigger-time actuals flow through the fourth argument.
	pip2, diags := Build(lr, testReg(t), defaultStarOpts(), map[string]any{"threshold": 250})
	if pip2 == nil {
		t.Fatalf("build with actuals failed: %+v", diags)
	}
	if pip2.Parameters["threshold"] != 250 {
		t.Errorf("actual parameter not bound: %+v", pip2.Parameters)
	}
}

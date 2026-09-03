package config

import (
	"strings"
	"testing"
	"time"
)

const jobPipeline = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: nightly }
run:
  mode: job
  schedule: "0 1 * * *"
  overlap: skip
  catchup_window: 2h
  skip_if_successful: true
  retention: { history: 90d }
parameters:
  from: { type: string, default: cursor }
  to:   { type: string, default: now }
  region: { type: string, enum: [eu, us], default: eu }
hooks:
  failure: { http: { url: "https://alert.example.com/hook" } }
sources:
  pull: { decoder: json, file: { path: in.jsonl } }
sinks:
  out: { from: [pull], file: { path: out.jsonl } }
`

func TestJobSectionsParse(t *testing.T) {
	res := LoadBytes("job.yaml", []byte(jobPipeline))
	if res.HasErrors() {
		t.Fatalf("unexpected errors: %+v", res.Diagnostics)
	}
	p := res.Pipeline
	if !p.IsJob() {
		t.Fatalf("IsJob = false, run = %+v", p.Run)
	}
	r := p.Run
	if r.Schedule != "0 1 * * *" || r.Overlap != "skip" || !r.SkipIfSuccessful {
		t.Errorf("run spec = %+v", r)
	}
	if r.CatchupWindow != 2*time.Hour {
		t.Errorf("catchup_window = %v", r.CatchupWindow)
	}
	if r.Retention != 90*24*time.Hour {
		t.Errorf("retention = %v", r.Retention)
	}
	if len(p.Parameters) != 3 {
		t.Fatalf("parameters = %+v", p.Parameters)
	}
	if spec := p.Parameters["from"]; spec.Type != "string" || spec.Default != "cursor" {
		t.Errorf("from = %+v", spec)
	}
	if spec := p.Parameters["region"]; len(spec.Enum) != 2 {
		t.Errorf("region enum = %+v", spec)
	}
	if p.Hooks == nil || p.Hooks.Failure == nil || p.Hooks.Failure.Plugin != "http" {
		t.Errorf("hooks = %+v", p.Hooks)
	}
	if p.Hooks.Failure.PluginConfig["url"] != "https://alert.example.com/hook" {
		t.Errorf("hook sink config = %+v", p.Hooks.Failure.PluginConfig)
	}
}

func TestJobSectionsValidation(t *testing.T) {
	base := func(body string) *Result {
		return LoadBytes("job.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: nightly }
`+body+`
sources:
  pull: { decoder: json, file: { path: in.jsonl } }
sinks:
  out: { from: [pull], file: { path: out.jsonl } }
`))
	}
	cases := []struct {
		name string
		body string
		code string
		part string
	}{
		{"bad mode", `run: { mode: batch }`, "cfg_run_mode", ""},
		{"schedule needs job mode", `run: { mode: continuous, schedule: "0 1 * * *" }`, "cfg_run_schedule", "only meaningful"},
		{"bad overlap", `run: { mode: job, overlap: cancel }`, "cfg_run_overlap", ""},
		{"bad catchup", `run: { mode: job, catchup_window: forever }`, "cfg_run_catchup", ""},
		{"skip not bool", `run: { mode: job, skip_if_successful: yes_please }`, "cfg_run_skip", ""},
		{"retention not mapping", `run: { mode: job, retention: 90d }`, "cfg_run_retention", ""},
		{"unknown run field", `run: { mode: job, retries: 9 }`, "cfg_unknown_field", ""},
		{"parameters outside job", `parameters: { from: { type: string } }`, "cfg_parameters_not_job", ""},
		{"param bad type", "run: { mode: job }\nparameters: { from: { type: float } }", "cfg_parameters_decl", "type must be one of"},
		{"param default mismatch", "run: { mode: job }\nparameters: { n: { type: integer, default: abc } }", "cfg_parameters_decl", "does not match type"},
		{"default outside enum", "run: { mode: job }\nparameters: { region: { type: string, enum: [eu, us], default: apac } }", "cfg_parameters_decl", "not one of the enum"},
		{"required with default", "run: { mode: job }\nparameters: { from: { type: string, required: true, default: x } }", "cfg_parameters_decl", "cannot declare a default"},
		{"bad pattern", "run: { mode: job }\nparameters: { d: { type: string, pattern: '[' } }", "cfg_parameters_decl", "does not compile"},
		{"unknown hook", "run: { mode: job }\nhooks: { teardown: { drop: {} } }", "cfg_unknown_field", "unknown hook"},
		{"hook two plugins", "run: { mode: job }\nhooks: { failure: { http: { url: x }, drop: {} } }", "cfg_hooks_sink", "exactly one"},
	}
	for _, tc := range cases {
		res := base(tc.body)
		found := false
		for _, d := range res.Diagnostics {
			if d.Code == tc.code && (tc.part == "" || strings.Contains(d.Message, tc.part)) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: want %s mentioning %q, got %+v", tc.name, tc.code, tc.part, res.Diagnostics)
		}
	}
}

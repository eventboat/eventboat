package ir

import (
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

func testReg(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

func build(t *testing.T, yamlText string) (*Pipeline, []config.Diagnostic) {
	t.Helper()
	lr := config.LoadBytes("p.yaml", []byte(yamlText))
	if lr.HasErrors() {
		return nil, lr.Diagnostics
	}
	return Build(lr.Pipeline, testReg(t), starhost.DefaultOptions())
}

func hasCode(diags []config.Diagnostic, code string) bool {
	for _, d := range diags {
		if d.Code == code {
			return true
		}
	}
	return false
}

func TestBuildHappyPath(t *testing.T) {
	pip, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a.jsonl } }
transforms:
  enrich:
    from: [in]
    script: |
      payload.total = payload.price * payload.qty
sinks:
  out:
    from: { enrich: { when: 'meta.region == "eu"' } }
    file: { path: out.jsonl }
`)
	if pip == nil {
		t.Fatalf("build failed: %+v", diags)
	}
	node := pip.Nodes["out"]
	if len(node.In) != 1 || node.In[0].When == nil {
		t.Fatalf("when predicate not compiled: %+v", node.In)
	}
	if node.In[0].Required != true || node.In[0].Retries != 3 {
		t.Errorf("edge defaults not applied: %+v", node.In[0])
	}
	if pip.Nodes["enrich"].Script == nil {
		t.Error("script not compiled")
	}
}

func TestCycleDetected(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  a: { from: [in, b], script: "payload.x = 1" }
  b: { from: [a], script: "payload.x = 2" }
sinks:
  out: { from: [b], file: { path: o } }
`)
	if !hasCode(diags, "topo_cycle") {
		t.Fatalf("expected topo_cycle, got %+v", diags)
	}
}

func TestMissingReferenceAndOrphan(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  t: { from: [nosuch], script: "payload.x = 1" }
sinks:
  out: { from: [t], file: { path: o } }
`)
	if !hasCode(diags, "topo_missing_ref") {
		t.Fatalf("expected topo_missing_ref, got %+v", diags)
	}
}

func TestNoSourceSinkPath(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
  unused: { decoder: json, file: { path: b } }
sinks:
  out: { from: [in], file: { path: o } }
`)
	if !hasCode(diags, "topo_orphan") {
		t.Fatalf("expected topo_orphan for unreachable source, got %+v", diags)
	}
}

func TestDuplicateNameAcrossSections(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  node: { decoder: json, file: { path: a } }
transforms:
  node: { from: [node], script: "payload.x = 1" }
sinks:
  out: { from: [node], file: { path: o } }
`)
	if !hasCode(diags, "topo_dup_name") {
		t.Fatalf("expected topo_dup_name, got %+v", diags)
	}
}

func TestSinkAsUpstreamRejected(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: [in], file: { path: o } }
  out2: { from: [out], file: { path: o2 } }
`)
	if !hasCode(diags, "topo_sink_as_upstream") {
		t.Fatalf("expected topo_sink_as_upstream, got %+v", diags)
	}
}

func TestRouteSugarCompilesAndDanglingDetected(t *testing.T) {
	// No meta.route assignment upstream => dangling route error.
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  classify:
    from: [in]
    script: |
      payload.x = 1
sinks:
  vip: { from: { classify: { route: high-value } }, file: { path: o } }
  rest: { from: [classify], file: { path: o2 } }
`)
	if !hasCode(diags, "expr_route_dangling") {
		t.Fatalf("expected expr_route_dangling, got %+v", diags)
	}

	// With the assignment present, route compiles to a when predicate.
	pip, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  classify:
    from: [in]
    script: |
      if payload.total > 100:
          meta.route = "high-value"
      else:
          meta.route = "standard"
sinks:
  vip: { from: { classify: { route: high-value } }, file: { path: o } }
  rest: { from: [classify], file: { path: o2 } }
`)
	if pip == nil {
		t.Fatalf("build failed: %+v", diags)
	}
	edge := pip.Nodes["vip"].In[0]
	if edge.When == nil || edge.RouteName != "high-value" {
		t.Fatalf("route not compiled: %+v", edge)
	}
	if edge.WhenSource != `meta.route == "high-value"` {
		t.Errorf("route sugar compiled to %q", edge.WhenSource)
	}
}

func TestCelCompileErrorDiagnostic(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: { in: { when: 'meta.region == "eu" &&' } }, file: { path: o } }
`)
	if !hasCode(diags, "expr_cel_compile") {
		t.Fatalf("expected expr_cel_compile, got %+v", diags)
	}
}

func TestStarlarkCompileErrorDiagnostic(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { decoder: json, file: { path: a } }
transforms:
  t: { from: [in], script: "payload.x = nosuch" }
sinks:
  out: { from: [t], file: { path: o } }
`)
	if !hasCode(diags, "expr_starlark_compile") {
		t.Fatalf("expected expr_starlark_compile, got %+v", diags)
	}
}

func TestPluginSchemaUnknownFieldIsError(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in:
    decoder: json
    file: { path: a, bogus_field: 1 }
sinks:
  out: { from: [in], file: { path: o } }
`)
	if !hasCode(diags, "plugin_schema") {
		t.Fatalf("expected plugin_schema, got %+v", diags)
	}
	for _, d := range diags {
		if d.Code == "plugin_schema" && strings.Contains(d.Message, "bogus_field") {
			return // diagnostic names the offending field
		}
	}
	t.Errorf("plugin_schema diagnostic should name bogus_field: %+v", diags)
}

func TestLintWarnsOnLiteralAndUnusedConstant(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
constants:
  unused_one: 7
sources:
  in: { decoder: json, file: { path: a } }
sinks:
  out: { from: { in: { when: 'true' } }, file: { path: o } }
`)
	if !hasCode(diags, "lint_when_literal") || !hasCode(diags, "lint_constant_unused") {
		t.Fatalf("expected lint warnings, got %+v", diags)
	}
	for _, d := range diags {
		if d.Code == "lint_when_literal" && d.Severity != "warning" {
			t.Errorf("lint must be warning severity, got %q", d.Severity)
		}
	}
}

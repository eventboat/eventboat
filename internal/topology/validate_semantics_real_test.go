package topology_test

import (
	"strings"
	"testing"

	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/topology"
	_ "github.com/edgesets/edgestream/plugins/all"
)

func realPluginsFixture() *topology.TopologyIR {
	return &topology.TopologyIR{
		Name: "real-plugins",
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "cron", Config: map[string]any{"schedule": "0 0 * * * *"}},
			{ID: "tr", Kind: topology.KindTransform, Type: "map", Config: map[string]any{"dsl": "payload.x = 1"}},
			{ID: "out", Kind: topology.KindSink, Type: "drop"},
		},
		Edges: []topology.EdgeIR{{From: "src", To: "tr"}, {From: "tr", To: "out"}},
	}
}

// A map transform whose DSL does not compile must fail validate with the
// stage name in the error.
func TestValidateSemanticsRealMapDSLCompileError(t *testing.T) {
	ir := realPluginsFixture()
	ir.Stages[1].Config = map[string]any{"dsl": "payload.x = ("}
	err := topology.ValidateSemantics(ir, registry.Default)
	if err == nil {
		t.Fatal("expected map DSL compile error")
	}
	if !strings.Contains(err.Error(), `stage "tr"`) {
		t.Fatalf("error %q does not name the stage", err)
	}
}

// A filter DSL with a CEL type error (timestamp + int has no overload) must
// fail validate with the stage name in the error.
func TestValidateSemanticsRealFilterCELTypeError(t *testing.T) {
	ir := realPluginsFixture()
	ir.Stages[1].Type = "filter"
	ir.Stages[1].Config = map[string]any{"dsl": "now + 1"}
	err := topology.ValidateSemantics(ir, registry.Default)
	if err == nil {
		t.Fatal("expected filter CEL type error")
	}
	if !strings.Contains(err.Error(), `stage "tr"`) {
		t.Fatalf("error %q does not name the stage", err)
	}
}

// A route transform with an uncompilable route expression must fail with the
// stage name in the error.
func TestValidateSemanticsRealRouteCompileError(t *testing.T) {
	ir := realPluginsFixture()
	ir.Stages[1].Type = "route"
	ir.Stages[1].Config = map[string]any{"routes": map[string]any{"a": "payload.x + "}}
	err := topology.ValidateSemantics(ir, registry.Default)
	if err == nil {
		t.Fatal("expected route DSL compile error")
	}
	if !strings.Contains(err.Error(), `stage "tr"`) {
		t.Fatalf("error %q does not name the stage", err)
	}
}

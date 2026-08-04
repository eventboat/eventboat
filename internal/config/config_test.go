package config_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/topology"
)

func testdata(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "pipelines", name)
}

func TestLinearYAML_Validate(t *testing.T) {
	cfg, err := config.LoadYAML(testdata(t, "linear.yaml"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	if ir.Name != "order-processing" {
		t.Fatalf("name = %q", ir.Name)
	}
	if len(ir.Stages) != 4 {
		t.Fatalf("stages = %d, want 4", len(ir.Stages))
	}
	if len(ir.Edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(ir.Edges))
	}
}

func TestFlatPipelineYAML_Validate(t *testing.T) {
	cfg := &config.PipelineConfig{
		Metadata: map[string]string{"name": "flat-pipeline"},
		Pipeline: []config.StageConfig{
			{ID: "in", Kind: "source", Type: "cron", Config: map[string]any{"schedule": "0 */1 * * * *"}},
			{ID: "enrich", Kind: "transform", Type: "map", DependsOn: config.DependsOnList{{Upstream: "in"}}, Config: map[string]any{"dsl": "payload.x = 1"}},
			{ID: "out", Kind: "sink", Type: "drop", DependsOn: config.DependsOnList{{Upstream: "enrich"}}},
		},
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	if len(ir.Stages) != 3 {
		t.Fatalf("stages = %d, want 3", len(ir.Stages))
	}
	if len(ir.Edges) != 2 {
		t.Fatalf("edges = %d, want 2", len(ir.Edges))
	}
}

func TestStepsAndPipelineMutuallyExclusive(t *testing.T) {
	cfg := &config.PipelineConfig{
		Steps: map[string]config.StepConfig{
			"in": {Source: &config.SourceBlock{Type: "cron"}},
		},
		Pipeline: []config.StageConfig{
			{ID: "out", Kind: "sink", Type: "drop"},
		},
	}
	_, err := topology.FromConfig(cfg)
	if err == nil {
		t.Fatal("expected error for steps and pipeline together")
	}
}

func TestDependsOnMap_ExpandRoute(t *testing.T) {
	cfg := &config.PipelineConfig{
		Metadata: map[string]string{"name": "route-test"},
		Steps: map[string]config.StepConfig{
			"in": {Source: &config.SourceBlock{Type: "cron", Config: map[string]any{"schedule": "0 */1 * * * *"}}},
			"splitter": {
				DependsOn: config.DependsOnList{{Upstream: "in"}},
				Transform: &config.TransformBlock{
					Type: "route",
					Config: map[string]any{
						"routes": map[string]any{
							"us":       "true",
							"_default": "false",
						},
					},
				},
			},
			"us-sink": {
				DependsOn: config.DependsOnList{{
					Upstream: "splitter",
					Edge:     &config.EdgeAttrs{Route: "us"},
				}},
				Sink: &config.SinkBlock{Type: "drop"},
			},
		},
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var cond string
	for _, e := range ir.Edges {
		if e.From == "splitter" && e.To == "us-sink" {
			cond = e.Condition
		}
	}
	want := `metadata["er-route"] == "us"`
	if cond != want {
		t.Fatalf("condition = %q, want %q", cond, want)
	}
}

func TestStepsTransformSinkCombo(t *testing.T) {
	cfg := &config.PipelineConfig{
		Metadata: map[string]string{"name": "combo"},
		Steps: map[string]config.StepConfig{
			"in": {Source: &config.SourceBlock{Type: "cron", Config: map[string]any{"schedule": "0 */1 * * * *"}}},
			"enrich": {
				DependsOn: config.DependsOnList{{Upstream: "in"}},
				Transform: &config.TransformBlock{Type: "map", Config: map[string]any{"dsl": "payload.x = 1"}},
				Sink:      &config.SinkBlock{Type: "drop"},
			},
		},
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hasInternal := false
	for _, e := range ir.Edges {
		if e.From == "enrich" && e.To == "enrich-sink" {
			hasInternal = true
		}
	}
	if !hasInternal {
		t.Fatal("missing internal edge enrich -> enrich-sink")
	}
}

package topology

import (
	"testing"

	"github.com/edgesets/edgestream/internal/config"
)

func TestValidateRejectsCycle(t *testing.T) {
	ir := &TopologyIR{
		Name: "bad",
		Stages: []StageIR{
			{ID: "a", Kind: KindTransform},
			{ID: "b", Kind: KindTransform},
			{ID: "src", Kind: KindSource},
			{ID: "sink", Kind: KindSink},
		},
		Edges: []EdgeIR{
			{From: "src", To: "a"},
			{From: "a", To: "b"},
			{From: "b", To: "a"},
			{From: "b", To: "sink"},
		},
	}
	if err := Validate(ir); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateRejectsMissingSinkPath(t *testing.T) {
	ir := &TopologyIR{
		Name: "bad",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource},
			{ID: "orphan", Kind: KindTransform},
			{ID: "sink", Kind: KindSink},
		},
		Edges: []EdgeIR{
			{From: "src", To: "orphan"},
		},
	}
	if err := Validate(ir); err == nil {
		t.Fatal("expected validation error for disconnected sink")
	}
}

func TestValidateRejectsUnknownCodecRef(t *testing.T) {
	ir := &TopologyIR{
		Name: "bad",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource, Decoder: &config.CodecRef{Ref: "missing"}},
			{ID: "sink", Kind: KindSink},
		},
		Edges:  []EdgeIR{{From: "src", To: "sink"}},
		Codecs: map[string]CodecIR{},
	}
	if err := Validate(ir); err == nil {
		t.Fatal("expected unknown codec ref error")
	}
}

func TestValidateAcceptsLinearPipeline(t *testing.T) {
	ir := &TopologyIR{
		Name: "ok",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource},
			{ID: "tr", Kind: KindTransform},
			{ID: "sink", Kind: KindSink},
		},
		Edges: []EdgeIR{
			{From: "src", To: "tr"},
			{From: "tr", To: "sink"},
		},
	}
	if err := Validate(ir); err != nil {
		t.Fatal(err)
	}
}

// A sink referenced by pipeline-level dlq.sink legitimately has no incoming
// edge (the engine writes to it directly on failure).
func TestValidateAcceptsPipelineDLQSinkWithoutIncomingEdge(t *testing.T) {
	ir := &TopologyIR{
		Name: "dlq-sink",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource},
			{ID: "out", Kind: KindSink},
			{ID: "dlq-sink", Kind: KindSink},
		},
		Edges: []EdgeIR{{From: "src", To: "out"}},
		DLQ:   &config.DLQConfig{Sink: "dlq-sink"},
	}
	if err := Validate(ir); err != nil {
		t.Fatalf("pipeline-level DLQ sink without incoming edge rejected: %v", err)
	}
}

// Same exemption for an edge-level delivery.dlq reference.
func TestValidateAcceptsEdgeDLQSinkWithoutIncomingEdge(t *testing.T) {
	ir := &TopologyIR{
		Name: "edge-dlq-sink",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource},
			{ID: "out", Kind: KindSink},
			{ID: "edge-dlq", Kind: KindSink},
		},
		Edges: []EdgeIR{{
			From: "src", To: "out",
			Delivery: &config.DeliverySpec{DLQ: "edge-dlq"},
		}},
	}
	if err := Validate(ir); err != nil {
		t.Fatalf("edge DLQ sink without incoming edge rejected: %v", err)
	}
}

// Unreferenced non-source stages are still rejected: the exemption applies
// only to stages named by dlq.sink / delivery.dlq.
func TestValidateStillRejectsUnreferencedStageWithoutIncomingEdge(t *testing.T) {
	ir := &TopologyIR{
		Name: "orphan",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource},
			{ID: "out", Kind: KindSink},
			{ID: "orphan", Kind: KindTransform},
		},
		Edges: []EdgeIR{{From: "src", To: "out"}},
	}
	if err := Validate(ir); err == nil {
		t.Fatal("expected rejection of unreferenced stage without incoming edges")
	}
}

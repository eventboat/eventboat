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
		Edges: []EdgeIR{{From: "src", To: "sink"}},
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

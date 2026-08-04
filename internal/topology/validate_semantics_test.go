package topology

import (
	"context"
	"strings"
	"testing"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	cel "github.com/google/cel-go/cel"
)

// stub stages and codec for exercising ValidateSemantics without the real
// plugin set.

type stubSource struct {
	basestage.Base
}

func (s *stubSource) Consume(context.Context, chan<- *message.Message) error { return nil }

type stubTransform struct {
	basestage.Base
}

func (s *stubTransform) Process(context.Context, []*message.Message) ([]*message.Message, error) {
	return nil, nil
}

type stubSink struct {
	basestage.Base
}

func (s *stubSink) Write(context.Context, []*message.Message) error { return nil }
func (s *stubSink) Flush(context.Context) error                     { return nil }

type stubCodec struct{}

func (c stubCodec) Name() string                        { return "stub" }
func (c stubCodec) Decode([]byte) (any, error)          { return nil, nil }
func (c stubCodec) Encode(any) ([]byte, error)          { return nil, nil }
func (c stubCodec) OutputType() *cel.Type               { return nil }
func (c stubCodec) ValidateConfig(map[string]any) error { return nil }

// semanticsRegistry returns a registry with every type referenced by
// semanticsFixture registered, so negative tests can swap one type out.
func semanticsRegistry() *registry.Registry {
	reg := registry.New()
	reg.RegisterSource("gen", func(id string, _ map[string]any) (stage.Source, error) {
		return &stubSource{Base: basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: "gen"}}, nil
	})
	reg.RegisterTransform("map", func(id string, _ map[string]any) (stage.Transform, error) {
		return &stubTransform{Base: basestage.Base{IDVal: id, KindVal: stage.KindTransform, TypeVal: "map"}}, nil
	})
	reg.RegisterSink("drop", func(id string, _ map[string]any) (stage.Sink, error) {
		return &stubSink{Base: basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "drop"}}, nil
	})
	reg.RegisterCodec("json", func(map[string]any) (codec.Codec, error) { return stubCodec{}, nil })
	return reg
}

func semanticsFixture() *TopologyIR {
	return &TopologyIR{
		Name: "sem",
		Stages: []StageIR{
			{ID: "src", Kind: KindSource, Type: "gen"},
			{ID: "tr", Kind: KindTransform, Type: "map", Config: map[string]any{"dsl": "payload.x = 1"}},
			{ID: "out", Kind: KindSink, Type: "drop"},
		},
		Edges: []EdgeIR{{From: "src", To: "tr"}, {From: "tr", To: "out"}},
	}
}

func TestValidateSemanticsAcceptsValidPipeline(t *testing.T) {
	reg := semanticsRegistry()
	if err := ValidateSemantics(semanticsFixture(), reg); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSemanticsRejectsUnknownStageType(t *testing.T) {
	cases := []struct {
		name      string
		stage     string
		kind      string
		good, bad string
	}{
		{"source", "src", KindSource, "gen", "nope"},
		{"transform", "tr", KindTransform, "map", "nope"},
		{"sink", "out", KindSink, "drop", "nope"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := semanticsRegistry()
			ir := semanticsFixture()
			for i := range ir.Stages {
				if ir.Stages[i].ID == tc.stage {
					ir.Stages[i].Type = tc.bad
				}
			}
			err := ValidateSemantics(ir, reg)
			if err == nil {
				t.Fatalf("expected unknown %s type error", tc.kind)
			}
			if !strings.Contains(err.Error(), `stage "`+tc.stage+`"`) {
				t.Fatalf("error %q does not name stage %q", err, tc.stage)
			}
		})
	}
}

func TestValidateSemanticsRejectsUnregisteredCodecType(t *testing.T) {
	reg := semanticsRegistry()
	ir := semanticsFixture()
	ir.Stages[1].Decoder = &config.CodecRef{Type: "nope"}
	err := ValidateSemantics(ir, reg)
	if err == nil {
		t.Fatal("expected unknown codec type error")
	}
	if !strings.Contains(err.Error(), `stage "tr"`) {
		t.Fatalf("error %q does not name the stage", err)
	}
}

// A codecs entry whose referenced type is not registered must be caught too.
func TestValidateSemanticsRejectsCodecEntryWithUnregisteredType(t *testing.T) {
	reg := semanticsRegistry()
	ir := semanticsFixture()
	ir.Codecs = map[string]CodecIR{"custom": {Name: "custom", Type: "nope"}}
	ir.Stages[1].Decoder = &config.CodecRef{Ref: "custom"}
	err := ValidateSemantics(ir, reg)
	if err == nil {
		t.Fatal("expected unknown codec type error for codec entry")
	}
	if !strings.Contains(err.Error(), "custom") {
		t.Fatalf("error %q does not name the codec entry", err)
	}
}

func TestValidateSemanticsRejectsBadPredicate(t *testing.T) {
	reg := semanticsRegistry()
	ir := semanticsFixture()
	ir.Stages[1].Predicate = "payload.x + " // dangling operator — compile error
	err := ValidateSemantics(ir, reg)
	if err == nil {
		t.Fatal("expected predicate compile error")
	}
	if !strings.Contains(err.Error(), `stage "tr"`) {
		t.Fatalf("error %q does not name the stage", err)
	}
}

func TestValidateSemanticsRejectsBadEdgeCondition(t *testing.T) {
	reg := semanticsRegistry()
	ir := semanticsFixture()
	ir.Edges[1].Condition = "payload.x + "
	err := ValidateSemantics(ir, reg)
	if err == nil {
		t.Fatal("expected condition compile error")
	}
	if !strings.Contains(err.Error(), "tr->out") {
		t.Fatalf("error %q does not name the edge", err)
	}
}

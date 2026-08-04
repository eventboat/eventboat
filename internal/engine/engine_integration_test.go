package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/testutil"
	"github.com/edgesets/edgestream/internal/topology"
	_ "github.com/edgesets/edgestream/plugins/all"
)

func TestPipelineMapFilterIntegration(t *testing.T) {
	testutil.Register(nil)

	ir := &topology.TopologyIR{
		Name: "integration-test",
		Stages: []topology.StageIR{
			{
				ID: "src", Kind: topology.KindSource, Type: testutil.SourceTypeGenerator,
				Config:  map[string]any{"messages": []any{`{"price":10,"quantity":5}`}},
				Decoder: &config.CodecRef{Type: "json"},
			},
			{
				ID: "enrich", Kind: topology.KindTransform, Type: "map",
				Config: map[string]any{"dsl": "payload.total = payload.price * payload.quantity"},
			},
			{
				ID: "flt", Kind: topology.KindTransform, Type: "filter",
				Config: map[string]any{"dsl": "payload.total > 20"},
			},
			{ID: "out", Kind: topology.KindSink, Type: testutil.SinkTypeCapture},
		},
		Edges: []topology.EdgeIR{
			{From: "src", To: "enrich", Required: true},
			{From: "enrich", To: "flt", Required: true},
			{From: "flt", To: "out", Required: true},
		},
	}
	if err := topology.Validate(ir); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := New(nil)
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cap, ok := testutil.CaptureSinkFor("out")
	if !ok {
		t.Fatal("capture sink missing")
	}
	pipe, _ := eng.Pipeline(ir.Name)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(cap.Messages()) == 1 && pipe.Inflight() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = eng.Stop(stopCtx)

	msgs := cap.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	var payload map[string]any
	if err := json.Unmarshal(msgs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["total"] != float64(50) {
		t.Fatalf("total = %v, want 50", payload["total"])
	}
}

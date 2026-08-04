package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/buffer"
	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/topology"
)

func setupTestEdge(t *testing.T, from, to string) *buffer.EdgeInbound {
	t.Helper()
	eb, err := buffer.NewEdgeInbound(buffer.EdgeOptions{
		Pipeline: "test",
		From:     from,
		To:       to,
		Config:   config.EdgeBufferConfig{Size: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	eb.Start(context.Background())
	return eb
}

func TestDispatchTransformOutputs_AcksParentAfterChild(t *testing.T) {
	eb := setupTestEdge(t, "map", "sink")
	p := &Pipeline{graph: &runtimeGraph{
		nodes:        map[string]*runtimeNode{},
		edgeInbounds: map[string]*buffer.EdgeInbound{edgeKey("map", "sink"): eb},
	}}

	var parentAcks atomic.Int32
	parent := message.New([]byte(`{"x":1}`), nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		parentAcks.Add(1)
	})

	child := parent.ShallowCopy()
	child.SetAckFn(nil)

	p.graph.nodes["sink"] = &runtimeNode{inboundEdges: []*buffer.EdgeInbound{eb}}
	p.graph.outgoing = map[string][]topology.EdgeIR{
		"map": {{From: "map", To: "sink"}},
	}

	p.dispatchTransformOutputs(context.Background(), "map", []*message.Message{parent}, []*message.Message{child})

	select {
	case got := <-eb.Out():
		got.Ack(nil)
	case <-time.After(time.Second):
		t.Fatal("expected child dispatched to sink")
	}
	if parentAcks.Load() != 1 {
		t.Fatalf("parent ack count = %d, want 1", parentAcks.Load())
	}
}

func TestDispatchTransformOutputs_AcksDroppedInputs(t *testing.T) {
	p := &Pipeline{graph: &runtimeGraph{nodes: map[string]*runtimeNode{}, outgoing: map[string][]topology.EdgeIR{}}}

	var acks atomic.Int32
	parent := message.New(nil, nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		acks.Add(1)
	})

	p.dispatchTransformOutputs(context.Background(), "filter", []*message.Message{parent}, nil)

	if acks.Load() != 1 {
		t.Fatalf("dropped input ack count = %d, want 1", acks.Load())
	}
}

func TestDispatchTransformOutputs_PassThroughUsesExistingAck(t *testing.T) {
	eb := setupTestEdge(t, "filter", "sink")
	p := &Pipeline{graph: &runtimeGraph{
		nodes:        map[string]*runtimeNode{},
		edgeInbounds: map[string]*buffer.EdgeInbound{edgeKey("filter", "sink"): eb},
	}}
	var acks atomic.Int32
	msg := message.New(nil, nil)
	msg.ID = "msg-1"
	msg.SetAckFn(func(error) {
		acks.Add(1)
	})

	p.graph.nodes["sink"] = &runtimeNode{inboundEdges: []*buffer.EdgeInbound{eb}}
	p.graph.outgoing = map[string][]topology.EdgeIR{
		"filter": {{From: "filter", To: "sink"}},
	}

	p.dispatchTransformOutputs(context.Background(), "filter", []*message.Message{msg}, []*message.Message{msg})

	got := <-eb.Out()
	if got == msg {
		t.Fatal("dispatchFrom should fan-out a shallow copy")
	}
	got.Ack(nil)
	if acks.Load() != 1 {
		t.Fatalf("pass-through ack count = %d, want 1", acks.Load())
	}
}

func TestSendToInbound_DropNewest(t *testing.T) {
	eb, err := buffer.NewEdgeInbound(buffer.EdgeOptions{
		Pipeline: "test",
		From:     "a",
		To:       "b",
		Config:   config.EdgeBufferConfig{Size: 1, Strategy: "drop_newest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// no Start: the drain loop must not race the second Enqueue for mem room
	_, _, _ = eb.Enqueue(context.Background(), message.New(nil, nil))

	var acks atomic.Int32
	msg := message.New(nil, nil)
	msg.SetAckFn(func(error) {
		acks.Add(1)
	})

	dropped, reason, _ := eb.Enqueue(context.Background(), msg)
	if !dropped || reason != "drop_newest" {
		t.Fatalf("dropped=%v reason=%q", dropped, reason)
	}
	if acks.Load() != 1 {
		t.Fatalf("expected dropped message to be acked")
	}
}

func TestDispatchTransformOutputs_UnmatchedOutputNacked(t *testing.T) {
	p := &Pipeline{
		ir:             &topology.TopologyIR{},
		stageErrorMode: map[string]string{},
		graph:          &runtimeGraph{nodes: map[string]*runtimeNode{}, outgoing: map[string][]topology.EdgeIR{}},
	}

	var parentAcks atomic.Int32
	parent := message.New(nil, nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		parentAcks.Add(1)
	})

	var orphanAcks atomic.Int32
	var orphanErr error
	orphan := message.New(nil, nil)
	orphan.ID = "no-such-input"
	orphan.SetAckFn(func(err error) {
		orphanAcks.Add(1)
		orphanErr = err
	})

	p.dispatchTransformOutputs(context.Background(), "map", []*message.Message{parent}, []*message.Message{orphan})

	if parentAcks.Load() != 1 {
		t.Fatalf("parent ack count = %d, want 1", parentAcks.Load())
	}
	if orphanAcks.Load() != 1 {
		t.Fatalf("unmatched output ack count = %d, want 1 (output silently dropped)", orphanAcks.Load())
	}
	if orphanErr == nil {
		t.Fatal("unmatched output should be acked with an error")
	}
}

func TestDispatchTransformOutputs_SameIDMultiOutputKeepsParentAck(t *testing.T) {
	eb := setupTestEdge(t, "map", "sink")
	p := &Pipeline{graph: &runtimeGraph{
		nodes:        map[string]*runtimeNode{},
		edgeInbounds: map[string]*buffer.EdgeInbound{edgeKey("map", "sink"): eb},
	}}
	p.graph.nodes["sink"] = &runtimeNode{inboundEdges: []*buffer.EdgeInbound{eb}}
	p.graph.outgoing = map[string][]topology.EdgeIR{
		"map": {{From: "map", To: "sink"}},
	}

	var parentAcks atomic.Int32
	parent := message.New([]byte(`{"x":1}`), nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		parentAcks.Add(1)
	})

	// Transform emitted the original message plus another output with the
	// same ID (e.g. split): both must be dispatched, and the parent's own
	// ackFn must survive and fire exactly once.
	sibling := parent.ShallowCopy()
	sibling.SetAckFn(nil)

	p.dispatchTransformOutputs(context.Background(), "map", []*message.Message{parent}, []*message.Message{parent, sibling})

	for i := 0; i < 2; i++ {
		select {
		case got := <-eb.Out():
			got.Ack(nil)
		case <-time.After(time.Second):
			t.Fatalf("expected 2 children dispatched, got %d", i)
		}
	}
	if parentAcks.Load() != 1 {
		t.Fatalf("parent ack count = %d, want 1", parentAcks.Load())
	}
}

package engine

import (
	"fmt"

	"github.com/edgesets/edgestream/internal/buffer"
	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/topology"
)

type runtimeNode struct {
	id           string
	kind         string
	inbound      chan *message.Message
	inboundEdges []*buffer.EdgeInbound
	batchIn      chan []*message.Message
	outBuffer    int
	workers      int
	conditions   map[string]*eql.Program
	predicate    *eql.Program
}

type runtimeGraph struct {
	nodes         map[string]*runtimeNode
	outgoing      map[string][]topology.EdgeIR
	edgeInbounds  map[string]*buffer.EdgeInbound
}

func edgeKey(from, to string) string {
	return from + "->" + to
}

func buildRuntimeGraph(ir *topology.TopologyIR) (*runtimeGraph, error) {
	g := &runtimeGraph{
		nodes:        make(map[string]*runtimeNode),
		outgoing:     make(map[string][]topology.EdgeIR),
		edgeInbounds: make(map[string]*buffer.EdgeInbound),
	}

	inboundBuf := make(map[string]int)
	for _, edge := range ir.Edges {
		size := edge.BufferSize()
		if size > inboundBuf[edge.To] {
			inboundBuf[edge.To] = size
		}
	}

	for _, st := range ir.Stages {
		buf := inboundBuf[st.ID]
		if buf == 0 {
			buf = 64
		}
		workers := st.Workers
		if workers == 0 {
			workers = 1
		}
		node := &runtimeNode{
			id:        st.ID,
			kind:      st.Kind,
			inbound:   make(chan *message.Message, buf),
			batchIn:   make(chan []*message.Message, workers),
			outBuffer: buf,
			workers:   workers,
		}
		if st.Predicate != "" && st.Kind == topology.KindTransform {
			prg, err := eql.CompileFilter(st.Predicate)
			if err != nil {
				return nil, fmt.Errorf("stage %q predicate: %w", st.ID, err)
			}
			node.predicate = prg
		}
		g.nodes[st.ID] = node
	}

	for _, edge := range ir.Edges {
		eb, err := buffer.NewEdgeInbound(buffer.EdgeOptions{
			Pipeline: ir.Name,
			From:     edge.From,
			To:       edge.To,
			Config:   edge.Buffer,
		})
		if err != nil {
			return nil, fmt.Errorf("edge %s->%s buffer: %w", edge.From, edge.To, err)
		}
		key := edgeKey(edge.From, edge.To)
		g.edgeInbounds[key] = eb
		g.nodes[edge.To].inboundEdges = append(g.nodes[edge.To].inboundEdges, eb)

		g.outgoing[edge.From] = append(g.outgoing[edge.From], edge)
		if edge.Condition != "" {
			prg, err := eql.CompileCondition(edge.Condition)
			if err != nil {
				return nil, fmt.Errorf("edge %s->%s condition: %w", edge.From, edge.To, err)
			}
			if g.nodes[edge.From].conditions == nil {
				g.nodes[edge.From].conditions = make(map[string]*eql.Program)
			}
			g.nodes[edge.From].conditions[edge.To] = prg
		}
	}
	return g, nil
}

func (g *runtimeGraph) evalCondition(prg *eql.Program, msg *message.Message) (bool, error) {
	if prg == nil {
		return true, nil
	}
	payload := eql.PayloadMap(msg)
	ctx := &eql.EvalContext{
		Msg:     eql.NewMsgAdapter(msg),
		Input:   payload,
		Payload: payload,
		Meta:    msg.Metadata,
	}
	return prg.EvalFilter(ctx)
}

func (g *runtimeGraph) allEdgeInbounds() []*buffer.EdgeInbound {
	out := make([]*buffer.EdgeInbound, 0, len(g.edgeInbounds))
	seen := make(map[*buffer.EdgeInbound]bool)
	for _, eb := range g.edgeInbounds {
		if seen[eb] {
			continue
		}
		seen[eb] = true
		out = append(out, eb)
	}
	return out
}

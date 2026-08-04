package topology

import (
	"fmt"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/registry"
)

func Validate(ir *TopologyIR) error {
	stageIDs := make(map[string]StageIR, len(ir.Stages))
	kindByID := make(map[string]string, len(ir.Stages))
	for _, st := range ir.Stages {
		if st.ID == "" {
			return fmt.Errorf("stage missing id")
		}
		if _, exists := stageIDs[st.ID]; exists {
			return fmt.Errorf("duplicate stage id %q", st.ID)
		}
		stageIDs[st.ID] = st
		kindByID[st.ID] = st.Kind
	}

	incoming := make(map[string]int, len(ir.Stages))
	outgoing := make(map[string][]string, len(ir.Stages))
	for _, e := range ir.Edges {
		if _, ok := stageIDs[e.From]; !ok {
			return fmt.Errorf("edge references unknown from stage %q", e.From)
		}
		if _, ok := stageIDs[e.To]; !ok {
			return fmt.Errorf("edge references unknown to stage %q", e.To)
		}
		if e.Route != "" && e.Condition != "" {
			return fmt.Errorf("edge %s->%s: route and condition are mutually exclusive", e.From, e.To)
		}
		incoming[e.To]++
		outgoing[e.From] = append(outgoing[e.From], e.To)
	}

	var sources, sinks int
	for id, st := range stageIDs {
		switch st.Kind {
		case KindSource:
			sources++
			if incoming[id] > 0 {
				return fmt.Errorf("source %q must not have incoming edges", id)
			}
		case KindSink:
			sinks++
			if len(outgoing[id]) > 0 {
				return fmt.Errorf("sink %q must not have outgoing edges", id)
			}
		case KindTransform:
		default:
			return fmt.Errorf("stage %q: unknown kind %q", id, st.Kind)
		}
	}

	if sources == 0 {
		return fmt.Errorf("pipeline must have at least one source")
	}
	if sinks == 0 {
		return fmt.Errorf("pipeline must have at least one sink")
	}

	// DLQ sinks are written to directly by the engine on failure, so they
	// legitimately have no incoming edge (pipeline dlq.sink, edge
	// delivery.dlq).
	dlqSinks := make(map[string]bool)
	if ir.DLQ != nil && ir.DLQ.Sink != "" {
		dlqSinks[ir.DLQ.Sink] = true
	}
	for _, e := range ir.Edges {
		if e.Delivery != nil && e.Delivery.DLQ != "" {
			dlqSinks[e.Delivery.DLQ] = true
		}
	}

	for id, st := range stageIDs {
		if st.Kind != KindSource && incoming[id] == 0 && !dlqSinks[id] {
			return fmt.Errorf("stage %q has no incoming edges", id)
		}
	}

	if err := detectCycle(ir.Stages, ir.Edges); err != nil {
		return err
	}
	if !hasSourceToSinkPath(ir.Stages, ir.Edges) {
		return fmt.Errorf("no path from any source to any sink")
	}

	for _, st := range ir.Stages {
		if st.Ordering == "ordered" && st.MaxInFlight > 1 {
			return fmt.Errorf("stage %q: ordered sink cannot have max_in_flight > 1", st.ID)
		}
	}

	for _, st := range ir.Stages {
		if err := validateCodecRef(st.Decoder, ir.Codecs, "decoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		}
		if err := validateCodecRef(st.Encoder, ir.Codecs, "encoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		}
	}

	return nil
}

func validateCodecRef(ref *config.CodecRef, codecs map[string]CodecIR, role string) error {
	if ref == nil || ref.IsEmpty() {
		return nil
	}
	if ref.Ref != "" {
		if _, ok := codecs[ref.Ref]; !ok {
			return fmt.Errorf("%s ref %q not found in codecs", role, ref.Ref)
		}
	}
	return nil
}

// ValidateSemantics extends Validate with checks that need the plugin
// registry and the CEL compiler, so `edgestream validate` catches what would
// otherwise only surface at run start:
//
//   - stage type registered (source / transform / sink)
//   - transform config and DSL compile (map / filter / route factories)
//   - transform predicate and edge condition CEL compile
//   - codec type registered (stage decoder/encoder and codecs entries)
//
// Every error identifies the offending stage (or edge). A nil registry
// defaults to registry.Default. Stage construction is side-effect free, so
// validation never touches the network or disk.
func ValidateSemantics(ir *TopologyIR, reg *registry.Registry) error {
	if reg == nil {
		reg = registry.Default
	}
	if err := Validate(ir); err != nil {
		return err
	}
	for _, st := range ir.Stages {
		switch st.Kind {
		case KindSource:
			if _, err := reg.CreateSource(st.Type, st.ID, st.Config); err != nil {
				return fmt.Errorf("stage %q: %w", st.ID, err)
			}
		case KindTransform:
			if _, err := reg.CreateTransform(st.Type, st.ID, st.Config); err != nil {
				return fmt.Errorf("stage %q: %w", st.ID, err)
			}
			if st.Predicate != "" {
				if _, err := eql.CompileFilter(st.Predicate); err != nil {
					return fmt.Errorf("stage %q predicate: %w", st.ID, err)
				}
			}
		case KindSink:
			if _, err := reg.CreateSink(st.Type, st.ID, st.Config); err != nil {
				return fmt.Errorf("stage %q: %w", st.ID, err)
			}
		default:
			return fmt.Errorf("stage %q: unknown kind %q", st.ID, st.Kind)
		}
		if err := validateCodecRefType(st.Decoder, ir.Codecs, reg, "decoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		}
		if err := validateCodecRefType(st.Encoder, ir.Codecs, reg, "encoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		}
	}
	for _, e := range ir.Edges {
		if e.Condition == "" {
			continue
		}
		if _, err := eql.CompileCondition(e.Condition); err != nil {
			return fmt.Errorf("edge %s->%s condition: %w", e.From, e.To, err)
		}
	}
	for name, c := range ir.Codecs {
		if !reg.HasCodec(c.Type) {
			return fmt.Errorf("codec %q: unknown codec type %q", name, c.Type)
		}
	}
	return nil
}

// validateCodecRefType extends validateCodecRef with registration checks:
// a direct type must be registered, and a named ref must resolve to a codecs
// entry whose type is registered.
func validateCodecRefType(ref *config.CodecRef, codecs map[string]CodecIR, reg *registry.Registry, role string) error {
	if err := validateCodecRef(ref, codecs, role); err != nil {
		return err
	}
	if ref == nil || ref.IsEmpty() {
		return nil
	}
	if ref.Type != "" {
		if !reg.HasCodec(ref.Type) {
			return fmt.Errorf("%s type %q not registered", role, ref.Type)
		}
	}
	if ref.Ref != "" {
		if !reg.HasCodec(codecs[ref.Ref].Type) {
			return fmt.Errorf("%s ref %q type %q not registered", role, ref.Ref, codecs[ref.Ref].Type)
		}
	}
	return nil
}

func detectCycle(stages []StageIR, edges []EdgeIR) error {
	adj := make(map[string][]string)
	for _, e := range edges {
		adj[e.From] = append(adj[e.From], e.To)
	}
	visited := make(map[string]int)
	var visit func(string) error
	visit = func(node string) error {
		if visited[node] == 1 {
			return fmt.Errorf("cycle detected involving stage %q", node)
		}
		if visited[node] == 2 {
			return nil
		}
		visited[node] = 1
		for _, next := range adj[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		visited[node] = 2
		return nil
	}
	for _, st := range stages {
		if err := visit(st.ID); err != nil {
			return err
		}
	}
	return nil
}

func hasSourceToSinkPath(stages []StageIR, edges []EdgeIR) bool {
	adj := make(map[string][]string)
	sources := map[string]bool{}
	sinks := map[string]bool{}
	for _, st := range stages {
		switch st.Kind {
		case KindSource:
			sources[st.ID] = true
		case KindSink:
			sinks[st.ID] = true
		}
	}
	for _, e := range edges {
		adj[e.From] = append(adj[e.From], e.To)
	}
	for src := range sources {
		seen := map[string]bool{src: true}
		queue := []string{src}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if sinks[cur] {
				return true
			}
			for _, next := range adj[cur] {
				if !seen[next] {
					seen[next] = true
					queue = append(queue, next)
				}
			}
		}
	}
	return false
}

package topology

import (
	"fmt"

	"github.com/edgesets/edgestream/internal/config"
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

	for id, st := range stageIDs {
		if st.Kind != KindSource && incoming[id] == 0 {
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

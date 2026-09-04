package convert

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// marshalDocument renders the v3 document as canonical YAML: two-space
// indent, script blocks in literal style, deterministic key order, no
// document markers.
func marshalDocument(doc *outDoc) ([]byte, error) {
	root := &yaml.Node{Kind: yaml.MappingNode}
	add := func(key string, val *yaml.Node) {
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, val)
	}

	add("apiVersion", strNode("eventboat/v3"))
	add("kind", strNode("Pipeline"))
	meta := mapNode(strNode("name"), strNode(doc.Name))
	meta.Style = yaml.FlowStyle
	add("metadata", meta)

	if doc.Limits != nil {
		add("limits", anyToNode(doc.Limits))
	}
	if doc.EdgeDefaults != nil {
		add("edge_defaults", anyToNode(doc.EdgeDefaults))
	}
	if doc.Codecs != nil {
		add("codecs", anyToNode(doc.Codecs))
	}

	section := func(name string, nodes []*outNode) {
		if len(nodes) == 0 {
			return
		}
		sec := &yaml.Node{Kind: yaml.MappingNode}
		for _, n := range nodes {
			sec.Content = append(sec.Content, strNode(n.id), nodeBody(n))
		}
		add(name, sec)
	}
	section("sources", doc.Sources)
	section("transforms", doc.Transforms)
	section("sinks", doc.Sinks)

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func strNode(s string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
}

func intNode(i int) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: fmt.Sprintf("%d", i)}
}

func boolNode(b bool) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: fmt.Sprintf("%v", b)}
}

// mapNode builds a mapping from key/value node pairs.
func mapNode(pairs ...*yaml.Node) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode}
	m.Content = append(m.Content, pairs...)
	return m
}

// anyToNode converts a decoded config value (map/slice/scalar) to a node
// with deterministic key ordering.
func anyToNode(v any) *yaml.Node {
	switch t := v.(type) {
	case map[string]any:
		m := &yaml.Node{Kind: yaml.MappingNode}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			m.Content = append(m.Content, strNode(k), anyToNode(t[k]))
		}
		return m
	case map[string]int:
		conv := map[string]any{}
		for k, iv := range t {
			conv[k] = iv
		}
		return anyToNode(conv)
	case []any:
		s := &yaml.Node{Kind: yaml.SequenceNode}
		for _, el := range t {
			s.Content = append(s.Content, anyToNode(el))
		}
		return s
	case []string:
		s := &yaml.Node{Kind: yaml.SequenceNode}
		for _, el := range t {
			s.Content = append(s.Content, strNode(el))
		}
		return s
	case string:
		return strNode(t)
	case int:
		return intNode(t)
	case int64:
		return intNode(int(t))
	case float64:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: fmt.Sprintf("%v", t)}
	case bool:
		return boolNode(t)
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null"}
	default:
		return strNode(fmt.Sprintf("%v", t))
	}
}

// nodeBody renders one node's mapping with framework fields first, then the
// plugin block.
func nodeBody(n *outNode) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode}
	appendKV := func(k string, v *yaml.Node) {
		m.Content = append(m.Content, strNode(k), v)
	}

	if len(n.edges) > 0 {
		appendKV("from", fromNode(n.edges))
	}
	if n.decoder != "" {
		appendKV("decoder", strNode(n.decoder))
	}
	if n.encoder != "" {
		appendKV("encoder", strNode(n.encoder))
	}
	if n.workers > 0 {
		appendKV("workers", intNode(n.workers))
	}
	if n.batch != nil {
		appendKV("batch", anyToNode(n.batch))
	}
	if n.script != "" {
		script := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: strings.TrimSuffix(n.script, "\n"), Style: yaml.LiteralStyle}
		appendKV("script", script)
	}
	if n.plugin != "" {
		appendKV(n.plugin, anyToNode(n.plugCfg))
	}
	return m
}

// fromNode renders the from value: scalar / list of scalars / attribute
// forms (parseFrom accepts string, list, or single-key mapping).
func fromNode(edges []outEdge) *yaml.Node {
	attrs := func(e outEdge) bool {
		return e.When != "" || e.Route != "" || e.Delivery != nil || e.RequiredFalse || e.Buffer != nil
	}
	// Deterministic order: by upstream name.
	sorted := append([]outEdge(nil), edges...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].From < sorted[j].From })

	if len(sorted) == 1 && !attrs(sorted[0]) {
		return strNode(sorted[0].From)
	}
	anyAttrs := false
	for _, e := range sorted {
		if attrs(e) {
			anyAttrs = true
			break
		}
	}
	if !anyAttrs {
		names := make([]string, len(sorted))
		for i, e := range sorted {
			names[i] = e.From
		}
		return anyToNode(names)
	}
	list := &yaml.Node{Kind: yaml.SequenceNode}
	for _, e := range sorted {
		am := &yaml.Node{Kind: yaml.MappingNode}
		if e.When != "" {
			am.Content = append(am.Content, strNode("when"), strNode(e.When))
		}
		if e.Route != "" {
			am.Content = append(am.Content, strNode("route"), strNode(e.Route))
		}
		if e.Delivery != nil {
			am.Content = append(am.Content, strNode("delivery"), anyToNode(e.Delivery))
		}
		if e.RequiredFalse {
			am.Content = append(am.Content, strNode("required"), boolNode(false))
		}
		if e.Buffer != nil {
			am.Content = append(am.Content, strNode("buffer"), anyToNode(e.Buffer))
		}
		list.Content = append(list.Content, mapNode(strNode(e.From), am))
	}
	if len(sorted) == 1 {
		// Single edge with attrs: the compact single-key mapping form.
		return list.Content[0]
	}
	return list
}

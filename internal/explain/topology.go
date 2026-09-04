package explain

import (
	"fmt"
	"strings"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
)

// TopologyMermaid renders the DAG as a mermaid flowchart — nodes carry their
// section shape, edges carry their condition (or route name).
func TopologyMermaid(pip *ir.Pipeline) string {
	var b strings.Builder
	b.WriteString("flowchart LR\n")
	for _, name := range pip.Order {
		node := pip.Nodes[name]
		shape := "%s([%s])" // source: stadium
		switch node.Section {
		case config.SectionTransform:
			shape = "%s[%s]" // process box
		case config.SectionSink:
			shape = "%s[[%s]]" // sink: subroutine
		}
		label := name
		if node.Config.Plugin != "" {
			label = name + "<br/>" + node.Config.Plugin
		} else if node.Script != nil {
			label = name + "<br/>script"
		} else if node.IsSplit {
			label = name + "<br/>split"
		}
		fmt.Fprintf(&b, "  "+shape+"\n", name, label)
	}
	seen := map[string]bool{}
	for _, name := range pip.Order {
		for _, edge := range pip.Nodes[name].Out {
			key := fmt.Sprintf("%s->%s|%s", edge.From, edge.To, edge.WhenSource)
			if seen[key] {
				continue
			}
			seen[key] = true
			if edge.WhenSource != "" {
				cond := strings.ReplaceAll(edge.WhenSource, `"`, "'")
				fmt.Fprintf(&b, "  %s -->|%s| %s\n", edge.From, cond, edge.To)
			} else {
				fmt.Fprintf(&b, "  %s --> %s\n", edge.From, edge.To)
			}
		}
	}
	return b.String()
}

// TopologyASCII renders the DAG as layered ASCII (columns per section).
func TopologyASCII(pip *ir.Pipeline) string {
	var sources, transforms, sinks []string
	for _, name := range pip.Order {
		switch pip.Nodes[name].Section {
		case config.SectionSource:
			sources = append(sources, name)
		case config.SectionTransform:
			transforms = append(transforms, name)
		case config.SectionSink:
			sinks = append(sinks, name)
		}
	}
	col := func(names []string, title string) []string {
		out := []string{title}
		for _, n := range names {
			out = append(out, "  "+n)
		}
		return out
	}
	left := col(sources, "sources")
	mid := col(transforms, "transforms")
	right := col(sinks, "sinks")
	height := max(len(left), max(len(mid), len(right)))
	pad := func(lines []string) []string {
		out := append([]string(nil), lines...)
		for len(out) < height {
			out = append(out, "")
		}
		return out
	}
	left, mid, right = pad(left), pad(mid), pad(right)

	var b strings.Builder
	w := columnWidth(sources) + 4
	for i := 0; i < height; i++ {
		fmt.Fprintf(&b, "%-*s", w, left[i])
		if i == 0 && len(mid) > 1 {
			b.WriteString("──▶ ")
		} else if len(mid) > 1 {
			b.WriteString("    ")
		}
		fmt.Fprintf(&b, "%-*s", columnWidth(transforms)+4, mid[i])
		if i == 0 && len(right) > 1 {
			b.WriteString("──▶ ")
		} else if len(right) > 1 {
			b.WriteString("    ")
		}
		b.WriteString(right[i])
		b.WriteString("\n")
	}
	b.WriteString("\nedges:\n")
	for _, name := range pip.Order {
		for _, edge := range pip.Nodes[name].Out {
			fmt.Fprintf(&b, "  %s → %s", edge.From, edge.To)
			if edge.RouteName != "" {
				fmt.Fprintf(&b, "  (route %s ⇒ when meta.route == %q)", edge.RouteName, edge.RouteName)
			} else if edge.WhenSource != "" {
				fmt.Fprintf(&b, "  (when %s)", edge.WhenSource)
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

func columnWidth(names []string) int {
	w := 0
	for _, n := range names {
		if len(n)+2 > w {
			w = len(n) + 2
		}
	}
	if w < 10 {
		w = 10
	}
	return w
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

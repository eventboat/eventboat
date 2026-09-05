package config

import (
	"fmt"
	"strings"
)

// nodeWhitelist lists the framework fields allowed at node level, per section.
// Everything else at node level must be exactly one plugin key; plugin fields
// live only inside the plugin block (redesign-v3.md §5.6). Transforms follow
// the same rule — script/split/wasm are registered transform plugins, not
// framework fields, so the whitelist carries only the shared node fields.
var nodeWhitelist = map[Section]map[string]bool{
	SectionSource:    {"decoder": true, "grpc": true, "version": true},
	SectionTransform: {"from": true, "workers": true, "version": true},
	SectionSink:      {"from": true, "encoder": true, "workers": true, "order_key": true, "batch": true, "grpc": true, "version": true},
}

func parseSection(file string, raw map[string]any, sectionKey string, section Section, p *Pipeline, lines *lineIndex, res *Result) {
	sectionRaw, present := raw[sectionKey]
	if !present {
		// sources and sinks are required; a pipeline without transforms
		// (source -> sink directly) is legitimate (§5.10: at least one
		// source and one sink).
		if section == SectionSource || section == SectionSink {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_missing_section", File: file, Line: 0,
				Message: fmt.Sprintf("section %q is required", sectionKey),
				Hint:    "a pipeline needs at least one source and one sink",
			})
		}
		return
	}
	nodesRaw, ok := sectionRaw.(map[string]any)
	if !ok {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_section_type", File: file, Line: lines.line(sectionKey),
			Message: fmt.Sprintf("section %q must be a mapping of node name to node config", sectionKey), Hint: "",
		})
		return
	}
	if len(nodesRaw) == 0 {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_empty_section", File: file, Line: lines.line(sectionKey),
			Message: fmt.Sprintf("section %q must not be empty", sectionKey), Hint: "",
		})
		return
	}

	target := p.Sources
	switch section {
	case SectionTransform:
		target = p.Transforms
	case SectionSink:
		target = p.Sinks
	}

	for name, nodeRaw := range nodesRaw {
		node := parseNode(file, name, section, nodeRaw, lines.line(sectionKey, name), lines.line(sectionKey, name), res)
		if node == nil {
			continue
		}
		if _, dup := target[name]; dup {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "topo_dup_name", File: file, Line: lines.line(sectionKey, name),
				Message: fmt.Sprintf("duplicate node name %q in section %q", name, sectionKey), Hint: "",
			})
			continue
		}
		target[name] = node
		p.Order = append(p.Order, name)
	}
}

func parseNode(file, name string, section Section, nodeRaw any, line, pluginLine int, res *Result) *Node {
	n := &Node{Name: name, Section: section, Line: line, PluginLine: pluginLine, Workers: 0}
	m, ok := nodeRaw.(map[string]any)
	if !ok {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_node_type", File: file, Line: line,
			Message: fmt.Sprintf("node %q must be a mapping", name), Hint: "",
		})
		return nil
	}
	whitelist := nodeWhitelist[section]

	var pluginKeys []string
	for key := range m {
		if whitelist[key] {
			continue
		}
		if key == "from" && section == SectionSource {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_source_with_from", File: file, Line: line,
				Message: fmt.Sprintf("source %q must not declare from (sources have no in-edges)", name), Hint: "",
			})
			continue
		}
		pluginKeys = append(pluginKeys, key)
	}
	// Deterministic diagnostics order.
	for i := 1; i < len(pluginKeys); i++ {
		for j := i; j > 0 && pluginKeys[j] < pluginKeys[j-1]; j-- {
			pluginKeys[j], pluginKeys[j-1] = pluginKeys[j-1], pluginKeys[j]
		}
	}

	switch section {
	case SectionSource, SectionSink, SectionTransform:
		if len(pluginKeys) == 0 {
			hint := "add exactly one plugin key, e.g. kafka: { ... } (the plugin name is the key)"
			if section == SectionTransform {
				hint = "add exactly one transform plugin key: script: | (Starlark), split: {}, wasm: { module: ... }, or a registered plugin's name"
			}
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_missing_plugin", File: file, Line: line,
				Message: fmt.Sprintf("%s node %q has no plugin block", section, name),
				Hint:    hint,
			})
			return nil
		}
		if len(pluginKeys) > 1 {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_multiple_plugins", File: file, Line: line,
				Message: fmt.Sprintf("%s node %q has multiple plugin blocks (%s); exactly one is allowed",
					section, name, strings.Join(pluginKeys, ", ")),
				Hint: "one node = one plugin; split the node or move plugin options inside the plugin block",
			})
			return nil
		}
		n.Plugin = pluginKeys[0]
		n.PluginLine = pluginLine
		if section == SectionTransform {
			// Transform plugin blocks are not forced to mappings: the script
			// plugin's block is the Starlark source text itself. The plugin's
			// JSON Schema decides the shape at verify time.
			n.PluginConfig = m[pluginKeys[0]]
		} else if cfg, ok := m[pluginKeys[0]].(map[string]any); ok {
			n.PluginConfig = cfg
		} else {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_plugin_block_type", File: file, Line: line,
				Message: fmt.Sprintf("plugin block %q of node %q must be a mapping", pluginKeys[0], name),
				Hint:    fmt.Sprintf("write %s: { ... } with the plugin's own fields", pluginKeys[0]),
			})
			return nil
		}
	}

	// Framework fields.
	if v, ok := m["grpc"]; ok && (section == SectionSource || section == SectionSink) {
		gm, ok := v.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_grpc_type", File: file, Line: line,
				Message: fmt.Sprintf("grpc of node %q must be a mapping", name),
				Hint:    `grpc: { command: ["./my-plugin"], schema: "my-plugin/manifest.json" }`,
			})
		} else {
			for k := range gm {
				if k != "command" && k != "env" && k != "schema" && k != "restart" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
						Message: fmt.Sprintf("unknown grpc field %q", k), Hint: "allowed: command, env, schema, restart",
					})
				}
			}
			g := &GrpcConfig{}
			cmd, ok := gm["command"].([]any)
			if !ok || len(cmd) == 0 {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_grpc_command", File: file, Line: line,
					Message: fmt.Sprintf("grpc.command of node %q must be a non-empty argv array", name),
					Hint:    `command: ["go", "run", "./plugin"] or ["./bin/plugin"]`,
				})
			} else {
				for i, c := range cmd {
					s, ok := c.(string)
					if !ok || strings.TrimSpace(s) == "" {
						res.Diagnostics = append(res.Diagnostics, Diagnostic{
							Severity: "error", Code: "cfg_grpc_command", File: file, Line: line,
							Message: fmt.Sprintf("grpc.command of node %q contains a non-string entry at index %d", name, i), Hint: "",
						})
						break
					}
					g.Command = append(g.Command, s)
				}
			}
			if em, ok := gm["env"].(map[string]any); ok {
				g.Env = map[string]string{}
				for k, ev := range em {
					if s, ok := ev.(string); ok {
						g.Env[k] = s
					} else {
						res.Diagnostics = append(res.Diagnostics, Diagnostic{
							Severity: "error", Code: "cfg_grpc_env", File: file, Line: line,
							Message: fmt.Sprintf("grpc.env of node %q: value for %q must be a string", name, k), Hint: "",
						})
					}
				}
			}
			s, ok := gm["schema"].(string)
			if !ok || strings.TrimSpace(s) == "" {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_grpc_schema", File: file, Line: line,
					Message: fmt.Sprintf("grpc.schema of node %q must be the plugin manifest path", name),
					Hint:    "verify validates the plugin block against this manifest without spawning the process",
				})
			} else {
				g.Schema = s
			}
			if rv, ok := gm["restart"]; ok {
				r, ok := rv.(string)
				if !ok || (r != "fast-fail" && r != "restart") {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_grpc_restart", File: file, Line: line,
						Message: fmt.Sprintf("grpc.restart of node %q must be \"fast-fail\" or \"restart\", got %v", name, rv),
						Hint:    "fast-fail (default) preserves M3 semantics; restart respawns with exponential backoff",
					})
				} else {
					g.Restart = r
				}
			}
			n.Grpc = g
		}
	}
	if v, ok := m["version"]; ok && (section == SectionSource || section == SectionSink || section == SectionTransform) {
		ver := asInt(v, 0)
		if ver < 1 {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_version_range", File: file, Line: line,
				Message: fmt.Sprintf("version of node %q must be >= 1", name), Hint: "",
			})
		} else {
			n.Version = ver
		}
	}
	if v, ok := m["decoder"]; ok && section == SectionSource {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_decoder_type", File: file, Line: line,
				Message: fmt.Sprintf("decoder of source %q must be a codec name", name), Hint: "",
			})
		} else {
			n.Decoder = s
		}
	}
	if v, ok := m["encoder"]; ok && section == SectionSink {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_encoder_type", File: file, Line: line,
				Message: fmt.Sprintf("encoder of sink %q must be a codec name", name), Hint: "",
			})
		} else {
			n.Encoder = s
		}
	}
	if v, ok := m["workers"]; ok {
		n.Workers = asInt(v, 0)
		if n.Workers < 1 {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_workers_range", File: file, Line: line,
				Message: fmt.Sprintf("workers of node %q must be >= 1", name), Hint: "",
			})
			n.Workers = 1
		}
	}
	if n.Workers == 0 {
		n.Workers = 1
	}
	if v, ok := m["order_key"]; ok && section == SectionSink {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_order_key_type", File: file, Line: line,
				Message: fmt.Sprintf("order_key of sink %q must be a CEL expression", name), Hint: "",
			})
		} else {
			n.OrderKey = s
		}
	}
	if v, ok := m["batch"]; ok && section == SectionSink {
		bm, ok := v.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_batch_type", File: file, Line: line,
				Message: fmt.Sprintf("batch of sink %q must be a mapping", name), Hint: "batch: { size: 100, timeout_ms: 1000 }",
			})
		} else {
			for k := range bm {
				if k != "size" && k != "timeout_ms" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
						Message: fmt.Sprintf("unknown batch field %q", k), Hint: "allowed: size, timeout_ms",
					})
				}
			}
			b := &Batch{Size: 1}
			if v, ok := bm["size"]; ok {
				b.Size = asInt(v, 0)
				if b.Size < 1 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_batch_range", File: file, Line: line,
						Message: "batch.size must be >= 1", Hint: "",
					})
					b.Size = 1
				}
			}
			if v, ok := bm["timeout_ms"]; ok {
				b.TimeoutMs = asInt(v, 0)
				if b.TimeoutMs < 1 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_batch_range", File: file, Line: line,
						Message: "batch.timeout_ms must be >= 1", Hint: "",
					})
				}
			}
			n.Batch = b
		}
	}

	// from (not allowed on sources — already diagnosed above; required on
	// transforms and sinks).
	if rawFrom, present := m["from"]; present && section != SectionSource {
		n.From = parseFrom(file, name, rawFrom, line, res)
	} else if !present && section != SectionSource {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_missing_from", File: file, Line: line,
			Message: fmt.Sprintf("%s node %q must declare from", section, name),
			Hint:    `from: [upstream] or from: { upstream: { when: '...' } }`,
		})
	}
	return n
}

// parseFrom accepts a string, a list of elements, or a single object element.
// Each element is "name" or {name: {attrs}} (redesign-v3.md §5.3).
func parseFrom(file, node string, raw any, line int, res *Result) []Edge {
	var elements []any
	switch t := raw.(type) {
	case string:
		elements = []any{t}
	case []any:
		elements = t
	case map[string]any:
		elements = []any{t}
	default:
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_bad_from", File: file, Line: line,
			Message: fmt.Sprintf("from of node %q must be a name, a list, or a single-key mapping", node),
			Hint:    `from: [ingest] or from: { enrich: { when: '...' } }`,
		})
		return nil
	}
	var edges []Edge
	for _, el := range elements {
		switch t := el.(type) {
		case string:
			if strings.TrimSpace(t) == "" {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_bad_from", File: file, Line: line,
					Message: fmt.Sprintf("from of node %q contains an empty name", node), Hint: "",
				})
				continue
			}
			edges = append(edges, Edge{From: t, Line: line})
		case map[string]any:
			if len(t) != 1 {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_bad_from", File: file, Line: line,
					Message: fmt.Sprintf("from object elements of node %q must have exactly one key (the upstream name)", node),
					Hint:    `from: { enrich: { when: '...' } }`,
				})
				continue
			}
			for upstream, attrsRaw := range t {
				attrs, ok := attrsRaw.(map[string]any)
				if !ok {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_bad_from", File: file, Line: line,
						Message: fmt.Sprintf("edge attributes for %q -> %q must be a mapping", upstream, node),
						Hint:    `from: { enrich: { when: '...' } } — attrs are optional`,
					})
					continue
				}
				edges = append(edges, parseEdgeAttrs(file, "from", &upstream, attrs, line, res))
			}
		default:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_bad_from", File: file, Line: line,
				Message: fmt.Sprintf("from element of node %q must be a string or a single-key mapping", node), Hint: "",
			})
		}
	}
	return edges
}

// parseEdgeAttrs validates one edge attribute block (also used by
// edge_defaults, where From stays nil).
func parseEdgeAttrs(file, container string, from *string, attrs map[string]any, line int, res *Result) Edge {
	e := Edge{Line: line}
	if from != nil {
		e.From = *from
	}
	for k := range attrs {
		switch k {
		case "when", "route", "delivery", "required", "buffer":
		default:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
				Message: fmt.Sprintf("unknown edge attribute %q in %s", k, container),
				Hint:    "allowed edge attributes: when, route, buffer, delivery, required",
			})
		}
	}
	if v, ok := attrs["when"]; ok {
		switch t := v.(type) {
		case string:
			if strings.TrimSpace(t) == "" {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_when_type", File: file, Line: line,
					Message: "when must be a non-empty expression", Hint: "",
				})
			} else {
				e.When = t
			}
		case map[string]any:
			// Object form (§4.7): { lang: cel|cesql, expr: "..." }.
			for k := range t {
				if k != "lang" && k != "expr" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
						Message: fmt.Sprintf("unknown when field %q", k), Hint: "allowed: lang, expr",
					})
				}
			}
			lang, _ := t["lang"].(string)
			if lang == "" {
				lang = "cel"
			}
			expr, _ := t["expr"].(string)
			switch {
			case lang != "cel" && lang != "cesql":
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_when_lang", File: file, Line: line,
					Message: fmt.Sprintf("when.lang must be \"cel\" or \"cesql\", got %q", lang), Hint: "",
				})
			case strings.TrimSpace(expr) == "":
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_when_type", File: file, Line: line,
					Message: "when.expr must be a non-empty expression", Hint: "",
				})
			default:
				e.When, e.WhenLang = expr, lang
			}
		default:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_when_type", File: file, Line: line,
				Message: "when must be an expression string or { lang: cel|cesql, expr: ... }", Hint: "",
			})
		}
	}
	if v, ok := attrs["route"]; ok {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_route_type", File: file, Line: line,
				Message: "route must be a non-empty name", Hint: "",
			})
		} else {
			e.Route = s
		}
	}
	if e.When != "" && e.Route != "" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_when_route_exclusive", File: file, Line: line,
			Message: "when and route are mutually exclusive on one edge", Hint: "route compiles to when: meta.route == <name>",
		})
	}
	if v, ok := attrs["required"]; ok {
		b, ok := v.(bool)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_required_type", File: file, Line: line,
				Message: "required must be a boolean", Hint: "",
			})
		} else {
			e.Required = &b
		}
	}
	if v, ok := attrs["buffer"]; ok {
		bm, ok := v.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_buffer_type", File: file, Line: line,
				Message: "buffer must be a mapping", Hint: "buffer: { type: memory, max_events: 128 }",
			})
		} else {
			for k := range bm {
				if k != "type" && k != "max_events" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
						Message: fmt.Sprintf("unknown buffer field %q", k), Hint: "allowed: type (memory), max_events",
					})
				}
			}
			buf := &BufferConfig{Type: "memory", MaxEvents: 128}
			if v, ok := bm["type"].(string); ok && v != "" {
				if v != "memory" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_buffer_type", File: file, Line: line,
						Message: "buffer.type must be \"memory\" in the POC", Hint: "",
					})
				}
				buf.Type = v
			}
			if v, ok := bm["max_events"]; ok {
				buf.MaxEvents = asInt(v, 0)
				if buf.MaxEvents < 1 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_buffer_range", File: file, Line: line,
						Message: "buffer.max_events must be >= 1", Hint: "",
					})
					buf.MaxEvents = 128
				}
			}
			e.Buffer = buf
		}
	}
	if v, ok := attrs["delivery"]; ok {
		dm, ok := v.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_delivery_type", File: file, Line: line,
				Message: "delivery must be a mapping", Hint: "delivery: { retries: 3, backoff: exponential }",
			})
		} else {
			for k := range dm {
				if k != "retries" && k != "backoff" && k != "timeout_ms" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
						Message: fmt.Sprintf("unknown delivery field %q", k), Hint: "allowed: retries, backoff, timeout_ms",
					})
				}
			}
			d := &Delivery{Retries: 3, Backoff: "exponential"}
			if v, ok := dm["retries"]; ok {
				d.Retries = asInt(v, -1)
				if d.Retries < 0 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_delivery_range", File: file, Line: line,
						Message: "delivery.retries must be >= 0", Hint: "",
					})
					d.Retries = 3
				}
			}
			if v, ok := dm["backoff"].(string); ok && v != "" {
				if v != "exponential" && v != "constant" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_delivery_backoff", File: file, Line: line,
						Message: "delivery.backoff must be \"exponential\" or \"constant\"", Hint: "",
					})
				} else {
					d.Backoff = v
				}
			}
			if v, ok := dm["timeout_ms"]; ok {
				d.TimeoutMs = asInt(v, 0)
				if d.TimeoutMs < 1 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_delivery_range", File: file, Line: line,
						Message: "delivery.timeout_ms must be >= 1", Hint: "",
					})
				}
			}
			e.Delivery = d
		}
	}
	return e
}

func asInt(v any, def int) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case uint64:
		return int(t)
	case float64:
		if t == float64(int(t)) {
			return int(t)
		}
	}
	return def
}

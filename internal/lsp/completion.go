package lsp

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/eventboat/eventboat/internal/config"
)

// Completion context analysis is line/indent-based over the document text
// (YAML structural parsing of half-typed documents is unreliable; the
// heuristics target the pipeline shape: top-level sections -> node names ->
// framework fields / plugin blocks -> plugin fields). Data sources are the
// registry catalog, plugin JSON Schemas and the loader's framework-field
// whitelists — the same authorities verify enforces.

var topLevelSections = []string{
	"apiVersion", "kind", "metadata", "edge_defaults", "constants", "limits",
	"telemetry", "run", "parameters", "hooks", "codecs", "sources", "transforms", "sinks",
}

// Framework fields per section (mirrors config.sections.go nodeWhitelist).
var frameworkFields = map[string][]string{
	"sources":    {"decoder", "grpc", "version"},
	"transforms": {"from", "workers", "script", "split", "wasm"},
	"sinks":      {"from", "encoder", "workers", "order_key", "batch", "grpc", "version"},
}

var frameworkDocs = map[string]string{
	"from":      "upstream edges: a name, a list, or `{name: {when: ...}}` — one edge per entry",
	"decoder":   "codec name applied to inbound bytes at the source (default json)",
	"encoder":   "codec name applied to the payload at the sink (default json)",
	"workers":   "per-node concurrency for transforms (default 1)",
	"order_key": "CEL expression evaluated into the message key (e.g. kafka partition key)",
	"batch":     "engine-owned sink batching: {size, timeout_ms}",
	"script":    "Starlark statement sequence; payload/meta/constants bindings (§4.3)",
	"split":     "mark the transform as a splitter: a JSON array payload becomes one message per element",
	"wasm":      "WASM transform tier: {module, entrypoint, timeout_ms, max_memory_pages, allow} (docs/wasm.md)",
	"grpc":      "external gRPC plugin block: {command, env, schema} (docs/plugins.md)",
	"version":   "pin the plugin ABI version; mismatch with the registry is a verify error",
	"when":      "CEL predicate on the edge (§4.2); errors = not passed + counter",
	"route":     "named route sugar: compiles to `meta.route == \"<name>\"` (§5.4)",
	"delivery":  "per-edge delivery policy: {retries, backoff, timeout_ms}",
	"required":  "required edge (default true); false = failures drop instead of dead-lettering",
	"buffer":    "in-memory per-edge surge buffer: {type: memory, max_events}",
}

// stackEntry is one `key:` line above the cursor.
type stackEntry struct {
	indent int
	key    string
}

// analyze derives the completion context from the document lines up to and
// including the cursor line.
func analyze(lines []string, line, character int) (stack []stackEntry, prefix string, valueKey string) {
	if line < 0 || line >= len(lines) {
		return nil, "", ""
	}
	cur := lines[line]
	typed := cur
	if character <= len(cur) {
		typed = cur[:character]
	}
	// Prefix: trailing identifier-ish chars of the typed part.
	prefix = typed
	if i := strings.LastIndexAny(typed, " \t:"); i >= 0 {
		tail := typed[i+1:]
		prefix = tail
	}
	// Value position: the cursor line already has a `key:` before the cursor
	// and only spacing between the colon and the cursor.
	valueKey = ""
	if idx := strings.Index(typed, ":"); idx >= 0 {
		between := typed[idx+1:]
		if strings.TrimSpace(between) == "" {
			trimmed := strings.TrimSpace(typed[:idx])
			if !strings.Contains(trimmed, ":") && trimmed != "" {
				valueKey = trimmed
			}
		}
	}
	for i := 0; i < line; i++ {
		l := lines[i]
		if strings.TrimSpace(l) == "" || strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		trimmed := strings.TrimLeft(l, " ")
		if trimmed == l { // column 0: top level
			if k, _, ok := strings.Cut(l, ":"); ok && isWord(k) {
				stack = append(stack, stackEntry{indent: 0, key: k})
			}
			continue
		}
		indent := len(l) - len(trimmed)
		if k, _, ok := strings.Cut(trimmed, ":"); ok && isWord(k) {
			// Drop deeper entries: an entry encloses only less-indented ones.
			for len(stack) > 0 && stack[len(stack)-1].indent >= indent {
				stack = stack[:len(stack)-1]
			}
			stack = append(stack, stackEntry{indent: indent, key: k})
		}
	}
	return stack, prefix, valueKey
}

func isWord(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// enclosing returns the innermost stack entry strictly above the cursor
// line's effective indentation (empty lines inherit the enclosing block's
// level — see cursorIndent).
func enclosing(stack []stackEntry, lines []string, line int) *stackEntry {
	indent := cursorIndent(lines, line)
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].indent < indent {
			e := stack[i]
			return &e
		}
	}
	return nil
}

// cursorIndent returns the effective indentation of the cursor line. Empty
// (or whitespace-only) lines inherit from the previous non-empty line: one
// level deeper when that line opens a block (`key:`), else the same level.
func cursorIndent(lines []string, line int) int {
	if line < 0 || line >= len(lines) {
		return 0
	}
	cur := lines[line]
	if strings.TrimSpace(cur) != "" {
		return len(cur) - len(strings.TrimLeft(cur, " "))
	}
	for i := line - 1; i >= 0; i-- {
		l := lines[i]
		if strings.TrimSpace(l) == "" {
			continue
		}
		ind := len(l) - len(strings.TrimLeft(l, " "))
		if strings.HasSuffix(strings.TrimRight(l, " "), ":") {
			return ind + 2
		}
		return ind
	}
	return 0
}

type completionItem struct {
	Label         string `json:"label"`
	Kind          int    `json:"kind"`
	Detail        string `json:"detail,omitempty"`
	Documentation string `json:"documentation,omitempty"`
	InsertText    string `json:"insertText,omitempty"`
}

// CompletionItemKind constants (LSP).
const (
	kindField    = 5
	kindEnum     = 13
	kindProperty = 10
)

func (s *Server) completion(params []byte) (any, *ResponseError) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position struct {
			Line      int `json:"line"`
			Character int `json:"character"`
		} `json:"position"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &ResponseError{Code: codeInvalidRequest, Message: err.Error()}
	}
	text, _ := s.document(p.TextDocument.URI)
	items := s.completionsFor(text, p.Position.Line, p.Position.Character)
	if items == nil {
		items = []completionItem{}
	}
	return items, nil
}

func (s *Server) completionsFor(text string, line, character int) []completionItem {
	lines := strings.Split(text, "\n")
	stack, prefix, valueKey := analyze(lines, line, character)
	if line >= len(lines) {
		return nil
	}
	filter := func(items []completionItem) []completionItem {
		if prefix == "" {
			return items
		}
		out := items[:0]
		for _, it := range items {
			if strings.HasPrefix(it.Label, prefix) {
				out = append(out, it)
			}
		}
		return out
	}

	// Value completions: decoder/encoder codecs, backoff, route lang.
	if valueKey != "" {
		switch valueKey {
		case "decoder", "encoder":
			return filter(append(s.codecItems(), declaredCodecItems(text)...))
		case "backoff":
			return filter([]completionItem{
				{Label: "exponential", Kind: kindEnum, InsertText: "exponential"},
				{Label: "constant", Kind: kindEnum, InsertText: "constant"},
			})
		case "lang":
			return filter([]completionItem{
				{Label: "cel", Kind: kindEnum, InsertText: "cel"},
				{Label: "cesql", Kind: kindEnum, InsertText: "cesql"},
			})
		}
	}

	encl := enclosing(stack, lines, line)

	// Top level.
	if encl == nil {
		out := make([]completionItem, 0, len(topLevelSections))
		for _, k := range topLevelSections {
			out = append(out, completionItem{Label: k, Kind: kindField, Detail: "top-level section", InsertText: k + ":"})
		}
		return filter(out)
	}

	// Just under a section, at node-name depth: node names are free-form —
	// no completion to offer.
	if nodeLevel(stack, lines, line) {
		return nil
	}

	// Inside a from: block → edge attributes. Both shapes:
	//   from:\n  <upstream>:   ← cursor under the upstream name
	//   from: {<upstream>: ...} on one line (stack still ends at from)
	if encl.key == "from" || parentOnStack(stack, encl) == "from" {
		return filter([]completionItem{
			{Label: "when", Kind: kindProperty, Detail: frameworkDocs["when"], InsertText: "when:"},
			{Label: "route", Kind: kindProperty, Detail: frameworkDocs["route"], InsertText: "route:"},
			{Label: "delivery", Kind: kindProperty, Detail: frameworkDocs["delivery"], InsertText: "delivery:"},
			{Label: "required", Kind: kindProperty, Detail: frameworkDocs["required"], InsertText: "required:"},
			{Label: "buffer", Kind: kindProperty, Detail: frameworkDocs["buffer"], InsertText: "buffer:"},
		})
	}

	// Inside a node under a section: framework fields + plugin names.
	section := ""
	for _, e := range stack {
		if isSection(e.key) && e.indent == 0 {
			section = e.key
		}
	}
	if section == "" {
		return nil
	}
	// Find the node key (indent 2 typically — the entry right after section
	// on the stack) and check whether the enclosing key is the node itself
	// (complete framework fields + plugin names) or a plugin block (complete
	// plugin schema fields).
	nodeIdx := -1
	for i, e := range stack {
		if e.indent > 0 && i > 0 && isSection(stack[i-1].key) {
			nodeIdx = i
			break
		}
	}
	if nodeIdx < 0 {
		return nil
	}
	framework := frameworkFields[section]
	isFramework := func(k string) bool {
		for _, f := range framework {
			if f == k {
				return true
			}
		}
		return false
	}

	// Collect keys already used inside the current node (avoid dupes).
	present := map[string]bool{}
	{
		nodeIndent := stack[nodeIdx].indent
		for _, l := range lines[:line] {
			trimmed := strings.TrimLeft(l, " ")
			ind := len(l) - len(trimmed)
			if ind <= nodeIndent || trimmed == "" {
				continue
			}
			// Only count keys at node-child depth until the next shallower key.
			if k, _, ok := strings.Cut(trimmed, ":"); ok && isWord(k) && ind <= nodeIndent+4 {
				present[k] = true
			}
		}
	}

	// Node-child level (the level where framework fields and the plugin
	// block live) vs. deeper (inside a plugin block).
	if cursorIndent(lines, line) <= stack[nodeIdx].indent+2 {
		// Cursor at node-child depth: framework fields + plugins.
		var out []completionItem
		for _, f := range framework {
			if present[f] {
				continue
			}
			out = append(out, completionItem{Label: f, Kind: kindProperty, Detail: frameworkDocs[f], InsertText: f + ":"})
		}
		for _, it := range s.pluginItems(section) {
			if !present[it.Label] {
				out = append(out, it)
			}
		}
		return filter(out)
	}
	// Deeper: inside a plugin block (or from/batch/etc.) — if the enclosing
	// key is a known plugin, offer its schema fields.
	if !isFramework(encl.key) {
		return filter(s.pluginFieldItems(section, encl.key))
	}
	return nil
}

func isSection(k string) bool {
	switch k {
	case "sources", "transforms", "sinks":
		return true
	}
	return false
}

// parentOnStack returns the stack entry directly enclosing the given one.
func parentOnStack(stack []stackEntry, e *stackEntry) string {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].indent < e.indent {
			return stack[i].key
		}
	}
	return ""
}

// nodeLevel reports whether the cursor sits at node-name depth directly
// under a section (indent > section, and the innermost enclosing entry is
// the section itself at indent 0).
func nodeLevel(stack []stackEntry, lines []string, line int) bool {
	if len(stack) == 0 {
		return false
	}
	top := stack[len(stack)-1]
	return top.indent == 0 && isSection(top.key) && cursorIndent(lines, line) > 0
}

func (s *Server) pluginItems(section string) []completionItem {
	var out []completionItem
	switch section {
	case "sources":
		for _, m := range s.reg.Catalog().Sources {
			out = append(out, completionItem{
				Label: m.Name, Kind: kindProperty,
				Detail:     "source plugin (v" + itoa(m.Version) + ")" + capsSuffix(m.Capabilities),
				InsertText: m.Name + ":",
			})
		}
	case "sinks":
		for _, m := range s.reg.Catalog().Sinks {
			out = append(out, completionItem{
				Label: m.Name, Kind: kindProperty,
				Detail:     "sink plugin (v" + itoa(m.Version) + ")",
				InsertText: m.Name + ":",
			})
		}
	case "transforms":
		// Transforms are framework main fields, not registry plugins.
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out
}

func (s *Server) codecItems() []completionItem {
	var out []completionItem
	for _, m := range s.reg.Catalog().Codecs {
		out = append(out, completionItem{Label: m.Name, Kind: kindEnum, Detail: "codec (v" + itoa(m.Version) + ")", InsertText: m.Name})
	}
	return out
}

// declaredCodecItems lists `codecs:` declaration names found in the
// document text (they are valid decoder/encoder references too). The doc
// may be mid-edit; a failed parse simply yields no declarations.
func declaredCodecItems(text string) []completionItem {
	lr := config.LoadBytes("completion.yaml", []byte(text))
	if lr.Pipeline == nil || lr.Pipeline.Codecs == nil {
		return nil
	}
	names := make([]string, 0, len(lr.Pipeline.Codecs))
	for name := range lr.Pipeline.Codecs {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]completionItem, 0, len(names))
	for _, name := range names {
		out = append(out, completionItem{Label: name, Kind: kindEnum, Detail: "declared codec (codecs: section)", InsertText: name})
	}
	return out
}

// pluginFieldItems lists one plugin's schema properties.
func (s *Server) pluginFieldItems(section, plugin string) []completionItem {
	var schema string
	switch section {
	case "sources":
		if m, ok := s.reg.LookupSource(plugin); ok {
			schema = m.Schema
		}
	case "sinks":
		if m, ok := s.reg.LookupSink(plugin); ok {
			schema = m.Schema
		}
	}
	if schema == "" {
		return nil
	}
	props, _ := parseSchemaProperties(schema)
	names := make([]string, 0, len(props))
	for k := range props {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]completionItem, 0, len(names))
	for _, n := range names {
		p := props[n]
		detail := n + ": " + p.Type
		if p.Default != "" {
			detail += " (default " + p.Default + ")"
		}
		out = append(out, completionItem{Label: n, Kind: kindField, Detail: detail, Documentation: p.Description, InsertText: n + ":"})
	}
	return out
}

func capsSuffix(caps []string) string {
	if len(caps) == 0 {
		return ""
	}
	return " [" + strings.Join(caps, ",") + "]"
}

func itoa(i int) string { return strconv.Itoa(i) }

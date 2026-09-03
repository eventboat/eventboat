package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Result carries the outcome of loading one configuration file.
type Result struct {
	Pipeline    *Pipeline
	Diagnostics []Diagnostic
}

// HasErrors reports whether any diagnostic is an error.
func (r *Result) HasErrors() bool {
	for _, d := range r.Diagnostics {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

var envPattern = regexp.MustCompile(`\$\{(\??)([A-Za-z_][A-Za-z0-9_.]*)\}`)

// LoadFile reads and parses a pipeline configuration file.
func LoadFile(path string) *Result {
	data, err := os.ReadFile(path)
	if err != nil {
		return &Result{Diagnostics: []Diagnostic{{
			Severity: "error", Code: "io_read", File: path, Line: 0,
			Message: err.Error(), Hint: "check the file path",
		}}}
	}
	return LoadBytes(path, data)
}

// LoadBytes parses pipeline configuration bytes.
func LoadBytes(file string, data []byte) *Result {
	res := &Result{}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "yaml_parse", File: file, Line: 0,
			Message: err.Error(), Hint: "fix the YAML syntax",
		})
		return res
	}
	root := unwrapDocument(&doc)
	if root == nil {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "empty_config", File: file, Line: 1,
			Message: "configuration is empty", Hint: "a pipeline needs sources, transforms and sinks",
		})
		return res
	}
	if root.Kind != yaml.MappingNode {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "yaml_parse", File: file, Line: root.Line,
			Message: "top level must be a mapping", Hint: "expected apiVersion/kind/metadata and the three sections",
		})
		return res
	}

	// Pass 1: ${VAR}/${?VAR} substitution over the whole tree (values only).
	subst := &envSubstituter{file: file, diags: &res.Diagnostics}
	subst.walk(root, nil)

	// Pass 2: decode into generic maps.
	var raw map[string]any
	if err := root.Decode(&raw); err != nil {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "yaml_parse", File: file, Line: root.Line,
			Message: err.Error(), Hint: "",
		})
		return res
	}

	// Pass 3: ${constants.x} substitution over decoded values.
	constants := map[string]any{}
	if c, ok := raw["constants"].(map[string]any); ok {
		constants = c
	}
	cs := &constantsSubstituter{file: file, constants: constants, diags: &res.Diagnostics}
	for key, val := range raw {
		if key == "constants" {
			continue
		}
		raw[key] = cs.substitute(val)
	}

	lines := collectLines(root)

	// Pass 4: structural validation with whitelists.
	p := &Pipeline{
		File:         file,
		Constants:    constants,
		EdgeDefaults: EdgeAttrs{},
		Sources:      map[string]*Node{},
		Transforms:   map[string]*Node{},
		Sinks:        map[string]*Node{},
	}
	res.Pipeline = p

	allowedTop := map[string]bool{
		"apiVersion": true, "kind": true, "metadata": true,
		"edge_defaults": true, "constants": true,
		"sources": true, "transforms": true, "sinks": true,
	}
	for _, kv := range mappingPairs(root) {
		key := kv.key
		if !allowedTop[key] {
			hint := "supported top-level keys in the POC: apiVersion, kind, metadata, edge_defaults, constants, sources, transforms, sinks"
			if key == "run" || key == "parameters" || key == "hooks" || key == "limits" || key == "codecs" || key == "dlq" || key == "telemetry" {
				hint = key + " is defined by redesign-v3.md §5.10 but not implemented in the POC yet"
			}
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_unknown_top_section", File: file, Line: kv.line,
				Message: fmt.Sprintf("unknown top-level key %q", key), Hint: hint,
			})
		}
	}

	if v, ok := raw["apiVersion"].(string); !ok || v != "eventboat/v3" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_api_version", File: file, Line: lines.line("apiVersion"),
			Message: fmt.Sprintf("apiVersion must be %q", "eventboat/v3"), Hint: "set apiVersion: eventboat/v3",
		})
	}
	if v, ok := raw["kind"].(string); !ok || v != "Pipeline" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_kind", File: file, Line: lines.line("kind"),
			Message: fmt.Sprintf("kind must be %q", "Pipeline"), Hint: "set kind: Pipeline",
		})
	}
	if meta, ok := raw["metadata"].(map[string]any); ok {
		if name, ok := meta["name"].(string); ok && strings.TrimSpace(name) != "" {
			p.Name = name
		}
	}
	if p.Name == "" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_metadata_name", File: file, Line: lines.line("metadata", "name"),
			Message: "metadata.name is required", Hint: "add metadata: { name: <pipeline-name> }",
		})
	}

	if ed, ok := raw["edge_defaults"].(map[string]any); ok {
		e := parseEdgeAttrs(file, "edge_defaults", nil, ed, lines.line("edge_defaults"), res)
		p.EdgeDefaults = EdgeAttrs{Delivery: e.Delivery, Required: e.Required, Buffer: e.Buffer}
	}

	parseSection(file, raw, "sources", SectionSource, p, lines, res)
	parseSection(file, raw, "transforms", SectionTransform, p, lines, res)
	parseSection(file, raw, "sinks", SectionSink, p, lines, res)

	return res
}

// --- yaml helpers ---

type kvPair struct {
	key  string
	line int
	val  *yaml.Node
}

func unwrapDocument(n *yaml.Node) *yaml.Node {
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	if n.Kind == 0 {
		return nil
	}
	return n
}

func mappingPairs(n *yaml.Node) []kvPair {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	out := []kvPair{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		out = append(out, kvPair{key: k.Value, line: k.Line, val: v})
	}
	return out
}

// lineIndex maps dotted paths to the line of their key node.
type lineIndex struct {
	m map[string]int
}

func collectLines(root *yaml.Node) *lineIndex {
	li := &lineIndex{m: map[string]int{}}
	var walk func(prefix []string, n *yaml.Node)
	walk = func(prefix []string, n *yaml.Node) {
		switch n.Kind {
		case yaml.MappingNode:
			for _, kv := range mappingPairs(n) {
				path := append(append([]string{}, prefix...), kv.key)
				li.m[strings.Join(path, ".")] = kv.line
				walk(path, kv.val)
			}
		case yaml.SequenceNode:
			for i, el := range n.Content {
				path := append(append([]string{}, prefix...), strconv.Itoa(i))
				walk(path, el)
			}
		}
	}
	walk(nil, root)
	return li
}

func (li *lineIndex) line(path ...string) int {
	return li.m[strings.Join(path, ".")]
}

// --- env substitution ---

type envSubstituter struct {
	file  string
	diags *[]Diagnostic
}

// walk applies ${VAR}/${?VAR} to every string scalar exactly once. Keys
// whose whole value is an unset ${?VAR} are dropped from their parent
// mapping/sequence. Collection loops substitute their scalar members
// themselves (they own drop handling) and recurse only into non-scalars —
// the default branch covers the bare scalar root. Visiting a scalar from
// both a loop and the recursion used to substitute it twice, duplicating
// diagnostics (round-2 review #4).
func (s *envSubstituter) walk(n *yaml.Node, parent *yaml.Node) {
	switch n.Kind {
	case yaml.MappingNode:
		kept := n.Content[:0:0]
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if v.Kind == yaml.ScalarNode {
				if s.substituteScalar(v) {
					continue // omit key
				}
			} else {
				s.walk(v, n)
			}
			kept = append(kept, k, v)
		}
		n.Content = kept
	case yaml.SequenceNode:
		kept := n.Content[:0:0]
		for _, el := range n.Content {
			if el.Kind == yaml.ScalarNode {
				if s.substituteScalar(el) {
					continue // omit element
				}
			} else {
				s.walk(el, n)
			}
			kept = append(kept, el)
		}
		n.Content = kept
	default:
		s.substituteScalar(n)
	}
}

// substituteScalar rewrites one node in place; it reports whether the node
// should be dropped (unset ${?VAR}).
func (s *envSubstituter) substituteScalar(n *yaml.Node) bool {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		return false
	}
	dropWhole, keep := substituteEnvString(n.Value, n.Line, s.file, s.diags)
	if dropWhole {
		return true
	}
	if keep == n.Value {
		return false
	}
	n.Value = keep
	retagScalar(n)
	return false
}

func retagScalar(n *yaml.Node) {
	v := n.Value
	if _, err := strconv.ParseBool(v); err == nil {
		n.Tag = "!!bool"
		n.Style = 0
		return
	}
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		n.Tag = "!!int"
		n.Style = 0
		return
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		n.Tag = "!!float"
		n.Style = 0
		return
	}
	n.Tag = "!!str"
	n.Style = yaml.DoubleQuotedStyle
}

// substituteEnvString expands ${VAR} (unset = error) and ${?VAR} (unset =
// omit). Dotted names (${constants.x}, ${parameters.x}) are scoping
// references, not environment variables, and are left for later phases.
func substituteEnvString(val string, line int, file string, diags *[]Diagnostic) (drop bool, replaced string) {
	matches := envPattern.FindAllStringSubmatch(val, -1)
	if len(matches) == 0 {
		return false, val
	}
	out := val
	for _, m := range matches {
		optional, name := m[1] == "?", m[2]
		if strings.Contains(name, ".") {
			continue // scoping reference (${constants.x}); not an env var
		}
		envVal, set := os.LookupEnv(name)
		if !set {
			if optional {
				if strings.TrimSpace(val) == m[0] {
					return true, ""
				}
				out = strings.ReplaceAll(out, m[0], "")
				continue
			}
			*diags = append(*diags, Diagnostic{
				Severity: "error", Code: "cfg_env_unset", File: file, Line: line,
				Message: fmt.Sprintf("environment variable %s is not set", name),
				Hint:    "set the variable, or use ${?" + name + "} to omit the key when unset",
			})
			continue
		}
		out = strings.ReplaceAll(out, m[0], envVal)
	}
	return false, out
}

// --- constants substitution ---

type constantsSubstituter struct {
	file      string
	constants map[string]any
	diags     *[]Diagnostic
}

// substitute expands ${constants.x} references and rejects any other scoped
// reference ("unknown means error", redesign-v3.md §5.5): only `constants` is
// a legal scope in the POC; `parameters` arrives with M2 job pipelines. The
// optional marker `?` is only meaningful for plain environment variables —
// any dotted reference combined with `?` is an error (round-2 review #1).
func (s *constantsSubstituter) substitute(v any) any {
	switch t := v.(type) {
	case string:
		matches := envPattern.FindAllStringSubmatch(t, -1)
		if len(matches) == 0 {
			return t
		}
		out := t
		for _, m := range matches {
			if !strings.Contains(m[2], ".") {
				continue // plain ${VAR}/${?VAR} was handled (or rejected) by the env pass
			}
			ref := m[0] // full reference text, including any optional marker
			scope := m[2][:strings.Index(m[2], ".")]
			optional := m[1] == "?"
			if scope == "constants" && !optional {
				name := strings.TrimPrefix(m[2], "constants.")
				cv, ok := s.constants[name]
				if !ok {
					*s.diags = append(*s.diags, Diagnostic{
						Severity: "error", Code: "cfg_constant_unknown", File: s.file, Line: 0,
						Message: fmt.Sprintf("unknown constant %q", name),
						Hint:    "declare it under constants:",
					})
					continue
				}
				out = strings.ReplaceAll(out, m[0], fmt.Sprintf("%v", cv))
				continue
			}
			msg := fmt.Sprintf("unknown scoped reference %s: scope %q is not defined (allowed: constants)", ref, scope)
			hint := "use ${constants.name} or a plain environment variable ${VAR}"
			switch {
			case scope == "parameters":
				msg = fmt.Sprintf("%s: parameters will land in M2 (job pipelines); not available yet", ref)
				hint = "pass the value as a constant or environment variable until job pipelines ship"
			case scope == "constants" && optional:
				msg = fmt.Sprintf("%s: the optional marker '?' is only valid for environment variables; constants are always defined in configuration", ref)
				hint = "reference it as ${constants.name} (it is always present)"
			}
			*s.diags = append(*s.diags, Diagnostic{
				Severity: "error", Code: "cfg_scope_unknown", File: s.file, Line: 0,
				Message: msg,
				Hint:    hint,
			})
		}
		return out
	case map[string]any:
		for k, val := range t {
			t[k] = s.substitute(val)
		}
		return t
	case []any:
		for i, val := range t {
			t[i] = s.substitute(val)
		}
		return t
	default:
		return v
	}
}

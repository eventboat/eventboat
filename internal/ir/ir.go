// Package ir builds the static intermediate representation: the DAG of nodes
// and edges, compiled CEL predicates, compiled Starlark programs, codec and
// plugin resolution. Everything statically knowable lives here; the runtime
// layer only consumes it (redesign-v3.md §6.1).
package ir

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/robfig/cron/v3"
	"go.starlark.net/starlark"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
)

// Edge is one resolved DAG edge with its compiled condition.
type Edge struct {
	From, To   string
	Line       int
	When       *celhost.Predicate // nil = unconditional
	WhenSource string             // original CEL text (or route-compiled text)
	RouteName  string             // set when the edge used route sugar
	Required   bool
	Retries    int
	Backoff    string
	TimeoutMs  int
	BufferMax  int
}

// Node is one resolved DAG node.
type Node struct {
	Name    string
	Section config.Section
	Config  *config.Node

	Out []Edge
	In  []Edge

	Script   *starhost.Program
	IsSplit  bool
	OrderKey *celhost.Predicate // sinks
}

// Pipeline is the compiled, ready-to-run form.
type Pipeline struct {
	Config           *config.Pipeline
	Nodes            map[string]*Node
	Order            []string // topological order
	Constants        map[string]any
	FrozenConstants  starlark.Value
	Parameters       map[string]any // resolved parameter values (job pipelines)
	FrozenParameters starlark.Value
	StarOptions      starhost.Options
}

// Build compiles a configuration into the static IR, producing diagnostics
// for every verify finding (schema, topology, expression and script errors,
// plus lint warnings).
//
// parameters carries resolved parameter values for job pipelines (verify
// passes the declared defaults; the jobs runner passes trigger-time
// actuals). A nil map means no parameters.
func Build(cfg *config.Pipeline, reg *registry.Registry, starOpts starhost.Options, parameters map[string]any) (*Pipeline, []config.Diagnostic) {
	var diags []config.Diagnostic
	file := cfg.File
	add := func(d config.Diagnostic) { diags = append(diags, d) }

	if parameters == nil {
		parameters = map[string]any{}
	}
	if cfg.IsJob() {
		// Fill unspecified parameters with declared defaults for static
		// compilation; the jobs runner substitutes actuals per run.
		for name, spec := range cfg.Parameters {
			if _, ok := parameters[name]; !ok && spec != nil {
				parameters[name] = spec.Default
			}
		}
	}

	p := &Pipeline{
		Config:           cfg,
		Nodes:            map[string]*Node{},
		Constants:        cfg.Constants,
		FrozenConstants:  starhost.FreezeConstants(cfg.Constants),
		Parameters:       parameters,
		FrozenParameters: starhost.FreezeConstants(parameters),
		StarOptions:      starOpts,
	}

	// Materialize nodes.
	for _, name := range cfg.Order {
		var cn *config.Node
		switch sectionOf(cfg, name) {
		case config.SectionSource:
			cn = cfg.Sources[name]
		case config.SectionTransform:
			cn = cfg.Transforms[name]
		case config.SectionSink:
			cn = cfg.Sinks[name]
		}
		n := &Node{Name: name, Section: cn.Section, Config: cn}
		p.Nodes[name] = n
		p.Order = append(p.Order, name)
	}

	// Resolve edges, compile conditions, apply edge defaults.
	celEnv, err := celhost.NewEnv(cfg.Constants, parameters)
	if err != nil {
		add(config.Diagnostic{Severity: "error", Code: "expr_cel_env", File: file, Message: err.Error()})
	}

	edgeDefaults := cfg.EdgeDefaults
	for _, name := range p.Order {
		to := p.Nodes[name]
		for _, ce := range to.Config.From {
			e := Edge{
				From:      ce.From,
				To:        name,
				Line:      ce.Line,
				Required:  true,
				Retries:   3,
				Backoff:   "exponential",
				BufferMax: 128,
			}
			if edgeDefaults.Delivery != nil {
				e.Retries = edgeDefaults.Delivery.Retries
				e.Backoff = edgeDefaults.Delivery.Backoff
				e.TimeoutMs = edgeDefaults.Delivery.TimeoutMs
				if e.Backoff == "" {
					e.Backoff = "exponential"
				}
			}
			if edgeDefaults.Required != nil {
				e.Required = *edgeDefaults.Required
			}
			if edgeDefaults.Buffer != nil {
				e.BufferMax = edgeDefaults.Buffer.MaxEvents
			}
			whenText := ce.When
			if ce.Route != "" {
				whenText = fmt.Sprintf("meta.route == %q", ce.Route)
				e.RouteName = ce.Route
			}
			if whenText != "" {
				e.WhenSource = whenText
				pred, cerr := celEnv.Compile(whenText)
				if cerr != nil {
					sev, code := "error", "expr_cel_compile"
					if strings.Contains(cerr.Error(), "route") && e.RouteName != "" {
						code = "expr_route_compile"
					}
					add(config.Diagnostic{Severity: sev, Code: code, File: file, Line: ce.Line,
						Message: cerr.Error(), Hint: "CEL predicates use payload.*, meta.* and constants.*"})
				} else {
					e.When = pred
				}
			}
			if ce.Delivery != nil {
				e.Retries = ce.Delivery.Retries
				e.Backoff = ce.Delivery.Backoff
				e.TimeoutMs = ce.Delivery.TimeoutMs
			}
			if ce.Required != nil {
				e.Required = *ce.Required
			}
			if ce.Buffer != nil {
				e.BufferMax = ce.Buffer.MaxEvents
			}
			to.In = append(to.In, e)
		}
	}

	// Dangling route check: an edge with route sugar needs an upstream that
	// assigns meta.route (redesign-v3.md §5.4).
	for _, name := range p.Order {
		for _, e := range p.Nodes[name].In {
			if e.RouteName == "" {
				continue
			}
			up := p.Nodes[e.From]
			if up.Section != config.SectionTransform || !assignsMetaRoute(up.Config.Script) {
				add(config.Diagnostic{Severity: "error", Code: "expr_route_dangling", File: file, Line: e.Line,
					Message: fmt.Sprintf("edge %s -> %s uses route %q but upstream %q does not assign meta.route", e.From, e.To, e.RouteName, e.From),
					Hint:    "assign meta.route in the upstream script or use an explicit when condition"})
			}
		}
	}

	// Cross-section duplicate names (config checks per-section only).
	seen := map[string]string{}
	for _, name := range p.Order {
		if prev, dup := seen[name]; dup {
			add(config.Diagnostic{Severity: "error", Code: "topo_dup_name", File: file, Line: p.Nodes[name].Config.Line,
				Message: fmt.Sprintf("node name %q is used in both %s and %s; names are unique across all three sections", name, prev, p.Nodes[name].Section)})
		} else {
			seen[name] = string(p.Nodes[name].Section)
		}
	}

	// Resolve edges into upstream Out lists; validate references.
	for _, name := range p.Order {
		n := p.Nodes[name]
		for _, e := range n.In {
			up, ok := p.Nodes[e.From]
			if !ok {
				add(config.Diagnostic{Severity: "error", Code: "topo_missing_ref", File: file, Line: e.Line,
					Message: fmt.Sprintf("from references unknown node %q", e.From),
					Hint:    "node names must exist in sources, transforms or sinks"})
				continue
			}
			if up.Section == config.SectionSink {
				add(config.Diagnostic{Severity: "error", Code: "topo_sink_as_upstream", File: file, Line: e.Line,
					Message: fmt.Sprintf("from references sink %q; sinks have no out-edges", e.From)})
				continue
			}
			up.Out = append(up.Out, e)
		}
	}

	// Acyclicity, reachability, orphans (§3.1 checklist item 2).
	checkTopology(p, file, add)

	// Compile transforms and sink order keys; validate plugins and codecs.
	for _, name := range p.Order {
		n := p.Nodes[name]
		switch n.Section {
		case config.SectionTransform:
			if n.Config.Script != "" {
				prog, err := starhost.Compile("transforms."+name+".script", n.Config.Script, starOpts)
				if err != nil {
					add(config.Diagnostic{Severity: "error", Code: "expr_starlark_compile", File: file, Line: n.Config.Line,
						Message: err.Error(), Hint: "scripts bind payload, meta, constants; while/recursion are disabled"})
				} else {
					n.Script = prog
				}
			}
			if n.Config.Split != nil {
				n.IsSplit = true
			}
		case config.SectionSource:
			if _, ok := reg.LookupSource(n.Config.Plugin); !ok {
				add(config.Diagnostic{Severity: "error", Code: "plugin_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown source plugin %q", n.Config.Plugin),
					Hint:    "run `eventboat verify` against a binary that registers this plugin; see plugin catalog"})
			} else if _, err := reg.NewSource(n.Config.Plugin, n.Config.PluginConfig); err != nil {
				addSchemaDiags(file, n, err, add)
			}
			codec := n.Config.Decoder
			if codec == "" {
				codec = "json"
			}
			if _, err := reg.NewCodec(codec, nil); err != nil {
				add(config.Diagnostic{Severity: "error", Code: "codec_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown decoder %q on source %q", codec, name)})
			}
		case config.SectionSink:
			if _, ok := reg.LookupSink(n.Config.Plugin); !ok {
				add(config.Diagnostic{Severity: "error", Code: "plugin_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown sink plugin %q", n.Config.Plugin)})
			} else if _, err := reg.NewSink(n.Config.Plugin, n.Config.PluginConfig); err != nil {
				addSchemaDiags(file, n, err, add)
			}
			codec := n.Config.Encoder
			if codec == "" {
				codec = "json"
			}
			if _, err := reg.NewCodec(codec, nil); err != nil {
				add(config.Diagnostic{Severity: "error", Code: "codec_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown encoder %q on sink %q", codec, name)})
			}
			if n.Config.OrderKey != "" {
				pred, err := celEnv.Compile(n.Config.OrderKey)
				if err != nil {
					add(config.Diagnostic{Severity: "error", Code: "expr_cel_compile", File: file, Line: n.Config.Line,
						Message: fmt.Sprintf("order_key on sink %q: %s", name, err.Error())})
				} else {
					n.OrderKey = pred
				}
			}
		}
	}

	// Topological order for execution.
	p.Order = topoSort(p)

	// Job pipeline semantics (§3.1 item 4, §5.8).
	checkJobSemantics(p, reg, parameters, file, add)

	lint(p, file, add)

	if hasError(diags) {
		return nil, diags
	}
	return p, diags
}

func sectionOf(cfg *config.Pipeline, name string) config.Section {
	if _, ok := cfg.Sources[name]; ok {
		return config.SectionSource
	}
	if _, ok := cfg.Transforms[name]; ok {
		return config.SectionTransform
	}
	return config.SectionSink
}

// parametersRefPattern matches ${parameters.name} tokens in any string value.
var parametersRefPattern = regexp.MustCompile(`\$\{parameters\.([A-Za-z_][A-Za-z0-9_]*)\}`)

// parametersBindingPattern matches parameters.<name> bindings surviving in
// script and predicate text (the ${...} form was substituted at load). RE2
// has no lookbehind: the match includes one leading char when present.
var parametersBindingPattern = regexp.MustCompile(`(?:^|[^A-Za-z0-9_.])parameters\.[A-Za-z_][A-Za-z0-9_]*`)

// bindingParameterNames extracts the parameter names from parameters.<name>
// bindings matched by parametersBindingPattern.
func bindingParameterNames(text string) []string {
	var out []string
	for _, m := range parametersBindingPattern.FindAllString(text, -1) {
		idx := strings.Index(m, "parameters.")
		if idx < 0 {
			continue
		}
		name := m[idx+len("parameters."):]
		out = append(out, name)
	}
	return out
}

// checkJobSemantics enforces the job-pipeline rules of §5.8 (M2 review):
// pull-capability sources, cron syntax, parameter reference legality, hook
// sink schemas and the continuous-pipeline rejections.
func checkJobSemantics(p *Pipeline, reg *registry.Registry, parameters map[string]any, file string, add func(config.Diagnostic)) {
	cfg := p.Config
	job := cfg.IsJob()

	// Cron syntax.
	if job && cfg.Run.Schedule != "" {
		if _, err := cron.ParseStandard(cfg.Run.Schedule); err != nil {
			add(config.Diagnostic{Severity: "error", Code: "job_bad_schedule", File: file,
				Line: 0, Message: fmt.Sprintf("run.schedule %q is not a valid 5-field cron expression: %v", cfg.Run.Schedule, err)})
		}
	}

	// Source capability + sql-in-continuous lint.
	for _, name := range p.Order {
		n := p.Nodes[name]
		if n.Section != config.SectionSource {
			continue
		}
		meta, ok := reg.LookupSource(n.Config.Plugin)
		if !ok {
			continue // unknown plugin already diagnosed
		}
		pull := false
		for _, c := range meta.Capabilities {
			if c == "pull" {
				pull = true
			}
		}
		if job && !pull {
			add(config.Diagnostic{Severity: "error", Code: "job_source_not_pull", File: file,
				Line:    n.Config.Line,
				Message: fmt.Sprintf("job pipeline source %q uses plugin %q which has no pull capability", name, n.Config.Plugin),
				Hint:    "job pipelines need sources that page through data and signal exhaustion (capabilities: [pull])"})
		}
		if !job && n.Config.Plugin == "sql" {
			add(config.Diagnostic{Severity: "warning", Code: "lint_sql_continuous", File: file,
				Line:    n.Config.Line,
				Message: fmt.Sprintf("source %q uses the sql (pull) source in a continuous pipeline: it pulls once from the last watermark at startup, then idles", name),
				Hint:    "job pipelines (run.mode: job) are the intended home for sql sources"})
		}
	}

	// Parameters legality.
	if !job {
		for _, name := range p.Order {
			n := p.Nodes[name]
			for _, text := range []string{n.Config.Script, n.Config.OrderKey} {
				if text != "" && parametersBindingPattern.MatchString(text) {
					add(config.Diagnostic{Severity: "error", Code: "job_parameters_in_continuous", File: file,
						Line:    n.Config.Line,
						Message: fmt.Sprintf("node %q references parameters in a continuous pipeline; parameters exist only in job pipelines (run.mode: job)", name),
						Hint:    "use constants (load-time) or add a run block"})
				}
			}
			for _, e := range n.In {
				if e.WhenSource != "" && parametersBindingPattern.MatchString(e.WhenSource) {
					add(config.Diagnostic{Severity: "error", Code: "job_parameters_in_continuous", File: file,
						Line:    e.Line,
						Message: fmt.Sprintf("edge %s -> %s references parameters in a continuous pipeline", e.From, e.To),
						Hint:    "parameters are job-pipeline only (run.mode: job)"})
				}
			}
		}
		return
	}

	// Job pipeline: every ${parameters.x} reference must name a declared
	// parameter (the loader lets the tokens through).
	declared := map[string]bool{}
	for name := range cfg.Parameters {
		declared[name] = true
	}
	var scanRefs func(v any, where string, line int)
	scanRefs = func(v any, where string, line int) {
		switch t := v.(type) {
		case string:
			for _, m := range parametersRefPattern.FindAllStringSubmatch(t, -1) {
				if !declared[m[1]] {
					add(config.Diagnostic{Severity: "error", Code: "job_parameter_unknown", File: file, Line: line,
						Message: fmt.Sprintf("%s references undeclared parameter %q", where, m[1]),
						Hint:    "declare it under parameters:"})
				}
			}
			if parametersBindingPattern.MatchString(t) && !declaredAnyBinding(t, declared) {
				add(config.Diagnostic{Severity: "error", Code: "job_parameter_unknown", File: file, Line: line,
					Message: fmt.Sprintf("%s references parameters.* but no parameters are declared", where),
					Hint:    "declare parameters under parameters: or use constants"})
			}
		case []any:
			for _, el := range t {
				scanRefs(el, where, line)
			}
		case map[string]any:
			for _, el := range t {
				scanRefs(el, where, line)
			}
		}
	}
	for _, name := range p.Order {
		n := p.Nodes[name]
		scanRefs(n.Config.PluginConfig, fmt.Sprintf("source/transform/sink %q", name), n.Config.Line)
		scanRefs(n.Config.Script, fmt.Sprintf("script of %q", name), n.Config.Line)
		scanRefs(n.Config.OrderKey, fmt.Sprintf("order_key of %q", name), n.Config.Line)
		for _, e := range n.In {
			scanRefs(e.WhenSource, fmt.Sprintf("edge %s -> %s", e.From, e.To), e.Line)
		}
	}

	// Hook sinks validate against their plugin schemas (R14).
	if cfg.Hooks != nil {
		for _, hk := range []struct {
			label string
			sink  *config.HookSink
		}{{"hooks.failure", cfg.Hooks.Failure}, {"hooks.success", cfg.Hooks.Success}} {
			if hk.sink == nil {
				continue
			}
			if _, ok := reg.LookupSink(hk.sink.Plugin); !ok {
				add(config.Diagnostic{Severity: "error", Code: "plugin_unknown", File: file, Line: hk.sink.Line,
					Message: fmt.Sprintf("%s references unknown sink plugin %q", hk.label, hk.sink.Plugin)})
				continue
			}
			if _, err := reg.NewSink(hk.sink.Plugin, hk.sink.PluginConfig); err != nil {
				add(config.Diagnostic{Severity: "error", Code: "plugin_schema", File: file, Line: hk.sink.Line,
					Message: fmt.Sprintf("%s: %s", hk.label, strings.ReplaceAll(err.Error(), "\n", "; ")),
					Hint:    "hooks are inline sinks: the plugin block is validated against the plugin's JSON Schema"})
			}
		}
	}
}

// declaredAnyBinding reports whether a parameters.* binding in text names at
// least one declared parameter.
func declaredAnyBinding(text string, declared map[string]bool) bool {
	for _, name := range bindingParameterNames(text) {
		if declared[name] {
			return true
		}
	}
	return false
}

func hasError(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

// addSchemaDiags converts registry schema errors into line-annotated
// diagnostics anchored at the plugin block.
func addSchemaDiags(file string, n *Node, err error, add func(config.Diagnostic)) {
	add(config.Diagnostic{Severity: "error", Code: "plugin_schema", File: file, Line: n.Config.PluginLine,
		Message: fmt.Sprintf("node %q: %s", n.Name, strings.ReplaceAll(err.Error(), "\n", "; ")),
		Hint:    "the plugin block is validated against the plugin's JSON Schema; unknown or mistyped fields are errors"})
}

var metaRouteAssign = regexp.MustCompile(`(?m)^\s*meta\.route\s*=`)

func assignsMetaRoute(script string) bool {
	return metaRouteAssign.MatchString(script)
}

// checkTopology enforces the six topology invariants of §3.1 item 2.
func checkTopology(p *Pipeline, file string, add func(config.Diagnostic)) {
	// Cycle detection via DFS colors.
	const (
		white = 0
		grey  = 1
		black = 2
	)
	color := map[string]int{}
	var visit func(name string, stack []string) bool
	visit = func(name string, stack []string) bool {
		color[name] = grey
		stack = append(stack, name)
		for _, e := range p.Nodes[name].Out {
			switch color[e.To] {
			case grey:
				add(config.Diagnostic{Severity: "error", Code: "topo_cycle", File: file, Line: e.Line,
					Message: fmt.Sprintf("cycle detected: %s -> %s", strings.Join(stack, " -> "), e.To),
					Hint:    "the DAG must be acyclic"})
				return true
			case white:
				if visit(e.To, stack) {
					return true
				}
			}
		}
		color[name] = black
		return false
	}
	for _, name := range p.Order {
		if color[name] == white {
			if visit(name, nil) {
				break
			}
		}
	}

	// Reachability from sources; at least one source->sink path.
	reachable := map[string]bool{}
	var mark func(name string)
	mark = func(name string) {
		if reachable[name] {
			return
		}
		reachable[name] = true
		for _, e := range p.Nodes[name].Out {
			mark(e.To)
		}
	}
	anySource := false
	for _, name := range p.Order {
		if p.Nodes[name].Section == config.SectionSource {
			anySource = true
			mark(name)
		}
	}
	_ = anySource
	sinkReachable := false
	for _, name := range p.Order {
		if p.Nodes[name].Section == config.SectionSink && reachable[name] {
			sinkReachable = true
		}
	}
	if !sinkReachable {
		add(config.Diagnostic{Severity: "error", Code: "topo_no_path", File: file, Line: 0,
			Message: "no source-to-sink path exists", Hint: "connect at least one source to one sink via from"})
	}

	// Orphans: sources nothing consumes, and non-sources with no in-edges.
	for _, name := range p.Order {
		n := p.Nodes[name]
		switch n.Section {
		case config.SectionSource:
			if len(n.Out) == 0 {
				add(config.Diagnostic{Severity: "error", Code: "topo_orphan", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("source %q has no downstream (nothing reads from it)", name),
					Hint:    "remove it or add a from: reference to it"})
			}
		default:
			if len(n.In) == 0 {
				add(config.Diagnostic{Severity: "error", Code: "topo_orphan", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("node %q has no in-edges", name),
					Hint:    "wire it into the DAG via from:"})
			}
		}
	}
}

// topoSort orders nodes so every node appears after its upstream nodes.
func topoSort(p *Pipeline) []string {
	var out []string
	seen := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		if n, ok := p.Nodes[name]; ok {
			for _, e := range n.In {
				if _, upstream := p.Nodes[e.From]; upstream {
					visit(e.From)
				}
			}
		}
		out = append(out, name)
	}
	// Stable input order first (determinism), DFS ensures dependencies first.
	for _, name := range p.Order {
		visit(name)
	}
	return out
}

// lint implements the POC subset of §3.1 item 5.
func lint(p *Pipeline, file string, add func(config.Diagnostic)) {
	// Literal predicates: dead branches.
	for _, name := range p.Order {
		for _, e := range p.Nodes[name].In {
			src := strings.TrimSpace(e.WhenSource)
			if e.When == nil || src == "" {
				continue
			}
			if src == "true" || src == "false" {
				add(config.Diagnostic{Severity: "warning", Code: "lint_when_literal", File: file, Line: e.Line,
					Message: fmt.Sprintf("edge %s -> %s has constant condition %q", e.From, e.To, src),
					Hint:    "a constant condition is either redundant (true) or a dead branch (false)"})
			}
		}
	}
	// Unused constants. Usage has two sources: binding-form references
	// (constants.x) that survive in script/predicate text, and ${constants.x}
	// references that the loader already substituted — the latter are counted
	// on the loader's pre-substitution record (Pipeline.ConstantsUsed),
	// otherwise every substituted reference reads as unused.
	used := map[string]bool{}
	for name, ok := range p.Config.ConstantsUsed {
		if ok {
			used[name] = true
		}
	}
	var scan func(v any)
	scan = func(v any) {
		switch t := v.(type) {
		case string:
			for name := range p.Config.Constants {
				if strings.Contains(t, "constants."+name) || strings.Contains(t, "${constants."+name+"}") {
					used[name] = true
				}
			}
		case []any:
			for _, el := range t {
				scan(el)
			}
		case map[string]any:
			for _, el := range t {
				scan(el)
			}
		}
	}
	for _, name := range p.Order {
		n := p.Nodes[name]
		scan(n.Config.Script)
		scan(n.Config.PluginConfig)
		for _, e := range n.In {
			scan(e.WhenSource)
		}
		scan(n.Config.OrderKey)
	}
	var names []string
	for name := range p.Config.Constants {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		if !used[name] {
			hint := fmt.Sprintf("remove it or reference it via constants.%s / ${constants.%s}", name, name)
			add(config.Diagnostic{Severity: "warning", Code: "lint_constant_unused", File: file, Line: 0,
				Message: fmt.Sprintf("constant %q is never referenced", name),
				Hint:    hint})
		}
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

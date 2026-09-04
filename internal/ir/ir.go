// Package ir builds the static intermediate representation: the DAG of nodes
// and edges, compiled CEL predicates, compiled Starlark programs, codec and
// plugin resolution. Everything statically knowable lives here; the runtime
// layer only consumes it (redesign-v3.md §6.1).
package ir

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/robfig/cron/v3"
	"go.starlark.net/starlark"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/lang/cesqlhost"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

func hasCap(caps []string, want string) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}

// WhenPredicate is a compiled edge condition, dialect-agnostic. Both hosts
// implement it; evaluation errors mean "condition does not pass" plus a
// counter (the shared error contract, §4.2/§4.7).
type WhenPredicate interface {
	Lang() string // "cel" | "cesql"
	Eval(payload, meta any) (bool, error)
}

// celWhen adapts celhost's EvalError-returning predicate to the interface.
type celWhen struct{ p *celhost.Predicate }

func (c celWhen) Lang() string { return "cel" }

func (c celWhen) Eval(payload, meta any) (bool, error) {
	ok, evalErr := c.p.Eval(payload, meta)
	if evalErr != nil {
		return false, evalErr
	}
	return ok, nil
}

// Edge is one resolved DAG edge with its compiled condition.
type Edge struct {
	From, To   string
	Line       int
	When       WhenPredicate // nil = unconditional
	WhenSource string        // original expression text (or route-compiled text)
	RouteName  string        // set when the edge used route sugar
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
	Wasm     *wasmhost.Compiled // set when the main field is wasm (tier 3)
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
	// Codecs holds instantiated named codec declarations (`codecs:`, §5.10):
	// decoder/encoder referencing a declared name resolve to these; bare
	// registered names still instantiate through the registry (engine-side).
	Codecs map[string]registry.Codec
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

	// Named codec declarations (`codecs:`, §5.10): resolve each against the
	// registry (type existence, config schema validation with the pipeline
	// directory for relative paths) and instantiate once. Declaration names
	// must not shadow registered codecs — the two namespaces stay disjoint.
	p.Codecs = map[string]registry.Codec{}
	for _, declName := range sortedDeclNames(cfg.Codecs) {
		decl := cfg.Codecs[declName]
		if _, isRegistered := reg.LookupCodec(decl.Name); isRegistered {
			add(config.Diagnostic{Severity: "error", Code: "cfg_codec_shadow", File: file, Line: decl.Line,
				Message: fmt.Sprintf("codec declaration %q shadows the registered codec %q", decl.Name, decl.Name),
				Hint:    "declaration names and registered codec names are separate namespaces; pick another name"})
			continue
		}
		c, err := reg.NewCodec(decl.Type, decl.Config, filepath.Dir(file))
		if err != nil {
			addSchemaDiags(file, &Node{Config: &config.Node{Name: decl.Name, Line: decl.Line, Plugin: decl.Type, PluginConfig: decl.Config}}, err, add)
			continue
		}
		p.Codecs[decl.Name] = c
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
				if ce.WhenLang == "cesql" && ce.Route == "" {
					// Opt-in CESQL dialect (§4.7): CloudEvents interop.
					pred, cerr := cesqlhost.Compile(whenText)
					if cerr != nil {
						add(config.Diagnostic{Severity: "error", Code: "expr_cesql_compile", File: file, Line: ce.Line,
							Message: cerr.Error(),
							Hint:    "CESQL binds meta (context attributes) and data.* (documented extension); run `eventboat plugin catalog` docs for the dialect notes"})
					} else {
						e.When = pred
					}
				} else {
					pred, cerr := celEnv.Compile(whenText)
					if cerr != nil {
						sev, code := "error", "expr_cel_compile"
						if strings.Contains(cerr.Error(), "route") && e.RouteName != "" {
							code = "expr_route_compile"
						}
						add(config.Diagnostic{Severity: sev, Code: code, File: file, Line: ce.Line,
							Message: cerr.Error(), Hint: "CEL predicates use payload.*, meta.* and constants.*"})
					} else {
						e.When = celWhen{p: pred}
					}
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
			if n.Config.Wasm != nil {
				// Compile the guest at verify time: existence and ABI export
				// check are gate-1 findings, not first-message failures.
				// Compilation does not execute guest code.
				modPath := n.Config.Wasm.Module
				if !filepath.IsAbs(modPath) && filepath.Dir(file) != "." {
					modPath = filepath.Join(filepath.Dir(file), modPath)
				}
				compiled, err := wasmhost.Compile(context.Background(), modPath, n.Config.Wasm)
				if err != nil {
					add(config.Diagnostic{Severity: "error", Code: "expr_wasm_compile", File: file, Line: n.Config.Line,
						Message: fmt.Sprintf("transform %q: %v", name, err),
						Hint:    "the module must be a wasm32-wasip1 reactor exporting _initialize, eb_alloc and transform (docs/wasm.md)"})
				} else {
					n.Wasm = compiled
				}
				// M3-audit J2: fast mode is the default, so an unset
				// timeout_ms deserves an explicit warning (--strict upgrades).
				if n.Config.Wasm.TimeoutMs < 0 {
					add(config.Diagnostic{Severity: "warning", Code: "wasm_no_kill_switch", File: file, Line: n.Config.Line,
						Message: fmt.Sprintf("transform %q runs the WASM guest without a kill switch (timeout_ms unset): a runaway guest wedges one worker until the pipeline restarts — no data is lost and the seven invariants hold", name),
						Hint:    "set wasm.timeout_ms to a positive wall-clock budget to arm per-invoke killing"})
				}
			}
		case config.SectionSource:
			if n.Config.Grpc != nil {
				validateExternal(p, n, reg, "source", file, add)
			} else if _, ok := reg.LookupSource(n.Config.Plugin); !ok {
				add(config.Diagnostic{Severity: "error", Code: "plugin_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown source plugin %q", n.Config.Plugin),
					Hint:    "run `eventboat verify` against a binary that registers this plugin; see plugin catalog"})
			} else {
				meta, _ := reg.LookupSource(n.Config.Plugin)
				checkDeclaredVersion(p, n, meta.Version, file, add)
				if _, err := reg.NewSource(n.Config.Plugin, n.Config.PluginConfig); err != nil {
					addSchemaDiags(file, n, err, add)
				}
			}
			codec := n.Config.Decoder
			if codec == "" {
				codec = "json"
			}
			if err := resolveCodec(p, reg, codec, file, n, add); err != nil {
				add(config.Diagnostic{Severity: "error", Code: "codec_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown decoder %q on source %q", codec, name)})
			}
		case config.SectionSink:
			if n.Config.Grpc != nil {
				validateExternal(p, n, reg, "sink", file, add)
			} else if _, ok := reg.LookupSink(n.Config.Plugin); !ok {
				add(config.Diagnostic{Severity: "error", Code: "plugin_unknown", File: file, Line: n.Config.Line,
					Message: fmt.Sprintf("unknown sink plugin %q", n.Config.Plugin)})
			} else {
				meta, _ := reg.LookupSink(n.Config.Plugin)
				checkDeclaredVersion(p, n, meta.Version, file, add)
				if _, err := reg.NewSink(n.Config.Plugin, n.Config.PluginConfig); err != nil {
					addSchemaDiags(file, n, err, add)
				}
			}
			codec := n.Config.Encoder
			if codec == "" {
				codec = "json"
			}
			if err := resolveCodec(p, reg, codec, file, n, add); err != nil {
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

	checkTelemetry(cfg, file, add)

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

// validateExternal runs the static checks for one out-of-process plugin node:
// no name collision with compiled-in plugins, schema validation against the
// manifest, and the declared-version check (review-m3 R5/R6/R9). No process
// is spawned here — verify stays side-effect free.
func validateExternal(p *Pipeline, n *Node, reg *registry.Registry, kind string, file string, add func(config.Diagnostic)) {
	if kind == "source" {
		if _, ok := reg.LookupSource(n.Config.Plugin); ok {
			add(config.Diagnostic{Severity: "error", Code: "grpc_builtin_conflict", File: file, Line: n.Config.Line,
				Message: fmt.Sprintf("plugin %q is compiled into this binary; a grpc block is only for external plugins", n.Config.Plugin),
				Hint:    "remove the grpc block to use the compiled-in plugin, or rename the external plugin"})
			return
		}
	} else if _, ok := reg.LookupSink(n.Config.Plugin); ok {
		add(config.Diagnostic{Severity: "error", Code: "grpc_builtin_conflict", File: file, Line: n.Config.Line,
			Message: fmt.Sprintf("plugin %q is compiled into this binary; a grpc block is only for external plugins", n.Config.Plugin),
			Hint:    "remove the grpc block to use the compiled-in plugin, or rename the external plugin"})
		return
	}
	m := n.Config.Manifest
	if m == nil {
		return // manifest read/shape errors were diagnosed at load time
	}
	if m.Name != n.Config.Plugin {
		add(config.Diagnostic{Severity: "error", Code: "grpc_manifest_name", File: file, Line: n.Config.Line,
			Message: fmt.Sprintf("manifest of plugin declares name %q but the plugin block key is %q", m.Name, n.Config.Plugin),
			Hint:    "the plugin block key must equal the manifest (and handshake) name"})
		return
	}
	checkDeclaredVersion(p, n, m.Version, file, add)
	schemaJSON, err := json.Marshal(m.ConfigSchema)
	if err != nil {
		add(config.Diagnostic{Severity: "error", Code: "grpc_manifest_schema", File: file, Line: n.Config.Line,
			Message: fmt.Sprintf("manifest schema of plugin %q: %v", n.Config.Plugin, err)})
		return
	}
	if err := registry.ValidateSchema(n.Config.Plugin, string(schemaJSON), n.Config.PluginConfig); err != nil {
		addSchemaDiags(file, n, err, add)
	}
}

// checkDeclaredVersion enforces the optional plugin version pin: a config
// referencing a version other than the registered one is a verify error
// (redesign-v3.md §6.5).
func checkDeclaredVersion(p *Pipeline, n *Node, registered int, file string, add func(config.Diagnostic)) {
	if n.Config.Version == 0 || n.Config.Version == registered {
		return
	}
	add(config.Diagnostic{Severity: "error", Code: "plugin_version_mismatch", File: file, Line: n.Config.Line,
		Message: fmt.Sprintf("node %q declares plugin %q version %d but version %d is %s", n.Name, n.Config.Plugin, n.Config.Version, registered, func() string {
			if n.Config.Grpc != nil {
				return "declared in its manifest"
			}
			return "registered in this binary"
		}()),
		Hint: "update the version pin or the plugin; the mismatch is fatal before any message flows"})
}

// checkJobSemantics enforces the job-pipeline rules of §5.8 (M2 review):
// pull-capability sources, cron syntax, parameter reference legality, hook
// sink schemas and the continuous-pipeline rejections.
func checkJobSemantics(p *Pipeline, reg *registry.Registry, parameters map[string]any, file string, add func(config.Diagnostic)) {	cfg := p.Config
	job := cfg.IsJob()

	// Cron syntax.
	if job && cfg.Run.Schedule != "" {
		if _, err := cron.ParseStandard(cfg.Run.Schedule); err != nil {
			add(config.Diagnostic{Severity: "error", Code: "job_bad_schedule", File: file,
				Line: 0, Message: fmt.Sprintf("run.schedule %q is not a valid 5-field cron expression: %v", cfg.Run.Schedule, err)})
		}
	}

	// Source capability + sql-in-continuous lint + multi-pull warning (M2
	// review #6, M3 R12): more than one pull source in a job pipeline makes
	// the `cursor` parameter binding ambiguous (it binds the first).
	warnMultiPull := func(line int) {
		add(config.Diagnostic{Severity: "warning", Code: "job_multiple_pull_sources", File: file,
			Line:    line,
			Message: "job pipeline has multiple pull sources; the cursor parameter binds the watermark of the first (declaration order)",
			Hint:    "reference per-source cursors explicitly or split into one pipeline per source"})
	}
	pullCount := 0
	for _, name := range p.Order {
		n := p.Nodes[name]
		if n.Section != config.SectionSource {
			continue
		}
		// External plugins declare capabilities in their manifest.
		if n.Config.Grpc != nil {
			if n.Config.Manifest == nil {
				continue // manifest errors were diagnosed at load time
			}
			if !hasCap(n.Config.Manifest.Capabilities, "pull") {
				if job {
					add(config.Diagnostic{Severity: "error", Code: "job_source_not_pull", File: file,
						Line:    n.Config.Line,
						Message: fmt.Sprintf("job pipeline source %q uses external plugin %q which has no pull capability", name, n.Config.Plugin),
						Hint:    "job pipelines need sources that page through data and signal exhaustion (capabilities: [pull])"})
				}
				continue
			}
			pullCount++
			if job && pullCount > 1 {
				warnMultiPull(n.Config.Line)
			}
			if !job {
				add(config.Diagnostic{Severity: "warning", Code: "lint_sql_continuous", File: file,
					Line:    n.Config.Line,
					Message: fmt.Sprintf("source %q is a pull source in a continuous pipeline: it pulls once at startup, then idles", name),
					Hint:    "job pipelines (run.mode: job) are the intended home for pull sources"})
			}
			continue
		}
		meta, ok := reg.LookupSource(n.Config.Plugin)
		if !ok {
			continue // unknown plugin already diagnosed
		}
		pull := hasCap(meta.Capabilities, "pull")
		if pull {
			pullCount++
		}
		if job && !pull {
			add(config.Diagnostic{Severity: "error", Code: "job_source_not_pull", File: file,
				Line:    n.Config.Line,
				Message: fmt.Sprintf("job pipeline source %q uses plugin %q which has no pull capability", name, n.Config.Plugin),
				Hint:    "job pipelines need sources that page through data and signal exhaustion (capabilities: [pull])"})
		}
		if job && pull && pullCount > 1 {
			warnMultiPull(n.Config.Line)
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
// checkTelemetry validates telemetry.redact patterns: each is a
// dot-separated field path where every segment is a path.Match glob. A bad
// pattern would silently never match — it is a verify error instead.
func checkTelemetry(cfg *config.Pipeline, file string, add func(config.Diagnostic)) {
	if cfg.Telemetry == nil {
		return
	}
	for _, pattern := range cfg.Telemetry.Redact {
		if pattern == "" {
			continue // the loader already rejects empty entries
		}
		for _, seg := range strings.Split(pattern, ".") {
			if _, err := path.Match(seg, ""); err != nil {
				add(config.Diagnostic{Severity: "error", Code: "telemetry_redact_pattern", File: file,
					Line:    0,
					Message: fmt.Sprintf("telemetry.redact pattern %q is not a valid field-path glob: %v", pattern, err),
					Hint:    `dot-separated segments, each a glob, e.g. payload.user.email or payload.credit_card*`})
				break
			}
		}
	}
}

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

// resolveCodec validates the codec a decoder/encoder references: declared
// names resolve to the pre-instantiated p.Codecs; bare names must be
// registered codecs whose factory accepts an empty configuration. A
// returned error means "not found" (the caller reports codec_unknown);
// configuration failures surface here as codec_config.
func resolveCodec(p *Pipeline, reg *registry.Registry, name, file string, n *Node, add func(config.Diagnostic)) error {
	if _, ok := p.Codecs[name]; ok {
		return nil
	}
	if _, ok := reg.LookupCodec(name); !ok {
		return fmt.Errorf("unknown codec %q", name)
	}
	if _, err := reg.NewCodec(name, nil, filepath.Dir(file)); err != nil {
		add(config.Diagnostic{Severity: "error", Code: "codec_config", File: file, Line: n.Config.Line,
			Message: fmt.Sprintf("codec %q: %v", name, err),
			Hint:    "codecs that need configuration (csv/avro/protobuf) must be declared under `codecs:` and referenced by name"})
	}
	return nil
}

func sortedDeclNames(m map[string]*config.CodecDecl) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

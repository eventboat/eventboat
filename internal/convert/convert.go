package convert

import (
	"fmt"
	"sort"
	"strings"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

// Result is the converter output: the v3 document plus the migration report.
type Result struct {
	YAML   []byte
	Report *Report
}

// Convert translates one archived v2 pipeline (any writing style, YAML or
// HOCON) into the v3 canonical three-section form. It is a pure function of
// (path, data): same input, same output — the property the CI snapshot tests
// rely on (redesign-v3.md §7.3).
func Convert(path string, data []byte) (*Result, error) {
	cfg, err := parseV2(path, data)
	if err != nil {
		return nil, err
	}
	rep := &Report{Source: path, Style: describeStyle(path, cfg)}

	stages, edges, notes, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	rep.Notes = append(rep.Notes, notes...)

	// Dead-letter sinks: v3 dead letters live in the store (replay --dlq),
	// so the v2 dlq section and its target sink stage disappear.
	stages, edges = dropDLQSink(cfg, stages, edges, rep)

	edges, stages, err = foldGates(stages, edges, rep)
	if err != nil {
		return nil, err
	}

	doc := buildDocument(cfg, stages, edges, rep)

	out, err := marshalDocument(doc)
	if err != nil {
		return nil, err
	}

	// Machine gate (review R2): "auto" means the output passes the real
	// verify pipeline — LoadBytes + ir.Build, the same path CLI verify uses.
	lr := config.LoadBytes("converted.yaml", out)
	diags := append([]config.Diagnostic(nil), lr.Diagnostics...)
	if lr.Pipeline != nil {
		_, buildDiags := ir.Build(lr.Pipeline, convertRegistry(), starhost.DefaultOptions(), nil)
		diags = append(diags, buildDiags...)
	}
	rep.VerifyDiags = diags
	rep.VerifyOK = true
	for _, d := range diags {
		if d.Severity == "error" {
			rep.VerifyOK = false
		}
	}
	return &Result{YAML: out, Report: rep}, nil
}

var regOnce struct {
	reg *registry.Registry
	err error
}

// convertRegistry is the converter's own registry instance (NOT the process
// Default() singleton): convert may run in the same process as other
// command paths that also register builtins, and double registration into
// Default() is an error. Content is identical to the CLI registry.
func convertRegistry() *registry.Registry {
	if regOnce.reg == nil {
		regOnce.reg = registry.New()
		regOnce.err = builtin.RegisterAll(regOnce.reg)
	}
	return regOnce.reg
}

func describeStyle(path string, cfg *v2PipelineConfig) string {
	format := "YAML"
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".conf") || strings.HasSuffix(lower, ".hocon") {
		format = "HOCON"
	}
	if len(cfg.Steps) > 0 {
		return "steps (" + format + ")"
	}
	return "pipeline[] (" + format + ")"
}

// dropDLQSink removes the sink stage referenced by the v2 dlq section (it
// had no inbound edges by design) and notes the semantic move.
func dropDLQSink(cfg *v2PipelineConfig, stages []*stage, edges []edge, rep *Report) ([]*stage, []edge) {
	if cfg.DLQ == nil || cfg.DLQ.Sink == "" {
		if cfg.HasDLQKey {
			rep.Notes = append(rep.Notes, "v2 `dlq:` section dropped: v3 dead letters go to the built-in dead-letter store (`eventboat replay --dlq`)")
		}
		return stages, edges
	}
	rep.Notes = append(rep.Notes,
		fmt.Sprintf("v2 `dlq:` section and its target sink %q dropped: v3 dead letters go to the built-in dead-letter store (`eventboat replay --dlq`)", cfg.DLQ.Sink))
	var kept []*stage
	for _, s := range stages {
		if s.Kind == "sink" && s.ID == cfg.DLQ.Sink {
			continue
		}
		kept = append(kept, s)
	}
	var keptEdges []edge
	for _, e := range edges {
		if e.To == cfg.DLQ.Sink {
			continue
		}
		keptEdges = append(keptEdges, e)
	}
	return kept, keptEdges
}

// foldGates removes filter and route transforms, propagating their semantics
// into ordered edge guards (review R3). Route transforms whose er-route value
// is referenced beyond their direct downstream edges are kept as script nodes
// writing meta.route; their downstream route attrs become v3 route sugar.
func foldGates(stages []*stage, edges []edge, rep *Report) ([]edge, []*stage, error) {
	byID := map[string]*stage{}
	for _, s := range stages {
		byID[s.ID] = s
	}
	sticky := false
	for i := range edges {
		e := &edges[i]
		if e.Route != "" {
			if up := byID[e.From]; up == nil || up.Kind != "transform" || up.Type != "route" {
				sticky = true
			}
		}
		if strings.Contains(e.Condition, "er-route") {
			sticky = true
		}
	}
	for _, s := range stages {
		if s.Kind == "transform" && (strings.Contains(fmt.Sprint(s.Config), "er-route") || strings.Contains(s.Predicate, "er-route")) {
			sticky = true
		}
	}
	if sticky {
		rep.Notes = append(rep.Notes, "route table referenced non-adjacently (sticky er-route): route transforms kept as scripts writing meta.route; route attrs become v3 `route:` edge sugar")
	}

	for {
		folded := false
		for _, s := range stages {
			if s.Kind != "transform" || sticky {
				continue
			}
			switch s.Type {
			case "filter":
				pred := dslOf(s)
				if pred == "" {
					rep.Notes = append(rep.Notes, fmt.Sprintf("filter transform %q had no dsl; treated as pass-through and removed", s.ID))
				} else {
					pred = rewritePredicate(pred)
				}
				var err error
				stages, edges, err = foldStage(stages, edges, s.ID, func(out edge) string {
					return andConds(pred, rewritePredicate(out.Condition))
				}, fmt.Sprintf("filter transform %q folded into its outgoing edges' `when` guards", s.ID), rep)
				if err != nil {
					return nil, nil, err
				}
				folded = true
			case "route":
				order, preds, err := routeTable(s.Config)
				if err != nil {
					return nil, nil, fmt.Errorf("route transform %q: %w", s.ID, err)
				}
				rewritten := make([]string, len(order))
				for i, name := range order {
					rewritten[i] = rewritePredicate(preds[name])
				}
				orAll := ""
				orAllTrue := false
				for _, p := range rewritten {
					if isTrue(p) {
						orAllTrue = true
						break
					}
				}
				if !orAllTrue {
					for _, p := range rewritten {
						orAll = orConds(orAll, p)
					}
				}
				stages, edges, err = foldStage(stages, edges, s.ID, func(out edge) string {
					if out.Route != "" {
						idx := -1
						for i, name := range order {
							if name == out.Route {
								idx = i
								break
							}
						}
						if idx < 0 {
							return "false" // unknown route name: never matches; invalid v2 surfaced by the report
						}
						return guardFor(idx, rewritten)
					}
					return andConds(rewritePredicate(out.Condition), orAll)
				}, fmt.Sprintf("route transform %q folded into ordered edge guards (first-match semantics preserved)", s.ID), rep)
				if err != nil {
					return nil, nil, err
				}
				folded = true
			}
			if folded {
				break
			}
		}
		if !folded {
			return edges, stages, nil
		}
	}
}

// guardFor computes the first-match guard for route i: p_i when no earlier
// route exists, else p_i && !(p_1 || ... || p_{i-1}); a literal-true route
// reduces to the negation of its priors.
func guardFor(i int, preds []string) string {
	p := preds[i]
	var prior string
	for j := 0; j < i; j++ {
		prior = orConds(prior, preds[j])
	}
	switch {
	case isTrue(p):
		if prior == "" {
			return ""
		}
		return "!(" + prior + ")"
	case prior == "":
		return p
	default:
		return p + " && !(" + prior + ")"
	}
}

func isTrue(p string) bool { return strings.TrimSpace(p) == "true" }

// andConds joins two CEL predicates; empty/true operands pass through.
func andConds(a, b string) string {
	switch {
	case a == "" || isTrue(a):
		return b
	case b == "" || isTrue(b):
		return a
	default:
		return wrapLogical(a) + " && " + wrapLogical(b)
	}
}

func orConds(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return wrapLogical(a) + " || " + wrapLogical(b)
	}
}

// wrapLogical parenthesizes predicates that contain logical connectives so
// composed guards keep their precedence.
func wrapLogical(p string) string {
	if strings.Contains(p, "&&") || strings.Contains(p, "||") {
		return "(" + p + ")"
	}
	return p
}

// foldStage rewires one gate stage's through-traffic: every (in, out) edge
// pair becomes an edge in→out whose condition is conditionFor(out); the out
// edge's attrs carry over. The stage and its own edges are removed.
func foldStage(stages []*stage, edges []edge, id string, conditionFor func(out edge) string, note string, rep *Report) ([]*stage, []edge, error) {
	var ins, outs, rest []edge
	for _, e := range edges {
		switch {
		case e.To == id:
			ins = append(ins, e)
		case e.From == id:
			outs = append(outs, e)
		default:
			rest = append(rest, e)
		}
	}
	if len(ins) == 0 {
		return nil, nil, fmt.Errorf("cannot fold %q: no incoming edges", id)
	}
	rep.Notes = append(rep.Notes, note)
	var keptStages []*stage
	for _, s := range stages {
		if s.ID != id {
			keptStages = append(keptStages, s)
		}
	}
	if len(outs) == 0 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("gate %q had no outgoing edges; removed as a dead end", id))
		return keptStages, rest, nil
	}
	for _, in := range ins {
		if in.Buffer != nil || in.Delivery != nil || in.Required != nil || in.Condition != "" || in.Route != "" {
			rep.Notes = append(rep.Notes, fmt.Sprintf("edge %s -> %s carried attributes into folded gate %q; those attributes no longer apply (the gate transform's own retry policy vanished with it)", in.From, in.To, id))
			break
		}
	}
	for _, in := range ins {
		for _, out := range outs {
			rest = append(rest, edge{
				From: in.From, To: out.To, Condition: conditionFor(out),
				Buffer: out.Buffer, Delivery: out.Delivery, Required: out.Required,
			})
		}
	}
	return keptStages, rest, nil
}

func dslOf(s *stage) string {
	if s.Config == nil {
		return ""
	}
	d, _ := s.Config["dsl"].(string)
	return strings.TrimSpace(d)
}

// ---- v3 document construction ----

// outEdge is the converted v3 edge-attribute form.
type outEdge struct {
	From         string
	When         string
	Route        string // v3 route sugar (sticky path only)
	Delivery     map[string]any
	RequiredFalse bool
	Buffer       map[string]any
}

// outNode is one v3 node (before YAML emission).
type outNode struct {
	id      string
	section string // "sources" | "transforms" | "sinks"
	decoder string
	encoder string
	workers int
	plugin  string
	plugCfg map[string]any
	script  string
	edges   []outEdge
	batch   map[string]any
}

// outDoc assembles the v3 document with deterministic ordering.
type outDoc struct {
	Name         string
	Limits       map[string]any
	EdgeDefaults map[string]any
	Codecs       map[string]any
	Sources      []*outNode
	Transforms   []*outNode
	Sinks        []*outNode
}

func buildDocument(cfg *v2PipelineConfig, stages []*stage, edges []edge, rep *Report) *outDoc {
	doc := &outDoc{Name: "converted"}
	if n := cfg.Metadata["name"]; n != "" {
		doc.Name = n
	}

	if limits := engineLimits(cfg, rep); limits != nil {
		doc.Limits = limits
	}
	if ed := edgeDefaultsDoc(cfg.EdgeDefaults, rep); ed != nil {
		doc.EdgeDefaults = ed
	}
	codecs := &codecState{}

	var sources, transforms, sinks []*outNode
	for _, s := range stages {
		switch s.Kind {
		case "source":
			sources = append(sources, &outNode{
				id: s.ID, section: "sources",
				decoder: codecs.refDoc(s.Decoder, "decoder", s.ID, rep),
				plugin:  pluginName(s), plugCfg: sourcePluginCfg(s, rep),
			})
		case "sink":
			n := &outNode{
				id: s.ID, section: "sinks",
				encoder: codecs.refDoc(s.Encoder, "encoder", s.ID, rep),
				plugin:  pluginName(s), plugCfg: sinkPluginCfg(s, rep),
			}
			n.batch = batchDoc(s.Batch, rep)
			sinkExtras(s, rep)
			sinks = append(sinks, n)
		case "transform":
			transforms = append(transforms, transformNode(s, rep))
		}
	}
	if decls := codecs.mergeDecls(cfg, rep); len(decls) > 0 {
		doc.Codecs = decls
	}

	// Convert edges to their v3 form and attach to targets.
	index := map[string]*outNode{}
	for _, n := range append(append(append([]*outNode{}, sources...), transforms...), sinks...) {
		index[n.id] = n
	}
	for _, e := range edges {
		target := index[e.To]
		if target == nil {
			continue
		}
		oe := outEdge{From: e.From}
		if e.Route != "" {
			oe.Route = e.Route
		} else if cond := rewritePredicate(e.Condition); cond != "" {
			oe.When = cond
		}
		if e.Delivery != nil {
			oe.Delivery = deliveryDoc(e.Delivery, rep)
		}
		if e.Required != nil && !*e.Required {
			oe.RequiredFalse = true
		}
		if e.Buffer != nil {
			oe.Buffer = bufferDoc(e.Buffer, rep)
		}
		target.edges = append(target.edges, oe)
	}

	doc.Sources = topoSort(sources, edges, "sources")
	doc.Transforms = topoSort(transforms, edges, "transforms")
	doc.Sinks = topoSort(sinks, edges, "sinks")
	return doc
}

func engineLimits(cfg *v2PipelineConfig, rep *Report) map[string]any {
	limits := map[string]any{}
	if cfg.Engine.MaxInflight > 0 {
		limits["max_in_flight"] = cfg.Engine.MaxInflight
	}
	if cfg.Engine.DrainTimeout != "" {
		limits["drain_timeout"] = cfg.Engine.DrainTimeout
	}
	if cfg.Engine.MaxWorkers > 0 {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "engine.max_workers", Reason: "v3 has no global worker cap; concurrency is per-node `workers`",
			Suggestion: "set `workers:` on the transform nodes that need it (default 1)",
		})
	}
	if cfg.Engine.ErrorMode != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "engine.error_mode", Reason: "the v2 four-layer error-mode inheritance chain does not exist in v3",
			Suggestion: "per-edge `delivery:` (retries/backoff) and `required:` replace it",
		})
	}
	if cfg.HasObsKey {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "observability:", Reason: "v3 separates pipeline resources from runtime deployment config",
			Suggestion: "move endpoints to a `kind: Runtime` file (storage/admin/mcp/telemetry) next to the pipeline",
		})
	}
	if len(limits) == 0 {
		return nil
	}
	return limits
}

// codecState carries synthesized `codecs:` declarations while the document
// is built (inline v2 codec configs become named declarations); it keeps
// Convert a pure function (no package state).
type codecState struct {
	synthesized map[string]map[string]any
}

// refDoc resolves a v2 CodecRef to a v3 codec NAME: a `ref` maps to the
// same-name declaration converted from the v2 codecs list; an inline
// `{type, config}` synthesizes a `<node>-<field>-codec` declaration; a bare
// scalar stays the codec type name.
func (st *codecState) refDoc(c *v2CodecRef, field, node string, rep *Report) string {
	if c == nil {
		return ""
	}
	switch {
	case c.Ref != "":
		return c.Ref
	case len(c.Config) > 0:
		name := node + "-" + field + "-codec"
		decl := map[string]any{"type": c.Type}
		for k, v := range c.Config {
			decl[k] = v
		}
		if st.synthesized == nil {
			st.synthesized = map[string]map[string]any{}
		}
		st.synthesized[name] = decl
		rep.Notes = append(rep.Notes, fmt.Sprintf("v2 inline %s config on %q → synthesized codecs: declaration %q", field, node, name))
		return name
	default:
		return c.Type
	}
}

// mergeDecls combines the v2 codecs list with synthesized declarations.
func (st *codecState) mergeDecls(cfg *v2PipelineConfig, rep *Report) map[string]any {
	out := map[string]any{}
	for _, c := range cfg.Codecs {
		decl := map[string]any{"type": c.Type}
		for k, v := range c.Config {
			decl[k] = v
		}
		out[c.Name] = decl
		rep.Notes = append(rep.Notes, fmt.Sprintf("v2 codec %q → v3 codecs: declaration (type %s)", c.Name, c.Type))
	}
	for _, name := range sortedKeys(st.synthesized) {
		if _, taken := out[name]; taken {
			continue
		}
		out[name] = st.synthesized[name]
	}
	return out
}

func edgeDefaultsDoc(attrs v2EdgeAttrs, rep *Report) map[string]any {
	out := map[string]any{}
	if attrs.Condition != "" || attrs.Route != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "edgeDefaults.condition/route", Reason: "v3 edge_defaults carry only delivery/required/buffer defaults",
			Suggestion: "apply the condition on the concrete edges' `when:`",
		})
	}
	if attrs.Delivery != nil {
		out["delivery"] = deliveryDoc(attrs.Delivery, rep)
	}
	if attrs.Required != nil && !*attrs.Required {
		out["required"] = false
	}
	if attrs.Buffer != nil {
		if b := bufferDoc(attrs.Buffer, rep); b != nil {
			out["buffer"] = b
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func deliveryDoc(d *v2DeliverySpec, rep *Report) map[string]any {
	out := map[string]any{}
	if d.Retry != nil {
		if d.Retry.Max > 0 {
			out["retries"] = d.Retry.Max
		}
		if d.Retry.Backoff != "" {
			out["backoff"] = d.Retry.Backoff
		}
	}
	if d.Timeout != "" {
		if ms, ok := durationToMs(d.Timeout); ok {
			out["timeout_ms"] = ms
		} else {
			rep.Manuals = append(rep.Manuals, manualItem{
				Where: "delivery.timeout", Reason: fmt.Sprintf("timeout %q is not a Go duration", d.Timeout),
				Suggestion: "use e.g. 5s → timeout_ms: 5000",
			})
		}
	}
	if d.DLQ != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "delivery.dlq", Reason: fmt.Sprintf("per-edge dlq target %q has no v3 equivalent", d.DLQ),
			Suggestion: "v3 dead letters land in the built-in store; query/replay with `eventboat replay --dlq`",
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func bufferDoc(b *v2EdgeBuffer, rep *Report) map[string]any {
	if b.Type != "" && b.Type != "memory" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "buffer.type", Reason: fmt.Sprintf("buffer type %q has no v3 equivalent (buffers are in-memory surge absorption; durability comes from the spool)", b.Type),
			Suggestion: "keep the memory buffer sizing, rely on the spool for durability",
		})
	}
	if b.Strategy != "" && b.Strategy != "block" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "buffer.strategy", Reason: fmt.Sprintf("buffer strategy %q has no v3 equivalent (memory buffers block; the spool absorbs surges)", b.Strategy),
			Suggestion: "for best-effort edges use `required: false` so failures drop instead of blocking",
		})
	}
	out := map[string]any{"type": "memory"}
	if b.Size > 0 {
		out["max_events"] = b.Size
	}
	return out
}

func batchDoc(b *v2BatchConfig, rep *Report) map[string]any {
	if b == nil {
		return nil
	}
	out := map[string]any{}
	if b.Size > 0 {
		out["size"] = b.Size
	}
	if b.Timeout != "" {
		if ms, ok := durationToMs(b.Timeout); ok {
			out["timeout_ms"] = ms
		}
	}
	if b.MaxBytes > 0 {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: "batch.max_bytes", Reason: "v3 batches size by count and flush interval only",
			Suggestion: "drop max_bytes or enforce byte limits downstream",
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func durationToMs(s string) (int, bool) {
	d, err := config.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, false
	}
	return int(d.Milliseconds()), true
}

// codecRefDoc resolves a v2 CodecRef to a plain codec name, reporting the
// shapes v3 cannot express as a bare name (until the codecs: section lands).
func pluginName(s *stage) string { return s.Type }

// sourcePluginCfg maps known v2 source config field differences; unknown
// plugins pass through untouched (verify flags what does not exist in v3).
func sourcePluginCfg(s *stage, rep *Report) map[string]any {
	cfg := cloneMap(s.Config)
	switch s.Type {
	case "cron":
		if sched, ok := cfg["schedule"].(string); ok {
			fields := strings.Fields(sched)
			expr := sched
			if len(fields) == 6 {
				expr = strings.Join(fields[1:], " ")
				rep.Notes = append(rep.Notes, fmt.Sprintf("cron source %q: 6-field schedule %q → 5-field expression %q (v3 uses standard 5-field cron)", s.ID, sched, expr))
			}
			delete(cfg, "schedule")
			cfg["expression"] = expr
		}
		if tz, ok := cfg["timezone"]; ok {
			delete(cfg, "timezone")
			rep.Manuals = append(rep.Manuals, manualItem{
				Where: fmt.Sprintf("source %q timezone %v", s.ID, tz), Reason: "the v3 cron source has no timezone knob; it ticks in the host's local time",
				Suggestion: "run the eventboat process (or container) in the target timezone",
			})
		}
	case "http_server":
		// v2 `address` → v3 `listen` (same ":8080" value shape).
		if addr, ok := cfg["address"]; ok {
			delete(cfg, "address")
			cfg["listen"] = addr
			rep.Notes = append(rep.Notes, fmt.Sprintf("http_server source %q: address → listen (same \":port\" value)", s.ID))
		}
	}
	return cfg
}

func sinkPluginCfg(s *stage, rep *Report) map[string]any {
	cfg := cloneMap(s.Config)
	switch s.Type {
	case "http":
		if method, ok := cfg["method"].(string); ok {
			delete(cfg, "method")
			if !strings.EqualFold(method, "POST") {
				rep.Manuals = append(rep.Manuals, manualItem{
					Where: fmt.Sprintf("sink %q method %q", s.ID, method), Reason: "the v3 http sink always POSTs",
					Suggestion: "proxy non-POST targets, or implement a gRPC/http sink plugin",
				})
			}
		}
	}
	return cfg
}

// sinkExtras reports sink-block fields with no v3 node equivalent.
func sinkExtras(s *stage, rep *Report) {
	if s.Ordering != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("sink %q ordering %q", s.ID, s.Ordering), Reason: "v3 dropped the global ordered switch for per-key ordering",
			Suggestion: "set `order_key:` on a business key (e.g. 'payload.order_no') for partition-level ordering",
		})
	}
	if s.MaxInFlight > 0 {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("sink %q max_in_flight %d", s.ID, s.MaxInFlight), Reason: "v3 has no per-sink inflight cap; the pipeline-level limits.max_in_flight governs admission",
			Suggestion: "set limits.max_in_flight on the pipeline if the source needs throttling",
		})
	}
}

// transformNode converts one v2 transform into a v3 transform node.
func transformNode(s *stage, rep *Report) *outNode {
	n := &outNode{id: s.ID, section: "transforms", workers: s.Workers}
	if s.ErrorMode != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q error_mode %q", s.ID, s.ErrorMode), Reason: "per-transform error modes do not exist in v3",
			Suggestion: "control failure handling per edge with `delivery:` and `required:`",
		})
	}
	if s.Predicate != "" {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q predicate", s.ID), Reason: "the v2 per-stage predicate field has no v3 transform equivalent",
			Suggestion: "move the condition onto the outgoing edges' `when:`",
		})
	}
	switch s.Type {
	case "map":
		dsl := dslOf(s)
		script, rows, manuals, notes := renderScript(fmt.Sprintf("transform %q (map)", s.ID), dsl)
		rep.StmtGroups = append(rep.StmtGroups, StmtGroup{Where: fmt.Sprintf("transform %q (map dsl)", s.ID), Rows: rows})
		rep.Manuals = append(rep.Manuals, manuals...)
		rep.Notes = append(rep.Notes, notes...)
		// Compile gate: a generated script that does not compile under the
		// real Starlark host is downgraded to a TODO comment + manual item.
		if _, err := starhost.Compile("convert/"+s.ID, script, starhost.DefaultOptions()); err != nil {
			rep.Manuals = append(rep.Manuals, manualItem{
				Where: fmt.Sprintf("transform %q script", s.ID), Reason: "generated script failed the Starlark compile gate: " + err.Error(),
				Suggestion: "finish the conversion by hand — the report rows mark the statements that did not translate",
			})
			script = "# TODO(convert): statement(s) below did not auto-convert; see the migration report\n"
		}
		n.script = script
	case "filter":
		// Only reachable on the sticky path (normally filters fold away).
		pred := rewritePredicate(dslOf(s))
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q (filter)", s.ID), Reason: "sticky er-route references prevented folding this filter, and scripts cannot drop messages",
			Suggestion: "move the predicate onto the outgoing edges' `when:` by hand: " + pred,
		})
		n.script = fmt.Sprintf("# TODO(convert): filter %q — put `%s` on the outgoing edges' when:\n", s.ID, pred)
	case "route":
		n.script = routeScript(s, rep)
	case "wasm":
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q (wasm)", s.ID), Reason: "the v2 wasm transform was an unimplemented stub, so the node carried no runtime semantics",
			Suggestion: "v3 has a real wasm tier (docs/wasm.md) — port the logic as a guest and declare `wasm:` on a transform node",
		})
		n.script = "# TODO(convert): v2 wasm transform stub — port to the v3 wasm tier (docs/wasm.md)\n"
	default:
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q type %q", s.ID, s.Type), Reason: "unknown v2 transform type; kept as an identity node",
			Suggestion: "replace the script with the equivalent Starlark (or wasm/script) logic",
		})
		n.script = fmt.Sprintf("# TODO(convert): unknown v2 transform type %q — implement me\n", s.Type)
	}
	return n
}

// routeScript synthesizes the sticky-path route transform: an if/elif chain
// writing meta.route with first-match semantics (spec §5.4 sugar target).
func routeScript(s *stage, rep *Report) string {
	order, preds, err := routeTable(s.Config)
	if err != nil {
		rep.Manuals = append(rep.Manuals, manualItem{
			Where: fmt.Sprintf("transform %q (route)", s.ID), Reason: err.Error(),
			Suggestion: "hand-write the route as if/elif over meta.route",
		})
		return "# TODO(convert): route table unreadable — see the migration report\n"
	}
	var b strings.Builder
	first := true
	for _, name := range order {
		pred := rewritePredicate(preds[name])
		rend := &starlarkRenderer{}
		rendered, rerr := func() (string, error) {
			env, err := eqlEnv()
			if err != nil {
				return "", err
			}
			east, issues := env.Compile(pred)
			if issues != nil && issues.Err() != nil {
				return "", issues.Err()
			}
			return rend.render(east.NativeRep().Expr())
		}()
		if rerr != nil {
			rep.Manuals = append(rep.Manuals, manualItem{
				Where: fmt.Sprintf("transform %q route %q", s.ID, name), Reason: "route predicate did not render to Starlark: " + rerr.Error(),
				Suggestion: "hand-write the branch as an if over meta.route",
			})
			continue
		}
		kw := "if"
		if !first {
			kw = "elif"
		}
		fmt.Fprintf(&b, "%s %s:\n    meta.route = %q\n", kw, rendered, name)
		first = false
	}
	if !first { // at least one branch emitted
		b.WriteString("else:\n    pass  # no match: v2 dropped the message (v3 settles it as filtered)\n")
	}
	if b.Len() == 0 {
		return "# TODO(convert): route predicates did not render — see the migration report\n"
	}
	return b.String()
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// topoSort orders nodes deterministically: upstream before downstream,
// alphabetical within a depth level.
func topoSort(nodes []*outNode, edges []edge, section string) []*outNode {
	allDeps := map[string][]string{} // node -> upstream ids
	for _, e := range edges {
		allDeps[e.To] = append(allDeps[e.To], e.From)
	}
	remaining := map[string]*outNode{}
	for _, n := range nodes {
		remaining[n.id] = n
	}
	placed := map[string]bool{}
	var out []*outNode
	for len(remaining) > 0 {
		var ready []string
		for id := range remaining {
			ok := true
			for _, d := range allDeps[id] {
				if !placed[d] {
					ok = false
					break
				}
			}
			if ok {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			// Cycle/dangling: flush by name; verify on the output flags it.
			for id := range remaining {
				ready = append(ready, id)
			}
		}
		sort.Strings(ready)
		for _, id := range ready {
			if n := remaining[id]; n != nil {
				out = append(out, n)
			}
			placed[id] = true
			delete(remaining, id)
		}
	}
	return out
}

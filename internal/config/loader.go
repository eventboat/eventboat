package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	// Pass 3: ${constants.x} substitution over decoded values. Successful
	// substitutions are recorded: the substituted tree no longer carries the
	// reference text, so lint_constant_unused must count usage on this record
	// plus the binding-form (constants.x) references that survive in
	// scripts and predicates (M1 debt fix).
	//
	// ${parameters.x} is NOT substituted here: parameters are set at trigger
	// time (§5.9). In a job pipeline the token passes through untouched (the
	// jobs runner resolves it per run); anywhere else it is an error.
	constants := map[string]any{}
	if c, ok := raw["constants"].(map[string]any); ok {
		constants = c
	}
	jobMode := false
	if rn, ok := raw["run"].(map[string]any); ok {
		jobMode = rn["mode"] == "job"
	}
	cs := &constantsSubstituter{file: file, constants: constants, diags: &res.Diagnostics, used: map[string]bool{}, jobParameters: jobMode}
	for key, val := range raw {
		if key == "constants" {
			continue
		}
		raw[key] = cs.substitute(val)
	}

	lines := collectLines(root)

	// Pass 4: structural validation with whitelists.
	p := &Pipeline{
		File:          file,
		Constants:     constants,
		ConstantsUsed: cs.used,
		EdgeDefaults:  EdgeAttrs{},
		Sources:       map[string]*Node{},
		Transforms:    map[string]*Node{},
		Sinks:         map[string]*Node{},
	}
	res.Pipeline = p

	allowedTop := map[string]bool{
		"apiVersion": true, "kind": true, "metadata": true,
		"edge_defaults": true, "constants": true, "limits": true,
		"run": true, "parameters": true, "hooks": true,
		"sources": true, "transforms": true, "sinks": true,
	}
	for _, kv := range mappingPairs(root) {
		key := kv.key
		if !allowedTop[key] {
			hint := "supported top-level keys: apiVersion, kind, metadata, edge_defaults, constants, limits, run, parameters, hooks, sources, transforms, sinks"
			if key == "codecs" || key == "dlq" || key == "telemetry" {
				hint = key + " is defined by redesign-v3.md §5.10 but not implemented yet"
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

	if lim, ok := raw["limits"]; ok {
		lm, ok := lim.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_limits_type", File: file, Line: lines.line("limits"),
				Message: "limits must be a mapping", Hint: "limits: { max_in_flight: 10000, drain_timeout: 10s }",
			})
		} else {
			l := &Limits{}
			for k := range lm {
				if k != "max_in_flight" && k != "drain_timeout" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: lines.line("limits"),
						Message: fmt.Sprintf("unknown limits field %q", k), Hint: "allowed: max_in_flight, drain_timeout",
					})
				}
			}
			if v, ok := lm["max_in_flight"]; ok {
				l.MaxInFlight = asInt(v, 0)
				if l.MaxInFlight < 1 {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_limits_range", File: file, Line: lines.line("limits"),
						Message: "limits.max_in_flight must be >= 1", Hint: "",
					})
					l.MaxInFlight = 0
				}
			}
			if v, ok := lm["drain_timeout"]; ok {
				s, ok := v.(string)
				if !ok {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_limits_type", File: file, Line: lines.line("limits"),
						Message: "limits.drain_timeout must be a duration string (e.g. 10s, 2m)", Hint: "",
					})
				} else {
					d, err := ParseDuration(s)
					if err != nil || d <= 0 {
						res.Diagnostics = append(res.Diagnostics, Diagnostic{
							Severity: "error", Code: "cfg_limits_range", File: file, Line: lines.line("limits"),
							Message: fmt.Sprintf("limits.drain_timeout %q is not a positive duration", s), Hint: "",
						})
					} else {
						l.DrainTimeout = d
					}
				}
			}
			p.Limits = l
		}
	}

	parseRun(file, raw, p, lines, res)
	parseParameters(file, raw, p, lines, res)
	parseHooks(file, raw, p, lines, res)

	parseSection(file, raw, "sources", SectionSource, p, lines, res)
	parseSection(file, raw, "transforms", SectionTransform, p, lines, res)
	parseSection(file, raw, "sinks", SectionSink, p, lines, res)

	loadManifests(file, p, res)

	return res
}

// loadManifests reads the plugin manifest of every external (grpc) node.
// Manifests keep verify static: the schema check runs against the file, not a
// spawned process (redesign-v3-review-m3.md R5).
func loadManifests(file string, p *Pipeline, res *Result) {
	dir := "."
	if i := strings.LastIndexByte(file, '/'); i >= 0 {
		dir = file[:i]
	}
	if i := strings.LastIndexByte(file, '\\'); i >= 0 && i > len(dir)-1 {
		dir = file[:i]
	}
	for _, node := range p.Order {
		var n *Node
		if v, ok := p.Sources[node]; ok {
			n = v
		} else if v, ok := p.Transforms[node]; ok {
			n = v
		} else {
			n = p.Sinks[node]
		}
		if n == nil || n.Grpc == nil {
			continue
		}
		path := n.Grpc.Schema
		if strings.Contains(path, "${parameters.") {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_read", File: file, Line: n.Line,
				Message: fmt.Sprintf("grpc.schema of plugin %q must not reference job parameters: the manifest is read at load time, before parameters resolve", n.Plugin),
				Hint:    "use a static path; parameterize grpc.command instead",
			})
			continue
		}
		if !filepath.IsAbs(path) && dir != "" && dir != "." {
			path = dir + "/" + n.Grpc.Schema
		}
		data, err := os.ReadFile(path)
		if err != nil {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_read", File: file, Line: n.Line,
				Message: fmt.Sprintf("manifest for plugin %q: %v", n.Plugin, err),
				Hint:    "grpc.schema is resolved relative to the pipeline file",
			})
			continue
		}
		var m PluginManifest
		if err := json.Unmarshal(data, &m); err != nil {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_parse", File: file, Line: n.Line,
				Message: fmt.Sprintf("manifest for plugin %q is not valid JSON: %v", n.Plugin, err), Hint: "",
			})
			continue
		}
		kind := "source"
		if n.Section == SectionSink {
			kind = "sink"
		}
		// The name-vs-block-key and builtin-conflict checks live in ir (they
		// need the registry); here we only validate the manifest's shape.
		switch {
		case m.Kind != kind:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_kind", File: file, Line: n.Line,
				Message: fmt.Sprintf("manifest of plugin %q declares kind %q; node %q is in %s", n.Plugin, m.Kind, n.Name, n.Section),
				Hint:    "source plugins serve in sources:, sink plugins in sinks:",
			})
		case m.Version < 1:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_version", File: file, Line: n.Line,
				Message: fmt.Sprintf("manifest of plugin %q must declare version >= 1", n.Plugin), Hint: "",
			})
		case m.ConfigSchema == nil:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "grpc_manifest_schema", File: file, Line: n.Line,
				Message: fmt.Sprintf("manifest of plugin %q has no config_schema", n.Plugin), Hint: "",
			})
		default:
			n.Manifest = &m
		}
	}
}

// parseRun validates the run block (redesign-v3.md §5.8). Cron syntax is
// validated later in ir.Build (the config layer stays parser-free).
func parseRun(file string, raw map[string]any, p *Pipeline, lines *lineIndex, res *Result) {
	rn, present := raw["run"]
	if !present {
		return
	}
	rm, ok := rn.(map[string]any)
	if !ok {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_run_type", File: file, Line: lines.line("run"),
			Message: "run must be a mapping", Hint: `run: { mode: job, schedule: "0 1 * * *" }`,
		})
		return
	}
	for k := range rm {
		switch k {
		case "mode", "schedule", "overlap", "catchup_window", "skip_if_successful", "retention":
		default:
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_unknown_field", File: file, Line: lines.line("run"),
				Message: fmt.Sprintf("unknown run field %q", k),
				Hint:    "allowed: mode, schedule, overlap, catchup_window, skip_if_successful, retention",
			})
		}
	}
	r := &RunSpec{Mode: "continuous", Overlap: "skip"}
	if v, ok := rm["mode"]; ok {
		s, ok := v.(string)
		if !ok || (s != "continuous" && s != "job") {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_mode", File: file, Line: lines.line("run", "mode"),
				Message: fmt.Sprintf("run.mode must be \"continuous\" or \"job\", got %v", v), Hint: "",
			})
		} else {
			r.Mode = s
		}
	}
	if v, ok := rm["schedule"]; ok {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_schedule", File: file, Line: lines.line("run", "schedule"),
				Message: "run.schedule must be a non-empty cron expression (5-field standard)", Hint: "",
			})
		} else {
			r.Schedule = s
		}
	}
	if r.Schedule != "" && r.Mode != "job" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_run_schedule", File: file, Line: lines.line("run", "schedule"),
			Message: "run.schedule is only meaningful with run.mode: job", Hint: "set run.mode: job or drop the schedule",
		})
	}
	if v, ok := rm["overlap"]; ok {
		s, ok := v.(string)
		if !ok || (s != "skip" && s != "all" && s != "latest") {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_overlap", File: file, Line: lines.line("run", "overlap"),
				Message: fmt.Sprintf("run.overlap must be one of skip|all|latest, got %v", v), Hint: "",
			})
		} else {
			r.Overlap = s
		}
	}
	if v, ok := rm["catchup_window"]; ok {
		s, ok := v.(string)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_catchup", File: file, Line: lines.line("run", "catchup_window"),
				Message: "run.catchup_window must be a duration (e.g. 2h)", Hint: "",
			})
		} else {
			d, err := ParseDuration(s)
			if err != nil || d < 0 {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_run_catchup", File: file, Line: lines.line("run", "catchup_window"),
					Message: fmt.Sprintf("run.catchup_window %q is not a valid duration", s), Hint: "",
				})
			} else {
				r.CatchupWindow = d
			}
		}
	}
	if v, ok := rm["skip_if_successful"]; ok {
		b, ok := v.(bool)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_skip", File: file, Line: lines.line("run", "skip_if_successful"),
				Message: "run.skip_if_successful must be a boolean", Hint: "",
			})
		} else {
			r.SkipIfSuccessful = b
		}
	}
	if v, ok := rm["retention"]; ok {
		rt, ok := v.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_run_retention", File: file, Line: lines.line("run", "retention"),
				Message: "run.retention must be a mapping", Hint: "retention: { history: 90d }",
			})
		} else {
			for k := range rt {
				if k != "history" {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_unknown_field", File: file, Line: lines.line("run", "retention"),
						Message: fmt.Sprintf("unknown retention field %q", k), Hint: "allowed: history",
					})
				}
			}
			if v, ok := rt["history"]; ok {
				s, ok := v.(string)
				if !ok {
					res.Diagnostics = append(res.Diagnostics, Diagnostic{
						Severity: "error", Code: "cfg_run_retention", File: file, Line: lines.line("run", "retention", "history"),
						Message: "retention.history must be a duration (e.g. 90d)", Hint: "",
					})
				} else {
					d, err := ParseDuration(s)
					if err != nil || d <= 0 {
						res.Diagnostics = append(res.Diagnostics, Diagnostic{
							Severity: "error", Code: "cfg_run_retention", File: file, Line: lines.line("run", "retention", "history"),
							Message: fmt.Sprintf("retention.history %q is not a positive duration", s), Hint: "",
						})
					} else {
						r.Retention = d
					}
				}
			}
		}
	}
	p.Run = r
}

// parseParameters validates typed parameter declarations (§5.9).
func parseParameters(file string, raw map[string]any, p *Pipeline, lines *lineIndex, res *Result) {
	pn, present := raw["parameters"]
	if !present {
		return
	}
	pm, ok := pn.(map[string]any)
	if !ok {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_parameters_type", File: file, Line: lines.line("parameters"),
			Message: "parameters must be a mapping of name to declaration", Hint: `from: { type: string, default: cursor }`,
		})
		return
	}
	if p.Run == nil || p.Run.Mode != "job" {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_parameters_not_job", File: file, Line: lines.line("parameters"),
			Message: "parameters are only valid in job pipelines (run.mode: job)", Hint: "add run: { mode: job } or remove the parameters section",
		})
		return
	}
	out := map[string]*ParameterSpec{}
	for name, decl := range pm {
		line := lines.line("parameters", name)
		dm, ok := decl.(map[string]any)
		if !ok {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_parameters_type", File: file, Line: line,
				Message: fmt.Sprintf("parameter %q must be a mapping", name), Hint: `from: { type: string, default: cursor }`,
			})
			continue
		}
		spec := &ParameterSpec{Name: name, Type: "string", Line: line}
		for k := range dm {
			switch k {
			case "type", "default", "required", "enum", "pattern", "min", "max":
			default:
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
					Message: fmt.Sprintf("unknown parameter field %q on %q", k, name),
					Hint:    "allowed: type, default, required, enum, pattern, min, max",
				})
			}
		}
		if v, ok := dm["type"]; ok {
			s, ok := v.(string)
			switch {
			case !ok:
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, "type must be a string"))
			case s == "integer" || s == "number" || s == "string" || s == "boolean":
				spec.Type = s
			default:
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
					fmt.Sprintf("type must be one of string|integer|number|boolean, got %q", s)))
			}
		}
		if v, ok := dm["default"]; ok {
			if err := checkParamType(spec.Type, v); err != nil {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
					fmt.Sprintf("default %v does not match type %s (%v)", v, spec.Type, err)))
			} else {
				spec.Default = v
			}
		}
		if v, ok := dm["required"]; ok {
			b, ok := v.(bool)
			if !ok {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, "required must be a boolean"))
			} else {
				spec.Required = b
			}
		}
		if v, ok := dm["enum"]; ok {
			list, ok := v.([]any)
			if !ok || len(list) == 0 {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, "enum must be a non-empty list"))
			} else {
				for _, el := range list {
					if err := checkParamType(spec.Type, el); err != nil {
						res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
							fmt.Sprintf("enum value %v does not match type %s", el, spec.Type)))
					}
				}
				spec.Enum = list
			}
		}
		if v, ok := dm["pattern"]; ok {
			s, ok := v.(string)
			if !ok {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, "pattern must be a string (regular expression)"))
			} else if _, err := regexp.Compile(s); err != nil {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
					fmt.Sprintf("pattern %q does not compile: %v", s, err)))
			} else {
				spec.Pattern = s
			}
		}
		for key, dst := range map[string]**float64{"min": &spec.Min, "max": &spec.Max} {
			if v, ok := dm[key]; ok {
				f, ok := toFloat(v)
				if !ok {
					res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, key+" must be a number"))
				} else {
					*dst = &f
				}
			}
		}
		if spec.Min != nil && spec.Max != nil && *spec.Min > *spec.Max {
			res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name, "min must be <= max"))
		}
		// Self-consistency: default inside enum/pattern/min/max.
		if spec.Default != nil {
			if len(spec.Enum) > 0 && !valueIn(spec.Default, spec.Enum) {
				res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
					fmt.Sprintf("default %v is not one of the enum values", spec.Default)))
			}
			if spec.Pattern != "" {
				if s, ok := spec.Default.(string); ok && !regexp.MustCompile(spec.Pattern).MatchString(s) {
					res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
						fmt.Sprintf("default %q does not match pattern %q", s, spec.Pattern)))
				}
			}
			if f, ok := toFloat(spec.Default); ok {
				if spec.Min != nil && f < *spec.Min {
					res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
						fmt.Sprintf("default %v is below min %v", f, *spec.Min)))
				}
				if spec.Max != nil && f > *spec.Max {
					res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
						fmt.Sprintf("default %v is above max %v", f, *spec.Max)))
				}
			}
		}
		if spec.Required && spec.Default != nil {
			res.Diagnostics = append(res.Diagnostics, diagParam(file, line, name,
				"required parameters cannot declare a default (nothing to fall back to)"))
		}
		out[name] = spec
	}
	p.Parameters = out
}

func diagParam(file string, line int, name, msg string) Diagnostic {
	return Diagnostic{
		Severity: "error", Code: "cfg_parameters_decl", File: file, Line: line,
		Message: fmt.Sprintf("parameter %q: %s", name, msg), Hint: "",
	}
}

func checkParamType(typ string, v any) error {
	switch typ {
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("want string, got %T", v)
		}
	case "integer":
		if _, ok := toInt64(v); !ok {
			return fmt.Errorf("want integer, got %T", v)
		}
	case "number":
		if _, ok := toFloat(v); !ok {
			return fmt.Errorf("want number, got %T", v)
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("want boolean, got %T", v)
		}
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint64:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case uint64:
		return int64(t), true
	case float64:
		if t == float64(int64(t)) {
			return int64(t), true
		}
	}
	return 0, false
}

func valueIn(v any, list []any) bool {
	for _, el := range list {
		switch a := v.(type) {
		case string:
			if b, ok := el.(string); ok && a == b {
				return true
			}
		case bool:
			if b, ok := el.(bool); ok && a == b {
				return true
			}
		default:
			if fa, ok := toFloat(v); ok {
				if fb, ok2 := toFloat(el); ok2 && fa == fb {
					return true
				}
			}
		}
	}
	return false
}

// parseHooks validates lifecycle hooks: failure/success inline sinks
// (plugin name as key, R14).
func parseHooks(file string, raw map[string]any, p *Pipeline, lines *lineIndex, res *Result) {
	hn, present := raw["hooks"]
	if !present {
		return
	}
	hm, ok := hn.(map[string]any)
	if !ok {
		res.Diagnostics = append(res.Diagnostics, Diagnostic{
			Severity: "error", Code: "cfg_hooks_type", File: file, Line: lines.line("hooks"),
			Message: "hooks must be a mapping", Hint: `hooks: { failure: { http: { url: ... } } }`,
		})
		return
	}
	h := &HooksSpec{}
	for k, v := range hm {
		line := lines.line("hooks", k)
		if k != "failure" && k != "success" {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_unknown_field", File: file, Line: line,
				Message: fmt.Sprintf("unknown hook %q", k), Hint: "allowed hooks: failure, success",
			})
			continue
		}
		vm, ok := v.(map[string]any)
		if !ok || len(vm) != 1 {
			res.Diagnostics = append(res.Diagnostics, Diagnostic{
				Severity: "error", Code: "cfg_hooks_sink", File: file, Line: line,
				Message: fmt.Sprintf("hook %q must be exactly one inline sink (plugin name as key)", k),
				Hint:    `failure: { http: { url: "https://..." } }`,
			})
			continue
		}
		var plugin string
		var cfg map[string]any
		for pk, pv := range vm {
			plugin = pk
			if cm, ok := pv.(map[string]any); ok {
				cfg = cm
			} else {
				res.Diagnostics = append(res.Diagnostics, Diagnostic{
					Severity: "error", Code: "cfg_hooks_sink", File: file, Line: line,
					Message: fmt.Sprintf("hook %q plugin block %q must be a mapping", k, pk), Hint: "",
				})
			}
		}
		hs := &HookSink{Plugin: plugin, PluginConfig: cfg, Line: line}
		if k == "failure" {
			h.Failure = hs
		} else {
			h.Success = hs
		}
	}
	p.Hooks = h
}

// SubstituteParameters replaces ${parameters.name} tokens across the typed
// pipeline (plugin configs, edge conditions, scripts, order keys, hooks and
// constants) with per-run values (redesign-v3.md §5.9: parameters are set at
// trigger time). valuesFor returns the value map to use inside one source
// node's plugin config (sources resolve `cursor` against their own
// watermark); pass nil for the global map elsewhere. Unknown names are
// reported as diagnostics (they were validated at verify; this is the
// runtime backstop).
func SubstituteParameters(p *Pipeline, global map[string]any, valuesFor func(sourceNode string) map[string]any) []Diagnostic {
	var diags []Diagnostic
	var sub func(v any, values map[string]any) any
	sub = func(v any, values map[string]any) any {
		switch t := v.(type) {
		case string:
			matches := envPattern.FindAllStringSubmatch(t, -1)
			if len(matches) == 0 {
				return t
			}
			out := t
			for _, m := range matches {
				if !strings.Contains(m[2], ".") || m[2][:strings.Index(m[2], ".")] != "parameters" {
					continue // plain env vars were handled at load; constants too
				}
				name := strings.TrimPrefix(m[2], "parameters.")
				val, ok := values[name]
				if !ok {
					diags = append(diags, Diagnostic{
						Severity: "error", Code: "job_parameter_unknown", File: p.File, Line: 0,
						Message: fmt.Sprintf("run references undeclared parameter %q", name),
						Hint:    "declare it under parameters:",
					})
					continue
				}
				out = strings.ReplaceAll(out, m[0], fmt.Sprintf("%v", val))
			}
			return out
		case map[string]any:
			for k, val := range t {
				t[k] = sub(val, values)
			}
			return t
		case []any:
			for i, val := range t {
				t[i] = sub(val, values)
			}
			return t
		default:
			return v
		}
	}
	if global == nil {
		global = map[string]any{}
	}
	// Constants (plain values).
	for k, v := range p.Constants {
		p.Constants[k] = sub(v, global)
	}
	for _, node := range append(append(mapValues(p.Sources), mapValues(p.Transforms)...), mapValues(p.Sinks)...) {
		values := global
		if node.Section == SectionSource && valuesFor != nil {
			if per := valuesFor(node.Name); per != nil {
				merged := make(map[string]any, len(global)+len(per))
				for k, v := range global {
					merged[k] = v
				}
				for k, v := range per {
					merged[k] = v
				}
				values = merged
			}
		}
		node.PluginConfig, _ = sub(node.PluginConfig, values).(map[string]any)
		node.Script, _ = sub(node.Script, values).(string)
		node.OrderKey, _ = sub(node.OrderKey, values).(string)
		node.Decoder, _ = sub(node.Decoder, values).(string)
		node.Encoder, _ = sub(node.Encoder, values).(string)
		if node.Grpc != nil {
			for i := range node.Grpc.Command {
				node.Grpc.Command[i], _ = sub(node.Grpc.Command[i], values).(string)
			}
			for k, v := range node.Grpc.Env {
				node.Grpc.Env[k], _ = sub(v, values).(string)
			}
			node.Grpc.Schema, _ = sub(node.Grpc.Schema, values).(string)
		}
		for i := range node.From {
			node.From[i].When, _ = sub(node.From[i].When, values).(string)
			node.From[i].Route, _ = sub(node.From[i].Route, values).(string)
		}
	}
	if p.Hooks != nil {
		for _, hk := range []*HookSink{p.Hooks.Failure, p.Hooks.Success} {
			if hk == nil {
				continue
			}
			hk.PluginConfig, _ = sub(hk.PluginConfig, global).(map[string]any)
		}
	}
	return diags
}

func mapValues[T any](m map[string]*T) []*T {
	out := make([]*T, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
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
	file          string
	constants     map[string]any
	diags         *[]Diagnostic
	used          map[string]bool // constants referenced via ${constants.x}
	jobParameters bool            // pipeline is a job: ${parameters.x} passes through unresolved
}

// substitute expands ${constants.x} references and rejects any other scoped
// reference ("unknown means error", redesign-v3.md §5.5). ${parameters.x} is
// legal only in a job pipeline, where it passes through unresolved for the
// jobs runner (values are set at trigger time, §5.9). The optional marker `?`
// is only meaningful for plain environment variables — any dotted reference
// combined with `?` is an error (round-2 review #1).
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
				s.used[name] = true
				out = strings.ReplaceAll(out, m[0], fmt.Sprintf("%v", cv))
				continue
			}
			if scope == "parameters" && !optional && s.jobParameters {
				continue // resolved per run by the jobs runner
			}
			msg := fmt.Sprintf("unknown scoped reference %s: scope %q is not defined (allowed: constants)", ref, scope)
			hint := "use ${constants.name} or a plain environment variable ${VAR}"
			switch {
			case scope == "parameters" && !s.jobParameters:
				msg = fmt.Sprintf("%s: parameters are only available in job pipelines (run.mode: job)", ref)
				hint = "declare a run: { mode: job } block, or pass the value via constants or an environment variable"
			case scope == "parameters" && optional:
				msg = fmt.Sprintf("%s: the optional marker '?' is only valid for environment variables; parameters are validated against their declaration", ref)
				hint = "reference it as ${parameters.name}"
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

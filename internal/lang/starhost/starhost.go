// Package starhost is the Starlark host for transforms.*.script
// (redesign-v3.md §4.3). Scripts are statement sequences bound to payload,
// meta and constants; programs are compiled once per pipeline and are
// immutable, so one Program serves many concurrent executions.
//
// Sandbox: no while loops, no recursion, no global reassignment; top-level
// control flow (if/for) is enabled because scripts are statement sequences,
// not function bodies (redesign-v3-review.md R2). Loadable modules are
// whitelisted to json and math — no time, no I/O, no entropy, so evaluation
// is deterministic and replayable. A hard step budget bounds CPU per message.
package starhost

import (
	"fmt"

	"go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

// Options configures the host.
type Options struct {
	MaxSteps uint64
}

// DefaultOptions is the documented default budget (100k steps).
func DefaultOptions() Options { return Options{MaxSteps: 100_000} }

// allowedModules is the load whitelist. Note there is no loadable "strings"
// module in go-starlark: string methods are built into the string type
// (redesign-v3-review.md R3).
var allowedModules = map[string]*starlarkstruct.Module{
	"json": json.Module,
	"math": math.Module,
}

func init() {
	for _, m := range allowedModules {
		m.Freeze()
	}
}

func loadModule(thread *starlark.Thread, module string) (starlark.StringDict, error) {
	if m, ok := allowedModules[module]; ok {
		return m.Members, nil
	}
	return nil, fmt.Errorf("module %q is not in the whitelist (allowed: json, math)", module)
}

func fileOptions() *syntax.FileOptions {
	return &syntax.FileOptions{
		Set:             true, // sets are standard Starlark
		TopLevelControl: true, // scripts are statement sequences (review R2)
		While:           false,
		Recursion:       false,
		GlobalReassign:  false,
	}
}

func isPredeclared(name string) bool {
	switch name {
	case "payload", "meta", "constants", "parameters", "safe_json_decode", "remove":
		return true
	}
	return false
}

// Program is a compiled, reusable script.
type Program struct {
	name    string
	src     string
	prog    *starlark.Program
	opts    Options
	safeDec *starlark.Builtin
	removeB *starlark.Builtin
}

// Compile parses and resolves a script. Resolution errors (undefined names,
// bad arity, disallowed constructs) surface here — this is what verify runs.
func Compile(name, src string, opts Options) (*Program, error) {
	if opts.MaxSteps == 0 {
		opts = DefaultOptions()
	}
	_, prog, err := starlark.SourceProgramOptions(fileOptions(), name, src, isPredeclared)
	if err != nil {
		return nil, fmt.Errorf("starlark compile error: %w", err)
	}
	return &Program{
		name:    name,
		src:     src,
		prog:    prog,
		opts:    opts,
		safeDec: starlark.NewBuiltin("safe_json_decode", safeJSONDecode),
		removeB: starlark.NewBuiltin("remove", removeKey),
	}, nil
}

// Source returns the original script text.
func (p *Program) Source() string { return p.src }

// Name returns the script identifier (usually "transforms.<node>.script").
func (p *Program) Name() string { return p.name }

// ScriptError is a runtime script failure with its backtrace. The engine
// turns this into a dead letter carrying the trace (no exceptions exist in
// Starlark, so nothing can swallow it).
type ScriptError struct {
	Msg       string
	Backtrace string
	Line      int
}

func (e *ScriptError) Error() string {
	if e.Backtrace != "" {
		return e.Msg + "\n" + e.Backtrace
	}
	return e.Msg
}

// Run executes the script against one message. payload/meta are lazily bound
// (copy-on-write) message states; constants must be a frozen value from
// FreezeConstants. Run returns nil on success.
func (p *Program) Run(payload, meta *MsgState, constants starlark.Value) *ScriptError {
	return p.RunWithParams(payload, meta, constants, nil)
}

// RunWithParams additionally binds `parameters` (frozen job parameters,
// §5.9). A nil params binds an empty frozen dict: continuous pipelines reject
// parameters references at verify time, so scripts never see a value here
// unless the pipeline is a job.
func (p *Program) RunWithParams(payload, meta *MsgState, constants, params starlark.Value) *ScriptError {
	if params == nil {
		params = FreezeConstants(nil)
	}
	thread := &starlark.Thread{Name: p.name, Load: loadModule}
	thread.SetMaxExecutionSteps(p.opts.MaxSteps)
	predeclared := starlark.StringDict{
		"payload":          payload.Binding(),
		"meta":             meta.Binding(),
		"constants":        constants,
		"parameters":       params,
		"safe_json_decode": p.safeDec,
		"remove":           p.removeB,
	}
	if _, err := p.prog.Init(thread, predeclared); err != nil {
		return asScriptError(err)
	}
	return nil
}

func asScriptError(err error) *ScriptError {
	if ee, ok := err.(*starlark.EvalError); ok {
		// The innermost frame carries the user-visible line; frames are
		// ordered outermost-first with a synthetic entry, so take the max.
		line := 0
		for _, fr := range ee.CallStack {
			if int(fr.Pos.Line) > line {
				line = int(fr.Pos.Line)
			}
		}
		return &ScriptError{Msg: ee.Msg, Backtrace: ee.Backtrace(), Line: line}
	}
	return &ScriptError{Msg: err.Error()}
}

// FreezeConstants converts a constants map into a frozen Starlark value once
// per pipeline; the frozen value is shared by all executions.
func FreezeConstants(constants map[string]any) starlark.Value {
	if constants == nil {
		constants = map[string]any{}
	}
	v := GoToStarlark(constants)
	v.Freeze()
	return v
}

// safeJSONDecode(s, default) decodes a JSON string; on failure it returns the
// provided default instead of aborting (the one sanctioned escape hatch).
func safeJSONDecode(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var s starlark.String
	var fallback starlark.Value = starlark.None
	if err := starlark.UnpackArgs(b.Name(), args, kwargs, "s", &s, "default", &fallback); err != nil {
		return nil, err
	}
	v, err := decodeJSONGo(string(s))
	if err != nil {
		return fallback, nil
	}
	return GoToStarlark(v), nil
}

package builtin

import (
	"errors"
	"fmt"
	"strings"

	"go.starlark.net/starlark"

	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
)

// scriptTransform is the built-in script plugin: a Starlark statement
// sequence (redesign-v3.md §4.3) compiled once per node and shared by all
// worker goroutines — programs are immutable, and per-message state lives in
// copy-on-write MsgStates created inside Apply, so a failed attempt's writes
// never leak into the retry. The plugin block's value is the script source
// text itself, which makes script the one plugin whose root config is a
// plain string rather than a mapping.
type scriptTransform struct {
	prog       *starhost.Program
	constants  starlark.Value
	parameters starlark.Value
}

func registerScriptTransform(reg *registry.Registry) error {
	return registry.RegisterTransformT[*scriptTransform](reg, "script", 1, []string{"explain-safe"},
		func(src string, _ string) (*scriptTransform, error) {
			if strings.TrimSpace(src) == "" {
				return nil, fmt.Errorf("script must be a non-empty Starlark statement sequence")
			}
			prog, err := starhost.Compile("script", src, starhost.DefaultOptions())
			if err != nil {
				// Compile only produces compile failures; fall back loudly
				// if a future host error carries no kind.
				kind := registry.FailureCompile
				var serr *starhost.ScriptError
				if errors.As(err, &serr) && serr.Kind != "" {
					kind = scriptKind(serr.Kind)
				}
				return nil, &registry.TransformError{Err: err, Kind: kind, DiagCode: "expr_starlark_compile",
					Hint: "scripts bind payload, meta, constants; while/recursion are disabled"}
			}
			return &scriptTransform{prog: prog}, nil
		})
}

// scriptKind maps the Starlark host's typed kind onto the registry failure
// kinds (the adapter seam; the engine never reads host errors).
func scriptKind(k starhost.Kind) registry.FailureKind {
	switch k {
	case starhost.KindCompile:
		return registry.FailureCompile
	case starhost.KindSteps:
		return registry.FailureSteps
	case starhost.KindRuntime:
		return registry.FailureRuntime
	default:
		return registry.FailureRuntime
	}
}

func (s *scriptTransform) Init(env *registry.TransformEnv) error {
	s.constants = starhost.FreezeConstants(env.Constants)
	s.parameters = starhost.FreezeConstants(env.Parameters)
	return nil
}

func (s *scriptTransform) Apply(msg *registry.Message) ([]*registry.Message, error) {
	ps := starhost.NewMsgState("payload", msg.Decoded)
	ms := starhost.NewMsgState("meta", msg.Meta)
	if serr := s.prog.RunWithParams(ps, ms, s.constants, s.parameters); serr != nil {
		return nil, &registry.TransformError{Err: fmt.Errorf("%s", serr.Msg), Backtrace: serr.Backtrace, Kind: scriptKind(serr.Kind)}
	}
	if ps.Dirty() {
		msg.Decoded = ps.GoValue()
	}
	if ms.Dirty() {
		if m, ok := ms.MapValue(); ok {
			msg.Meta = m
		}
	}
	return []*registry.Message{msg}, nil
}

func (s *scriptTransform) Close() error { return nil }

// Flavor feeds the engine's script metrics (duration histogram, step-budget
// exhaustion counter); the failure kind travels typed on TransformError.
func (s *scriptTransform) Flavor() string { return "script" }

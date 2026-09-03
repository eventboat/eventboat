// Package celhost hosts CEL predicates exactly as-is: no custom functions, no
// syntax extensions (redesign-v3.md §4.2). The evaluation environment binds
// payload, meta and constants. An evaluation error means the condition does
// not pass and is counted — never a panic, never a silent pass.
package celhost

import (
	"fmt"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// Env is a compiled CEL environment for one pipeline.
type Env struct {
	env       *cel.Env
	constants map[string]any
	params    map[string]any
}

// NewEnv builds the predicate environment. constants and parameters may be
// nil (parameters exist only in job pipelines, §5.9; continuous pipelines
// reject references at verify time).
func NewEnv(constants map[string]any, parameters map[string]any) (*Env, error) {
	if constants == nil {
		constants = map[string]any{}
	}
	if parameters == nil {
		parameters = map[string]any{}
	}
	env, err := cel.NewEnv(
		cel.Variable("payload", cel.DynType),
		cel.Variable("meta", cel.DynType),
		cel.Variable("constants", cel.DynType),
		cel.Variable("parameters", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("celhost: environment: %w", err)
	}
	return &Env{env: env, constants: constants, params: parameters}, nil
}

// CompileError reports a predicate that failed to compile or type-check.
type CompileError struct {
	Expr   string
	Detail string
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("CEL compile error in %q: %s", e.Expr, e.Detail)
}

// Predicate is a compiled CEL predicate.
type Predicate struct {
	Source     string
	program    cel.Program
	constants  map[string]any
	parameters map[string]any
}

// Compile parses, checks and plans one predicate.
func (e *Env) Compile(src string) (*Predicate, error) {
	ast, issues := e.env.Compile(src)
	if issues != nil && issues.Err() != nil {
		return nil, &CompileError{Expr: src, Detail: issues.Err().Error()}
	}
	program, err := e.env.Program(ast)
	if err != nil {
		return nil, &CompileError{Expr: src, Detail: err.Error()}
	}
	return &Predicate{Source: src, program: program, constants: e.constants, parameters: e.params}, nil
}

// EvalError reports a runtime evaluation failure (or a non-boolean result).
// Per the error contract the caller treats this as "condition does not pass"
// and increments a counter.
type EvalError struct {
	Expr   string
	Detail string
}

func (e *EvalError) Error() string {
	return fmt.Sprintf("CEL evaluation error in %q: %s", e.Expr, e.Detail)
}

// Eval evaluates the predicate against payload/meta. It never panics and
// never returns (true, err).
func (p *Predicate) Eval(payload, meta any) (bool, *EvalError) {
	val, evalErr := p.evalValue(payload, meta)
	if evalErr != nil {
		return false, evalErr
	}
	b, ok := val.(types.Bool)
	if !ok {
		return false, &EvalError{Expr: p.Source, Detail: fmt.Sprintf("predicate result has type %s, want bool", val.Type())}
	}
	return bool(b), nil
}

// EvalString evaluates the expression to a string (used by sink order_key).
func (p *Predicate) EvalString(payload, meta any) (string, *EvalError) {
	val, evalErr := p.evalValue(payload, meta)
	if evalErr != nil {
		return "", evalErr
	}
	s, ok := val.(types.String)
	if !ok {
		return "", &EvalError{Expr: p.Source, Detail: fmt.Sprintf("expression result has type %s, want string", val.Type())}
	}
	return string(s), nil
}

func (p *Predicate) evalValue(payload, meta any) (ref.Val, *EvalError) {
	val, _, err := p.program.Eval(map[string]any{
		"payload":    payload,
		"meta":       meta,
		"constants":  p.constants,
		"parameters": p.parameters,
	})
	if err != nil {
		return nil, &EvalError{Expr: p.Source, Detail: tidyErr(err.Error())}
	}
	return val, nil
}

func tidyErr(s string) string {
	s = strings.TrimPrefix(s, "evaluation error: ")
	return s
}

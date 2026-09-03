package eql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

var (
	assignRe = regexp.MustCompile(`^([a-zA-Z_][\w\[\]"'.-]*)\s*=\s*(.+)$`)
	delRe    = regexp.MustCompile(`^del\((.+)\)$`)
)

type ProgramKind int

const (
	KindFilter ProgramKind = iota
	KindMapping
	KindCondition
)

type Program struct {
	Kind       ProgramKind
	Statements []Statement
	Expr       cel.Program
}

type Statement interface {
	Execute(ctx *EvalContext) error
}

type AssignStmt struct {
	Path string
	Expr cel.Program
}

type DeleteStmt struct {
	Path string
}

type EvalContext struct {
	Msg     MessageView
	Input   map[string]any
	Payload map[string]any
	Meta    map[string]any
}

type MessageView interface {
	EnsureWritable()
	SetParsedData(any)
	Metadata() map[string]any
}

func NewEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Declarations(
			decls.NewVar("payload", decls.Dyn),
			decls.NewVar("metadata", decls.NewMapType(decls.String, decls.Dyn)),
			decls.NewVar("input", decls.Dyn),
		),
		cel.Function("now",
			cel.Overload("now", []*cel.Type{}, cel.TimestampType,
				cel.FunctionBinding(func(_ ...ref.Val) ref.Val {
					return types.Timestamp{Time: time.Now().UTC()}
				}),
			),
		),
	)
}

func CompileMapping(dsl string) (*Program, error) {
	env, err := NewEnv()
	if err != nil {
		return nil, err
	}
	stmts, err := parseStatements(dsl, env)
	if err != nil {
		return nil, err
	}
	return &Program{Kind: KindMapping, Statements: stmts}, nil
}

func CompileFilter(dsl string) (*Program, error) {
	env, err := NewEnv()
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(strings.TrimSpace(dsl))
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &Program{Kind: KindFilter, Expr: prg}, nil
}

func CompileCondition(dsl string) (*Program, error) {
	if strings.TrimSpace(dsl) == "" {
		return &Program{Kind: KindCondition}, nil
	}
	prg, err := CompileFilter(dsl)
	if err != nil {
		return nil, err
	}
	prg.Kind = KindCondition
	return prg, nil
}

func parseStatements(dsl string, env *cel.Env) ([]Statement, error) {
	lines := splitLines(dsl)
	var out []Statement
	for _, line := range lines {
		if m := delRe.FindStringSubmatch(line); len(m) == 2 {
			out = append(out, DeleteStmt{Path: strings.TrimSpace(m[1])})
			continue
		}
		if m := assignRe.FindStringSubmatch(line); len(m) == 3 {
			ast, issues := env.Compile(m[2])
			if issues != nil && issues.Err() != nil {
				return nil, fmt.Errorf("compile %q: %w", m[2], issues.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				return nil, err
			}
			out = append(out, AssignStmt{Path: strings.TrimSpace(m[1]), Expr: prg})
			continue
		}
		return nil, fmt.Errorf("unsupported statement: %q", line)
	}
	return out, nil
}

func splitLines(dsl string) []string {
	raw := strings.Split(dsl, "\n")
	out := make([]string, 0, len(raw))
	for _, line := range raw {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

func (p *Program) EvalFilter(ctx *EvalContext) (bool, error) {
	if p.Expr == nil {
		return true, nil
	}
	val, _, err := p.Expr.Eval(bindings(ctx))
	if err != nil {
		return false, err
	}
	b, ok := val.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter expression must evaluate to bool, got %T", val.Value())
	}
	return b, nil
}

func (p *Program) EvalMapping(ctx *EvalContext) error {
	for _, stmt := range p.Statements {
		if err := stmt.Execute(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (a AssignStmt) Execute(ctx *EvalContext) error {
	val, _, err := a.Expr.Eval(bindings(ctx))
	if err != nil {
		return err
	}
	if ctx.Msg != nil {
		ctx.Msg.EnsureWritable()
	}
	if strings.HasPrefix(a.Path, "payload") {
		root := ensureMap(ctx.Payload)
		if a.Path == "payload" {
			if m, ok := val.Value().(map[string]any); ok {
				if ctx.Msg != nil {
					ctx.Msg.SetParsedData(m)
				}
				ctx.Payload = m
			} else {
				return fmt.Errorf("payload assignment expects map value")
			}
			return nil
		}
		setPath(root, trimRoot(a.Path, "payload"), val.Value())
		if ctx.Msg != nil {
			ctx.Msg.SetParsedData(root)
		}
		ctx.Payload = root
		return nil
	}
	if strings.HasPrefix(a.Path, "metadata") {
		setPath(ctx.Meta, trimRoot(a.Path, "metadata"), val.Value())
		return nil
	}
	return fmt.Errorf("assignment path must start with payload or metadata")
}

func (d DeleteStmt) Execute(ctx *EvalContext) error {
	if ctx.Msg != nil {
		ctx.Msg.EnsureWritable()
	}
	path := strings.TrimSpace(d.Path)
	if strings.HasPrefix(path, "payload") {
		sub := trimRoot(path, "payload")
		if sub == "" {
			ctx.Payload = make(map[string]any)
		} else {
			delPath(ensureMap(ctx.Payload), sub)
		}
		if ctx.Msg != nil {
			ctx.Msg.SetParsedData(ctx.Payload)
		}
		return nil
	}
	if strings.HasPrefix(path, "metadata") {
		sub := trimRoot(path, "metadata")
		if sub == "" {
			ctx.Meta = make(map[string]any)
		} else {
			delPath(ctx.Meta, sub)
		}
		return nil
	}
	return fmt.Errorf("delete path must start with payload or metadata")
}

func bindings(ctx *EvalContext) map[string]any {
	return map[string]any{
		"payload":  ctx.Payload,
		"metadata": ctx.Meta,
		"input":    ctx.Input,
	}
}

func trimRoot(path, root string) string {
	p := strings.TrimPrefix(path, root)
	p = strings.TrimPrefix(p, ".")
	if strings.HasPrefix(p, `["`) {
		p = strings.TrimPrefix(p, `["`)
		p = strings.TrimSuffix(p, `"]`)
		return p
	}
	if strings.HasPrefix(p, "[") && strings.HasSuffix(p, "]") {
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		p = strings.Trim(p, `"`)
	}
	return p
}

func ensureMap(v map[string]any) map[string]any {
	if v == nil {
		return make(map[string]any)
	}
	return v
}

func setPath(root map[string]any, path string, value any) {
	if path == "" {
		return
	}
	parts := splitPath(path)
	cur := root
	for i, part := range parts {
		if i == len(parts)-1 {
			cur[part] = value
			return
		}
		next, ok := cur[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			cur[part] = next
		}
		cur = next
	}
}

func delPath(root map[string]any, path string) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return
	}
	cur := root
	for i, part := range parts {
		if i == len(parts)-1 {
			delete(cur, part)
			return
		}
		next, ok := cur[part].(map[string]any)
		if !ok {
			return
		}
		cur = next
	}
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, ".")
}

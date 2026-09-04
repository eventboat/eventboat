package convert

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// The eql1 DSL (legacy/internal/eql/eql.go) is line-oriented: each line is
// `path = <CEL expr>` or `del(path)`; comments start with `#`. The converter
// reuses the same two statement regexes, then renders the RHS from the CEL
// AST into Starlark (redesign-v3-review-m4.md R2). Anything outside the
// renderer's subset becomes a manual report item — never a guess.

var (
	assignRe = regexp.MustCompile(`^([a-zA-Z_][\w\[\]"'.-]*)\s*=\s*(.+)$`)
	delRe    = regexp.MustCompile(`^del\((.+)\)$`)
	// A v2 assignment path: payload/metadata root plus dotted or ["key"]/[i]
	// segments. Whole-root assignment (`payload = ...`) is handled separately.
	pathRe = regexp.MustCompile(`^(payload|metadata)(\.[A-Za-z_]\w*|\["[^"]*"\]|\[[0-9]+\])+$`)
)

// stmtRow is one line of the report's statement table.
type stmtRow struct {
	Source string // original eql statement
	Result string // converted form ("" when manual)
	Status string // "auto" | "manual"
	Note   string // reason + suggested rewrite when manual
}

// manualItem is a report card for a non-auto-convertible construct.
type manualItem struct {
	Where      string // location, e.g. `step "enrich" script line 2`
	Reason     string
	Suggestion string
}

// eqlEnv compiles v2 eql expressions. The v2 environment declared
// payload/metadata/input and a custom now(); now() is declared so programs
// using it still parse, and is reported as manual when actually rendered.
func eqlEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("payload", cel.DynType),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("input", cel.DynType),
		cel.Function("now",
			cel.Overload("now", []*cel.Type{}, cel.TimestampType,
				cel.FunctionBinding(func(...ref.Val) ref.Val {
					return types.Timestamp{Time: time.Now().UTC()}
				}),
			),
		),
	)
}

// renderScript translates one v2 `dsl:` block into a Starlark script.
func renderScript(where, dsl string) (script string, rows []stmtRow, manuals []manualItem, notes []string) {
	env, err := eqlEnv()
	if err != nil {
		manuals = append(manuals, manualItem{Where: where, Reason: "internal: cel env: " + err.Error()})
		return "", nil, manuals, nil
	}
	var lines []string
	for _, line := range strings.Split(dsl, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	var out []string
	for i, line := range lines {
		loc := fmt.Sprintf("%s line %d", where, i+1)
		row := stmtRow{Source: line, Status: "manual"}

		if m := delRe.FindStringSubmatch(line); len(m) == 2 {
			converted, note, err := convertDel(strings.TrimSpace(m[1]))
			if err != nil {
				row.Note = err.Error()
				manuals = append(manuals, manualItem{Where: loc, Reason: err.Error(),
					Suggestion: "rewrite as field assignments or use remove() on a nested dict"})
			} else {
				row.Status, row.Result = "auto", converted
				if note != "" {
					notes = append(notes, note)
				}
			}
			rows = append(rows, row)
			out = append(out, convertedOrComment(row))
			continue
		}

		if m := assignRe.FindStringSubmatch(line); len(m) == 3 {
			path := strings.TrimSpace(m[1])
			rhs := strings.TrimSpace(m[2])
			r := renderAssign(env, loc, path, rhs)
			if r.Status == "manual" {
				manuals = append(manuals, manualItem{Where: loc, Reason: r.Note, Suggestion: suggestFor(r.Note)})
			} else {
				notes = appendNotes(notes, r.extraNotes)
			}
			rows = append(rows, r.stmtRow)
			out = append(out, convertedOrComment(r.stmtRow))
			continue
		}

		row.Note = "unsupported eql statement form (v2 allowed only `path = expr` and `del(path)` per line)"
		manuals = append(manuals, manualItem{Where: loc, Reason: row.Note, Suggestion: "express the logic as Starlark statements"})
		rows = append(rows, row)
		out = append(out, convertedOrComment(row))
	}
	return strings.Join(out, "\n") + "\n", rows, manuals, notes
}

// convertedOrComment keeps manual statements visible (commented) in the
// generated script so the converted node stays syntactically valid while the
// report carries the manual work.
func convertedOrComment(r stmtRow) string {
	if r.Status == "auto" {
		return r.Result
	}
	if r.Result != "" {
		return r.Result
	}
	return "# TODO(convert): " + strings.ReplaceAll(r.Source, "\n", " ")
}

type assignResult struct {
	stmtRow
	extraNotes []string
}

func renderAssign(env *cel.Env, loc, path, rhs string) assignResult {
	res := assignResult{stmtRow: stmtRow{Source: path + " = " + rhs, Status: "manual"}}

	if path == "payload" || path == "metadata" {
		res.Note = "whole-root assignment has no v3 equivalent (Starlark bindings cannot be rebound)"
		return res
	}
	if !pathRe.MatchString(path) {
		res.Note = fmt.Sprintf("assignment path %q is not a plain payload/metadata field path", path)
		return res
	}
	v2ast, issues := env.Compile(rhs)
	if issues != nil && issues.Err() != nil {
		res.Note = fmt.Sprintf("RHS did not compile under eql rules: %v", issues.Err())
		return res
	}
	rend := &starlarkRenderer{}
	rendered, err := rend.render(v2ast.NativeRep().Expr())
	if err != nil {
		res.Note = err.Error()
		return res
	}
	v3path := renderPath(path, false)
	res.extraNotes = rend.notes

	// Statement-level ternary chains flatten to if/elif/else (spec §4.8).
	if flat, ok := flattenTernary(v2ast.NativeRep().Expr(), rend); ok {
		var b strings.Builder
		first := true
		for _, br := range flat {
			var val string
			var err error
			if br.cond == nil { // final else
				val, err = rend.render(br.val)
				if err != nil {
					res.Note = err.Error()
					return res
				}
				b.WriteString("else:\n    " + v3path + " = " + val + "\n")
				break
			}
			cond, cErr := rend.render(br.cond)
			val, vErr := rend.render(br.val)
			if cErr != nil || vErr != nil {
				res.Note = firstErr(cErr, vErr).Error()
				return res
			}
			keyword := "if"
			if !first {
				keyword = "elif"
			}
			b.WriteString(keyword + " " + cond + ":\n    " + v3path + " = " + val + "\n")
			first = false
		}
		res.Status, res.Result = "auto", strings.TrimSuffix(b.String(), "\n")
		return res
	}
	res.Status, res.Result = "auto", v3path+" = "+rendered
	return res
}

type ternaryBranch struct {
	cond ast.Expr
	val  ast.Expr
}

// flattenTernary walks the else-chain of top-level ternaries. It returns
// false when the expression is not a top-level ternary chain.
func flattenTernary(e ast.Expr, rend *starlarkRenderer) ([]ternaryBranch, bool) {
	if e.Kind() != ast.CallKind {
		return nil, false
	}
	call := e.AsCall()
	if call.FunctionName() != operators.Conditional || call.IsMemberFunction() || len(call.Args()) != 3 {
		return nil, false
	}
	branches := []ternaryBranch{{cond: call.Args()[0], val: call.Args()[1]}}
	if rest, ok := flattenTernary(call.Args()[2], rend); ok {
		branches = append(branches, rest...)
	} else {
		branches = append(branches, ternaryBranch{cond: nil, val: call.Args()[2]}) // final else
	}
	return branches, true
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// convertDel maps `del(payload.a.b)` to `remove(payload.a, "b")`.
func convertDel(path string) (string, string, error) {
	if path == "payload" || path == "metadata" {
		return "", "", fmt.Errorf("whole-root delete has no v3 equivalent")
	}
	if !pathRe.MatchString(path) {
		return "", "", fmt.Errorf("delete path %q is not a plain payload/metadata field path", path)
	}
	// Split into segments; the parent path is re-emitted readable (index
	// form for names that shadow Starlark dict methods, quoted keys kept).
	segs := splitPath(path)
	if len(segs) < 2 {
		return "", "", fmt.Errorf("delete path %q has no field to remove", path)
	}
	last := segs[len(segs)-1]
	for _, s := range segs[:len(segs)-1] {
		if s.index {
			return "", "", fmt.Errorf("delete through list element %q is not supported (v3 scripts manage lists directly)", path)
		}
	}
	parent := renderPath(strings.Join(segNames(segs[:len(segs)-1]), "."), true)
	if last.index {
		return "", "", fmt.Errorf("delete of list element %q is not supported (v3 scripts manage lists directly)", path)
	}
	return fmt.Sprintf("remove(%s, %q)", parent, last.name), "", nil
}

func segNames(segs []pathSeg) []string {
	names := make([]string, len(segs))
	for i, s := range segs {
		if s.quoted {
			names[i] = `["` + s.name + `"]`
			continue
		}
		names[i] = s.name
	}
	return names
}

type pathSeg struct {
	name   string
	quoted bool // came from ["name"]
	index  bool // came from [123]
}

func splitPath(path string) []pathSeg {
	var segs []pathSeg
	i := 0
	for i < len(path) {
		switch {
		case path[i] == '.':
			i++
		case path[i] == '[':
			j := strings.IndexByte(path[i:], ']')
			if j < 0 {
				return segs
			}
			inner := path[i+1 : i+j]
			if strings.HasPrefix(inner, `"`) && strings.HasSuffix(inner, `"`) {
				name, err := strconv.Unquote(inner)
				if err != nil {
					name = inner
				}
				segs = append(segs, pathSeg{name: name, quoted: true})
			} else {
				segs = append(segs, pathSeg{name: inner, index: true})
			}
			i += j + 1
		default:
			j := i
			for j < len(path) && path[j] != '.' && path[j] != '[' {
				j++
			}
			segs = append(segs, pathSeg{name: path[i:j]})
			i = j
		}
	}
	return segs
}

// rewritePath rewrites the root: metadata.* → meta.* (payload stays).
func rewritePath(path string) string {
	if path == "metadata" || strings.HasPrefix(path, "metadata.") || strings.HasPrefix(path, `metadata[`) {
		return "meta" + path[len("metadata"):]
	}
	return path
}

// renderPath re-emits a v2 assignment path for v3: the metadata root becomes
// meta, and field names shadowing Starlark dict methods (keys/values/items/
// get) use index access in any position that will be READ. The final segment
// of a pure assignment may stay dotted (SetField is not intercepted by the
// method table); parent positions inside remove() must be readable.
func renderPath(path string, finalRead bool) string {
	segs := splitPath(path)
	if len(segs) == 0 {
		return rewritePath(path)
	}
	var b strings.Builder
	root := segs[0].name
	if root == "metadata" {
		b.WriteString("meta")
	} else {
		b.WriteString(root)
	}
	for i := 1; i < len(segs); i++ {
		s := segs[i]
		isFinal := i == len(segs)-1
		mustRead := !isFinal || finalRead
		switch {
		case s.index:
			b.WriteString("[" + s.name + "]")
		case s.quoted:
			b.WriteString("[" + strconv.Quote(s.name) + "]")
		case mustRead && dictMethodNames[s.name]:
			b.WriteString("[" + strconv.Quote(s.name) + "]")
		default:
			b.WriteString("." + s.name)
		}
	}
	return b.String()
}

// dictMethodNames shadow payload/meta field access in attribute position
// (Starlark dict semantics: the method wins).
var dictMethodNames = map[string]bool{
	"keys": true, "values": true, "items": true, "get": true,
}

// starlarkRenderer renders a CEL AST as a Starlark expression. Unknown or
// untranslatable nodes return an error that becomes a manual report item.
type starlarkRenderer struct {
	notes []string
}

func (r *starlarkRenderer) note(format string, args ...any) {
	r.notes = append(r.notes, fmt.Sprintf(format, args...))
}

func (r *starlarkRenderer) render(e ast.Expr) (string, error) {
	switch e.Kind() {
	case ast.LiteralKind:
		return r.renderLiteral(e.AsLiteral())
	case ast.IdentKind:
		switch e.AsIdent() {
		case "payload":
			return "payload", nil
		case "metadata":
			return "meta", nil
		default:
			return "", fmt.Errorf("identifier %q is not available in v3 scripts (payload/meta/constants only)", e.AsIdent())
		}
	case ast.SelectKind:
		sel := e.AsSelect()
		if sel.IsTestOnly() {
			return "", fmt.Errorf("has() presence tests have no direct Starlark form")
		}
		operand, err := r.render(sel.Operand())
		if err != nil {
			return "", err
		}
		// Field names that shadow the Starlark dict methods must use index
		// access — attribute syntax would resolve the method instead.
		if dictMethodNames[sel.FieldName()] {
			return operand + "[" + strconv.Quote(sel.FieldName()) + "]", nil
		}
		return operand + "." + sel.FieldName(), nil
	case ast.ListKind:
		list := e.AsList()
		parts := make([]string, 0, len(list.Elements()))
		for _, el := range list.Elements() {
			s, err := r.render(el)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	case ast.MapKind:
		m := e.AsMap()
		parts := make([]string, 0, len(m.Entries()))
		for _, entry := range m.Entries() {
			me := entry.AsMapEntry()
			k, err := r.render(me.Key())
			if err != nil {
				return "", err
			}
			v, err := r.render(me.Value())
			if err != nil {
				return "", err
			}
			parts = append(parts, k+": "+v)
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	case ast.CallKind:
		return r.renderCall(e.AsCall())
	default:
		return "", fmt.Errorf("CEL construct %v has no mechanical Starlark form", e.Kind())
	}
}

func (r *starlarkRenderer) renderLiteral(v ref.Val) (string, error) {
	switch t := v.(type) {
	case types.Bool:
		if t {
			return "True", nil
		}
		return "False", nil
	case types.Int:
		return strconv.FormatInt(int64(t), 10), nil
	case types.Double:
		f := float64(t)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Sprintf("float(%q)", strconv.FormatFloat(f, 'g', -1, 64)), nil
		}
		return strconv.FormatFloat(f, 'g', -1, 64), nil
	case types.String:
		return strconv.Quote(string(t)), nil
	case types.Null:
		return "None", nil
	default:
		return "", fmt.Errorf("literal type %T has no Starlark literal form", v)
	}
}

var infixOps = map[string]string{
	operators.LogicalAnd: " and ", operators.LogicalOr: " or ",
	operators.Equals: " == ", operators.NotEquals: " != ",
	operators.Less: " < ", operators.LessEquals: " <= ",
	operators.Greater: " > ", operators.GreaterEquals: " >= ",
	operators.Add: " + ", operators.Subtract: " - ",
	operators.Multiply: " * ", operators.Modulo: " % ",
}

func (r *starlarkRenderer) renderCall(call ast.CallExpr) (string, error) {
	name := call.FunctionName()
	args := call.Args()

	// Ternary (nested occurrences only; statement level flattens in
	// renderAssign) renders as Starlark's conditional expression.
	if name == operators.Conditional && !call.IsMemberFunction() && len(args) == 3 {
		cond, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		tv, err := r.render(args[1])
		if err != nil {
			return "", err
		}
		fv, err := r.render(args[2])
		if err != nil {
			return "", err
		}
		return "(" + tv + " if " + cond + " else " + fv + ")", nil
	}

	// Unary operators.
	if name == operators.LogicalNot && len(args) == 1 {
		a, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		if needsParens(args[0]) {
			a = "(" + a + ")"
		}
		return "not " + a, nil
	}
	if name == operators.Negate && len(args) == 1 {
		a, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		if needsParens(args[0]) {
			a = "(" + a + ")"
		}
		return "-" + a, nil
	}

	// Membership: a in b.
	if (name == operators.In || name == operators.OldIn) && len(args) == 2 {
		a, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		b, err := r.render(args[1])
		if err != nil {
			return "", err
		}
		return "(" + a + " in " + b + ")", nil
	}

	// Index: a[b].
	if name == operators.Index && len(args) == 2 {
		a, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		b, err := r.render(args[1])
		if err != nil {
			return "", err
		}
		return a + "[" + b + "]", nil
	}

	// Binary operators.
	if op, ok := infixOps[name]; ok && len(args) == 2 {
		a, err := r.render(args[0])
		if err != nil {
			return "", err
		}
		b, err := r.render(args[1])
		if err != nil {
			return "", err
		}
		if name == operators.Divide {
			r.note("integer division: eql `/` truncates toward zero on ints; the rendered Starlark `//` floors — verify sign expectations")
		}
		if needsParens(args[0]) {
			a = "(" + a + ")"
		}
		if needsParens(args[1]) {
			b = "(" + b + ")"
		}
		return a + op + b, nil
	}

	// Functions.
	return r.renderFunction(call)
}

// functionRenames maps CEL standard functions to Starlark builtins.
var functionRenames = map[string]string{
	"size": "len", "string": "str", "int": "int", "double": "float",
	"bool": "bool", "type": "type",
}

// memberRenames maps receiver-style string methods.
var memberRenames = map[string]string{
	"startsWith": "startswith", "endsWith": "endswith",
}

func (r *starlarkRenderer) renderFunction(call ast.CallExpr) (string, error) {
	name := call.FunctionName()
	if name == "now" {
		return "", fmt.Errorf("eql custom function now() has no deterministic v3 equivalent (clocks are unreachable in scripts)")
	}
	args := call.Args()
	renderArgs := func() ([]string, error) {
		parts := make([]string, 0, len(args))
		for _, a := range args {
			s, err := r.render(a)
			if err != nil {
				return nil, err
			}
			parts = append(parts, s)
		}
		return parts, nil
	}

	if call.IsMemberFunction() {
		target, err := r.render(call.Target())
		if err != nil {
			return "", err
		}
		if name == "contains" && len(args) == 1 {
			sub, err := r.render(args[0])
			if err != nil {
				return "", err
			}
			return "(" + sub + " in " + target + ")", nil
		}
		if newName, ok := memberRenames[name]; ok && len(args) == 1 {
			parts, err := renderArgs()
			if err != nil {
				return "", err
			}
			return target + "." + newName + "(" + strings.Join(parts, ", ") + ")", nil
		}
		return "", fmt.Errorf("CEL method .%s() has no mechanical Starlark form", name)
	}

	if newName, ok := functionRenames[name]; ok {
		parts, err := renderArgs()
		if err != nil {
			return "", err
		}
		return newName + "(" + strings.Join(parts, ", ") + ")", nil
	}
	return "", fmt.Errorf("CEL function %s() has no mechanical Starlark form", name)
}

// needsParens reports whether a rendered operand needs parentheses in infix
// context: non-atomic subexpressions are always wrapped (correctness over
// aesthetics; comparisons/membership bind tighter than and/or in both
// languages but wrapping is harmless).
func needsParens(e ast.Expr) bool {
	switch e.Kind() {
	case ast.LiteralKind, ast.IdentKind:
		return false
	case ast.SelectKind:
		return needsParens(e.AsSelect().Operand())
	case ast.CallKind:
		call := e.AsCall()
		if !call.IsMemberFunction() {
			switch call.FunctionName() {
			case operators.Index, operators.In, operators.OldIn:
				return false
			}
		}
		return true
	default:
		return true
	}
}

// rewritePredicate rewrites a v2 CEL predicate for the v3 environment:
// `metadata` becomes `meta` (identifier boundary respected, string literals
// untouched). Predicates stay CEL (spec §4.8: zero migration).
func rewritePredicate(pred string) string {
	if !strings.Contains(pred, "metadata") {
		return pred
	}
	var b strings.Builder
	i := 0
	for i < len(pred) {
		c := pred[i]
		switch c {
		case '\'', '"':
			// Copy string literals verbatim.
			j := i + 1
			for j < len(pred) && pred[j] != c {
				if pred[j] == '\\' && j+1 < len(pred) {
					j++
				}
				j++
			}
			if j < len(pred) {
				j++
			}
			b.WriteString(pred[i:j])
			i = j
		case 'm', 'M':
			rest := pred[i:]
			if strings.HasPrefix(rest, "metadata") {
				end := i + len("metadata")
				nextIsBoundary := end >= len(pred)
				if !nextIsBoundary {
					nc := pred[end]
					nextIsBoundary = !(nc == '_' || nc == '.' || nc == '[' || (nc >= 'a' && nc <= 'z') || (nc >= 'A' && nc <= 'Z') || (nc >= '0' && nc <= '9'))
					// `metadata.` / `metadata[` must rewrite; a bare
					// `metadata` ident also rewrites (boundary = non-word).
					nextIsBoundary = nextIsBoundary || nc == '.' || nc == '['
				}
				prevIsBoundary := i == 0
				if !prevIsBoundary {
					pc := pred[i-1]
					prevIsBoundary = !(pc == '_' || pc == '.' || (pc >= 'a' && pc <= 'z') || (pc >= 'A' && pc <= 'Z') || (pc >= '0' && pc <= '9'))
				}
				if prevIsBoundary && nextIsBoundary {
					b.WriteString("meta")
					i = end
					continue
				}
			}
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func appendNotes(dst, src []string) []string {
	seen := map[string]bool{}
	for _, n := range dst {
		seen[n] = true
	}
	for _, n := range src {
		if !seen[n] {
			dst = append(dst, n)
			seen[n] = true
		}
	}
	return dst
}

// suggestFor derives a suggested rewrite from the renderer failure.
func suggestFor(reason string) string {
	switch {
	case strings.Contains(reason, "now()"):
		return "stamp time at the source or use meta.ingest_time; scripts have no clock access"
	case strings.Contains(reason, "has()"):
		return "use `payload.f != None` if absence and null are equivalent in your data, or restructure upstream"
	case strings.Contains(reason, "comprehension") || strings.Contains(reason, "macro"):
		return "rewrite all()/exists() as a Starlark for-loop"
	case strings.Contains(reason, "matches"):
		return "match with a Starlark string method or precompute a boolean field upstream"
	case strings.Contains(reason, "whole-root"):
		return "assign individual fields instead of replacing payload/metadata"
	default:
		return "rewrite the statement as Starlark by hand (payload/meta bindings are equivalent)"
	}
}

// sortedKeys is a tiny helper for deterministic map iteration in tests.
func sortedKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

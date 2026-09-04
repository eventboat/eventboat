// Package cesqlhost hosts the opt-in CESQL dialect for edge conditions
// (redesign-v3.md §4.7): an interoperability outlet for the CloudEvents
// ecosystem, not the primary dialect (CEL stays the main syntax).
//
// Parsing and evaluation reuse the official CloudEvents SDK parser
// (cloudevents/sdk-go/sql/v2) — implementing the spec means reusing it, not
// rewriting it. The SDK parser has no data access (the CESQL lexer has no
// '.' token) and CESQL identifiers are alphanumeric only (no underscores,
// no dots; CloudEvents attribute names have the same shape), so the
// documented `data.*` extension is implemented as a string-literal-aware
// pre-parse rewrite to synthetic camelCase identifiers
// (data.amount -> dataAmount, data.a.b -> dataAB) plus flattened payload
// injection as synthetic attributes (review-m3 R8). Identifiers starting
// with "data" (case-insensitive — the parser uppercases everything outside
// string literals) are reserved for this extension.
package cesqlhost

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	cesql "github.com/cloudevents/sdk-go/sql/v2"
	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
	ceevent "github.com/cloudevents/sdk-go/v2/event"
)

// reservedPrefix is the identifier namespace of the extension mode: every
// payload path compiles to "data" + TitleCased segments.
const reservedPrefix = "data"

// Compile parses one CESQL expression. Parse failures are verify-time
// errors (same position as CEL compile errors).
func Compile(src string) (*Predicate, error) {
	rewritten := rewriteDataPaths(src)
	expr, err := cesqlparser.Parse(rewritten)
	if err != nil {
		return nil, fmt.Errorf("cesql: %w", err)
	}
	return &Predicate{source: src, rewritten: rewritten, expr: expr}, nil
}

// Predicate is a compiled CESQL predicate. Eval follows the shared error
// contract: an evaluation error (including missing attributes) means the
// condition does NOT pass and is counted — never a panic, never a silent
// pass.
type Predicate struct {
	source    string
	rewritten string
	expr      cesql.Expression
}

// Lang identifies the dialect in diagnostics and metrics.
func (p *Predicate) Lang() string { return "cesql" }

// Source returns the user-written expression text.
func (p *Predicate) Source() string { return p.source }

// Eval evaluates against payload/meta. Mapping (documented extension
// semantics):
//   - meta keys map to CESQL context attributes. CESQL identifiers are
//     alphanumeric (like CloudEvents attribute names), so keys containing
//     underscores or other characters are unreachable in this dialect;
//     identifiers starting with "data" are reserved for the payload
//     extension. Values map string→string, bool→bool, integers→int32,
//     integral floats→int32, other numbers → their decimal string;
//     everything else (arrays/objects) → JSON string.
//   - payload object fields are injected as flattened synthetic attributes:
//     data.amount -> dataAmount, data.a.b -> dataAB (nested objects up to
//     depth 4); nulls are not injected (missing attribute); arrays become
//     JSON strings on their leaf name.
func (p *Predicate) Eval(payload, meta any) (bool, error) {
	ev := ceevent.New("1.0")
	if m, ok := meta.(map[string]any); ok {
		for k, v := range m {
			if !alnumIdentifier(k) || strings.HasPrefix(strings.ToLower(k), reservedPrefix) {
				continue
			}
			// Required context attributes (type, source, ...) must be set
			// through the event writer: CESQL resolves them from the context,
			// not from the extensions map.
			if s, ok := v.(string); ok {
				switch k {
				case "id":
					ev.SetID(s)
					continue
				case "source":
					ev.SetSource(s)
					continue
				case "type":
					ev.SetType(s)
					continue
				case "subject":
					ev.SetSubject(s)
					continue
				case "time":
					if ts, err := time.Parse(time.RFC3339, s); err == nil {
						ev.SetTime(ts)
						continue
					}
				}
			}
			setAttr(&ev, k, v)
		}
	}
	if obj, ok := payload.(map[string]any); ok {
		injectPayload(&ev, obj, "data", 0)
	}
	val, err := p.expr.Evaluate(ev)
	if err != nil {
		return false, fmt.Errorf("cesql: evaluate %q: %w", p.source, err)
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("cesql: predicate %q evaluates to %T, want bool", p.source, val)
	}
	return b, nil
}

func setAttr(ev *ceevent.Event, name string, v any) {
	switch t := v.(type) {
	case nil:
		// absent: a missing attribute, per the extension semantics
	case string:
		ev.SetExtension(name, t)
	case bool:
		ev.SetExtension(name, t)
	case int:
		ev.SetExtension(name, int32(t))
	case int32:
		ev.SetExtension(name, t)
	case int64:
		ev.SetExtension(name, int32(t))
	case float64:
		if t == float64(int64(t)) {
			ev.SetExtension(name, int32(t))
		} else {
			ev.SetExtension(name, strconv.FormatFloat(t, 'g', -1, 64))
		}
	default:
		if b, err := json.Marshal(v); err == nil {
			ev.SetExtension(name, string(b))
		}
	}
}

// injectPayload flattens nested objects: the synthetic attribute name is the
// prefix plus each segment with its first character upper-cased
// (data + amount -> dataAmount). Segments that are not valid CESQL
// identifier characters are skipped (unreachable by construction).
func injectPayload(ev *ceevent.Event, obj map[string]any, prefix string, depth int) {
	if depth > 4 {
		return
	}
	for k, v := range obj {
		if k == "" || !alnumIdentifier(k) {
			continue
		}
		name := prefix + titleFirst(k)
		if sub, ok := v.(map[string]any); ok && depth < 4 {
			injectPayload(ev, sub, name, depth+1)
			continue
		}
		setAttr(ev, name, v)
	}
}

func alnumIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func titleFirst(s string) string {
	if s[0] >= 'a' && s[0] <= 'z' {
		return string(s[0]-32) + s[1:]
	}
	return s
}

// rewriteDataPaths rewrites data.x(.y...) references into synthetic camelCase
// identifiers outside string literals: data.amount -> dataAmount,
// data.a.b -> dataAB. Case-sensitive on the data keyword (CloudEvents
// attribute names are lowercase).
func rewriteDataPaths(src string) string {
	var b strings.Builder
	b.Grow(len(src))
	i, n := 0, len(src)
	var inStr byte
	for i < n {
		c := src[i]
		if inStr != 0 {
			b.WriteByte(c)
			if c == '\\' && i+1 < n { // escape: copy next verbatim
				b.WriteByte(src[i+1])
				i += 2
				continue
			}
			if c == inStr {
				inStr = 0
			}
			i++
			continue
		}
		switch {
		case c == '\'' || c == '"':
			inStr = c
			b.WriteByte(c)
			i++
		case isIdentChar(c, true):
			j := i
			for j < n && isIdentChar(src[j], false) {
				j++
			}
			if src[i:j] == "data" {
				// Absorb .ident chains (data.a.b -> dataAB).
				k := j
				for k < n && src[k] == '.' && k+1 < n && isIdentChar(src[k+1], true) {
					k++
					for k < n && isIdentChar(src[k], false) {
						k++
					}
				}
				if k > j {
					b.WriteString("data")
					for seg := j; seg < k; {
						seg++ // skip the dot
						s := seg
						for seg < k && isIdentChar(src[seg], false) {
							seg++
						}
						b.WriteString(titleFirst(src[s:seg]))
					}
					i = k
					continue
				}
			}
			b.WriteString(src[i:j])
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func isIdentChar(c byte, first bool) bool {
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
		return true
	}
	return !first && c >= '0' && c <= '9'
}

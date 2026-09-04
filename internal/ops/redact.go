package ops

import (
	"encoding/json"
	"path"
	"strings"
)

// redactor is one compiled telemetry.redact pattern with its binding-root
// segment (payload./meta.) stripped: a dot-separated field path where every
// remaining segment is a path.Match glob ("*" within a segment; a "*"
// segment fans out over array elements).
type redactor struct {
	segs [][]string
}

func compilePattern(p string) redactor {
	var segs [][]string
	for _, seg := range strings.Split(p, ".") {
		segs = append(segs, []string{seg})
	}
	return redactor{segs: segs}
}

// compileRedactForRoot compiles telemetry.redact patterns for one binding
// root. Only patterns rooted at that binding apply (the tail document IS the
// payload — a meta.* pattern has nothing to match there); the root segment
// itself is stripped. The loader and verify guarantee non-empty,
// syntactically valid patterns, so compilation cannot fail here.
func compileRedactForRoot(patterns []string, root string) []redactor {
	var out []redactor
	for _, p := range patterns {
		rest, ok := strings.CutPrefix(p, root+".")
		if !ok || rest == "" {
			continue
		}
		out = append(out, compilePattern(rest))
	}
	return out
}

// redactJSON masks matched field values ("***") in a JSON payload. Non-JSON
// input is returned unchanged — field-level masking has nothing to anchor
// on, and byte-level guessing would corrupt data (documented behavior).
func redactJSON(payload string, rs []redactor) string {
	if len(rs) == 0 || payload == "" {
		return payload
	}
	var v any
	if err := json.Unmarshal([]byte(payload), &v); err != nil {
		return payload
	}
	out := redactValue(v, rs)
	b, err := json.Marshal(out)
	if err != nil {
		return payload // unmaskable; better honest than corrupt
	}
	return string(b)
}

func redactValue(v any, rs []redactor) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if _, hit := matchHere(k, rs); hit {
				out[k] = "***"
				continue
			}
			out[k] = redactValue(val, descend(k, rs))
		}
		return out
	case []any:
		// A leading "*" segment fans out over array elements: it is consumed
		// for the elements' interiors, and a pattern ENDING at "*" masks the
		// whole element.
		var into []redactor
		var whole bool
		for _, r := range rs {
			if len(r.segs) == 0 {
				continue
			}
			if r.segs[0][0] == "*" {
				if len(r.segs) == 1 {
					whole = true
				} else {
					into = append(into, redactor{segs: r.segs[1:]})
				}
			}
		}
		out := make([]any, len(t))
		for i, el := range t {
			if whole {
				out[i] = "***"
				continue
			}
			out[i] = redactValue(el, append(append([]redactor{}, rs...), into...))
		}
		return out
	default:
		return v
	}
}

// matchHere reports whether any remaining pattern is exactly one segment
// long and matches key.
func matchHere(key string, rs []redactor) (string, bool) {
	for _, r := range rs {
		if len(r.segs) == 1 {
			if ok, err := path.Match(r.segs[0][0], key); err == nil && ok {
				return "***", true
			}
		}
	}
	return "", false
}

// descend drops the pattern prefix matching key (a "*" segment matches any
// key).
func descend(key string, rs []redactor) []redactor {
	var out []redactor
	for _, r := range rs {
		if len(r.segs) <= 1 {
			continue
		}
		if ok, err := path.Match(r.segs[0][0], key); err == nil && ok {
			out = append(out, redactor{segs: r.segs[1:]})
		}
	}
	return out
}

package builtin

import (
	"fmt"
	"strings"
)

// rewriteNamedArgs converts `:name` placeholders in a user query into the
// driver's positional form and returns the rewritten SQL plus the argument
// NAMES in binding order (M2 review R8) — callers rebind values per page.
// The scanner skips string literals ('…'), double-quoted identifiers ("…"),
// backtick identifiers (`…`) and PostgreSQL `::` casts, so colons inside
// them are left alone.
func rewriteNamedArgs(dialect, query string, args map[string]any) (string, []string, error) {
	var b strings.Builder
	var order []string
	next := 0 // postgres $n counter
	runes := []rune(query)
	n := len(runes)
	for i := 0; i < n; i++ {
		c := runes[i]
		switch c {
		case '\'', '"', '`':
			quote := c
			b.WriteRune(c)
			i++
			for i < n {
				b.WriteRune(runes[i])
				if runes[i] == quote {
					// doubled quote = escaped literal, keep scanning
					if i+1 < n && runes[i+1] == quote {
						b.WriteRune(runes[i+1])
						i++
					} else {
						break
					}
				}
				i++
			}
		case ':':
			if i+1 < n && runes[i+1] == ':' { // PG cast operator
				b.WriteString("::")
				i++
				continue
			}
			j := i + 1
			for j < n && isNameRune(runes[j]) {
				j++
			}
			if j == i+1 {
				b.WriteRune(c) // lone colon (e.g. in an interval '1:30')
				continue
			}
			name := string(runes[i+1 : j])
			if _, ok := args[name]; !ok {
				return "", nil, fmt.Errorf("sql source: named argument :%s is not bound (declared args: %v)", name, keysOf(args))
			}
			switch dialect {
			case "postgres":
				next++
				fmt.Fprintf(&b, "$%d", next)
			default: // mysql, sqlite
				b.WriteByte('?')
			}
			order = append(order, name)
			i = j - 1
		default:
			b.WriteRune(c)
		}
	}
	return b.String(), order, nil
}

// argValueAt resolves one bound name against user args plus the synthetic
// page placeholders (keys and limit).
func argValueAt(name string, args map[string]any, pageKeys []any, pageSize int) any {
	if strings.HasPrefix(name, "__eb_key_") {
		idx := 0
		for _, r := range strings.TrimPrefix(name, "__eb_key_") {
			idx = idx*10 + int(r-'0')
		}
		if idx < len(pageKeys) {
			return pageKeys[idx]
		}
		return nil
	}
	if name == "__eb_limit" {
		return pageSize
	}
	return args[name]
}

func isNameRune(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortStringsInPlace(out)
	return out
}

func sortStringsInPlace(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// quoteCols quotes key column names per dialect (backticks on mysql, double
// quotes on postgres, bare on sqlite which accepts all styles).
func quoteCols(dialect string, cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		switch dialect {
		case "mysql":
			out[i] = "`" + c + "`"
		case "postgres":
			out[i] = `"` + c + `"`
		default: // sqlite
			out[i] = c
		}
	}
	return out
}

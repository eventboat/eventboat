package convert

import (
	"fmt"
	"regexp"
	"strings"

	hoconlib "github.com/gurkankaymak/hocon"
	"gopkg.in/yaml.v3"
)

// parseV2 detects the format (YAML vs HOCON by extension, like the v2 loader)
// and decodes the archived shape. KnownFields mirrors the v2 loader's strict
// decoding: a config the v2 loader would reject is rejected here too.
func parseV2(path string, data []byte) (*v2PipelineConfig, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".conf"), strings.HasSuffix(lower, ".hocon"):
		return parseV2HOCON(data)
	default:
		return parseV2YAML(data)
	}
}

func parseV2YAML(data []byte) (*v2PipelineConfig, error) {
	// First pass on the raw tree: record top-level section presence so the
	// report can name dropped sections even when their contents are empty.
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("v2 yaml parse: %w", err)
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	var cfg v2PipelineConfig
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("v2 yaml decode: %w", err)
	}
	markTopKeys(raw, &cfg)
	return &cfg, nil
}

func markTopKeys(raw map[string]any, cfg *v2PipelineConfig) {
	_, cfg.HasEngineKey = raw["engine"]
	_, cfg.HasObsKey = raw["observability"]
	_, cfg.HasDLQKey = raw["dlq"]
}

// parseV2HOCON follows the archived loader's path (legacy/internal/config/
// hocon.go): normalize CRLF (upstream parser defect workaround), parse with
// the same public library, convert to a plain tree, round-trip through YAML
// so the strict struct decoding and the DependsOnList/CodecRef unmarshalers
// apply identically to both formats.
//
// Env-shaped HOCON substitutions are protected before parsing and restored
// after: the library resolves ${VAR} against os.Environ at parse time (a
// config-path lookup first, then env), which would freeze convert-time env
// values into the output. v3 substitutes ${VAR} at load time uniformly, so
// the converter keeps the markers.
func parseV2HOCON(data []byte) (*v2PipelineConfig, error) {
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	protected := protectHoconEnv(normalized)
	conf, err := hoconlib.ParseString(protected)
	if err != nil {
		return nil, fmt.Errorf("v2 hocon parse: %w", err)
	}
	root := conf.GetRoot()
	if root == nil {
		return nil, fmt.Errorf("v2 hocon parse: empty config root")
	}
	tree, ok := hoconValueToAny(root).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("v2 hocon parse: root is not an object")
	}
	restoreHoconEnv(tree)
	yamlBytes, err := yaml.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("v2 hocon conversion: %w", err)
	}
	var raw map[string]any
	if err := yaml.Unmarshal(yamlBytes, &raw); err != nil {
		return nil, fmt.Errorf("v2 hocon conversion: %w", err)
	}
	dec := yaml.NewDecoder(strings.NewReader(string(yamlBytes)))
	dec.KnownFields(true)
	var cfg v2PipelineConfig
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("v2 hocon decode: %w", err)
	}
	markTopKeys(raw, &cfg)
	return &cfg, nil
}

// envMarker prefixes wrap an env-var name so the parser sees a plain
// string; the q-variant encodes the optional `${?VAR}` form.
const envMarkerPrefix = "_ebenv_"
const envMarkerOptPrefix = "_ebenvq_"
const envMarkerSuffix = "_"

var hoconEnvRe = regexp.MustCompile(`\$\{(\??)([A-Za-z_][A-Za-z0-9_]*)\}`)

// protectHoconEnv rewrites ${VAR}/${?VAR} to marker strings outside quoted
// spans and comments, so the parser never tries to resolve them.
func protectHoconEnv(text string) string {
	var b strings.Builder
	i := 0
	for i < len(text) {
		c := text[i]
		switch {
		case c == '#':
			j := strings.IndexByte(text[i:], '\n')
			if j < 0 {
				j = len(text) - i
			}
			b.WriteString(text[i : i+j])
			i += j
		case c == '"':
			// Quoted span: """ (triple) or " (single). Copied verbatim.
			if strings.HasPrefix(text[i:], `"""`) {
				end := strings.Index(text[i+3:], `"""`)
				if end < 0 {
					b.WriteString(text[i:])
					i = len(text)
				} else {
					end += 3 + 3
					b.WriteString(text[i : i+end])
					i += end
				}
			} else {
				end := strings.IndexByte(text[i+1:], '"')
				if end < 0 {
					b.WriteString(text[i:])
					i = len(text)
				} else {
					end += 2
					b.WriteString(text[i : i+end])
					i += end
				}
			}
		default:
			rest := text[i:]
			if loc := hoconEnvRe.FindStringSubmatchIndex(rest); loc != nil && loc[0] == 0 {
				name := rest[loc[4]:loc[5]]
				if rest[loc[2]:loc[3]] == "?" {
					b.WriteString(envMarkerOptPrefix + name + envMarkerSuffix)
				} else {
					b.WriteString(envMarkerPrefix + name + envMarkerSuffix)
				}
				i += loc[1]
			} else {
				b.WriteByte(c)
				i++
			}
		}
	}
	return b.String()
}

var envMarkerRe = regexp.MustCompile(`^(` + envMarkerPrefix + `|` + envMarkerOptPrefix + `)([A-Za-z_][A-Za-z0-9_]*)` + envMarkerSuffix + `$`)

// restoreHoconEnv walks the parsed tree and turns marker strings back into
// ${VAR} / ${?VAR} references (optional markers that resolved to nil were
// dropped by the parser — v3's ${?VAR} has the same omit-when-unset rule at
// load time, so nothing else is needed).
func restoreHoconEnv(v any) {
	restored := func(s string) (string, bool) {
		if m := envMarkerRe.FindStringSubmatch(s); m != nil {
			if m[1] == envMarkerOptPrefix {
				return "${?" + m[2] + "}", true
			}
			return "${" + m[2] + "}", true
		}
		return "", false
	}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if s, ok := val.(string); ok {
				if r, was := restored(s); was {
					t[k] = r
					continue
				}
			}
			restoreHoconEnv(val)
		}
	case []any:
		for i, val := range t {
			if s, ok := val.(string); ok {
				if r, was := restored(s); was {
					t[i] = r
					continue
				}
			}
			restoreHoconEnv(val)
		}
	}
}

// hoconValueToAny converts the library's value tree to plain Go values,
// including the HOCON-duration-to-string rule the archived loader used.
func hoconValueToAny(v hoconlib.Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case hoconlib.ObjectType:
		obj := v.(hoconlib.Object)
		m := make(map[string]any, len(obj))
		for k, child := range obj {
			m[k] = hoconValueToAny(child)
		}
		return m
	case hoconlib.ArrayType:
		arr := v.(hoconlib.Array)
		slice := make([]any, len(arr))
		for i, child := range arr {
			slice[i] = hoconValueToAny(child)
		}
		return slice
	case hoconlib.StringType:
		if d, ok := v.(hoconlib.Duration); ok {
			return d.String()
		}
		return string(v.(hoconlib.String))
	case hoconlib.BooleanType:
		return bool(v.(hoconlib.Boolean))
	case hoconlib.NumberType:
		switch n := v.(type) {
		case hoconlib.Int:
			return int(n)
		case hoconlib.Float32:
			return float32(n)
		case hoconlib.Float64:
			return float64(n)
		default:
			return v.String()
		}
	case hoconlib.NullType:
		return nil
	default:
		if d, ok := v.(hoconlib.Duration); ok {
			return d.String()
		}
		return v.String()
	}
}

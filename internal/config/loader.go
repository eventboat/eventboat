package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadFile(path string) (*PipelineConfig, error) {
	format, err := DetectFormat(path, "")
	if err != nil {
		return nil, err
	}
	switch format {
	case FormatYAML:
		return LoadYAML(path)
	case FormatHOCON:
		return LoadHOCON(path)
	default:
		return nil, fmt.Errorf("unsupported config format for %q", path)
	}
}

type Format string

const (
	FormatYAML  Format = "yaml"
	FormatHOCON Format = "hocon"
)

func DetectFormat(path, explicit string) (Format, error) {
	if explicit != "" {
		switch strings.ToLower(explicit) {
		case "yaml", "yml":
			return FormatYAML, nil
		case "hocon", "conf":
			return FormatHOCON, nil
		default:
			return "", fmt.Errorf("unknown format %q", explicit)
		}
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return FormatYAML, nil
	case ".conf", ".hocon":
		return FormatHOCON, nil
	default:
		return "", fmt.Errorf("cannot detect format from extension %q; use --format", ext)
	}
}

func LoadYAML(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg PipelineConfig
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("yaml decode: %w", err)
	}
	substituteInConfig(&cfg)
	return &cfg, nil
}

var envVarRe = regexp.MustCompile(`\$\{(\??[A-Za-z_][A-Za-z0-9_]*)\}`)

// SubstituteEnv replaces ${VAR} and ${?OPT} patterns in strings.
// ${VAR} is replaced with the env var value (empty if unset).
// ${?OPT} is replaced with the env var value or removed if unset.
func SubstituteEnv(s string) string {
	return envVarRe.ReplaceAllStringFunc(s, func(match string) string {
		inner := match[2 : len(match)-1] // strip ${ and }
		optional := false
		name := inner
		if strings.HasPrefix(inner, "?") {
			optional = true
			name = inner[1:]
		}
		val, ok := os.LookupEnv(name)
		if ok {
			return val
		}
		if optional {
			return ""
		}
		return match // keep literal for required vars
	})
}

// substituteInConfig walks the PipelineConfig tree and applies env substitution
// to all string values. This gives YAML configs ${VAR} support matching HOCON behavior.
func substituteInConfig(cfg *PipelineConfig) {
	if cfg == nil {
		return
	}
	for k, v := range cfg.Metadata {
		cfg.Metadata[k] = SubstituteEnv(v)
	}
	for name, step := range cfg.Steps {
		step = substituteStep(step)
		cfg.Steps[name] = step
	}
	for i, st := range cfg.Pipeline {
		cfg.Pipeline[i] = substituteStage(st)
	}
}

func substituteStep(s StepConfig) StepConfig {
	if s.Source != nil {
		s.Source.Config = substituteMap(s.Source.Config)
	}
	if s.Transform != nil {
		s.Transform.Config = substituteMap(s.Transform.Config)
	}
	if s.Sink != nil {
		s.Sink.Config = substituteMap(s.Sink.Config)
	}
	return s
}

func substituteStage(s StageConfig) StageConfig {
	s.Config = substituteMap(s.Config)
	return s
}

func substituteMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case string:
			out[k] = SubstituteEnv(t)
		case []any:
			out[k] = substituteSlice(t)
		case map[string]any:
			out[k] = substituteMap(t)
		default:
			out[k] = v
		}
	}
	return out
}

func substituteSlice(s []any) []any {
	out := make([]any, len(s))
	for i, v := range s {
		switch t := v.(type) {
		case string:
			out[i] = SubstituteEnv(t)
		case []any:
			out[i] = substituteSlice(t)
		case map[string]any:
			out[i] = substituteMap(t)
		default:
			out[i] = v
		}
	}
	return out
}

func LoadDir(dir string) ([]*PipelineConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var configs []*PipelineConfig
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" && ext != ".conf" && ext != ".hocon" {
			continue
		}
		cfg, err := LoadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		configs = append(configs, cfg)
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("no pipeline configs found in %s", dir)
	}
	return configs, nil
}

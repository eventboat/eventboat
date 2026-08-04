package basestage

import (
	"context"
	"fmt"
	"time"

	"github.com/edgesets/edgestream/internal/stage"
)

type Base struct {
	IDVal   string
	KindVal stage.Kind
	TypeVal string
}

func (b Base) ID() string           { return b.IDVal }
func (b Base) Kind() stage.Kind     { return b.KindVal }
func (b Base) ComponentType() string { return b.TypeVal }

func (b Base) Init(context.Context) error  { return nil }
func (b Base) Stop(context.Context) error { return nil }

func (b Base) HealthCheck(context.Context) stage.HealthStatus {
	return stage.HealthStatus{Healthy: true, Since: time.Now()}
}

func ConfigString(cfg map[string]any, key string) string {
	if cfg == nil {
		return ""
	}
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}

func ConfigStringSlice(cfg map[string]any, key string) []string {
	if cfg == nil {
		return nil
	}
	switch v := cfg[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return out
	case string:
		if v != "" {
			return []string{v}
		}
	}
	return nil
}

func ConfigInt(cfg map[string]any, key string, def int) int {
	if cfg == nil {
		return def
	}
	switch v := cfg[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return def
	}
}

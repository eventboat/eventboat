package kafka

import (
	"testing"
)

func TestMessageKeyTypes(t *testing.T) {
	cases := []struct {
		name string
		meta map[string]any
		want string
	}{
		{"string key", map[string]any{"kafka.key": "order-1"}, "order-1"},
		{"numeric key", map[string]any{"kafka.key": float64(42)}, "42"},
		{"int key", map[string]any{"kafka.key": 7}, "7"},
		{"no key", map[string]any{}, ""},
		{"nil metadata", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := messageKey(tc.meta)
			if string(got) != tc.want {
				t.Fatalf("messageKey = %q, want %q", got, tc.want)
			}
		})
	}
}

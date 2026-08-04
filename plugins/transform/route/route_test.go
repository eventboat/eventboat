package route

import (
	"testing"

	"github.com/edgesets/edgestream/internal/eql"
)

func TestRouteOrderUsesExplicitOrder(t *testing.T) {
	routes := map[string]*eql.Program{
		"b":   {},
		"a":   {},
		"_default": {},
	}
	order, err := routeOrder(map[string]any{
		"route_order": []any{"b", "a"},
	}, routes)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b", "a", "_default"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q", i, order[i], want[i])
		}
	}
}

func TestRouteOrderSortedWithoutExplicitOrder(t *testing.T) {
	routes := map[string]*eql.Program{
		"c": {},
		"a": {},
		"b": {},
	}
	order, err := routeOrder(map[string]any{}, routes)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q; full=%v", i, order[i], want[i], order)
		}
	}
}

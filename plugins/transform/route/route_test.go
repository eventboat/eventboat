package route

import (
	"context"
	"testing"

	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
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

func TestRouteTransformUsesParsedData(t *testing.T) {
	tr, err := registry.Default.CreateTransform("route", "r1", map[string]any{
		"routes": map[string]any{
			"high": "payload.value > 100",
			"low":  "payload.value <= 100",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{"value":150}`), nil)
	msg.SetParsedData(map[string]any{"value": int64(150)})

	out, err := tr.Process(context.Background(), []*message.Message{msg})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Metadata["er-route"] != "high" {
		t.Errorf("expected route high, got %v", out[0].Metadata["er-route"])
	}
}

func TestRouteTransformCOWIsolation(t *testing.T) {
	tr, err := registry.Default.CreateTransform("route", "r1", map[string]any{
		"routes": map[string]any{
			"high": "payload.value > 100",
			"low":  "payload.value <= 100",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{"value":150}`), nil)
	msg.SetParsedData(map[string]any{"value": int64(150)})

	branch1 := msg.ShallowCopy()
	branch2 := msg.ShallowCopy()

	out1, err := tr.Process(context.Background(), []*message.Message{branch1})
	if err != nil {
		t.Fatal(err)
	}
	out2, err := tr.Process(context.Background(), []*message.Message{branch2})
	if err != nil {
		t.Fatal(err)
	}

	if len(out1) != 1 || len(out2) != 1 {
		t.Fatalf("expected 1 output each, got %d and %d", len(out1), len(out2))
	}
	if out1[0].Metadata["er-route"] != "high" {
		t.Errorf("branch1 expected route high, got %v", out1[0].Metadata["er-route"])
	}
	if out2[0].Metadata["er-route"] != "high" {
		t.Errorf("branch2 expected route high, got %v", out2[0].Metadata["er-route"])
	}

	// Route is read-only; EnsureWritable guarantees shared parsedData is not
	// left in a writable/dirty state that could leak across branches.
	pd, ok := msg.ParsedData().(map[string]any)
	if !ok {
		t.Fatal("original parsedData should be a map")
	}
	if _, ok := pd["er-route"]; ok {
		t.Errorf("original parsedData was unexpectedly mutated: %v", pd)
	}
}

func TestRouteTransformInvalidJSON(t *testing.T) {
	tr, err := registry.Default.CreateTransform("route", "r1", map[string]any{
		"routes": map[string]any{
			"high": "payload.value > 100",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{not json`), nil)
	_, err = tr.Process(context.Background(), []*message.Message{msg})
	if err == nil {
		t.Fatal("expected error for invalid JSON payload, got nil")
	}
}

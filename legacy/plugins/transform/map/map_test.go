package maptransform

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
)

func TestMapTransformCOWIsolation(t *testing.T) {
	trA, err := registry.Default.CreateTransform("map", "m1", map[string]any{"dsl": "payload.a = 1"})
	if err != nil {
		t.Fatal(err)
	}
	trB, err := registry.Default.CreateTransform("map", "m2", map[string]any{"dsl": "payload.b = 2"})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{"x":1}`), nil)
	msg.SetParsedData(map[string]any{"x": int64(1)})

	branch1 := msg.ShallowCopy()
	branch2 := msg.ShallowCopy()

	out1, err := trA.Process(context.Background(), []*message.Message{branch1})
	if err != nil {
		t.Fatal(err)
	}
	out2, err := trB.Process(context.Background(), []*message.Message{branch2})
	if err != nil {
		t.Fatal(err)
	}

	if len(out1) != 1 || len(out2) != 1 {
		t.Fatalf("expected 1 output each, got %d and %d", len(out1), len(out2))
	}

	var got1, got2 map[string]any
	if err := json.Unmarshal(out1[0].Payload, &got1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out2[0].Payload, &got2); err != nil {
		t.Fatal(err)
	}

	if _, ok := got1["b"]; ok {
		t.Errorf("branch1 output unexpectedly contains branch2 mutation: %v", got1)
	}
	if _, ok := got2["a"]; ok {
		t.Errorf("branch2 output unexpectedly contains branch1 mutation: %v", got2)
	}
	if got1["a"] != float64(1) {
		t.Errorf("branch1 output missing own mutation: %v", got1)
	}
	if got2["b"] != float64(2) {
		t.Errorf("branch2 output missing own mutation: %v", got2)
	}
}

func TestMapTransformRawPayloadModification(t *testing.T) {
	tr, err := registry.Default.CreateTransform("map", "m1", map[string]any{"dsl": "payload.new = 42"})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{"x":1}`), nil)
	out, err := tr.Process(context.Background(), []*message.Message{msg})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}

	var got map[string]any
	if err := json.Unmarshal(out[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["new"] != float64(42) {
		t.Errorf("expected raw payload modification to be preserved, got %v", got)
	}
}

func TestMapTransformInvalidJSON(t *testing.T) {
	tr, err := registry.Default.CreateTransform("map", "m1", map[string]any{"dsl": "payload.new = 42"})
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([]byte(`{not json`), nil)
	_, err = tr.Process(context.Background(), []*message.Message{msg})
	if err == nil {
		t.Fatal("expected error for invalid JSON payload, got nil")
	}
}

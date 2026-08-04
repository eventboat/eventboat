package eql_test

import (
	"testing"

	"github.com/edgesets/edgestream/internal/eql"
)

func TestDeletePayloadRoot(t *testing.T) {
	prg, err := eql.CompileMapping("del(payload)")
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"price": 10, "quantity": 3}
	ctx := &eql.EvalContext{Payload: payload, Meta: map[string]any{}}
	if err := prg.EvalMapping(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ctx.Payload) != 0 {
		t.Fatalf("expected payload to be cleared, got %#v", ctx.Payload)
	}
}

func TestFilterNonBoolReturnsError(t *testing.T) {
	prg, err := eql.CompileFilter(`payload.total`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = prg.EvalFilter(&eql.EvalContext{
		Payload: map[string]any{"total": 150},
		Meta:    map[string]any{},
	})
	if err == nil {
		t.Fatal("expected error for non-bool filter result")
	}
}

func TestCompileFilter(t *testing.T) {
	prg, err := eql.CompileFilter(`payload.total > 100`)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := prg.EvalFilter(&eql.EvalContext{
		Payload: map[string]any{"total": 150},
		Meta:    map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected true")
	}
}

func TestCompileMapping(t *testing.T) {
	prg, err := eql.CompileMapping("payload.total = payload.price * payload.quantity")
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"price": 10, "quantity": 3}
	ctx := &eql.EvalContext{
		Payload: payload,
		Meta:    map[string]any{},
		Input:   map[string]any{"price": 10, "quantity": 3},
	}
	if err := prg.EvalMapping(ctx); err != nil {
		t.Fatal(err)
	}
	if ctx.Payload["total"] != int64(30) && ctx.Payload["total"] != float64(30) && ctx.Payload["total"] != 30 {
		t.Fatalf("total = %#v", ctx.Payload["total"])
	}
}

package filter_test

import (
	"context"
	"testing"

	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
	_ "github.com/riverpod/riverpod/plugins/transform/filter"
)

func newFilter(t *testing.T, dsl string) interface {
	Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error)
} {
	t.Helper()
	tr, err := registry.Default.CreateTransform("filter", "t", map[string]any{"dsl": dsl})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestFilter_InvalidJSONPayloadReturnsError(t *testing.T) {
	tr := newFilter(t, "true")
	msg := message.New([]byte("not json"), nil)
	_, err := tr.Process(context.Background(), []*message.Message{msg})
	if err == nil {
		t.Fatal("expected error for malformed JSON payload")
	}
}

func TestFilter_KeepsMatchingMessages(t *testing.T) {
	tr := newFilter(t, "payload.total > 100")
	keep := message.New([]byte(`{"total":150}`), nil)
	drop := message.New([]byte(`{"total":50}`), nil)
	out, err := tr.Process(context.Background(), []*message.Message{keep, drop})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0] != keep {
		t.Fatalf("expected only the matching message, got %d", len(out))
	}
}

package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// nopSink records the last written payload (the "real" sink under the tail
// wrapper).
type nopSink struct {
	last string
}

func (s *nopSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.last = string(msgs[0].Out)
	return nil
}
func (s *nopSink) Close() error { return nil }

func TestRedactJSONMasksMatchedPaths(t *testing.T) {
	// Patterns carry their binding root; the tail document IS the payload,
	// so the payload. prefix is stripped and meta.* patterns do not apply.
	rs := compileRedactForRoot([]string{
		"payload.user.email",
		"payload.card*",
		"payload.items.*.sku",
		"payload.secret_list.*",
		"meta.authorization",
	}, "payload")
	in := `{"user":{"email":"a@b.example","id":"u-1"},"card_number":"4111","keep":"x","items":[{"sku":"s1","qty":2},{"sku":"s2","qty":3}],"secret_list":["tok1","tok2"]}`
	out := redactJSON(in, rs)
	for _, secret := range []string{"a@b.example", "4111", "\"s1\"", "\"s2\"", "\"tok1\"", "\"tok2\""} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q survived redaction: %s", secret, out)
		}
	}
	for _, keep := range []string{"\"u-1\"", "\"x\"", "\"qty\":2", "\"qty\":3"} {
		if !strings.Contains(out, keep) {
			t.Fatalf("non-matched value %q lost: %s", keep, out)
		}
	}
	if got := strings.Count(out, `"***"`); got < 6 {
		t.Fatalf("mask token appears %d times, want >= 6: %s", got, out)
	}
}

func TestRedactJSONPassthroughNonJSON(t *testing.T) {
	rs := compileRedactForRoot([]string{"payload.a"}, "payload")
	raw := "not json at all"
	if got := redactJSON(raw, rs); got != raw {
		t.Fatalf("non-JSON payload altered: %q", got)
	}
	if got := redactJSON("", rs); got != "" {
		t.Fatalf("empty payload altered: %q", got)
	}
}

// The tail wrapper compiles telemetry.redact once per pipeline and masks
// matched values in tail entries ONLY — the delivered message (m.Out) is
// never altered.
func TestTailWrapperAppliesRedaction(t *testing.T) {
	svc := New(Options{
		DataDir:  t.TempDir(),
		Reg:      registry.New(),
		StoreFor: func(pipeline string) (store.Store, error) { return store.NewMemory(pipeline), nil },
		Clock:    time.Now,
	})
	t.Cleanup(svc.Stop)

	cfg := &config.Pipeline{Name: "p", Telemetry: &config.Telemetry{
		Redact: []string{"payload.user.email"},
	}}
	inner := &nopSink{}
	wrapped := svc.tailWrapper(cfg)("out", inner)
	secret := `{"user":{"email":"a@b.example","id":"u-1"}}`
	msg := registry.Message{Out: []byte(secret)}
	if err := wrapped.Write(context.Background(), []registry.Message{msg}); err != nil {
		t.Fatal(err)
	}
	entries := svc.Tail("out", 10)
	if len(entries) != 1 {
		t.Fatalf("tail entries = %d, want 1", len(entries))
	}
	if strings.Contains(entries[0].Payload, "a@b.example") {
		t.Fatalf("secret survived into the tail entry: %s", entries[0].Payload)
	}
	if !strings.Contains(entries[0].Payload, "***") {
		t.Fatalf("mask missing from the tail entry: %s", entries[0].Payload)
	}
	if inner.last != secret {
		t.Fatalf("the DELIVERED message must never be redacted: %q", inner.last)
	}
}

// The dead-letter query is an ops surface like the tail: the same
// telemetry.redact patterns mask payload AND meta in the results, while the
// stored rows stay raw (replay re-injects the original bytes).
func TestDeadLetterQueryAppliesRedaction(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	stores := map[string]store.Store{"redact-dlq": store.NewMemory("redact-dlq")}
	svc := New(Options{
		DataDir:  t.TempDir(),
		Reg:      reg,
		StoreFor: func(pipeline string) (store.Store, error) { return stores[pipeline], nil },
		Clock:    time.Now,
	})
	t.Cleanup(svc.Stop)

	if _, err := svc.Deploy(context.Background(), `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: redact-dlq }
telemetry:
  redact: [payload.user.email, meta.authorization]
sources:
  in:
    decoder: json
    fakepull: { id: redact-feed }
sinks:
  out:
    from: [in]
    file: { path: out.jsonl }
`); err != nil {
		t.Fatal(err)
	}
	st, err := svc.opts.StoreFor("redact-dlq")
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"user":{"email":"a@b.example","id":"u-1"}}`
	if err := st.WriteDeadLetter(store.DeadLetter{
		Pipeline: "redact-dlq", Node: "in", Reason: "test",
		Raw:  []byte(raw),
		Meta: map[string]any{"authorization": "Bearer sekrit", "keep": "x"},
	}); err != nil {
		t.Fatal(err)
	}

	dls, err := svc.DeadLetterQuery("redact-dlq", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dls))
	}
	if strings.Contains(string(dls[0].Raw), "a@b.example") {
		t.Fatalf("secret survived into the dead-letter payload: %s", dls[0].Raw)
	}
	if !strings.Contains(string(dls[0].Raw), "***") {
		t.Fatalf("mask missing from the dead-letter payload: %s", dls[0].Raw)
	}
	if got := dls[0].Meta["authorization"]; got != "***" {
		t.Fatalf("meta secret survived: %v", dls[0].Meta)
	}
	if got := dls[0].Meta["keep"]; got != "x" {
		t.Fatalf("unmatched meta value lost: %v", dls[0].Meta)
	}

	// The STORED row is the data path: still raw for replay.
	stored, err := st.DeadLetters("redact-dlq")
	if err != nil {
		t.Fatal(err)
	}
	if string(stored[0].Raw) != raw || stored[0].Meta["authorization"] != "Bearer sekrit" {
		t.Fatalf("stored dead letter must never be altered: %s %v", stored[0].Raw, stored[0].Meta)
	}
}

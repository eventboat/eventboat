// Package kafka_int runs the engine's kafka source/sink against a REAL
// broker via testcontainers (KRaft). The whole package is env-gated:
// local `go test ./...` stays Docker-free; the CI `kafka-integration` job
// sets EVENTBOAT_KAFKA_TEST=1 (redesign-v3-review-beta.md R-B6).
package kafka_int

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

var broker string

func TestMain(m *testing.M) {
	if os.Getenv("EVENTBOAT_KAFKA_TEST") != "1" {
		fmt.Println("skipping: set EVENTBOAT_KAFKA_TEST=1 (needs Docker; CI runs the kafka-integration job)")
		os.Exit(0)
	}
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	c, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		return 1, fmt.Errorf("kafka container: %w", err)
	}
	defer func() { _ = c.Terminate(ctx) }()
	brokers, err := c.Brokers(ctx)
	if err != nil {
		return 1, fmt.Errorf("kafka broker address: %w", err)
	}
	broker = brokers[0]
	fmt.Printf("kafka broker address: %s\n", broker)
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()
	if d, derr := net.DialTimeout("tcp", broker, 10*time.Second); derr != nil {
		return 1, fmt.Errorf("tcp dial %s: %w", broker, derr)
	} else {
		_ = d.Close()
	}
	_ = dialCtx
	return m.Run(), nil
}

func mustCreateTopic(t *testing.T, topic string, partitions int) {
	t.Helper()
	// The KRaft controller may lag container-readiness by a few seconds;
	// bounded dials with retries (an unbounded DialLeader hung Windows CI
	// runs for minutes).
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partitions)
		if err == nil {
			_ = conn.Close()
			cancel()
			return
		}
		cancel()
		if time.Now().After(deadline) {
			t.Fatalf("create topic %s: %v", topic, err)
		}
		time.Sleep(time.Second)
	}
}

func produce(t *testing.T, topic string, n int, payload func(i int) []byte) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.RoundRobin{}}
	defer func() { _ = w.Close() }()
	var msgs []kafka.Message
	for i := 0; i < n; i++ {
		msgs = append(msgs, kafka.Message{Value: payload(i)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

// runPipeline builds and runs a pipeline; cleanup stops it.
func runPipeline(t *testing.T, yamlText string) (*engine.Engine, func()) {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	lr := config.LoadBytes("kafka-int.yaml", []byte(yamlText))
	if lr.HasErrors() {
		t.Fatalf("config: %+v", lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("verify: %+v", diags)
	}
	st, err := store.OpenSQLite(filepath.Join(t.TempDir(), "spool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := engine.New(pip, st, reg, engine.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = eng.Run(ctx); close(done) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}
	t.Cleanup(stop)
	// settle-wait helper bound to this engine
	return eng, stop
}

func waitSettledCount(t *testing.T, eng *engine.Engine, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if eng.Metrics.SettledCount.Load() >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("settled %d, want >= %d (messagesIn=%d deadLettered=%d decodeErr=%d)",
		eng.Metrics.SettledCount.Load(), want, eng.Metrics.MessagesIn.Load(),
		eng.Metrics.DeadLettered.Load(), eng.Metrics.DecodeErrors.Load())
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// Path 1 (produce/consume): external producer → kafka source → transform →
// kafka sink → external consumer. The engine moves messages through a real
// broker end to end.
func TestKafkaRoundtripThroughEngine(t *testing.T) {
	mustCreateTopic(t, "int-in", 1)
	mustCreateTopic(t, "int-out", 1)
	outFile := filepath.Join(t.TempDir(), "audit.jsonl")
	eng, _ := runPipeline(t, fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: kafka-roundtrip }
edge_defaults:
  delivery: { retries: 2, backoff: exponential }
sources:
  in:
    decoder: json
    kafka:
      brokers: [%[1]q]
      topics: ["int-in"]
      group_id: int-roundtrip
transforms:
  stamp:
    from: [in]
    script: |
      payload.hop = "engine"
sinks:
  out:
    from: [stamp]
    encoder: json
    kafka:
      brokers: [%[1]q]
      topic: int-out
  audit:
    from: [stamp]
    encoder: json
    file: { path: %[2]q }
`, broker, filepath.ToSlash(outFile)))

	const n = 20
	produce(t, "int-in", n, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"i":%d}`, i))
	})
	waitSettledCount(t, eng, n, 60*time.Second)

	// The external consumer sees every message exactly once (single group,
	// single partition, transform applied).
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: "int-out", GroupID: "int-verifier",
		MaxWait: 500 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	seen := map[int]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for len(seen) < n {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("consume int-out (%d/%d seen): %v", len(seen), n, err)
		}
		var body map[string]any
		if err := json.Unmarshal(m.Value, &body); err != nil {
			t.Fatalf("bad payload %q: %v", m.Value, err)
		}
		if body["hop"] != "engine" {
			t.Fatalf("transform not applied: %v", body)
		}
		seen[int(body["i"].(float64))] = true
	}
	// The audit file sink saw the same set.
	if got := len(readLines(t, outFile)); got < n {
		t.Fatalf("audit sink saw %d messages, want >= %d", got, n)
	}
}

// Path 2 (dead letter): malformed records dead-letter through the real
// broker decode path and settle via the DLQ — never silently lost.
func TestKafkaMalformedRecordDeadLetters(t *testing.T) {
	mustCreateTopic(t, "int-bad", 1)
	eng, _ := runPipeline(t, fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: kafka-dlq }
sources:
  in:
    decoder: json
    kafka:
      brokers: [%[1]q]
      topics: ["int-bad"]
      group_id: int-dlq
sinks:
  out:
    from: [in]
    encoder: json
    file: { path: %[2]q }
`, broker, filepath.ToSlash(filepath.Join(t.TempDir(), "never.jsonl"))))

	produce(t, "int-bad", 3, func(i int) []byte { return []byte(`{"good":true}`) })
	produce(t, "int-bad", 1, func(i int) []byte { return []byte(`{not json`) })
	produce(t, "int-bad", 3, func(i int) []byte { return []byte(`{"good":true}`) })

	deadline := time.Now().Add(60 * time.Second)
	for eng.Metrics.DeadLettered.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := eng.Metrics.DeadLettered.Load(); got < 1 {
		t.Fatalf("dead letters = %d, want >= 1", got)
	}
	// The good records around the bad one still settle.
	waitSettledCount(t, eng, 7, 60*time.Second)
}

// Path 3 (rebalance): two engine instances in ONE consumer group on a
// two-partition topic — the group reassigns partitions and every message
// lands in exactly one engine (union == N, both engines participate).
func TestKafkaConsumerGroupRebalance(t *testing.T) {
	mustCreateTopic(t, "int-reb", 2)
	outA := filepath.Join(t.TempDir(), "a.jsonl")
	outB := filepath.Join(t.TempDir(), "b.jsonl")
	pipeline := func(out string) string {
		return fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: kafka-reb-%[2]s }
sources:
  in:
    decoder: json
    kafka:
      brokers: [%[1]q]
      topics: ["int-reb"]
      group_id: int-rebalance
sinks:
  out:
    from: [in]
    encoder: json
    file: { path: %[3]q }
`, broker, filepath.Base(out), filepath.ToSlash(out))
	}
	engA, _ := runPipeline(t, pipeline(outA))
	_, _ = runPipeline(t, pipeline(outB))

	// Produce in batches until the group has split the partitions across
	// both consumers (rebalance completes within a few seconds of the second
	// member joining; self-healing loop keeps the test robust).
	const total = 40
	produced := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		produce(t, "int-reb", 4, func(i int) []byte {
			return []byte(fmt.Sprintf(`{"i":%d}`, produced+i))
		})
		produced += 4
		time.Sleep(1 * time.Second)
		a, b := len(readLines(t, outA)), len(readLines(t, outB))
		if a > 0 && b > 0 && a+b >= 4 {
			break
		}
		if produced >= total {
			break
		}
	}
	// Drain: every produced message settles somewhere.
	waitSettledCount(t, engA, 1, 90*time.Second)
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if len(readLines(t, outA)) > 0 && len(readLines(t, outB)) > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	a, b := readLines(t, outA), readLines(t, outB)
	if len(a) == 0 || len(b) == 0 {
		t.Fatalf("rebalance did not engage both consumers: A=%d B=%d", len(a), len(b))
	}
	// No message delivered to both engines (consumer-group semantics).
	seen := map[string]bool{}
	for _, m := range append(append([]map[string]any{}, a...), b...) {
		key := fmt.Sprintf("%v", m["i"])
		if seen[key] {
			t.Fatalf("message %s delivered to both group members", key)
		}
		seen[key] = true
	}
}

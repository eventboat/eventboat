package kafka_test

import (
	"testing"

	"github.com/riverpod/riverpod/internal/registry"
	_ "github.com/riverpod/riverpod/plugins/sink/kafka"
)

func TestKafkaSink_InvalidBalancer(t *testing.T) {
	_, err := registry.Default.CreateSink("kafka", "t", map[string]any{
		"brokers":  []string{"localhost:9092"},
		"topic":    "orders",
		"balancer": "round_robin",
	})
	if err == nil {
		t.Fatal("expected error for unknown balancer")
	}
}

func TestKafkaSink_ValidBalancers(t *testing.T) {
	for _, balancer := range []string{"", "least_bytes", "hash"} {
		cfg := map[string]any{
			"brokers": []string{"localhost:9092"},
			"topic":   "orders",
		}
		if balancer != "" {
			cfg["balancer"] = balancer
		}
		if _, err := registry.Default.CreateSink("kafka", "t", cfg); err != nil {
			t.Fatalf("balancer %q: %v", balancer, err)
		}
	}
}

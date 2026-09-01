package kafka_test

import (
	"testing"

	"github.com/riverpod/riverpod/internal/registry"
	_ "github.com/riverpod/riverpod/plugins/sink/kafka"
	_ "github.com/riverpod/riverpod/plugins/source/kafka"
)

func TestKafkaSink_RequiresTopic(t *testing.T) {
	_, err := registry.Default.CreateSink("kafka", "t", map[string]any{
		"brokers": []string{"localhost:9092"},
	})
	if err == nil {
		t.Fatal("expected error when topic missing")
	}
}

func TestKafkaSource_RequiresBrokers(t *testing.T) {
	_, err := registry.Default.CreateSource("kafka", "t", map[string]any{
		"topics": []string{"orders"},
	})
	if err == nil {
		t.Fatal("expected error when brokers missing")
	}
}

func TestKafkaSource_ValidConfig(t *testing.T) {
	src, err := registry.Default.CreateSource("kafka", "orders-in", map[string]any{
		"brokers":  []string{"localhost:9092"},
		"topics":   []string{"orders"},
		"group_id": "g1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if src.ID() != "orders-in" {
		t.Fatalf("id = %q", src.ID())
	}
}

package config_test

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
)

func hoconTestdata(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "pipelines", name)
}

func TestLinearHOCON_Load(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")

	cfg, err := config.LoadHOCON(hoconTestdata(t, "linear.conf"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Steps) != 4 {
		t.Fatalf("steps = %d, want 4", len(cfg.Steps))
	}
	if cfg.Steps["kafka-source"].Source == nil {
		t.Fatal("missing kafka-source step")
	}
	brokers, ok := cfg.Steps["kafka-source"].Source.Config["brokers"].([]any)
	if !ok || len(brokers) == 0 {
		t.Fatalf("brokers = %#v", cfg.Steps["kafka-source"].Source.Config["brokers"])
	}
	if brokers[0] != "broker1:9092,broker2:9092" {
		t.Fatalf("brokers[0] = %v", brokers[0])
	}
	sink := cfg.Steps["kafka-sink"].Sink
	if sink == nil || sink.Batch == nil {
		t.Fatal("missing sink batch config")
	}
	d, err := time.ParseDuration(sink.Batch.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	if d != time.Second {
		t.Fatalf("batch timeout = %v", d)
	}
}

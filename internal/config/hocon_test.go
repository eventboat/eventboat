package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
)

func hoconExamples(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "_examples", name)
}

func hoconTestdata(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "pipelines", name)
}

func TestHOCONIntTypeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.conf")
	content := `engine { max_workers = "abc" }
steps { s { source { type = cron } } }
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := config.LoadHOCON(path)
	if err == nil {
		t.Fatal("expected error for int type mismatch")
	}
	if !strings.Contains(err.Error(), "engine.max_workers") {
		t.Fatalf("expected field path in error, got: %v", err)
	}
}

func TestHOCONCodecsNonObject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.conf")
	content := `codecs = ["json"]
steps { s { source { type = cron } } }
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := config.LoadHOCON(path)
	if err == nil {
		t.Fatal("expected error for non-object codec entry")
	}
	if !strings.Contains(err.Error(), "codecs") {
		t.Fatalf("expected codecs path in error, got: %v", err)
	}
}

func TestHOCONEdgesNonObject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.conf")
	content := `edges = ["a->b"]
steps { s { source { type = cron } } }
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := config.LoadHOCON(path)
	if err == nil {
		t.Fatal("expected error for non-object edge entry")
	}
	if !strings.Contains(err.Error(), "edges") {
		t.Fatalf("expected edges path in error, got: %v", err)
	}
}

func TestHOCONExampleFile(t *testing.T) {
	cfg, err := config.LoadHOCON(hoconExamples(t, "08-hocon-linear.conf"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Steps) != 4 {
		t.Fatalf("steps = %d, want 4", len(cfg.Steps))
	}
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

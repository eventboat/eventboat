package cron_test

import (
	"testing"

	"github.com/riverpod/riverpod/internal/registry"
	_ "github.com/riverpod/riverpod/plugins/source/cron"
)

func TestCronSource_InvalidSchedule(t *testing.T) {
	_, err := registry.Default.CreateSource("cron", "t", map[string]any{
		"schedule": "not a cron",
	})
	if err == nil {
		t.Fatal("expected error for invalid schedule")
	}
}

func TestCronSource_ValidScheduleWithSeconds(t *testing.T) {
	src, err := registry.Default.CreateSource("cron", "t", map[string]any{
		"schedule": "*/30 * * * * *",
	})
	if err != nil {
		t.Fatal(err)
	}
	if src.ComponentType() != "cron" {
		t.Fatalf("type = %q", src.ComponentType())
	}
}

func TestCronSource_RequiresSchedule(t *testing.T) {
	_, err := registry.Default.CreateSource("cron", "t", map[string]any{})
	if err == nil {
		t.Fatal("expected error when schedule missing")
	}
}

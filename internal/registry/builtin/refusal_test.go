package builtin

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// Candidate 01 refusal contract for the HTTP entry: a refused emission (spool
// append failed) answers 503 instead of 202 — one client's retry is this
// source's policy, unlike the other builtins which report a failed source.
func TestHTTPServerSourceRefusalAnswers503(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	reg := registry.New()
	if err := registerHTTPServerSource(reg); err != nil {
		t.Fatal(err)
	}
	src, err := reg.NewSource("http_server", map[string]any{"listen": addr, "path": "/in"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Run(ctx, func(registry.Message) error { return errors.New("spool down") })
	}()

	url := "http://" + addr + "/in"
	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		resp, err = http.Post(url, "application/json", strings.NewReader(`{"i":1}`))
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("refused emission answered %d, want 503", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("http source did not stop on cancellation")
	}
	_ = src.Close()
}

package lsp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func testContext() context.Context { return context.Background() }

// harness drives the server over in-process pipes — the same byte protocol
// a real editor speaks over stdio.
type harness struct {
	t       *testing.T
	srv     *Server
	w       io.Writer          // client → server
	r       *bufio.Reader      // server → client
	next    int
	done    chan error
	exited  bool // set when done was consumed by a waitExit call
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	upR, upW := io.Pipe()    // client writes → server reads
	downR, downW := io.Pipe() // server writes → client reads
	h := &harness{t: t, srv: srv, w: upW, r: bufio.NewReader(downR), done: make(chan error, 1)}
	go func() { h.done <- srv.Serve(testContext(), upR, downW) }()
	t.Cleanup(func() {
		upW.Close()
		downW.Close()
		if h.exited {
			return
		}
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return h
}

// waitExit consumes the server's exit exactly once.
func (h *harness) waitExit(timeout time.Duration) error {
	h.t.Helper()
	h.exited = true
	select {
	case err := <-h.done:
		return err
	case <-time.After(timeout):
		h.t.Fatal("server did not exit")
		return nil
	}
}

func (h *harness) send(method string, params any) json.RawMessage {
	h.next++
	id, _ := json.Marshal(h.next)
	p, _ := json.Marshal(params)
	h.write(&Message{JSONRPC: "2.0", ID: id, Method: method, Params: p})
	return id
}

func (h *harness) notify(method string, params any) {
	p, _ := json.Marshal(params)
	h.write(&Message{JSONRPC: "2.0", Method: method, Params: p})
}

func (h *harness) write(msg *Message) {
	h.t.Helper()
	if err := writeMessage(h.w, msg); err != nil {
		h.t.Fatalf("write %s: %v", msg.Method, err)
	}
}

// await reads server output until a response with the given id arrives,
// skipping interleaved notifications (diagnostics).
func (h *harness) await(id json.RawMessage) *Message {
	h.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			h.t.Fatalf("timed out waiting for response %s", id)
		case err := <-h.done:
			h.t.Fatalf("server exited early: %v", err)
		default:
		}
		msg, err := readMessage(h.r)
		if err != nil {
			h.t.Fatalf("read: %v", err)
		}
		if msg.Method == "" && string(msg.ID) == string(id) {
			return msg
		}
		// else: notification or unrelated response — keep reading
	}
}

// awaitNotification reads until a notification with the given method.
func (h *harness) awaitNotification(method string) *Message {
	h.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			h.t.Fatalf("timed out waiting for notification %s", method)
		case err := <-h.done:
			h.t.Fatalf("server exited early: %v", err)
		default:
		}
		msg, err := readMessage(h.r)
		if err != nil {
			h.t.Fatalf("read: %v", err)
		}
		if msg.Method == method {
			return msg
		}
	}
}

func (h *harness) request(method string, params any, out any) {
	h.t.Helper()
	id := h.send(method, params)
	resp := h.await(id)
	if resp.Error != nil {
		h.t.Fatalf("%s failed: %d %s", method, resp.Error.Code, resp.Error.Message)
	}
	if out != nil && len(resp.Result) > 0 {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			h.t.Fatalf("%s: bad result: %v", method, err)
		}
	}
}

const brokenDoc = `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: lsp-test}
sources:
  ingest:
    decoder: json
    nope: {}
sinks:
  out:
    from: [ingest]
    file: {path: out.jsonl}
`

const fixedDoc = `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: lsp-test}
sources:
  ingest:
    decoder: json
    cron: {expression: "*/5 * * * *"}
sinks:
  out:
    from: [ingest]
    file: {path: out.jsonl}
`

func TestInitializeHandshake(t *testing.T) {
	h := newHarness(t)
	var result struct {
		Capabilities struct {
			TextDocumentSync   map[string]any `json:"textDocumentSync"`
			CompletionProvider map[string]any `json:"completionProvider"`
			HoverProvider      bool           `json:"hoverProvider"`
		} `json:"capabilities"`
	}
	h.request("initialize", map[string]any{}, &result)
	if result.Capabilities.HoverProvider != true {
		t.Fatalf("hoverProvider not advertised: %+v", result)
	}
	if result.Capabilities.CompletionProvider == nil {
		t.Fatal("completionProvider not advertised")
	}
	h.notify("initialized", map[string]any{})
}

func TestDiagnosticsRoundTrip(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	h.notify("initialized", map[string]any{})

	// didOpen a broken pipeline → publishDiagnostics with the verify error.
	h.notify("textDocument/didOpen", map[string]any{
		"textDocument": map[string]any{"uri": "file:///p.yaml", "languageId": "yaml", "version": 1, "text": brokenDoc},
	})
	notif := h.awaitNotification("textDocument/publishDiagnostics")
	var params publishDiagnosticsParams
	if err := json.Unmarshal(notif.Params, &params); err != nil {
		t.Fatal(err)
	}
	if params.URI != "file:///p.yaml" {
		t.Fatalf("uri = %s", params.URI)
	}
	found := false
	for _, d := range params.Diagnostics {
		if d.Severity == 1 && d.Code == "plugin_unknown" {
			found = true
			// The engine anchors plugin diagnostics on the node line
			// (`ingest:`, 1-based line 5 → LSP line 4).
			if d.Range.Start.Line != 4 {
				t.Errorf("plugin_unknown line = %d, want 4", d.Range.Start.Line)
			}
		}
	}
	if !found {
		t.Fatalf("expected a plugin_unknown error diagnostic, got %+v", params.Diagnostics)
	}

	// didChange to a valid pipeline → diagnostics clear.
	h.notify("textDocument/didChange", map[string]any{
		"textDocument": map[string]any{"uri": "file:///p.yaml", "version": 2},
		"contentChanges": []map[string]any{{"text": fixedDoc}},
	})
	notif = h.awaitNotification("textDocument/publishDiagnostics")
	if err := json.Unmarshal(notif.Params, &params); err != nil {
		t.Fatal(err)
	}
	if len(params.Diagnostics) != 0 {
		t.Fatalf("expected empty diagnostics after fix, got %+v", params.Diagnostics)
	}

	// didClose clears too.
	h.notify("textDocument/didClose", map[string]any{
		"textDocument": map[string]any{"uri": "file:///p.yaml"},
	})
	notif = h.awaitNotification("textDocument/publishDiagnostics")
	if err := json.Unmarshal(notif.Params, &params); err != nil {
		t.Fatal(err)
	}
	if len(params.Diagnostics) != 0 {
		t.Fatalf("expected empty diagnostics after close, got %+v", params.Diagnostics)
	}
}

func pos(line, char int) map[string]int { return map[string]int{"line": line, "character": char} }

func completionAt(t *testing.T, h *harness, text string, line, char int) []completionItem {
	t.Helper()
	h.notify("textDocument/didOpen", map[string]any{
		"textDocument": map[string]any{"uri": "file:///c.yaml", "languageId": "yaml", "version": 1, "text": text},
	})
	h.awaitNotification("textDocument/publishDiagnostics")
	var items []completionItem
	h.request("textDocument/completion", map[string]any{
		"textDocument": map[string]any{"uri": "file:///c.yaml"},
		"position":     pos(line, char),
	}, &items)
	return items
}

func labels(items []completionItem) map[string]bool {
	out := map[string]bool{}
	for _, it := range items {
		out[it.Label] = true
	}
	return out
}

func TestCompletionInSourceNode(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    `
	items := completionAt(t, h, doc, 5, 6)
	got := labels(items)
	for _, want := range []string{"decoder", "grpc", "version", "kafka", "cron", "file", "http_server", "sql"} {
		if !got[want] {
			t.Errorf("missing completion %q; got %v", want, got)
		}
	}
	if got["encoder"] {
		t.Errorf("encoder offered inside sources; got %v", got)
	}
}

func TestCompletionInsidePluginBlock(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    kafka:
`
	// No prefix on the line after `kafka:` → all schema fields.
	items := completionAt(t, h, doc, 6, 6)
	got := labels(items)
	for _, want := range []string{"brokers", "topics", "group_id"} {
		if !got[want] {
			t.Errorf("missing kafka field %q; got %v", want, got)
		}
	}

	// With a prefix, the set filters.
	doc2 := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    kafka:
      br
`
	items = completionAt(t, h, doc2, 6, 8)
	got = labels(items)
	if !got["brokers"] || got["topics"] || got["group_id"] {
		t.Errorf("prefix filtering wrong: %v", got)
	}
}

func TestCompletionTopLevel(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := "apiVersion: eventboat/v3\nkind: Pipeline\n"
	items := completionAt(t, h, doc, 2, 0)
	got := labels(items)
	if !got["sources"] || !got["sinks"] || !got["transforms"] || !got["constants"] {
		t.Errorf("top-level completion incomplete: %v", got)
	}
}

func TestCompletionDecoderValue(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    decoder:
`
	items := completionAt(t, h, doc, 5, 12)
	got := labels(items)
	if !got["json"] || !got["raw"] {
		t.Errorf("codec value completion missing json/raw: %v", got)
	}
}

func TestCompletionInFromObject(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    cron: {expression: "* * * * *"}
sinks:
  out:
    from:
      ingest:
`
	// The cursor sits right after the upstream name inside the from mapping
	// → edge attributes.
	items := completionAt(t, h, doc, 9, 13)
	got := labels(items)
	for _, want := range []string{"when", "route", "delivery", "required", "buffer"} {
		if !got[want] {
			t.Errorf("missing edge attr %q; got %v", want, got)
		}
	}
}

func TestHover(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	doc := `apiVersion: eventboat/v3
kind: Pipeline
metadata: {name: c}
sources:
  ingest:
    decoder: json
    cron: {expression: "* * * * *"}
`
	h.notify("textDocument/didOpen", map[string]any{
		"textDocument": map[string]any{"uri": "file:///h.yaml", "languageId": "yaml", "version": 1, "text": doc},
	})
	h.awaitNotification("textDocument/publishDiagnostics")

	// Hover over the cron plugin key (line 6, col 4).
	var hover map[string]any
	h.request("textDocument/hover", map[string]any{
		"textDocument": map[string]any{"uri": "file:///h.yaml"},
		"position":     pos(6, 4),
	}, &hover)
	contents, _ := hover["contents"].(map[string]any)
	value, _ := contents["value"].(string)
	if !contains(value, "expression") || !contains(value, "source plugin") {
		t.Errorf("cron hover missing schema summary: %q", value)
	}

	// Hover over a framework field (decoder, line 5 col 4).
	h.request("textDocument/hover", map[string]any{
		"textDocument": map[string]any{"uri": "file:///h.yaml"},
		"position":     pos(5, 4),
	}, &hover)
	contents, _ = hover["contents"].(map[string]any)
	value, _ = contents["value"].(string)
	if !contains(value, "codec") {
		t.Errorf("decoder hover missing description: %q", value)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestUnknownMethodRespondsError(t *testing.T) {
	h := newHarness(t)
	id := h.send("textDocument/definition", map[string]any{})
	resp := h.await(id)
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("expected method-not-found, got %+v", resp)
	}
}

func TestShutdownExit(t *testing.T) {
	h := newHarness(t)
	h.request("initialize", map[string]any{}, nil)
	h.request("shutdown", map[string]any{}, nil)
	h.notify("exit", nil)
	if err := h.waitExit(5 * time.Second); err != nil {
		t.Fatalf("exit returned error: %v", err)
	}
}

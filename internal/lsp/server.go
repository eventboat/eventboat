package lsp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

// Server is the Eventboat language server. One instance serves one stdio
// connection; documents live in an in-memory map keyed by URI.
type Server struct {
	reg *registry.Registry
	svc *ops.Service

	mu       sync.Mutex
	docs     map[string]string // uri -> current text
	shutdown bool

	outMu sync.Mutex
	out   io.Writer // connection writer, set by Serve
}

// NewServer builds a server over the default registry (builtins only, like
// the CLI; external plugins are filesystem manifests, out of LSP scope).
func NewServer() (*Server, error) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		return nil, err
	}
	return &Server{
		reg:  reg,
		svc:  ops.New(ops.Options{Reg: reg}),
		docs: map[string]string{},
	}, nil
}

// Serve runs the message loop until the client exits, sends `exit`, or the
// context is canceled. Transport errors are returned; protocol-level errors
// are answered inline.
func (s *Server) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	s.outMu.Lock()
	s.out = w
	s.outMu.Unlock()
	reader := bufio.NewReader(r)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := readMessage(reader)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.IsRequest() {
			result, rpcErr := s.handle(ctx, msg)
			if err := s.write(respond(msg.ID, result, rpcErr)); err != nil {
				return err
			}
			continue
		}
		if msg.IsNotification() {
			if msg.Method == "exit" {
				return nil
			}
			if _, err := s.handle(ctx, msg); err != nil {
				// Notifications have no response channel; drop protocol
				// errors silently (the client cannot receive them).
				_ = err
			}
			continue
		}
		// A response from the client (we sent no requests) — ignore.
	}
}

func (s *Server) write(msg *Message) error {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if s.out == nil {
		return nil
	}
	return writeMessage(s.out, msg)
}

// handle dispatches one method. A nil result answers with null.
func (s *Server) handle(ctx context.Context, msg *Message) (any, *ResponseError) {
	switch msg.Method {
	case "initialize":
		return s.initialize(), nil
	case "initialized", "shutdown":
		return nil, nil
	case "$/cancelRequest", "$/setTrace", "workspace/didChangeConfiguration":
		return nil, nil
	case "textDocument/didOpen":
		return s.didOpen(msg.Params)
	case "textDocument/didChange":
		return s.didChange(msg.Params)
	case "textDocument/didClose":
		return s.didClose(msg.Params)
	case "textDocument/completion":
		return s.completion(msg.Params)
	case "textDocument/hover":
		return s.hover(msg.Params)
	default:
		return nil, &ResponseError{Code: codeMethodNotFound, Message: "eventboat lsp: method not handled: " + msg.Method}
	}
}

func (s *Server) initialize() map[string]any {
	return map[string]any{
		"capabilities": map[string]any{
			"textDocumentSync": map[string]any{
				"openClose": true,
				"change":    1, // full-text sync
			},
			"completionProvider": map[string]any{
				"triggerCharacters": []string{":", " ", "."},
				"resolveProvider":   false,
			},
			"hoverProvider": true,
		},
		"serverInfo": map[string]any{"name": "eventboat-lsp"},
	}
}

type textDocumentParams struct {
	TextDocument struct {
		URI  string `json:"uri"`
		Text string `json:"text"`
	} `json:"textDocument"`
}

func (s *Server) didOpen(params json.RawMessage) (any, *ResponseError) {
	var p textDocumentParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &ResponseError{Code: codeInvalidRequest, Message: err.Error()}
	}
	s.mu.Lock()
	s.docs[p.TextDocument.URI] = p.TextDocument.Text
	s.mu.Unlock()
	s.publishDiagnostics(p.TextDocument.URI, p.TextDocument.Text)
	return nil, nil
}

type didChangeParams struct {
	TextDocument struct {
		URI string `json:"uri"`
	} `json:"textDocument"`
	ContentChanges []struct {
		Text string `json:"text"`
	} `json:"contentChanges"`
}

func (s *Server) didChange(params json.RawMessage) (any, *ResponseError) {
	var p didChangeParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &ResponseError{Code: codeInvalidRequest, Message: err.Error()}
	}
	s.mu.Lock()
	text := s.docs[p.TextDocument.URI]
	if len(p.ContentChanges) > 0 {
		text = p.ContentChanges[len(p.ContentChanges)-1].Text // full sync
	}
	s.docs[p.TextDocument.URI] = text
	s.mu.Unlock()
	s.publishDiagnostics(p.TextDocument.URI, text)
	return nil, nil
}

func (s *Server) didClose(params json.RawMessage) (any, *ResponseError) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &ResponseError{Code: codeInvalidRequest, Message: err.Error()}
	}
	s.mu.Lock()
	delete(s.docs, p.TextDocument.URI)
	s.mu.Unlock()
	// Clear diagnostics for the closed document.
	s.publishDiagnostics(p.TextDocument.URI, "")
	return nil, nil
}

func (s *Server) document(uri string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	text, ok := s.docs[uri]
	return text, ok
}

// publishDiagnostics runs the real verify pipeline (config.LoadBytes +
// ir.Build via ops.Service.Verify — the identical path the CLI and MCP
// verify tools use) on the document text and pushes
// textDocument/publishDiagnostics. Empty text publishes an empty set
// (document closed or never had content).
func (s *Server) publishDiagnostics(uri, text string) {
	var diags []config.Diagnostic
	if text != "" {
		diags = s.svc.Verify(text)
	}
	_ = s.write(notify("textDocument/publishDiagnostics", publishDiagnosticsParams{
		URI:         uri,
		Diagnostics: toLspDiagnostics(text, diags),
	}))
}

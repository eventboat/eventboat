// Package lsp implements the Eventboat language server (redesign-v3.md
// §3.1/§4.11, M4): textDocument diagnostics, completion and hover over
// stdio. The protocol layer is a hand-written minimal JSON-RPC 2.0 (review
// redesign-v3-review-m4.md R6: go.lsp.dev/protocol requires Go 1.26 > the
// module's 1.25, and jsonrpc2 without protocol saves nothing once the LSP
// types are hand-written anyway). All data sources are the existing engine
// surfaces — the verify pipeline, the registry catalog and plugin schemas —
// zero second validation logic.
package lsp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Message is one JSON-RPC 2.0 message (request, notification or response).
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *ResponseError  `json:"error,omitempty"`
}

// ResponseError is a JSON-RPC error body.
type ResponseError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Standard JSON-RPC / LSP error codes.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInternal       = -32603
)

// IsRequest reports whether the message expects a response (has an id).
func (m *Message) IsRequest() bool { return len(m.ID) > 0 && m.Method != "" }

// IsNotification reports whether the message is a notification.
func (m *Message) IsNotification() bool { return len(m.ID) == 0 && m.Method != "" }

// readMessage reads one Content-Length framed message (LSP base protocol).
func readMessage(r *bufio.Reader) (*Message, error) {
	contentLength := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && line == "" {
				return nil, io.EOF
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break // end of headers
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("lsp: malformed header %q", line)
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return nil, fmt.Errorf("lsp: bad Content-Length %q", value)
			}
			contentLength = n
		}
	}
	if contentLength < 0 {
		return nil, fmt.Errorf("lsp: missing Content-Length header")
	}
	body := make([]byte, contentLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("lsp: short body: %w", err)
	}
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("lsp: bad json body: %w", err)
	}
	return &msg, nil
}

// writeMessage writes one framed message.
func writeMessage(w io.Writer, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n", len(body)); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// respond builds a response for a request id.
func respond(id json.RawMessage, result any, err *ResponseError) *Message {
	msg := &Message{JSONRPC: "2.0", ID: id}
	if err != nil {
		msg.Error = err
		return msg
	}
	if result != nil {
		b, _ := json.Marshal(result)
		msg.Result = b
	}
	return msg
}

// notify builds a server-initiated notification.
func notify(method string, params any) *Message {
	msg := &Message{JSONRPC: "2.0", Method: method}
	if params != nil {
		b, _ := json.Marshal(params)
		msg.Params = b
	}
	return msg
}

package main

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The lsp command is thin stdio wiring over internal/lsp (protocol behavior
// is covered by internal/lsp's suite). Here: the full command path answers
// initialize and stops on exit over pipes, the same shape production uses
// for os.Stdin/os.Stdout.
func TestLSPCommandWiring(t *testing.T) {
	inR, inW := io.Pipe() // editor → server
	outR, outW := io.Pipe()

	done := make(chan int, 1)
	go func() { done <- runLSP(context.Background(), inR, outW) }()

	writeFrame := func(body string) {
		t.Helper()
		if _, err := inW.Write([]byte("Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body)); err != nil {
			t.Fatal(err)
		}
	}
	writeFrame(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)

	msg, err := readFrame(bufio.NewReader(outR))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, `"capabilities"`) {
		t.Fatalf("bad initialize response: %s", msg)
	}

	writeFrame(`{"jsonrpc":"2.0","method":"exit"}`)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runLSP exit = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
	_ = inW.Close()
	_ = outW.Close()
}

// Usage errors.
func TestLSPCommandUsage(t *testing.T) {
	if code := cmdLSP([]string{"--bogus"}, false); code != 2 {
		t.Fatalf("usage exit = %d, want 2", code)
	}
}

// readFrame reads one Content-Length framed JSON body.
func readFrame(r *bufio.Reader) (string, error) {
	length := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return "", err
			}
			length = n
		}
	}
	if length < 0 {
		return "", io.ErrUnexpectedEOF
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return "", err
	}
	return string(body), nil
}

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/eventboat/eventboat/internal/lsp"
)

// cmdLSP runs the language server over stdio (redesign-v3.md §3.1/§4.11,
// M4): diagnostics from the verify pipeline, completion and hover from the
// registry catalog + plugin schemas. Editors launch this binary directly.
func cmdLSP(args []string, jsonOut bool) int {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: eventboat lsp   (stdio; no flags)")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runLSP(ctx, os.Stdin, os.Stdout)
}

func runLSP(ctx context.Context, r io.Reader, w io.Writer) int {
	srv, err := lsp.NewServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "lsp: %v\n", err)
		return 1
	}
	if err := srv.Serve(ctx, r, w); err != nil {
		fmt.Fprintf(os.Stderr, "lsp: %v\n", err)
		return 1
	}
	return 0
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/eventboat/eventboat/internal/admin"
	"github.com/eventboat/eventboat/internal/mcpserver"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/runtimecfg"
	"github.com/eventboat/eventboat/internal/store"
)

// cmdMCP runs the operational surface: the MCP server (stdio for agent
// hosts, or HTTP with the admin REST + SSE + UI alongside), managing the
// deployed pipelines in-process.
func cmdMCP(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	stdio := fs.Bool("stdio", false, "speak MCP over stdin/stdout (for agent hosts to spawn)")
	httpMode := fs.Bool("http", false, "serve MCP over HTTP (Streamable HTTP) together with the admin surface")
	configDir := fs.String("config-dir", "", "directory of pipeline YAML files to deploy at startup")
	runtimeFile := fs.String("runtime", "", "Runtime configuration file (default: ./eventboat.yaml)")
	dataDir := fs.String("data-dir", "", "override storage.data_dir")
	ephemeral := fs.Bool("ephemeral", false, "override storage.ephemeral (in-memory stores)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if !*stdio && !*httpMode {
		fmt.Fprintln(os.Stderr, "mcp: choose --stdio or --http")
		return 2
	}

	rt, err := runtimecfg.Load(*runtimeFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcp: %v\n", err)
		return 2
	}
	if *dataDir != "" {
		rt.Storage.DataDir = *dataDir
	}
	if *ephemeral {
		rt.Storage.Ephemeral = true
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcp: %v\n", err)
		return 2
	}

	svc, metricsHandler, obsShutdown := newOpsService(reg, rt)
	defer func() { _ = obsShutdown(context.Background()) }()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if *configDir != "" {
		if err := deployDir(ctx, svc, *configDir); err != nil {
			fmt.Fprintf(os.Stderr, "mcp: %v\n", err)
			svc.Stop()
			return 1
		}
	}
	defer svc.Stop()

	server := mcpserver.NewServer(svc, "eventboat", "v3")

	if *stdio {
		if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "mcp: %v\n", err)
			return 1
		}
		return 0
	}

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	adminH := admin.Handler(svc, metricsHandler, handler)
	fmt.Printf("eventboat: admin + MCP listening on %s (UI: http://%s/admin/)\n", rt.Admin.Listen, rt.Admin.Listen)
	if err := admin.Serve(ctx, rt.Admin.Listen, adminH); err != nil {
		fmt.Fprintf(os.Stderr, "mcp: %v\n", err)
		return 1
	}
	return 0
}

// newOpsService builds the ops service from the runtime config (including
// the telemetry stack; metricsHandler serves /metrics when Prometheus is on).
func newOpsService(reg *registry.Registry, rt runtimecfg.Config) (svc *ops.Service, metricsHandler http.Handler, shutdown func(context.Context) error) {
	dir := rt.Storage.DataDir
	observer, err := obs.Setup(context.Background(), obs.Config{
		OTLPEndpoint: rt.Telemetry.OTLPEndpoint,
		SampleRatio:  rt.Telemetry.SampleRatio,
		Prometheus:   rt.Telemetry.Prometheus,
	})
	if err != nil {
		observer = nil // telemetry must never take the runtime down
	}
	svc = ops.New(ops.Options{
		Obs:     observer,
		DataDir: dir,
		Reg:     reg,
		StoreFor: func(pipeline string) (store.Store, error) {
			if rt.Storage.Ephemeral {
				return store.NewMemory(pipeline), nil
			}
			sdir := filepath.Join(dir, "stores")
			if err := os.MkdirAll(sdir, 0o755); err != nil {
				return nil, err
			}
			return store.OpenSQLite(filepath.Join(sdir, sanitize(pipeline)+".db"))
		},
	})
	if observer != nil {
		return svc, observer.Handler(), observer.Shutdown
	}
	return svc, nil, func(context.Context) error { return nil }
}

func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
}

// deployDir verifies and deploys every pipeline YAML in a directory.
func deployDir(ctx context.Context, svc *ops.Service, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || (!strings.HasSuffix(e.Name(), ".yaml") && !strings.HasSuffix(e.Name(), ".yml")) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := svc.Deploy(ctx, string(raw)); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

// cmdRunDir runs a config directory: continuous pipelines under engines,
// job pipelines under their managers, plus the admin surface when enabled.
func cmdRunDir(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("run-dir", flag.ContinueOnError)
	configDir := fs.String("config-dir", "", "directory of pipeline YAML files")
	runtimeFile := fs.String("runtime", "", "Runtime configuration file")
	dataDir := fs.String("data-dir", "", "override storage.data_dir")
	ephemeral := fs.Bool("ephemeral", false, "in-memory stores")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configDir == "" {
		fmt.Fprintln(os.Stderr, "run: --config-dir is required for directory mode")
		return 2
	}
	rt, err := runtimecfg.Load(*runtimeFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}
	if *dataDir != "" {
		rt.Storage.DataDir = *dataDir
	}
	if *ephemeral {
		rt.Storage.Ephemeral = true
	}
	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}
	svc, metricsHandler, obsShutdown := newOpsService(reg, rt)
	defer func() { _ = obsShutdown(context.Background()) }()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := deployDir(ctx, svc, *configDir); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		svc.Stop()
		return 1
	}
	defer svc.Stop()

	statuses, _ := json.Marshal(svc.Status())
	if jsonOut {
		fmt.Println(string(statuses))
	} else {
		fmt.Printf("eventboat: %d pipeline(s) running from %s\n", len(svc.Status()), *configDir)
	}

	if rt.Admin.Enable {
		server := mcpserver.NewServer(svc, "eventboat", "v3")
		mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
		handler := admin.Handler(svc, metricsHandler, mcpHandler)
		go func() {
			fmt.Printf("eventboat: admin + MCP on http://%s/admin/\n", rt.Admin.Listen)
			_ = admin.Serve(ctx, rt.Admin.Listen, handler)
		}()
	}
	<-ctx.Done()
	fmt.Println("eventboat: stopped")
	return 0
}

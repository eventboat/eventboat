package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/riverpod/riverpod/internal/config"
	"github.com/riverpod/riverpod/internal/engine"
	"github.com/riverpod/riverpod/internal/observability"
	"github.com/riverpod/riverpod/internal/registry"
	"github.com/riverpod/riverpod/internal/testrunner"
	"github.com/riverpod/riverpod/internal/topology"
	_ "github.com/riverpod/riverpod/plugins/all"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "run":
		runCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	case "test":
		testCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: riverpod <run|validate|test> [flags]\n")
}

func runCmd(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "", "pipeline config file")
	configDir := fs.String("config-dir", "", "directory of pipeline configs")
	format := fs.String("format", "", "config format (yaml|hocon)")
	_ = fs.Parse(args)

	cfgs, paths, err := loadConfigsWithPaths(*configPath, *configDir, *format)
	if err != nil {
		fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	eng := engine.New(registry.Default)
	for i, cfg := range cfgs {
		ir, err := topology.FromConfig(cfg)
		if err != nil {
			fatal(err)
		}
		for _, w := range ir.DeprecationWarnings {
			fmt.Fprintf(os.Stderr, "warning: %s\n", w)
		}
		if err := eng.Load(ctx, ir); err != nil {
			fatal(err)
		}
		if paths[i] != "" {
			eng.SetConfigPath(ir.Name, paths[i])
		}
		fmt.Printf("loaded pipeline %q (%d stages, %d edges)\n", ir.Name, len(ir.Stages), len(ir.Edges))
	}

	obsCfg := observability.MergeObservability(cfgs)
	if err := eng.StartObservability(ctx, obsCfg); err != nil {
		fatal(err)
	}
	if obsCfg.Metrics.Enabled {
		slog.Info("observability listening",
			"metrics_port", obsCfg.Metrics.Port,
			"metrics_path", obsCfg.Metrics.Path,
			"health_port", obsCfg.Health.Port,
		)
	}

	if err := eng.Start(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("riverpod running; press Ctrl+C to stop")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sigCh:
				if _, err := eng.BeginReloadAll(context.Background()); err != nil {
					slog.Error("SIGHUP reload failed", "error", err)
				} else {
					slog.Info("reload all triggered by SIGHUP")
				}
			}
		}
	}()

	<-ctx.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	_ = eng.Stop(stopCtx)
	_ = eng.StopObservability(stopCtx)
}

func validateCmd(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	configPath := fs.String("config", "", "pipeline config file")
	configDir := fs.String("config-dir", "", "directory of pipeline configs")
	format := fs.String("format", "", "config format (yaml|hocon)")
	_ = fs.Parse(args)

	cfgs, _, err := loadConfigsWithPaths(*configPath, *configDir, *format)
	if err != nil {
		fatal(err)
	}
	for _, cfg := range cfgs {
		ir, err := topology.FromConfig(cfg)
		if err != nil {
			fatal(err)
		}
		if err := topology.ValidateSemantics(ir, registry.Default); err != nil {
			fatal(err)
		}
		for _, w := range ir.DeprecationWarnings {
			fmt.Fprintf(os.Stderr, "warning: %s\n", w)
		}
		fmt.Printf("OK: pipeline %q (%d stages, %d edges)\n", ir.Name, len(ir.Stages), len(ir.Edges))
	}
}

func testCmd(args []string) {
	fs := flag.NewFlagSet("test", flag.ExitOnError)
	testPath := fs.String("config", "", "test suite YAML file")
	testDir := fs.String("dir", "", "directory of test suite YAML files")
	_ = fs.Parse(args)

	var files []string
	switch {
	case *testPath != "":
		files = []string{*testPath}
	case *testDir != "":
		entries, err := os.ReadDir(*testDir)
		if err != nil {
			fatal(err)
		}
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			ext := filepath.Ext(ent.Name())
			if ext == ".yaml" || ext == ".yml" {
				files = append(files, filepath.Join(*testDir, ent.Name()))
			}
		}
	default:
		fatal(fmt.Errorf("either --config or --dir is required"))
	}
	if len(files) == 0 {
		fatal(fmt.Errorf("no test files found"))
	}

	var all []testrunner.Result
	for _, f := range files {
		results, err := testrunner.RunFile(f, registry.Default)
		if err != nil {
			fatal(err)
		}
		all = append(all, results...)
	}
	fmt.Print(testrunner.FormatResults(all))
	if !testrunner.AllPassed(all) {
		os.Exit(1)
	}
}

func loadConfigsWithPaths(path, dir, format string) ([]*config.PipelineConfig, []string, error) {
	switch {
	case path != "":
		if format != "" {
			f, err := config.DetectFormat(path, format)
			if err != nil {
				return nil, nil, err
			}
			var cfg *config.PipelineConfig
			var err2 error
			switch f {
			case config.FormatYAML:
				cfg, err2 = config.LoadYAML(path)
			case config.FormatHOCON:
				cfg, err2 = config.LoadHOCON(path)
			}
			return []*config.PipelineConfig{cfg}, []string{path}, err2
		}
		cfg, err := config.LoadFile(path)
		return []*config.PipelineConfig{cfg}, []string{path}, err
	case dir != "":
		cfgs, err := config.LoadDir(dir)
		if err != nil {
			return nil, nil, err
		}
		paths := make([]string, len(cfgs))
		for i, cfg := range cfgs {
			name := config.PipelineName(cfg)
			for _, ext := range []string{".yaml", ".yml", ".conf", ".hocon"} {
				candidate := filepath.Join(dir, name+ext)
				if _, err := os.Stat(candidate); err == nil {
					paths[i] = candidate
					break
				}
			}
		}
		return cfgs, paths, nil
	default:
		return nil, nil, fmt.Errorf("either --config or --config-dir is required")
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

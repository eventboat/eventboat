package rpcplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/registry"
	pluginv1 "github.com/eventboat/eventboat/pkg/pluginv1"
)

// SpawnOption tunes SpawnSource/SpawnSink (variadic to keep call sites
// stable).
type SpawnOption func(*spawnOpts)

type spawnOpts struct {
	onRestart func(plugin string) // restart counter hook (metrics)
}

// WithRestartCounter reports every supervisor respawn (plugin name) to the
// host's metrics.
func WithRestartCounter(fn func(plugin string)) SpawnOption {
	return func(o *spawnOpts) { o.onRestart = fn }
}

// SpawnSource launches an external source plugin and returns it as a
// registry.Source. If the plugin declares the "pull" capability the returned
// value also implements registry.PullSource, mapping Pull's normal
// end-of-stream to "exhausted" (nil) and an errored stream to a failed pull.
func SpawnSource(ctx context.Context, cfg *config.GrpcConfig, manifest *config.PluginManifest, pluginCfg map[string]any, logf func(string, ...any), opts ...SpawnOption) (registry.Source, error) {
	if manifest.Kind != "source" {
		return nil, fmt.Errorf("rpcplugin: manifest of %q declares kind %q", manifest.Name, manifest.Kind)
	}
	var so spawnOpts
	for _, o := range opts {
		o(&so)
	}
	p, err := spawn(ctx, cfg, manifest, "source", logf)
	if err != nil {
		return nil, err
	}
	cfgJSON, err := json.Marshal(pluginCfg)
	if err != nil {
		p.stop()
		return nil, fmt.Errorf("rpcplugin: encode config of %q: %w", manifest.Name, err)
	}
	s := &source{plug: p, cfgJSON: cfgJSON, isPull: contains(manifest.Capabilities, "pull"), logf: logf}
	s.initRPC = func(p *process, state []byte) error {
		resp, err := p.source.Init(context.Background(), &pluginv1.InitRequest{
			State:      state,
			ConfigJson: string(s.cfgJSON),
		})
		if err != nil {
			return fmt.Errorf("plugin %q: init transport error: %w", p.hs.Name, err)
		}
		if resp.Error != "" {
			return fmt.Errorf("plugin %q: init: %s", p.hs.Name, resp.Error)
		}
		return nil
	}
	if cfg.Restart == "restart" {
		s.sup = newSupervisor(p, cfg, manifest, "source", logf, so.onRestart, s.initRPC)
	}
	return s, nil
}

type source struct {
	plug    *process
	cfgJSON []byte
	isPull  bool
	logf    func(string, ...any)

	// initRPC delivers config/state to one process (shared by the
	// fast-fail initOnce path and the supervisor's per-restart reinit).
	initRPC func(p *process, state []byte) error

	initOnce sync.Once
	initErr  error

	sup *supervisor // nil = fast-fail (M3 semantics)
}

// init delivers config (and any persisted state) to the plugin. The engine
// calls Init only when persisted state exists (M1 semantics); external
// plugins receive their config through the same RPC, so the adapter sends it
// itself before streaming if the engine did not.
func (s *source) init(state []byte) error {
	if s.sup != nil {
		s.sup.updateState(state)
		p, err := s.sup.live(context.Background())
		if err != nil {
			return err
		}
		_ = p
		return nil
	}
	s.initOnce.Do(func() {
		s.initErr = s.initRPC(s.plug, state)
	})
	return s.initErr
}

// Init satisfies registry.Source (persisted-state restore).
func (s *source) Init(state []byte) error { return s.init(state) }

// proc returns the process to call: the supervised live one or the static
// fast-fail one.
func (s *source) proc(ctx context.Context) (*process, error) {
	if s.sup != nil {
		return s.sup.live(ctx)
	}
	return s.plug, nil
}

func (s *source) Run(ctx context.Context, emit func(registry.Message)) {
	if err := s.init(nil); err != nil {
		if s.logf != nil {
			s.logf("%v", err)
		}
		return
	}
	_ = s.stream(ctx, emit, false)
}

func (s *source) Pull(ctx context.Context, emit func(registry.Message)) error {
	if err := s.init(nil); err != nil {
		return err
	}
	if !s.isPull {
		return fmt.Errorf("plugin %q has no pull capability", s.plug.hs.Name)
	}
	return s.stream(ctx, emit, true)
}

// stream drives the Run/Pull server stream. Continuous mode (pull=false)
// returns on end-of-stream or error without propagating (registry.Source.Run
// has no error channel; the engine treats an ended source as stopped and the
// error is logged). Pull mode distinguishes exhaustion from failure. Under
// the restart policy a FAILED stream respawns (the supervisor's backoff) and
// retries; clean end-of-stream is exhaustion and does not restart.
func (s *source) stream(ctx context.Context, emit func(registry.Message), pull bool) error {
	for {
		p, err := s.proc(ctx)
		if err != nil {
			return err
		}
		err = s.streamOnce(p, ctx, emit, pull)
		if err == nil || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		if s.sup == nil {
			return err
		}
		if s.logf != nil {
			s.logf("plugin %q: stream failed (%v); restarting", p.hs.Name, err)
		}
		s.sup.drop(p)
	}
}

func (s *source) streamOnce(p *process, ctx context.Context, emit func(registry.Message), pull bool) error {
	var (
		stream pluginv1.Source_RunClient
		err    error
	)
	if pull {
		stream, err = p.source.Pull(ctx, &pluginv1.RunRequest{})
	} else {
		stream, err = p.source.Run(ctx, &pluginv1.RunRequest{})
	}
	if err != nil {
		if s.logf != nil {
			s.logf("plugin %q: open stream: %v", p.hs.Name, err)
		}
		return fmt.Errorf("plugin %q: %w", p.hs.Name, err)
	}
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.logf != nil {
				s.logf("plugin %q: stream error: %v", p.hs.Name, err)
			}
			return fmt.Errorf("plugin %q: stream: %w", p.hs.Name, err)
		}
		emit(eventToMessage(ev))
	}
}

func (s *source) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	p, err := s.proc(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := p.source.Settled(ctx, &pluginv1.SettledRequest{ThroughSrcSeq: throughSrcSeq})
	if err != nil && s.sup != nil {
		// One respawn-and-retry per call; the engine's own retry cadence
		// handles persistent trouble.
		s.sup.drop(p)
		if p, err = s.proc(ctx); err != nil {
			return nil, err
		}
		resp, err = p.source.Settled(ctx, &pluginv1.SettledRequest{ThroughSrcSeq: throughSrcSeq})
	}
	if err != nil {
		return nil, fmt.Errorf("plugin %q: settled transport error: %w", p.hs.Name, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("plugin %q: settled: %s", p.hs.Name, resp.Error)
	}
	if s.sup != nil {
		s.sup.updateState(resp.State)
	}
	return resp.State, nil
}

func (s *source) Close() error {
	if s.sup != nil {
		s.sup.close()
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeRPCTimeout)
	defer cancel()
	if _, err := s.plug.source.Close(ctx, &pluginv1.CloseRequest{}); err != nil && s.logf != nil {
		s.logf("plugin %q: close rpc: %v", s.plug.hs.Name, err)
	}
	s.plug.stop()
	return nil
}

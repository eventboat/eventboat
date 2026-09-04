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

// SpawnSource launches an external source plugin and returns it as a
// registry.Source. If the plugin declares the "pull" capability the returned
// value also implements registry.PullSource, mapping Pull's normal
// end-of-stream to "exhausted" (nil) and an errored stream to a failed pull.
func SpawnSource(ctx context.Context, cfg *config.GrpcConfig, manifest *config.PluginManifest, pluginCfg map[string]any, logf func(string, ...any)) (registry.Source, error) {
	if manifest.Kind != "source" {
		return nil, fmt.Errorf("rpcplugin: manifest of %q declares kind %q", manifest.Name, manifest.Kind)
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
	return &source{plug: p, cfgJSON: cfgJSON, isPull: contains(manifest.Capabilities, "pull"), logf: logf}, nil
}

type source struct {
	plug    *process
	cfgJSON []byte
	isPull  bool
	logf    func(string, ...any)

	initOnce sync.Once
	initErr  error
}

// init delivers config (and any persisted state) to the plugin. The engine
// calls Init only when persisted state exists (M1 semantics); external
// plugins receive their config through the same RPC, so the adapter sends it
// itself before streaming if the engine did not.
func (s *source) init(state []byte) error {
	s.initOnce.Do(func() {
		resp, err := s.plug.source.Init(context.Background(), &pluginv1.InitRequest{
			State:      state,
			ConfigJson: string(s.cfgJSON),
		})
		if err != nil {
			s.initErr = fmt.Errorf("plugin %q: init transport error: %w", s.plug.hs.Name, err)
			return
		}
		if resp.Error != "" {
			s.initErr = fmt.Errorf("plugin %q: init: %s", s.plug.hs.Name, resp.Error)
		}
	})
	return s.initErr
}

// Init satisfies registry.Source (persisted-state restore).
func (s *source) Init(state []byte) error { return s.init(state) }

func (s *source) Run(ctx context.Context, emit func(registry.Message)) {
	if err := s.init(nil); err != nil {
		if s.logf != nil {
			s.logf("%v", err)
		}
		return
	}
	s.stream(ctx, emit, false)
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

// stream drives one Run or Pull server stream. Continuous mode (pull=false)
// returns on end-of-stream or error without propagating (registry.Source.Run
// has no error channel; the engine treats an ended source as stopped and the
// error is logged). Pull mode distinguishes exhaustion from failure.
func (s *source) stream(ctx context.Context, emit func(registry.Message), pull bool) error {
	var (
		stream pluginv1.Source_RunClient
		err    error
	)
	if pull {
		stream, err = s.plug.source.Pull(ctx, &pluginv1.RunRequest{})
	} else {
		stream, err = s.plug.source.Run(ctx, &pluginv1.RunRequest{})
	}
	if err != nil {
		if s.logf != nil {
			s.logf("plugin %q: open stream: %v", s.plug.hs.Name, err)
		}
		return fmt.Errorf("plugin %q: %w", s.plug.hs.Name, err)
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
				s.logf("plugin %q: stream error: %v", s.plug.hs.Name, err)
			}
			return fmt.Errorf("plugin %q: stream: %w", s.plug.hs.Name, err)
		}
		emit(eventToMessage(ev))
	}
}

func (s *source) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	resp, err := s.plug.source.Settled(ctx, &pluginv1.SettledRequest{ThroughSrcSeq: throughSrcSeq})
	if err != nil {
		return nil, fmt.Errorf("plugin %q: settled transport error: %w", s.plug.hs.Name, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("plugin %q: settled: %s", s.plug.hs.Name, resp.Error)
	}
	return resp.State, nil
}

func (s *source) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), closeRPCTimeout)
	defer cancel()
	if _, err := s.plug.source.Close(ctx, &pluginv1.CloseRequest{}); err != nil && s.logf != nil {
		s.logf("plugin %q: close rpc: %v", s.plug.hs.Name, err)
	}
	s.plug.stop()
	return nil
}

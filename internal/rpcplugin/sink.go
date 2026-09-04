package rpcplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/registry"
	pluginv1 "github.com/eventboat/eventboat/pkg/pluginv1"
)

// SpawnSink launches an external sink plugin and returns it as a
// registry.Sink. Write errors follow the same delivery policy as built-in
// sinks: the engine retries per the edge policy, then dead letters.
func SpawnSink(ctx context.Context, cfg *config.GrpcConfig, manifest *config.PluginManifest, pluginCfg map[string]any, logf func(string, ...any)) (registry.Sink, error) {
	if manifest.Kind != "sink" {
		return nil, fmt.Errorf("rpcplugin: manifest of %q declares kind %q", manifest.Name, manifest.Kind)
	}
	p, err := spawn(ctx, cfg, manifest, "sink", logf)
	if err != nil {
		return nil, err
	}
	cfgJSON, err := json.Marshal(pluginCfg)
	if err != nil {
		p.stop()
		return nil, fmt.Errorf("rpcplugin: encode config of %q: %w", manifest.Name, err)
	}
	return &sink{plug: p, cfgJSON: cfgJSON}, nil
}

type sink struct {
	plug    *process
	cfgJSON []byte

	initOnce sync.Once
	initErr  error
}

// init delivers the plugin config. registry.Sink has no Init step (the
// engine never calls one), so the adapter delivers config before the first
// Write (and before Close, so even a zero-write sink gets configured).
func (s *sink) init() error {
	s.initOnce.Do(func() {
		resp, err := s.plug.sink.Init(context.Background(), &pluginv1.InitRequest{
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

func (s *sink) Init(state []byte) error { return s.init() }

func (s *sink) Write(ctx context.Context, msgs []registry.Message) error {
	if err := s.init(); err != nil {
		return err
	}
	batch := make([]*pluginv1.Event, len(msgs))
	for i, m := range msgs {
		batch[i] = messageToEvent(m)
	}
	resp, err := s.plug.sink.Write(ctx, &pluginv1.WriteRequest{Batch: batch})
	if err != nil {
		return fmt.Errorf("plugin %q: write transport error: %w", s.plug.hs.Name, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("plugin %q: write: %s", s.plug.hs.Name, resp.Error)
	}
	return nil
}

func (s *sink) Close() error {
	_ = s.init() // zero-write sinks still receive their config before Close
	ctx, cancel := context.WithTimeout(context.Background(), closeRPCTimeout)
	defer cancel()
	if _, err := s.plug.sink.Close(ctx, &pluginv1.CloseRequest{}); err != nil {
		// Best effort: still stop the process.
		_ = err
	}
	s.plug.stop()
	return nil
}

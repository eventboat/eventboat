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
// sinks: the engine retries per the edge policy, then dead letters. Under
// grpc.restart: restart a transport error respawns the plugin once within
// the Write call before surfacing.
func SpawnSink(ctx context.Context, cfg *config.GrpcConfig, manifest *config.PluginManifest, pluginCfg map[string]any, logf func(string, ...any), opts ...SpawnOption) (registry.Sink, error) {
	if manifest.Kind != "sink" {
		return nil, fmt.Errorf("rpcplugin: manifest of %q declares kind %q", manifest.Name, manifest.Kind)
	}
	var so spawnOpts
	for _, o := range opts {
		o(&so)
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
	s := &sink{plug: p, cfgJSON: cfgJSON}
	s.initRPC = func(p *process, _ []byte) error {
		resp, err := p.sink.Init(context.Background(), &pluginv1.InitRequest{
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
		s.sup = newSupervisor(p, cfg, manifest, "sink", logf, so.onRestart, s.initRPC)
	}
	return s, nil
}

type sink struct {
	plug    *process
	cfgJSON []byte

	// initRPC delivers config to one process (fast-fail initOnce path and
	// the supervisor's per-restart reinit share it).
	initRPC func(p *process, state []byte) error

	initOnce sync.Once
	initErr  error

	sup *supervisor // nil = fast-fail (M3 semantics)
}

// init delivers the plugin config. registry.Sink has no Init step (the
// engine never calls one), so the adapter delivers config before the first
// Write (and before Close, so even a zero-write sink gets configured).
func (s *sink) init() error {
	if s.sup != nil {
		_, err := s.sup.live(context.Background())
		return err
	}
	s.initOnce.Do(func() {
		s.initErr = s.initRPC(s.plug, nil)
	})
	return s.initErr
}

func (s *sink) Init(state []byte) error { return s.init() }

func (s *sink) Write(ctx context.Context, msgs []registry.Message) error {
	if err := s.init(); err != nil {
		return err
	}
	p := s.plug
	if s.sup != nil {
		var err error
		p, err = s.sup.live(ctx)
		if err != nil {
			return err
		}
	}
	resp, err := p.sink.Write(ctx, &pluginv1.WriteRequest{Batch: writeBatch(msgs)})
	if err != nil && s.sup != nil {
		// Transport error: the process may be dead or wedged — respawn once
		// and retry this batch; the engine's edge policy handles the rest.
		s.sup.drop(p)
		if p, err = s.sup.live(ctx); err != nil {
			return err
		}
		resp, err = p.sink.Write(ctx, &pluginv1.WriteRequest{Batch: writeBatch(msgs)})
	}
	if err != nil {
		return fmt.Errorf("plugin %q: write transport error: %w", p.hs.Name, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("plugin %q: write: %s", p.hs.Name, resp.Error)
	}
	return nil
}

func writeBatch(msgs []registry.Message) []*pluginv1.Event {
	batch := make([]*pluginv1.Event, len(msgs))
	for i, m := range msgs {
		batch[i] = messageToEvent(m)
	}
	return batch
}

func (s *sink) Close() error {
	if s.sup != nil {
		s.sup.close()
		return nil
	}
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

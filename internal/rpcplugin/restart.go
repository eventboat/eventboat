package rpcplugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/config"
)

// Crash policy (§6.5, M3 trim closed by the beta round): the default stays
// fast-fail — a dead plugin surfaces as stream/write errors and a redeploy
// recovers. grpc.restart: restart hands the process to a supervisor that
// respawns it with exponential backoff and re-delivers its config (and the
// latest persisted state, so pull sources resume past the committed
// watermark — duplicates are the at-least-once contract, never loss).

const (
	restartBackoffBase = 250 * time.Millisecond
	restartBackoffCap  = 30 * time.Second
	// restartUptimeReset: after a process stayed up this long, the backoff
	// ladder resets (a crash-loop is what deserves growing delays, not an
	// occasional crash).
	restartUptimeReset = 30 * time.Second
)

// supervisor owns one plugin process across restarts. Respawn is lazy: RPC
// entry points ask for a live process; a dead one is replaced after the
// backoff wait. All state is guarded by mu; no background goroutines.
type supervisor struct {
	mu       sync.Mutex
	cfg      *config.GrpcConfig
	manifest *config.PluginManifest
	kind     string
	logf     func(format string, args ...any)
	onCount  func(plugin string) // restart counter hook (metrics)

	proc      *process
	startedAt time.Time
	attempt   int // consecutive rapid failures (backoff ladder)
	restarts  int64
	closed    bool

	// reinit delivers config/state to a freshly spawned process (the
	// adapter's Init RPC); state is the latest known persisted state.
	reinit func(p *process, state []byte) error
	state  []byte
	inited *process // the process reinit already ran against
}

func newSupervisor(p *process, cfg *config.GrpcConfig, manifest *config.PluginManifest, kind string, logf func(string, ...any), onCount func(string), reinit func(*process, []byte) error) *supervisor {
	if logf == nil {
		logf = nopLogf
	}
	return &supervisor{
		cfg: cfg, manifest: manifest, kind: kind, logf: logf, onCount: onCount,
		proc: p, startedAt: time.Now(), reinit: reinit,
	}
}

// live returns a usable process, respawning when the current one died.
// The reinit callback (config delivery) runs once per process before it is
// handed out.
func (s *supervisor) live(ctx context.Context) (*process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("rpcplugin: plugin %q supervisor closed", s.manifest.Name)
	}
	if s.proc != nil && s.proc.alive() {
		if s.inited != s.proc {
			if err := s.reinit(s.proc, s.state); err != nil {
				return nil, err
			}
			s.inited = s.proc
		}
		return s.proc, nil
	}
	// Dead: honor the backoff ladder, then respawn.
	if wait := s.backoffLocked(); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p, err := spawn(ctx, s.cfg, s.manifest, s.kind, s.logf)
	if err != nil {
		s.attempt++
		return nil, err
	}
	if time.Since(s.startedAt) >= restartUptimeReset {
		s.attempt = 0
	}
	s.attempt++
	s.restarts++
	s.proc = p
	s.startedAt = time.Now()
	s.inited = nil
	if s.onCount != nil {
		s.onCount(s.manifest.Name)
	}
	s.logf("plugin %q: restarted (restart #%d, attempt %d)", s.manifest.Name, s.restarts, s.attempt)
	if err := s.reinit(p, s.state); err != nil {
		s.inited = nil
		return nil, err
	}
	s.inited = p
	return p, nil
}

// backoffLocked returns the wait before the next respawn attempt (0 for the
// first restart).
func (s *supervisor) backoffLocked() time.Duration {
	if s.attempt == 0 {
		return 0
	}
	d := restartBackoffBase
	for i := 1; i < s.attempt && d < restartBackoffCap; i++ {
		d *= 2
	}
	if d > restartBackoffCap {
		d = restartBackoffCap
	}
	return d
}

// drop forgets a process that failed an RPC (it may still be "alive" from
// the OS perspective but the connection is broken — e.g. a wedged plugin).
func (s *supervisor) drop(p *process) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == p {
		s.proc.stop()
		s.proc = nil
	} else {
		p.stop()
	}
}

// updateState records the latest persisted state (used by the next reinit).
func (s *supervisor) updateState(state []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(state) > 0 {
		s.state = state
	}
}

// count reports total restarts (tests, metrics).
func (s *supervisor) count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarts
}

// close stops the current process and forbids further restarts.
func (s *supervisor) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.proc != nil {
		s.proc.stop()
		s.proc = nil
	}
}

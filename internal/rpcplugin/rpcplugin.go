// Package rpcplugin hosts out-of-process source/sink plugins over gRPC
// (redesign-v3.md §6.5, M3). The wire contract lives in
// proto/eventboat/plugin/v1 (generated Go: pkg/pluginproto) and is documented
// for third-party implementers in docs/plugins.md.
//
// Startup: the host launches grpc.command, the plugin listens on 127.0.0.1,
// prints one JSON handshake line to stdout, and serves gRPC. The host dials
// it with the handshake auth token as per-RPC metadata. Shutdown: the host
// calls Close, closes the plugin's stdin (the documented stop signal), waits
// the grace period, then kills the process.
package rpcplugin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/pkg/pluginproto"
)

// ProtocolVersion is the plugin ABI this host speaks (handshake field
// "eventboat_plugin").
const ProtocolVersion = 1

// MaxMessageSize is the explicit gRPC message cap on the plugin transport,
// both directions and both roles. grpc-go's default receive limit is 4 MiB,
// which a legitimate Event can exceed (the engine bounds messages by COUNT —
// limits.max_in_flight / the 10k spool high watermark — never by bytes, and
// sink Write batches several Events into one message), so leaving the
// default would fail legal workloads with opaque ResourceExhausted errors.
// 64 MiB is deliberately above any message the engine can produce today
// (payloads of a few MB plus a batch of siblings) while still bounding a
// buggy or hostile plugin's per-message memory. The number is part of the
// transport contract — documented in docs/plugins.md.
const MaxMessageSize = 64 << 20

// handshakeTimeout bounds the wait for the handshake line; stopGrace bounds
// the wait for voluntary exit after stdin closes.
const (
	handshakeTimeout = 15 * time.Second
	stopGrace        = 5 * time.Second
	closeRPCTimeout  = 3 * time.Second
)

// Handshake is the single JSON line a plugin prints to stdout on startup.
type Handshake struct {
	Protocol     int      `json:"eventboat_plugin"`
	Kind         string   `json:"kind"` // "source" | "sink"
	Name         string   `json:"name"`
	Version      int      `json:"version"`
	Capabilities []string `json:"capabilities,omitempty"`
	Listen       string   `json:"listen"` // host:port on 127.0.0.1
	Auth         string   `json:"auth"`
}

// process is one launched plugin subprocess with its gRPC connection. The
// exited channel closes when the OS-level process is gone (one watcher
// goroutine owns cmd.Wait), giving the supervisor a race-free liveness
// check.
type process struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	conn   *grpc.ClientConn
	source pluginproto.SourceClient
	sink   pluginproto.SinkClient
	hs     Handshake
	logf   func(format string, args ...any)

	exited  chan struct{}
	waitErr error // written once, before exited closes
}

// logf defaults to discarding plugin output; the engine wires a logger when
// one is available.
func nopLogf(string, ...any) {}

// spawn launches the plugin and validates its handshake against the manifest
// (review-m3 R5: name/version/kind/capabilities must match the static
// declaration — a drifting plugin fails loudly, never silently).
func spawn(ctx context.Context, cfg *config.GrpcConfig, manifest *config.PluginManifest, kind string, logf func(string, ...any)) (*process, error) {
	if len(cfg.Command) == 0 {
		return nil, fmt.Errorf("rpcplugin: empty command")
	}
	if logf == nil {
		logf = nopLogf
	}
	cmd := exec.Command(cfg.Command[0], cfg.Command[1:]...)
	if len(cfg.Env) > 0 {
		env := cmd.Environ()
		for k, v := range cfg.Env {
			env = append(env, k+"="+v)
		}
		cmd.Env = env
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("rpcplugin: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("rpcplugin: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("rpcplugin: stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("rpcplugin: start %v: %w", cfg.Command, err)
	}
	p := &process{cmd: cmd, stdin: stdin, logf: logf, exited: make(chan struct{})}
	// The one and only cmd.Wait: closes exited; stop/kill/watchers wait on
	// the channel instead of racing a second Wait call.
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()

	// Drain stderr (and post-handshake stdout) so the plugin never blocks on
	// a full pipe; both surface through the logger.
	errTail := newTail()
	go func() { _, _ = io.Copy(io.MultiWriter(errTail, writerFunc(p.logfStderr)), stderr) }()
	hsLine := make(chan string, 1)
	go func() {
		r := bufio.NewReader(stdout)
		line, err := r.ReadString('\n')
		if err != nil && line == "" {
			close(hsLine)
			return
		}
		// Handshake consumed; drain the rest.
		go func() { _, _ = io.Copy(writerFunc(p.logfStdout), r) }()
		hsLine <- strings.TrimSpace(line)
	}()

	select {
	case line, ok := <-hsLine:
		if !ok || line == "" {
			p.kill()
			return nil, fmt.Errorf("rpcplugin: plugin %q closed stdout without a handshake line (stderr tail: %s)", manifest.Name, errTail.String())
		}
		var hs Handshake
		if err := json.Unmarshal([]byte(line), &hs); err != nil {
			p.kill()
			return nil, fmt.Errorf("rpcplugin: plugin %q handshake is not JSON (%q): %v", manifest.Name, line, err)
		}
		if err := checkHandshake(hs, manifest, kind); err != nil {
			p.kill()
			return nil, err
		}
		p.hs = hs
	case <-time.After(handshakeTimeout):
		p.kill()
		return nil, fmt.Errorf("rpcplugin: plugin %q did not print a handshake line within %s (stderr tail: %s)", manifest.Name, handshakeTimeout, errTail.String())
	case <-ctx.Done():
		p.kill()
		return nil, fmt.Errorf("rpcplugin: plugin %q spawn canceled: %w", manifest.Name, ctx.Err())
	}

	conn, err := grpc.NewClient(p.hs.Listen,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Explicit message caps (see MaxMessageSize): raise the client-side
		// receive/send limits above grpc-go's 4 MiB default so legal large
		// Events pass, while keeping the per-message bound defined.
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
		grpc.WithUnaryInterceptor(authInterceptor(p.hs.Auth)),
		grpc.WithStreamInterceptor(authStreamInterceptor(p.hs.Auth)))
	if err != nil {
		p.kill()
		return nil, fmt.Errorf("rpcplugin: dial %s: %w", p.hs.Listen, err)
	}
	p.conn = conn
	p.source = pluginproto.NewSourceClient(conn)
	p.sink = pluginproto.NewSinkClient(conn)
	return p, nil
}

func checkHandshake(hs Handshake, manifest *config.PluginManifest, kind string) error {
	if hs.Protocol != ProtocolVersion {
		return fmt.Errorf("rpcplugin: plugin %q speaks protocol version %d; this host speaks %d", hs.Name, hs.Protocol, ProtocolVersion)
	}
	if hs.Kind != kind {
		return fmt.Errorf("rpcplugin: plugin %q handshakes as kind %q; the node needs %q", hs.Name, hs.Kind, kind)
	}
	if hs.Name != manifest.Name {
		return fmt.Errorf("rpcplugin: plugin handshakes as %q but its manifest declares %q", hs.Name, manifest.Name)
	}
	if hs.Version != manifest.Version {
		return fmt.Errorf("rpcplugin: plugin %q handshakes version %d but its manifest declares %d", hs.Name, hs.Version, manifest.Version)
	}
	if hs.Listen == "" || hs.Auth == "" {
		return fmt.Errorf("rpcplugin: plugin %q handshake misses listen or auth", hs.Name)
	}
	for _, c := range manifest.Capabilities {
		if !contains(hs.Capabilities, c) {
			return fmt.Errorf("rpcplugin: plugin %q does not declare capability %q promised by its manifest", hs.Name, c)
		}
	}
	return nil
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// alive reports whether the OS-level process is still running.
func (p *process) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// wait blocks until the process is gone (bounded by stopGrace as a safety
// net; a killed process always exits).
func (p *process) wait() {
	select {
	case <-p.exited:
	case <-time.After(stopGrace + time.Second):
	}
}

// stop performs the documented shutdown: RPC Close is the adapter's job;
// here stdin closes (stop signal), then kill after the grace period.
func (p *process) stop() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	_ = p.stdin.Close()
	select {
	case <-p.exited:
	case <-time.After(stopGrace):
		p.kill()
		<-p.exited
	}
}

func (p *process) kill() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	p.wait()
}

func (p *process) logfStdout(s string) { p.logf("plugin stdout: %s", s) }
func (p *process) logfStderr(s string) { p.logf("plugin stderr: %s", s) }

type writerFunc func(string)

func (f writerFunc) Write(b []byte) (int, error) {
	if len(b) > 0 {
		f(strings.TrimRight(string(b), "\r\n"))
	}
	return len(b), nil
}

// tail keeps the last limit bytes written to it (for error messages).
type tail struct {
	mu    chan struct{}
	buf   []byte
	limit int
}

func newTail() *tail { return &tail{mu: make(chan struct{}, 1), limit: 4096} }

func (t *tail) Write(b []byte) (int, error) {
	t.mu <- struct{}{}
	t.buf = append(t.buf, b...)
	if len(t.buf) > t.limit {
		t.buf = t.buf[len(t.buf)-t.limit:]
	}
	<-t.mu
	return len(b), nil
}

func (t *tail) String() string {
	t.mu <- struct{}{}
	s := strings.TrimSpace(string(t.buf))
	<-t.mu
	if s == "" {
		return "(empty)"
	}
	return s
}

func authInterceptor(auth string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, "eventboat-auth", auth), method, req, reply, cc, opts...)
	}
}

func authStreamInterceptor(auth string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(metadata.AppendToOutgoingContext(ctx, "eventboat-auth", auth), desc, cc, method, opts...)
	}
}

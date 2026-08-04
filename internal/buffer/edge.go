package buffer

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/topology"
)

// EdgeInbound is a per-edge inbound buffer with optional disk backing.
type EdgeInbound struct {
	pipeline string
	from     string
	to       string
	cfg      config.EdgeBufferConfig
	mem      chan *message.Message
	out      chan *message.Message
	disk     *DiskWAL
	strategy string
	bufType  string

	mu     sync.RWMutex // Close takes the write lock; Enqueue holds the read lock across its channel sends
	closed bool
	wg     sync.WaitGroup
}

type EdgeOptions struct {
	Pipeline string
	From     string
	To       string
	Config   config.EdgeBufferConfig
}

func NewEdgeInbound(opts EdgeOptions) (*EdgeInbound, error) {
	cfg := topology.DefaultBuffer(opts.Config)
	e := &EdgeInbound{
		pipeline: opts.Pipeline,
		from:     opts.From,
		to:       opts.To,
		cfg:      cfg,
		mem:      make(chan *message.Message, cfg.Size),
		out:      make(chan *message.Message, cfg.Size),
		strategy: cfg.Strategy,
		bufType:  cfg.Type,
	}
	if cfg.Type == "disk" || cfg.Type == "overflow" {
		dir := cfg.DiskPath
		if dir == "" {
			dir = filepath.Join(defaultDiskRoot, opts.Pipeline, opts.From+"__"+opts.To)
		} else {
			dir = filepath.Join(dir, opts.Pipeline, opts.From+"__"+opts.To)
		}
		syncInterval := DefaultSyncInterval
		if cfg.DiskSyncInterval != "" {
			if d, err := time.ParseDuration(cfg.DiskSyncInterval); err == nil {
				syncInterval = d
			}
		}
		disk, err := NewDiskWAL(DiskOptions{
			Dir:          dir,
			MaxSize:      cfg.DiskMaxSize,
			SyncInterval: syncInterval,
		})
		if err != nil {
			return nil, err
		}
		e.disk = disk
	}
	return e, nil
}

func (e *EdgeInbound) Start(ctx context.Context) {
	e.wg.Add(1)
	go e.drainLoop(ctx)
}

func (e *EdgeInbound) Out() <-chan *message.Message {
	return e.out
}

func (e *EdgeInbound) Len() int {
	return len(e.mem)
}

func (e *EdgeInbound) DiskBytes() int64 {
	if e.disk == nil {
		return 0
	}
	return e.disk.SizeBytes()
}

func (e *EdgeInbound) Enqueue(ctx context.Context, msg *message.Message) (dropped bool, reason string, err error) {
	// The read lock is held across the whole enqueue: Close closes e.mem
	// under the write lock, so a send can never race with channel close.
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		msg.Ack(context.Canceled)
		return false, "", context.Canceled
	}

	switch e.bufType {
	case "disk":
		if e.disk == nil {
			d, r := sendToMemory(ctx, e.mem, msg, e.strategy)
			return d, r, nil
		}
		if err := e.disk.Append(msg); err != nil {
			return e.handleDiskFull(ctx, msg, err)
		}
		return false, "", nil
	default:
		if e.bufType == "overflow" && e.disk != nil && len(e.mem) >= cap(e.mem) {
			if err := e.disk.Append(msg); err != nil {
				return e.handleDiskFull(ctx, msg, err)
			}
			return false, "overflow_spill", nil
		}
		select {
		case e.mem <- msg:
			return false, "", nil
		default:
		}
		if e.bufType == "overflow" && e.disk != nil {
			if err := e.disk.Append(msg); err != nil {
				return e.handleDiskFull(ctx, msg, err)
			}
			return false, "overflow_spill", nil
		}
		d, r := sendToMemory(ctx, e.mem, msg, e.strategy)
		return d, r, nil
	}
}

func (e *EdgeInbound) handleDiskFull(ctx context.Context, msg *message.Message, diskErr error) (bool, string, error) {
	switch e.strategy {
	case "drop_newest":
		msg.Ack(nil)
		return true, "disk_full_drop_newest", nil
	case "drop_oldest":
		if old, err := e.disk.ReadNext(); err == nil && old != nil {
			old.Ack(nil)
		}
		if err := e.disk.Append(msg); err != nil {
			msg.Ack(diskErr)
			return true, "disk_full", err
		}
		return true, "disk_full_drop_oldest", nil
	default:
		for {
			select {
			case <-ctx.Done():
				msg.Ack(ctx.Err())
				return false, "", ctx.Err()
			case <-time.After(5 * time.Millisecond):
				if err := e.disk.Append(msg); err == nil {
					return false, "", nil
				}
			}
		}
	}
}

func (e *EdgeInbound) drainLoop(ctx context.Context) {
	defer e.wg.Done()
	defer close(e.out)
	defer e.nackRemainingMem(ctx)
	var diskWake <-chan struct{}
	if e.disk != nil {
		diskWake = e.disk.WakeCh()
	}
	for {
		if ctx.Err() != nil && e.shouldStop(ctx) {
			return
		}
		if e.disk != nil {
			msg, err := e.disk.ReadNext()
			if err != nil {
				return
			}
			if msg != nil {
				if !e.emit(ctx, msg) {
					return
				}
				continue
			}
		}
		select {
		case <-ctx.Done():
			if e.shouldStop(ctx) {
				return
			}
		case <-diskWake:
			// a record spilled to disk while idle; loop back to ReadNext
		case msg, ok := <-e.mem:
			if !ok {
				return
			}
			if !e.emit(ctx, msg) {
				return
			}
		}
	}
}

// nackRemainingMem nacks messages still sitting in the mem channel after
// the drain loop stopped, so sources always see a terminal ack callback.
func (e *EdgeInbound) nackRemainingMem(ctx context.Context) {
	err := ctx.Err()
	if err == nil {
		err = context.Canceled
	}
	for {
		select {
		case msg, ok := <-e.mem:
			if !ok {
				return
			}
			msg.Ack(err)
		default:
			return
		}
	}
}

func (e *EdgeInbound) shouldStop(ctx context.Context) bool {
	if ctx.Err() == nil {
		return false
	}
	if len(e.mem) > 0 {
		return false
	}
	if e.disk != nil && e.disk.Pending() {
		return false
	}
	return true
}

func (e *EdgeInbound) emit(ctx context.Context, msg *message.Message) bool {
	select {
	case <-ctx.Done():
		msg.Ack(ctx.Err())
		return false
	case e.out <- msg:
		return true
	}
}

func (e *EdgeInbound) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.mem)
	e.mu.Unlock()
	e.wg.Wait()
	// the drain loop may have exited before e.mem was closed; make sure no
	// message is left without a terminal ack
	e.nackRemainingMem(context.Background())
	if e.disk != nil {
		return e.disk.Close()
	}
	return nil
}

func sendToMemory(ctx context.Context, ch chan *message.Message, msg *message.Message, strategy string) (dropped bool, reason string) {
	switch strategy {
	case "drop_newest":
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		default:
			msg.Ack(nil)
			return true, "drop_newest"
		}
	case "drop_oldest":
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		default:
			select {
			case old := <-ch:
				old.Ack(nil)
				reason = "drop_oldest"
			default:
			}
			select {
			case <-ctx.Done():
				msg.Ack(ctx.Err())
			case ch <- msg:
			}
			if reason != "" {
				return true, reason
			}
		}
	default:
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		}
	}
	return false, ""
}

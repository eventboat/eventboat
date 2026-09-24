package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Lease is the exclusive right to write one pipeline's store. A running
// engine holds it for the whole run; Release unlocks and closes the
// underlying handle and is idempotent. The OS releases the lock when the
// process exits — crashed or not — so there is no TTL heartbeat to maintain
// and a killed process never leaves a stale lease behind.
type Lease interface {
	Release() error
}

// Leaser hands out a pipeline's cross-process store lease (candidate 08,
// amending candidate 04's one-owner layout): the process that runs the
// pipeline is the single writer of its store, and a second process refuses
// instead of racing the spool. Owner implements it; a memory owner returns a
// no-op lease because in-memory stores cannot cross processes.
type Leaser interface {
	Acquire(pipeline string) (Lease, error)
}

// LeaseBusyError reports that another process already holds the pipeline's
// store lease. The store layer only names the conflict; surfaces wrap it
// with the guidance that fits them (the CLI points at admin/MCP).
type LeaseBusyError struct {
	Pipeline string
	Path     string
	Err      error
}

func (e *LeaseBusyError) Error() string {
	return fmt.Sprintf("store lease %s (pipeline %q) is held by another process: %v", e.Path, e.Pipeline, e.Err)
}

func (e *LeaseBusyError) Unwrap() error { return e.Err }

// errLockBusy is the platform lock helpers' signal that a live process holds
// the lock, as opposed to a locking failure (unsupported platform, I/O).
var errLockBusy = errors.New("file is locked by another process")

// noopLease is the memory owner's lease: one process cannot share its
// in-memory store, so single-process tests and --ephemeral runs are
// unaffected by the single-writer rule.
type noopLease struct{}

func (noopLease) Release() error { return nil }

// fileLease holds the exclusive OS file lock on a sidecar next to the
// canonical store path (<store>.lock). A sidecar keeps SQLite's own locking
// untouched. The sidecar file is never deleted: unlinking a lock file while
// another process may open it is the classic race that lands two processes
// on different inodes under the same name — an unlocked leftover file is
// expected and harmless.
type fileLease struct {
	mu sync.Mutex
	f  *os.File
}

func (l *fileLease) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	// Closing the handle releases the lock (both flock and LockFileEx).
	return f.Close()
}

// acquireFileLease opens (creating if needed) the pipeline's sidecar lock
// file and takes the non-blocking exclusive OS lock. The returned lease owns
// the open handle; the lock dies with the process.
func acquireFileLease(pipeline, storePath string) (Lease, error) {
	lockPath := storePath + ".lock"
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("store lease: create lock dir: %w", err)
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store lease: open %s: %w", lockPath, err)
	}
	if err := lockFileExclusive(f); err != nil {
		_ = f.Close()
		if errors.Is(err, errLockBusy) {
			return nil, &LeaseBusyError{Pipeline: pipeline, Path: lockPath, Err: err}
		}
		return nil, fmt.Errorf("store lease: lock %s: %w", lockPath, err)
	}
	return &fileLease{f: f}, nil
}

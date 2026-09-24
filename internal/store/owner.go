package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/eventboat/eventboat/internal/fsname"
)

// Provider returns the durable store for one pipeline. Callers (ops, tests)
// depend on this interface, never on the layout or the concrete owner.
type Provider interface {
	Open(pipeline string) (Store, error)
}

// Owner decides where a pipeline's durable store lives and owns its handle
// lifetime: one handle per pipeline per process, cached, closed together.
//
// Layout: every entry point (the one-shot verbs and the daemon) uses the same
// canonical file, dataDir/stores/<sanitized pipeline>.db, so a run history
// written by `trigger` is the one `jobs list` and the daemon's Status read.
// The old layouts (eventboat.db, stores/pipeline.db) are retired without
// migration — beta ruling: the old files are simply no longer read.
//
// The owner also sanitizes the pipeline name and rejects Windows reserved
// device names, because the name becomes a file base name here; both rules
// come from the shared leaf internal/fsname, the same ones the loader's
// metadata.name validation applies.
//
// The memory owner (NewMemoryOwner) returns cached per-pipeline in-memory
// stores. It is what tests and --ephemeral use, so every surface of one
// process sees the same data instead of a fresh store per Open.
//
// Owner implements Provider. Path exposes the on-disk file (stage 08's
// cross-process lease locks exactly this file); a memory owner has none.
type Owner struct {
	dataDir string
	memory  bool

	mu      sync.Mutex
	handles map[string]Store
	closed  bool
}

// NewOwner returns the owner of the durable stores under dataDir. Handles are
// opened lazily on first use and cached for the process's lifetime.
func NewOwner(dataDir string) *Owner {
	return &Owner{dataDir: dataDir, handles: map[string]Store{}}
}

// NewMemoryOwner returns an owner of cached per-pipeline in-memory stores:
// the --ephemeral and test substitution for NewOwner, with the same
// one-handle-per-pipeline contract.
func NewMemoryOwner() *Owner {
	return &Owner{memory: true, handles: map[string]Store{}}
}

// Open returns the pipeline's store, opening and caching it on first use.
// Repeated calls return the same handle. After Close, Open fails.
func (o *Owner) Open(pipeline string) (Store, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, fmt.Errorf("store owner: closed")
	}
	if st, ok := o.handles[pipeline]; ok {
		return st, nil
	}
	st, err := o.open(pipeline)
	if err != nil {
		return nil, err
	}
	o.handles[pipeline] = st
	return st, nil
}

// Path returns the on-disk location of the pipeline's store without opening
// it. The path is deterministic from the data dir and the pipeline name, so
// it is available before (and without) a handle.
func (o *Owner) Path(pipeline string) (string, error) {
	if o.memory {
		return "", fmt.Errorf("store owner: memory owner has no on-disk path")
	}
	return o.path(pipeline)
}

// OpenHandles reports how many pipeline handles are currently cached
// (diagnostics and handle-lifetime tests: one handle per pipeline per
// process).
func (o *Owner) OpenHandles() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.handles)
}

// Close closes every cached handle and marks the owner closed. It is
// idempotent: a second call is a no-op returning nil.
func (o *Owner) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	var first error
	for name, st := range o.handles {
		if err := st.Close(); err != nil && first == nil {
			first = fmt.Errorf("store owner: close %q: %w", name, err)
		}
	}
	o.handles = map[string]Store{}
	return first
}

func (o *Owner) open(pipeline string) (Store, error) {
	if o.memory {
		return NewMemory(), nil
	}
	path, err := o.path(pipeline)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store owner: create stores dir: %w", err)
	}
	return OpenSQLite(path)
}

// path is the canonical on-disk location: <dataDir>/stores/<sanitized>.db.
// The pipeline name flows into a file base name, so it is sanitized to the
// conservative charset and checked against the Windows reserved device names
// — a sanitized name can still spell CON, and CON.db targets the console,
// not a file.
func (o *Owner) path(pipeline string) (string, error) {
	name := fsname.Sanitize(pipeline)
	if name == "" {
		return "", fmt.Errorf("store owner: pipeline %q has an empty store name", pipeline)
	}
	if fsname.WindowsReservedName(name) {
		return "", fmt.Errorf("store owner: pipeline %q: store name %q is a Windows reserved device name", pipeline, name)
	}
	return filepath.Join(o.dataDir, "stores", name+".db"), nil
}

package cli

import (
	"fmt"

	"github.com/eventboat/eventboat/internal/runtimecfg"
	"github.com/eventboat/eventboat/internal/store"
)

// newStoreOwner builds the process-wide store owner for a deployment
// (candidate 04): the canonical per-pipeline layout under storage.data_dir,
// or the cached in-memory owner under storage.ephemeral. Every entry point —
// the daemon and each one-shot verb — builds exactly one owner and closes it
// on shutdown, so all of a process's surfaces share one handle per pipeline
// and every entry point reads the same file.
func newStoreOwner(storage runtimecfg.Storage) *store.Owner {
	if storage.Ephemeral {
		return store.NewMemoryOwner()
	}
	return store.NewOwner(storage.DataDir)
}

// acquireRunLease takes the pipeline's cross-process store lease for a
// write-capable one-shot verb (run --config, trigger, replay). A verb that
// cannot take it must refuse — with a message that names the conflict and
// points at the admin/MCP surface — rather than start a second engine on the
// same spool (candidate 08: one writer per pipeline store).
func acquireRunLease(owner *store.Owner, verb, pipeline string) (store.Lease, error) {
	lease, err := owner.Acquire(pipeline)
	if err == nil {
		return lease, nil
	}
	return nil, fmt.Errorf("%s: pipeline %q: %w; refusing to start a second engine on the same spool — if the pipeline is deployed in a daemon, use its admin/MCP surface instead (`trigger` for job runs, `dlq_replay` for dead letters)", verb, pipeline, err)
}

// storeDesc names the store a run is using for the startup line: the
// canonical path, or the in-memory owner.
func storeDesc(owner *store.Owner, ephemeral bool, pipeline string) string {
	if ephemeral {
		return "ephemeral (in-memory)"
	}
	path, _ := owner.Path(pipeline)
	return path + " (SQLite, WAL)"
}

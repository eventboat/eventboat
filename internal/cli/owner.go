package cli

import (
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

// storeDesc names the store a run is using for the startup line: the
// canonical path, or the in-memory owner.
func storeDesc(owner *store.Owner, ephemeral bool, pipeline string) string {
	if ephemeral {
		return "ephemeral (in-memory)"
	}
	path, _ := owner.Path(pipeline)
	return path + " (SQLite, WAL)"
}

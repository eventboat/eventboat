// Module cache: verify compiles every wasm node it meets (module existence
// and ABI exports are gate-1 findings, not first-message failures), and the
// LSP re-verifies a document on every keystroke — compiling the same
// unchanged guest over and over. Compile therefore keeps a small, bounded
// cache of compiled modules keyed by file identity (path/size/mtime) plus
// the compile-affecting config bits (memory cap, kill switch): a changed file
// or a changed compile-affecting config produces a new key and recompiles;
// everything else reuses the compiled module.
//
// The cache holds one reference per entry and Compiled is reference-counted,
// so evicting an entry that a live invoker still uses is safe — the wazero
// runtime closes when the last reference goes away (candidate 09).
package wasmhost

import (
	"os"
	"sync"
)

// moduleCacheMax bounds the cache: a document family has a handful of wasm
// nodes, and eviction is only a recompile.
const moduleCacheMax = 16

// cacheKey is the compile-affecting identity of one module: the file identity
// (path/size/mtime) plus the two config bits Compile passes to wazero
// (memory-pages cap, CloseOnContextDone kill-switch instrumentation).
type cacheKey struct {
	path       string
	size       int64
	modTimeNs  int64
	pages      uint32
	killSwitch bool
}

type cacheEntry struct {
	compiled *Compiled
	used     uint64 // logical clock for LRU eviction
}

var moduleCache = struct {
	mu      sync.Mutex
	entries map[cacheKey]*cacheEntry
	clock   uint64
	hits    int
	misses  int
}{entries: map[cacheKey]*cacheEntry{}}

// Stats reports the module-cache counters and the current entry count.
// Tests pin the LSP keystroke behavior (a repeated verify hits) and the
// invalidation rules (a changed file or config recompiles) with it.
type Stats struct {
	Hits    int
	Misses  int
	Entries int
}

// CacheStats returns a snapshot of the module cache.
func CacheStats() Stats {
	moduleCache.mu.Lock()
	defer moduleCache.mu.Unlock()
	return Stats{Hits: moduleCache.hits, Misses: moduleCache.misses, Entries: len(moduleCache.entries)}
}

// ResetCache empties the module cache (dropping its references; live callers
// keep their compiled modules alive). It exists for tests that need a clean
// baseline; production code never calls it.
func ResetCache() {
	moduleCache.mu.Lock()
	entries := moduleCache.entries
	moduleCache.entries = map[cacheKey]*cacheEntry{}
	moduleCache.clock = 0
	moduleCache.hits, moduleCache.misses = 0, 0
	moduleCache.mu.Unlock()
	for _, e := range entries {
		e.compiled.release()
	}
}

// cacheKeyFor derives the key from the file's identity and the resolved
// compile-affecting config. A missing file is the same typed read error
// Compile reports.
func cacheKeyFor(path string, cfg *Config) (cacheKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return cacheKey{}, kindError(KindCompile, "wasmhost: read module: %w", err)
	}
	return cacheKey{
		path:       path,
		size:       info.Size(),
		modTimeNs:  info.ModTime().UnixNano(),
		pages:      resolvePages(cfg),
		killSwitch: !ResolveMode(cfg).Fast(),
	}, nil
}

// cacheLookup returns a retained handle on a cached module (the caller's
// reference) or nil on a miss.
func cacheLookup(key cacheKey) *Compiled {
	moduleCache.mu.Lock()
	defer moduleCache.mu.Unlock()
	e, ok := moduleCache.entries[key]
	if !ok {
		moduleCache.misses++
		return nil
	}
	moduleCache.hits++
	moduleCache.clock++
	e.used = moduleCache.clock
	e.compiled.retain()
	return e.compiled
}

// cacheStore inserts one freshly compiled module, replacing a same-key entry
// (a concurrent miss) and evicting the least-recently-used entry when the
// cache is full. The cache takes its own reference; the caller's reference is
// untouched. Replaced/evicted entries drop the cache's reference outside the
// lock.
func cacheStore(key cacheKey, c *Compiled) {
	moduleCache.mu.Lock()
	moduleCache.clock++
	c.retain()
	prev := moduleCache.entries[key]
	moduleCache.entries[key] = &cacheEntry{compiled: c, used: moduleCache.clock}
	var evicted *Compiled
	for len(moduleCache.entries) > moduleCacheMax {
		var lruKey cacheKey
		var lru *cacheEntry
		for k, e := range moduleCache.entries {
			if lru == nil || e.used < lru.used {
				lruKey, lru = k, e
			}
		}
		delete(moduleCache.entries, lruKey)
		evicted = lru.compiled
	}
	moduleCache.mu.Unlock()
	if prev != nil && prev.compiled != c {
		prev.compiled.release()
	}
	if evicted != nil {
		evicted.release()
	}
}

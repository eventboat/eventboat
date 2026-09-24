package wasmhost

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// copyGuest copies the testdata guest to a unique temp path so cache tests
// own their keys.
func copyGuest(t *testing.T) string {
	t.Helper()
	src := skipIfGuestMissing(t)
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "guest.wasm")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// Candidate 09 acceptance 4: the compiled module is cached by file identity
// and compile-affecting config; an unchanged module is reused.
func TestCompileCacheReusesUnchangedModule(t *testing.T) {
	path := copyGuest(t)
	ctx := context.Background()

	first, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := CacheStats()

	second, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(ctx) }()
	after := CacheStats()

	if second != first {
		t.Error("unchanged module must reuse the cached compiled artifact")
	}
	if after.Hits != before.Hits+1 || after.Misses != before.Misses {
		t.Errorf("second compile: hits %d->%d, misses %d->%d; want one hit and no miss",
			before.Hits, after.Hits, before.Misses, after.Misses)
	}
}

// A changed file recompiles: the cache key covers the file identity.
func TestCompileCacheInvalidatesOnFileChange(t *testing.T) {
	path := copyGuest(t)
	ctx := context.Background()

	first, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close(ctx)

	// Append one byte: the module is now corrupt, so a recompile must fail
	// (a cache hit would have returned the stale compiled artifact).
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := Compile(ctx, path, nil); err == nil {
		t.Fatal("changed module must recompile (and fail on the corrupt bytes)")
	}

	// Restore the original content: it is a new file identity again, so the
	// artifact must be a fresh one, not the pre-change pointer.
	data, err := os.ReadFile(skipIfGuestMissing(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	restored, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close(ctx) }()
	if restored == first {
		t.Error("restored file must not reuse the pre-change artifact")
	}
}

// The cache separates compile-affecting config (memory cap, kill switch) and
// shares everything else (entrypoint, allow).
func TestCompileCacheSeparatesCompileAffectingConfig(t *testing.T) {
	path := copyGuest(t)
	ctx := context.Background()

	base, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Close(ctx) }()

	entry, err := Compile(ctx, path, &Config{Entrypoint: "other", Allow: []string{"log"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = entry.Close(ctx) }()
	if entry != base {
		t.Error("entrypoint/allow are not compile-affecting; the cache must share the artifact")
	}

	killed, err := Compile(ctx, path, &Config{TimeoutMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = killed.Close(ctx) }()
	if killed == base {
		t.Error("the kill switch changes compilation; the cache must not share")
	}

	pages, err := Compile(ctx, path, &Config{MaxMemoryPages: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pages.Close(ctx) }()
	if pages == base {
		t.Error("the memory cap is compile-affecting; the cache must not share")
	}
}

// Concurrent compiles of the same module are safe (the -race gate covers the
// data race; the assertion covers the reference counting).
func TestCompileCacheConcurrent(t *testing.T) {
	path := copyGuest(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Compile(context.Background(), path, nil)
			if err != nil {
				t.Error(err)
				return
			}
			if err := c.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

// Eviction drops only the cache's reference: a live caller keeps its module
// usable (the reference count is what makes bounded eviction safe). The test
// performs the eviction the way cacheStore does when the cache is full, so
// it needs one compile instead of moduleCacheMax+1.
func TestCompileCacheEvictionKeepsLiveModule(t *testing.T) {
	path := copyGuest(t)
	ctx := context.Background()
	c, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := cacheKeyFor(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	moduleCache.mu.Lock()
	entry, ok := moduleCache.entries[key]
	if ok {
		delete(moduleCache.entries, key)
	}
	moduleCache.mu.Unlock()
	if !ok {
		t.Fatal("compiled module missing from the cache")
	}
	entry.compiled.release() // exactly what an eviction does

	// The caller's reference must keep the runtime alive.
	inv := c.NewInvoker(nil, nil, 0)
	defer func() { _ = inv.Close() }()
	if _, err := inv.Invoke(ctx, []byte(`{"samples":[1,2,3]}`)); err != nil {
		t.Fatalf("evicted-but-held module unusable: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// ResetCache empties the cache (the documented test hook); live handles keep
// their reference.
func TestResetCacheEmpties(t *testing.T) {
	path := copyGuest(t)
	ctx := context.Background()
	c, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if CacheStats().Entries == 0 {
		t.Fatal("expected a cached entry")
	}
	ResetCache()
	if got := CacheStats().Entries; got != 0 {
		t.Fatalf("entries after ResetCache = %d, want 0", got)
	}
	inv := c.NewInvoker(nil, nil, 0)
	defer func() { _ = inv.Close() }()
	if _, err := inv.Invoke(ctx, []byte(`{"samples":[1,2,3]}`)); err != nil {
		t.Fatalf("handle invalidated by ResetCache: %v", err)
	}
}

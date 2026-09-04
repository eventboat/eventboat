package starhost

import "testing"

// remove() is the del() migration glue (redesign-v3.md §4.8): it must work
// on the lazy root (marking the state dirty so the engine writes the
// deletion back), on materialized trees, and be a no-op for missing keys.
func TestRemoveOnLazyRoot(t *testing.T) {
	ps, _, serr := runScript(t, `remove(payload, "temp")`,
		map[string]any{"temp": "x", "keep": 1}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if !ps.Dirty() {
		t.Fatal("remove on the lazy root must dirty the state (write-back gate)")
	}
	got := ps.GoValue().(map[string]any)
	if _, exists := got["temp"]; exists {
		t.Fatalf("temp not removed: %v", got)
	}
	if got["keep"] != 1 {
		t.Fatalf("keep lost: %v", got)
	}
}

func TestRemoveOnMetaRoot(t *testing.T) {
	_, ms, serr := runScript(t, `remove(meta, "tag")`, map[string]any{}, map[string]any{"tag": "a", "other": "b"})
	if serr != nil {
		t.Fatal(serr)
	}
	got := ms.GoValue().(map[string]any)
	if _, exists := got["tag"]; exists {
		t.Fatalf("tag not removed: %v", got)
	}
	if got["other"] != "b" {
		t.Fatalf("other lost: %v", got)
	}
}

func TestRemoveAfterMaterialize(t *testing.T) {
	// A prior write materializes the COW tree; remove must still work.
	ps, _, serr := runScript(t, `
payload.extra = 1
remove(payload, "temp")
`, map[string]any{"temp": "x"}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	got := ps.GoValue().(map[string]any)
	if _, exists := got["temp"]; exists {
		t.Fatalf("temp not removed from materialized tree: %v", got)
	}
	if got["extra"] != int64(1) {
		t.Fatalf("extra lost: %v", got)
	}
}

func TestRemoveNestedDict(t *testing.T) {
	// remove(payload.a, "b") — nested attrDict target (the del(payload.a.b)
	// conversion shape).
	ps, _, serr := runScript(t, `remove(payload.a, "b")`,
		map[string]any{"a": map[string]any{"b": 1, "c": 2}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	got := ps.GoValue().(map[string]any)
	a := got["a"].(map[string]any)
	if _, exists := a["b"]; exists {
		t.Fatalf("b not removed: %v", a)
	}
	if a["c"] != int64(2) {
		t.Fatalf("c lost: %v", a)
	}
}

func TestRemoveMissingKeyIsNoop(t *testing.T) {
	ps, _, serr := runScript(t, `remove(payload, "nope")`, map[string]any{"keep": 1}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	// Deleting an absent key from the lazy root: dirty is acceptable (the
	// deletion is a write attempt); the value must be untouched (still the
	// original Go value, not a materialized roundtrip).
	if ps.GoValue().(map[string]any)["keep"] != 1 {
		t.Fatal("keep lost")
	}
}

func TestRemoveRejectsNonMapping(t *testing.T) {
	_, _, serr := runScript(t, `remove(42, "x")`, map[string]any{}, map[string]any{})
	if serr == nil {
		t.Fatal("expected error for non-mapping first argument")
	}
}

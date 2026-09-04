package starhost

import "testing"

// Regression tests for nested-container reference semantics: reading a
// container field and mutating through it must affect the payload. These
// lock the fix for the silent-loss bug where fieldGet served fresh (copied)
// container values whose writes never propagated back (found while building
// the v2 del() conversion, redesign-v3.md §4.8).

func TestNestedFieldWritePropagates(t *testing.T) {
	ps, _, serr := runScript(t, `payload.nested.k = "changed"`,
		map[string]any{"nested": map[string]any{"k": "v"}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	nested := ps.GoValue().(map[string]any)["nested"].(map[string]any)
	if nested["k"] != "changed" {
		t.Fatalf("nested write lost: %v", nested)
	}
	if !ps.Dirty() {
		t.Fatal("a script that wrote a nested field must dirty the state")
	}
}

func TestNestedFieldWriteAfterOtherRead(t *testing.T) {
	ps, _, serr := runScript(t, `
x = payload.nested.k
payload.nested.k = "changed"
`, map[string]any{"nested": map[string]any{"k": "v"}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	nested := ps.GoValue().(map[string]any)["nested"].(map[string]any)
	if nested["k"] != "changed" {
		t.Fatalf("nested write lost after read: %v", nested)
	}
}

func TestNestedIndexWritePropagates(t *testing.T) {
	ps, _, serr := runScript(t, `payload["nested"]["k"] = 42`,
		map[string]any{"nested": map[string]any{"k": "v"}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	nested := ps.GoValue().(map[string]any)["nested"].(map[string]any)
	if nested["k"] != int64(42) {
		t.Fatalf("nested index write lost: %v", nested)
	}
}

func TestListElementWritePropagates(t *testing.T) {
	// `items` shadows a Starlark dict method name, so attribute access
	// resolves the method; index access reaches the field (dict semantics).
	ps, _, serr := runScript(t, `payload["items"][0] = 99`,
		map[string]any{"items": []any{1, 2}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	items := ps.GoValue().(map[string]any)["items"].([]any)
	if items[0] != int64(99) {
		t.Fatalf("list element write lost: %v", items)
	}
}

func TestListAppendPropagates(t *testing.T) {
	ps, _, serr := runScript(t, `payload["tags"].append(3)`,
		map[string]any{"tags": []any{1, 2}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	tags := ps.GoValue().(map[string]any)["tags"].([]any)
	if len(tags) != 3 || tags[2] != int64(3) {
		t.Fatalf("append lost: %v", tags)
	}
}

func TestScalarReadsStayLazy(t *testing.T) {
	// Reading scalar fields must not materialize (the COW fast path for
	// flat payloads — redesign-v3.md §4.3's headline optimization).
	ps, _, serr := runScript(t, `x = payload.price * 2`, map[string]any{"price": 3.0}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if ps.Dirty() {
		t.Fatal("scalar-only reads must stay lazy")
	}
}

func TestCOWStillProtectsOriginal(t *testing.T) {
	original := map[string]any{"a": 1.0, "nested": map[string]any{"k": "v"}}
	ps, _, serr := runScript(t, `
payload.a = 99
payload.nested.k = "changed"
`, original, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if original["a"] != 1.0 || original["nested"].(map[string]any)["k"] != "v" {
		t.Fatalf("original payload mutated: %v", original)
	}
	got := ps.GoValue().(map[string]any)
	if got["a"] != int64(99) || got["nested"].(map[string]any)["k"] != "changed" {
		t.Fatalf("writes lost: %v", got)
	}
}

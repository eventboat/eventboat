package starhost

import (
	"reflect"
	"testing"
)

// Precise dirty tracking (beta hardening): reading through containers must
// not dirty the binding — the engine skips the whole-tree StarlarkToGo
// write-back for clean states. Map-tree writes mark precisely through the
// conversion marker; list-bearing trees dirty conservatively (native list
// mutators cannot be intercepted — the boundary is documented and locked
// here so a future list wrapper flips exactly one test).

func TestContainerReadsStayLazy(t *testing.T) {
	original := map[string]any{
		"user": map[string]any{
			"id":      "u-1",
			"profile": map[string]any{"email": "a@b.example", "tier": "gold"},
		},
		"region": "eu",
	}
	ps, _, serr := runScript(t, `
x = payload.user.profile.email
y = payload.user.profile.tier
z = payload.user.id
w = payload.region
for k in payload.user:
    _ = k
`, original, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if ps.Dirty() {
		t.Fatal("read-only container access must not dirty the state (precise dirty tracking)")
	}
	if !reflect.DeepEqual(ps.GoValue(), original) {
		t.Fatalf("clean state must return the ORIGINAL decoded value, got %v", ps.GoValue())
	}
}

func TestNestedWriteOnListFreeTreeMarksPrecisely(t *testing.T) {
	ps, _, serr := runScript(t, `
a = payload.nested.k          # read first: materializes the tree, stays clean
payload.nested.k = "changed"  # now a precise write through the marker
`, map[string]any{"nested": map[string]any{"k": "v", "deep": map[string]any{"x": 1.0}}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if !ps.Dirty() {
		t.Fatal("nested write on a list-free tree must mark dirty precisely")
	}
	nested := ps.GoValue().(map[string]any)["nested"].(map[string]any)
	if nested["k"] != "changed" {
		t.Fatalf("nested write lost: %v", nested)
	}
}

func TestDictBuiltinMutationsMarkDirty(t *testing.T) {
	cases := []struct {
		name   string
		script string
	}{
		{"update", `payload.nested.update({"k": 2.0})`},
		{"pop", `payload.nested.pop("k")`},
		{"setdefault", `payload.nested.setdefault("k", 9.0)`},
		{"clear", `payload.other.clear()`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps, _, serr := runScript(t, tc.script,
				map[string]any{
					"nested": map[string]any{"k": 1.0},
					"other":  map[string]any{"a": 1.0},
				}, map[string]any{})
			if serr != nil {
				t.Fatal(serr)
			}
			if !ps.Dirty() {
				t.Fatalf("%s must mark the state dirty", tc.name)
			}
		})
	}
}

func TestRemoveOnNestedDictMarksDirty(t *testing.T) {
	ps, _, serr := runScript(t, `remove(payload.nested, "k")`,
		map[string]any{"nested": map[string]any{"k": "v", "keep": 1.0}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if !ps.Dirty() {
		t.Fatal("remove() on a nested dict must mark the state dirty")
	}
	nested := ps.GoValue().(map[string]any)["nested"].(map[string]any)
	if _, exists := nested["k"]; exists {
		t.Fatalf("remove lost: %v", nested)
	}
}

// TestListTreeStaysConservativeDirty locks the boundary: any list anywhere
// in the tree keeps today's conservative write-back, because native
// *starlark.List mutators (append, index writes) bypass interception.
func TestListTreeStaysConservativeDirty(t *testing.T) {
	// Index access reaches the field: `payload.items` resolves the dict
	// method (method names shadow fields, see nestedwrite_test.go).
	ps, _, serr := runScript(t, `x = payload["items"][0]`,
		map[string]any{"items": []any{1.0, 2.0}}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if !ps.Dirty() {
		t.Fatal("list-bearing trees must dirty conservatively (uninterceptable list mutators)")
	}
	// And the read must still see the value (reference semantics intact).
	if got := ps.GoValue().(map[string]any)["items"].([]any)[0]; got != 1.0 {
		t.Fatalf("read lost: %v", got)
	}
}

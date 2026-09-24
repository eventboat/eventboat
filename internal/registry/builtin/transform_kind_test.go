package builtin

import (
	"errors"
	"testing"

	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

// Candidate 07 acceptance 2: the plugin adapters map the host's typed kinds
// onto the registry kind enum. This is the ONLY translation step between the
// hosts and the engine; no message text participates.
func TestAdapterKindMapping(t *testing.T) {
	for _, tc := range []struct {
		host starhost.Kind
		want registry.FailureKind
	}{
		{starhost.KindCompile, registry.FailureCompile},
		{starhost.KindSteps, registry.FailureSteps},
		{starhost.KindRuntime, registry.FailureRuntime},
		{"", registry.FailureRuntime}, // untyped host failure: runtime, never a guess
	} {
		if got := scriptKind(tc.host); got != tc.want {
			t.Errorf("scriptKind(%q) = %q, want %q", tc.host, got, tc.want)
		}
	}
	for _, tc := range []struct {
		host wasmhost.Kind
		want registry.FailureKind
	}{
		{wasmhost.KindCompile, registry.FailureCompile},
		{wasmhost.KindTimeout, registry.FailureTimeout},
		{wasmhost.KindGuest, registry.FailureGuest},
		{wasmhost.KindTrap, registry.FailureRuntime},
		{"", registry.FailureRuntime},
	} {
		if got := wasmKind(tc.host); got != tc.want {
			t.Errorf("wasmKind(%q) = %q, want %q", tc.host, got, tc.want)
		}
	}
}

// Candidate 07 acceptance 5 (false-positive regression): a guest error whose
// text contains "exceeded" is a guest failure, never a wasm timeout. The old
// adapter ran strings.Contains(err.Error(), "exceeded") and flagged a timeout.
func TestGuestExceededTextIsNotTimeout(t *testing.T) {
	guest := &wasmhost.Error{Kind: wasmhost.KindGuest, Err: errors.New("sample count exceeded the guest's limit")}
	if got := wasmKindOf(guest); got != registry.FailureGuest {
		t.Fatalf("guest text classified as %q, want %q", got, registry.FailureGuest)
	}
	if got := wasmKindOf(guest); got == registry.FailureTimeout {
		t.Fatal("message text must never produce a timeout kind")
	}
	// The real timeout kind still maps to timeout.
	timeout := &wasmhost.Error{Kind: wasmhost.KindTimeout, Err: errors.New("wasm: transform exceeded 100ms (per-invoke budget)")}
	if got := wasmKindOf(timeout); got != registry.FailureTimeout {
		t.Fatalf("timeout kind = %q, want %q", got, registry.FailureTimeout)
	}
	// An untyped error (a third-party wrapper) is a runtime failure, not a
	// text-derived class.
	if got := wasmKindOf(errors.New("something exceeded")); got != registry.FailureRuntime {
		t.Fatalf("untyped error kind = %q, want %q", got, registry.FailureRuntime)
	}
}

// The script adapter carries the host kind end to end: compile at the factory,
// steps/runtime from Apply.
func TestScriptAdapterKinds(t *testing.T) {
	reg := newReg(t)

	// compile: the factory failure keeps its diagnostic and is typed.
	_, err := reg.NewTransform("script", `payload.x = nosuchname`, "")
	var te *registry.TransformError
	if !errors.As(err, &te) {
		t.Fatalf("compile failure is not a *TransformError: %T (%v)", err, err)
	}
	if te.Kind != registry.FailureCompile {
		t.Errorf("compile kind = %q, want %q", te.Kind, registry.FailureCompile)
	}
	if te.DiagCode != "expr_starlark_compile" {
		t.Errorf("compile diag = %q", te.DiagCode)
	}

	// steps: a runaway script is typed at the budget.
	tr, err := reg.NewTransform("script", `
for i in range(1000000):
    payload.x = i
`, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Init(&registry.TransformEnv{}); err != nil {
		t.Fatal(err)
	}
	_, aerr := tr.Apply(&registry.Message{Decoded: map[string]any{}, Meta: map[string]any{}})
	if !errors.As(aerr, &te) || te.Kind != registry.FailureSteps {
		t.Errorf("steps kind = %v (%v), want %q", aerr, te, registry.FailureSteps)
	}

	// runtime: an ordinary script failure.
	tr2, err := reg.NewTransform("script", `fail("boom")`, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr2.Init(&registry.TransformEnv{}); err != nil {
		t.Fatal(err)
	}
	_, aerr = tr2.Apply(&registry.Message{Decoded: map[string]any{}, Meta: map[string]any{}})
	if !errors.As(aerr, &te) || te.Kind != registry.FailureRuntime {
		t.Errorf("runtime kind = %v (%v), want %q", aerr, te, registry.FailureRuntime)
	}
}

// Flavor travels only through the interface (candidate 07): split implements
// it; the error it returns carries no flavor.
func TestSplitImplementsFlavor(t *testing.T) {
	reg := newReg(t)
	tr, err := reg.NewTransform("split", map[string]any{}, "")
	if err != nil {
		t.Fatal(err)
	}
	f, ok := tr.(registry.TransformFlavor)
	if !ok {
		t.Fatal("split must implement TransformFlavor")
	}
	if got := f.Flavor(); got != "split" {
		t.Fatalf("split flavor = %q", got)
	}
	_, aerr := tr.Apply(&registry.Message{Decoded: "not an array", Meta: map[string]any{}})
	var te *registry.TransformError
	if !errors.As(aerr, &te) {
		t.Fatalf("split failure is not a *TransformError: %T (%v)", aerr, aerr)
	}
	if te.Kind != registry.FailureOther {
		t.Errorf("split failure kind = %q, want %q", te.Kind, registry.FailureOther)
	}
}

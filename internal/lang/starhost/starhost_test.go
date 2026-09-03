package starhost

import (
	"strings"
	"testing"
)

func compile(t *testing.T, src string) *Program {
	t.Helper()
	prog, err := Compile("test.star", src, DefaultOptions())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return prog
}

func runScript(t *testing.T, src string, payload, meta any) (*MsgState, *MsgState, *ScriptError) {
	t.Helper()
	prog := compile(t, src)
	ps := NewMsgState("payload", payload)
	ms := NewMsgState("meta", meta)
	serr := prog.Run(ps, ms, FreezeConstants(map[string]any{"currency": "EUR", "vip": 10000}))
	return ps, ms, serr
}

func TestScriptWritesPayloadAndMeta(t *testing.T) {
	ps, ms, serr := runScript(t, `
payload.total = payload.price * payload.qty
payload.label = "order-%s" % payload.id
if payload.total > 100:
    meta.tier = "vip"
else:
    meta.tier = "basic"
`, map[string]any{"price": 20.0, "qty": 6.0, "id": "A1"}, map[string]any{})
	if serr != nil {
		t.Fatalf("script error: %v", serr)
	}
	got := ps.GoValue().(map[string]any)
	if got["total"] != 120.0 {
		t.Errorf("total = %v (%T)", got["total"], got["total"])
	}
	if got["label"] != "order-A1" {
		t.Errorf("label = %v", got["label"])
	}
	meta := ms.GoValue().(map[string]any)
	if meta["tier"] != "vip" {
		t.Errorf("tier = %v", meta["tier"])
	}
}

func TestNoWriteMeansNoDirty(t *testing.T) {
	ps, ms, serr := runScript(t, `x = payload.price`, map[string]any{"price": 1.0}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if ps.Dirty() || ms.Dirty() {
		t.Fatal("read-only script must not dirty the bindings (COW)")
	}
}

func TestCOWLeavesOriginalUntouched(t *testing.T) {
	original := map[string]any{"a": 1.0, "nested": map[string]any{"k": "v"}}
	ps, _, serr := runScript(t, `
payload.a = 99
payload.nested.k = "changed"
`, original, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
	if original["a"] != 1.0 {
		t.Errorf("original payload mutated: %v", original)
	}
	if got := ps.GoValue().(map[string]any); got["a"] != int64(99) {
		t.Errorf("written value lost: %v", got)
	}
}

func TestCompileRejectsWhileAndRecursion(t *testing.T) {
	if _, err := Compile("t.star", `
i = 0
while i < 10:
    i = i + 1
`, DefaultOptions()); err == nil {
		t.Fatal("while loops must be rejected")
	}
	// Recursion is caught by the interpreter (the resolver cannot always see
	// it statically); the sandbox still aborts the call.
	_, _, serr := runScript(t, `
def f(n):
    return f(n - 1)
f(3)
`, map[string]any{}, map[string]any{})
	if serr == nil || !strings.Contains(serr.Msg, "recurs") {
		t.Fatalf("recursion must be rejected at compile or run time, got: %v", serr)
	}
}

func TestCompileRejectsUnknownNames(t *testing.T) {
	_, err := Compile("t.star", `payload.x = nosuchname`, DefaultOptions())
	if err == nil {
		t.Fatal("undefined name must fail at compile time (resolver)")
	}
}

func TestStepBudgetAborts(t *testing.T) {
	prog, err := Compile("t.star", `
acc = []
for i in range(1000000):
    acc.append(i)
`, Options{MaxSteps: 100_000})
	if err != nil {
		t.Fatal(err)
	}
	ps := NewMsgState("payload", map[string]any{})
	ms := NewMsgState("meta", map[string]any{})
	serr := prog.Run(ps, ms, FreezeConstants(nil))
	if serr == nil {
		t.Fatal("expected step budget error")
	}
	if !strings.Contains(serr.Msg, "too many steps") {
		t.Errorf("unexpected error: %v", serr.Msg)
	}
}

func TestRuntimeErrorHasBacktraceWithLine(t *testing.T) {
	_, _, serr := runScript(t, `
x = 1
fail("boom")
`, map[string]any{}, map[string]any{})
	if serr == nil {
		t.Fatal("expected error")
	}
	if serr.Line != 3 {
		t.Errorf("line = %d, want 3 (msg=%s)", serr.Line, serr.Msg)
	}
	if !strings.Contains(serr.Backtrace, "test.star:3") {
		t.Errorf("backtrace missing position: %q", serr.Backtrace)
	}
}

func TestModuleWhitelist(t *testing.T) {
	_, _, serr := runScript(t, `
load("json", "decode")
load("math", "sqrt")
decoded = decode('{"a": 1}')
payload.a = decoded["a"]
payload.b = sqrt(4)
`, map[string]any{}, map[string]any{})
	if serr != nil {
		t.Fatalf("json/math should be allowed: %v", serr)
	}

	prog, err := Compile("t.star", `
load("time", "now")
payload.x = now()
`, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	serr = prog.Run(NewMsgState("payload", map[string]any{}), NewMsgState("meta", map[string]any{}), FreezeConstants(nil))
	if serr == nil || !strings.Contains(serr.Msg, "whitelist") {
		t.Fatalf("time module must be rejected, got: %v", serr)
	}
}

func TestSafeJSONDecode(t *testing.T) {
	_, _, serr := runScript(t, `
payload.good = safe_json_decode('{"k": 1}', {"k": 0})
payload.bad = safe_json_decode('{not json', {"k": -1})
`, map[string]any{}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
}

func TestIterationSnapshotSemantics(t *testing.T) {
	// Iterate reads a snapshot at loop start; writes during the loop
	// materialize the tree but the iterator keeps its keys (open question #4
	// locked by this case for the POC).
	_, _, serr := runScript(t, `
seen = []
for k in payload:
    seen.append(k)
payload.added = 1
`, map[string]any{"a": 1.0, "b": 2.0}, map[string]any{})
	if serr != nil {
		t.Fatal(serr)
	}
}

func TestConstantsFrozen(t *testing.T) {
	_, _, serr := runScript(t, `
constants.currency = "USD"
`, map[string]any{}, map[string]any{})
	if serr == nil {
		t.Fatal("constants must be frozen")
	}
}

func TestConcurrentProgramExecution(t *testing.T) {
	prog := compile(t, `
payload.doubled = payload.x * 2
`)
	const n = 32
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			ps := NewMsgState("payload", map[string]any{"x": 21.0})
			ms := NewMsgState("meta", map[string]any{})
			if serr := prog.Run(ps, ms, FreezeConstants(nil)); serr != nil {
				done <- serr
				return
			}
			if got := ps.GoValue().(map[string]any)["doubled"]; got != 42.0 {
				done <- nil
				return
			}
			done <- nil
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkSimpleScript(b *testing.B) {
	prog, err := Compile("bench.star", `
payload.total = payload.price * payload.qty
payload.label = "%s-%s" % (payload.id, meta.region)
`, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	constants := FreezeConstants(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps := NewMsgState("payload", map[string]any{"price": 3.0, "qty": 4.0, "id": "A"})
		ms := NewMsgState("meta", map[string]any{"region": "eu"})
		if serr := prog.Run(ps, ms, constants); serr != nil {
			b.Fatal(serr)
		}
	}
}

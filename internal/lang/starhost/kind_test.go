package starhost

import (
	"errors"
	"strings"
	"testing"
)

// Candidate 07 acceptance 2 (script host): the host types its failures where
// they are created — compile at Compile, budget exhaustion at the step-limit
// hook, everything else runtime. The "too many steps" text is produced, never
// sniffed (see the sniff gate in internal/engine).
func TestScriptErrorKinds(t *testing.T) {
	// compile: resolution failures surface as *ScriptError{KindCompile}.
	if _, err := Compile("t.star", `payload.x = nosuchname`, DefaultOptions()); err == nil {
		t.Fatal("undefined name must fail at compile time")
	} else {
		var serr *ScriptError
		if !errors.As(err, &serr) {
			t.Fatalf("compile error is not a *ScriptError: %T (%v)", err, err)
		}
		if serr.Kind != KindCompile {
			t.Errorf("compile kind = %q, want %q", serr.Kind, KindCompile)
		}
		if !strings.Contains(serr.Msg, "starlark compile error") {
			t.Errorf("compile message = %q", serr.Msg)
		}
	}

	// steps: the step-limit hook types it; the message text is unchanged.
	prog, err := Compile("t.star", `
for i in range(1000000):
    pass
`, Options{MaxSteps: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	serr := prog.Run(NewMsgState("payload", map[string]any{}), NewMsgState("meta", map[string]any{}), FreezeConstants(nil))
	if serr == nil {
		t.Fatal("expected step budget error")
	}
	if serr.Kind != KindSteps {
		t.Errorf("budget kind = %q, want %q", serr.Kind, KindSteps)
	}
	if !strings.Contains(serr.Msg, "too many steps") {
		t.Errorf("budget message = %q, want the historical text preserved", serr.Msg)
	}

	// runtime: an ordinary execution failure.
	_, _, serr = runScript(t, `fail("boom")`, map[string]any{}, map[string]any{})
	if serr == nil {
		t.Fatal("expected runtime error")
	}
	if serr.Kind != KindRuntime {
		t.Errorf("runtime kind = %q, want %q", serr.Kind, KindRuntime)
	}
}

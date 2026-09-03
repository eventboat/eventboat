package celhost

import (
	"strings"
	"testing"
)

func newTestEnv(t *testing.T) *Env {
	t.Helper()
	env, err := NewEnv(map[string]any{"region": "eu"})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestCompileAndEval(t *testing.T) {
	env := newTestEnv(t)
	pred, err := env.Compile(`meta.region == "eu" && payload.total > 100`)
	if err != nil {
		t.Fatal(err)
	}
	ok, evalErr := pred.Eval(
		map[string]any{"total": 120.0},
		map[string]any{"region": "eu"},
	)
	if evalErr != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, evalErr)
	}
	ok, evalErr = pred.Eval(
		map[string]any{"total": 50.0},
		map[string]any{"region": "eu"},
	)
	if evalErr != nil || ok {
		t.Fatalf("expected no match, got ok=%v err=%v", ok, evalErr)
	}
}

func TestCompileErrorCarriesExpression(t *testing.T) {
	env := newTestEnv(t)
	_, err := env.Compile(`meta.region ==`)
	if err == nil {
		t.Fatal("expected compile error")
	}
	if !strings.Contains(err.Error(), "meta.region ==") {
		t.Errorf("error should include the expression: %v", err)
	}
}

func TestEvalErrorMeansNotPassed(t *testing.T) {
	env := newTestEnv(t)
	// payload.total is a string here: ">" on string/number is a runtime
	// error — the contract says error == condition does not pass.
	pred, err := env.Compile(`payload.total > 100`)
	if err != nil {
		t.Fatal(err)
	}
	ok, evalErr := pred.Eval(map[string]any{"total": "NaN"}, map[string]any{})
	if evalErr == nil {
		t.Fatal("expected eval error")
	}
	if ok {
		t.Fatal("error must not pass the condition")
	}
}

func TestConstantsVisible(t *testing.T) {
	env, err := NewEnv(map[string]any{"vip": float64(10000)})
	if err != nil {
		t.Fatal(err)
	}
	pred, err := env.Compile(`payload.total > constants.vip`)
	if err != nil {
		t.Fatal(err)
	}
	ok, evalErr := pred.Eval(map[string]any{"total": 20000.0}, map[string]any{})
	if evalErr != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, evalErr)
	}
}

func TestEvalString(t *testing.T) {
	env := newTestEnv(t)
	pred, err := env.Compile(`payload.order_no`)
	if err != nil {
		t.Fatal(err)
	}
	s, evalErr := pred.EvalString(map[string]any{"order_no": "A-123"}, map[string]any{})
	if evalErr != nil || s != "A-123" {
		t.Fatalf("s=%q err=%v", s, evalErr)
	}
}

func BenchmarkPredicateEval(b *testing.B) {
	env, _ := NewEnv(map[string]any{"vip": float64(10000)})
	pred, err := env.Compile(`meta.region == "eu" && payload.total > constants.vip`)
	if err != nil {
		b.Fatal(err)
	}
	payload := map[string]any{"total": 12000.0}
	meta := map[string]any{"region": "eu"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ok, _ := pred.Eval(payload, meta); !ok {
			b.Fatal("expected match")
		}
	}
}

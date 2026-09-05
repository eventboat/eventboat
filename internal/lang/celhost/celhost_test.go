package celhost

import (
	"strings"
	"testing"
)

func newTestEnv(t *testing.T) *Env {
	t.Helper()
	env, err := NewEnv(map[string]any{"region": "eu"}, nil)
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
	env, err := NewEnv(map[string]any{"vip": float64(10000)}, nil)
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

// The runtime cost limit bounds payload-driven blowups: a size-driven
// operation over a huge payload (regex matches charge ~0.1 cost units per
// input byte, so a 16 MiB subject costs ~1.7e6) exceeds the budget and the
// evaluation is cancelled on the EXISTING error path — error == condition
// does not pass — while ordinary predicates on the same payload still
// evaluate.
func TestEvalCostLimit(t *testing.T) {
	env := newTestEnv(t)
	pred, err := env.Compile(`payload.s.matches("y*")`)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 16<<20)
	ok, evalErr := pred.Eval(map[string]any{"s": big}, map[string]any{})
	if evalErr == nil {
		t.Fatalf("size-driven predicate should exceed the cost limit, got ok=%v", ok)
	}
	if ok {
		t.Fatal("an evaluation error must never pass the condition")
	}
	if !strings.Contains(evalErr.Error(), "cost") {
		t.Errorf("error should name the cost limit: %v", evalErr)
	}

	// An ordinary predicate over the same large payload stays far under the
	// budget and evaluates normally.
	normal, err := env.Compile(`size(payload.s) > 1024 && meta.region == "eu"`)
	if err != nil {
		t.Fatal(err)
	}
	ok, evalErr = normal.Eval(map[string]any{"s": big}, map[string]any{"region": "eu"})
	if evalErr != nil || !ok {
		t.Fatalf("normal predicate: ok=%v err=%v", ok, evalErr)
	}
}

func BenchmarkPredicateEval(b *testing.B) {
	env, _ := NewEnv(map[string]any{"vip": float64(10000)}, nil)
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

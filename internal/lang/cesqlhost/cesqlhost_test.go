package cesqlhost

import "testing"

func eval(t *testing.T, expr string, payload, meta any) (bool, error) {
	t.Helper()
	p, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile %q: %v", expr, err)
	}
	return p.Eval(payload, meta)
}

func TestPureMetaMode(t *testing.T) {
	meta := map[string]any{
		"type":     "com.example.order",
		"region":   "EU",
		"attempts": int64(3),
		"paid":     true,
	}
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"type = 'com.example.order' AND region = 'EU'", true},
		{"attempts > 2", true},
		{"paid", true},
		{"NOT paid", false},
		{"region = 'US'", false},
		{"missingattr", false}, // error => not passed (checked below)
	} {
		got, err := eval(t, tc.expr, nil, meta)
		if tc.expr == "missingattr" {
			if err == nil {
				t.Errorf("%q: want evaluation error", tc.expr)
			}
			if got {
				t.Errorf("%q: error must not pass", tc.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.expr, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestDataExtensionMode(t *testing.T) {
	payload := map[string]any{
		"amount":  42.0, // integral float -> CESQL integer
		"symbol":  "USD/EUR",
		"enabled": true,
		"customer": map[string]any{
			"region": "eu-west-1",
		},
	}
	meta := map[string]any{"region": "EU"}

	cases := []struct {
		expr string
		want bool
	}{
		{"data.amount > 40", true},
		{"data.symbol = 'USD/EUR'", true},
		{"data.enabled", true},
		{"data.customer.region = 'eu-west-1'", true}, // nested flatten
		{"data.amount > 100", false},
		{"data.amount > 40 AND region = 'EU'", true}, // mixed meta + data
	}
	for _, tc := range cases {
		got, err := eval(t, tc.expr, payload, meta)
		if err != nil {
			t.Errorf("%q: %v", tc.expr, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestRewriteLeavesStringsAlone(t *testing.T) {
	for _, src := range []string{
		`'data.x' = data.x`,
		`"data.a.b" != data.a.b`,
		`'it''s data.ignored' = 'x'`,
		`'escaped \' data.y' = data.y`,
	} {
		if _, err := Compile(src); err != nil {
			t.Errorf("compile %q: %v", src, err)
		}
	}
	p, err := Compile(`'data.x' = data.x`)
	if err != nil {
		t.Fatal(err)
	}
	if p.rewritten != "'data.x' = dataX" {
		t.Fatalf("rewrite = %q", p.rewritten)
	}
	if p.Source() != `'data.x' = data.x` || p.Lang() != "cesql" {
		t.Fatalf("predicate metadata wrong: %q %q", p.Source(), p.Lang())
	}
}

func TestCompileParseError(t *testing.T) {
	if _, err := Compile("ABC("); err == nil {
		t.Fatal("parse error not reported")
	}
	// Identifiers are alphanumeric in CESQL; underscored meta keys are
	// unreachable in this dialect (documented) and fail to parse.
	if _, err := Compile("kafka_topic = 'x'"); err == nil {
		t.Fatal("underscored identifier accepted")
	}
}

func TestNonBoolResultIsError(t *testing.T) {
	// A predicate position requires a boolean result.
	if _, err := eval(t, "region", nil, map[string]any{"region": "EU"}); err == nil {
		t.Fatal("non-bool result accepted")
	}
}

func TestDataNamespaceShadowsMeta(t *testing.T) {
	// Identifiers starting with "data" are reserved for the payload
	// extension; a meta key in that namespace is shadowed by the payload.
	got, err := eval(t, "data.secret = 'from-payload'",
		map[string]any{"secret": "from-payload"},
		map[string]any{"dataSecret": "from-meta", "database": "also-shadowed"})
	if err != nil || got != true {
		t.Fatalf("payload did not shadow reserved meta namespace: %v, %v", got, err)
	}
}

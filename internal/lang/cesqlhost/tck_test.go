package cesqlhost

import (
	"encoding/json"
	"os"
	"path"
	"reflect"
	"runtime"
	"testing"

	"sigs.k8s.io/yaml"

	sqlerrors "github.com/cloudevents/sdk-go/sql/v2/errors"
	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/event"
	cetest "github.com/cloudevents/sdk-go/v2/test"
)

// The vendored official CloudEvents SQL TCK: the M3 acceptance gate is 100%
// of the suite passing (redesign-v3.md §7.4). Pure mode: these cases only
// touch context attributes, exactly what the SDK parser supports; the runner
// mirrors the semantics of the SDK's own sql/v2/test/tck_test.go.
var tckFiles = []string{
	"binary_math_operators",
	"binary_logical_operators",
	"binary_comparison_operators",
	"case_sensitivity",
	"casting_functions",
	"context_attributes_access",
	"exists_expression",
	"in_expression",
	"integer_builtin_functions",
	"like_expression",
	"literals",
	"negate_operator",
	"not_operator",
	"parse_errors",
	"spec_examples",
	"string_builtin_functions",
	"sub_expression",
	"subscriptions_api_recreations",
}

type errorType string

const (
	errParse              errorType = "parse"
	errMath               errorType = "math"
	errCast               errorType = "cast"
	errMissingAttribute   errorType = "missingAttribute"
	errMissingFunction    errorType = "missingFunction"
	errFunctionEvaluation errorType = "functionEvaluation"
	errGeneric            errorType = "generic"
)

type tckFile struct {
	Name  string        `json:"name"`
	Tests []tckTestCase `json:"tests"`
}

type tckTestCase struct {
	Name       string             `json:"name"`
	Expression string             `json:"expression"`
	Result     json.RawMessage    `json:"result"`
	Error      errorType          `json:"error"`
	Event      *cloudevents.Event `json:"event"`
	Overrides  map[string]any     `json:"eventOverrides"`
}

// inputEvent reproduces the upstream base event (test.FullEvent) with the
// TCK's event/eventOverrides applied.
func (tc tckTestCase) inputEvent(t *testing.T) cloudevents.Event {
	t.Helper()
	var in cloudevents.Event
	if tc.Event != nil {
		in = *tc.Event
	} else {
		in = cetest.FullEvent()
	}
	in.SetSpecVersion(event.CloudEventsVersionV1)
	for k, v := range tc.Overrides {
		if err := spec.V1.SetAttribute(in.Context, k, v); err != nil {
			t.Fatalf("override %s: %v", k, err)
		}
	}
	return in
}

// expectedResult normalizes JSON numbers to the int32 the parser yields
// (mirrors upstream ExpectedResult).
func (tc tckTestCase) expectedResult(t *testing.T) any {
	t.Helper()
	if tc.Result == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(tc.Result, &v); err != nil {
		t.Fatal(err)
	}
	switch n := v.(type) {
	case float64:
		return int32(n)
	case bool, string:
		return v
	default:
		return v
	}
}

func verifyErrorType(want errorType, err error) bool {
	switch want {
	case errParse:
		return sqlerrors.IsParseError(err)
	case errMath:
		return sqlerrors.IsMathError(err)
	case errCast:
		return sqlerrors.IsCastError(err)
	case errMissingFunction:
		return sqlerrors.IsMissingFunctionError(err)
	case errFunctionEvaluation:
		return sqlerrors.IsFunctionEvaluationError(err)
	case errMissingAttribute:
		return sqlerrors.IsMissingAttributeError(err)
	case errGeneric:
		return sqlerrors.IsGenericError(err)
	default:
		return false
	}
}

func TestCesqlTCK(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	base := path.Dir(thisFile)
	var failed, total int
	for _, name := range tckFiles {
		data, err := os.ReadFile(path.Join(base, "tck", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var f tckFile
		if err := yaml.Unmarshal(data, &f); err != nil {
			t.Fatal(err)
		}
		t.Run(f.Name, func(t *testing.T) {
			for _, tc := range f.Tests {
				tc := tc
				t.Run(tc.Name, func(t *testing.T) {
					total++
					if tc.Error == errParse {
						if _, err := cesqlparser.Parse(tc.Expression); err == nil || !sqlerrors.IsParseError(err) {
							failed++
							t.Errorf("expression %q: want parse error, got %v", tc.Expression, err)
						}
						return
					}
					expr, err := cesqlparser.Parse(tc.Expression)
					if err != nil {
						failed++
						t.Fatalf("parse %q: %v", tc.Expression, err)
					}
					result, evalErr := expr.Evaluate(tc.inputEvent(t))
					if tc.Error != "" {
						if evalErr == nil {
							failed++
							t.Fatalf("expression %q: want %s error, got result %v", tc.Expression, tc.Error, result)
						}
						if !verifyErrorType(tc.Error, evalErr) {
							failed++
							t.Fatalf("expression %q: want %s error, got %v", tc.Expression, tc.Error, evalErr)
						}
						return
					}
					if evalErr != nil {
						failed++
						t.Fatalf("expression %q: %v", tc.Expression, evalErr)
					}
					want := tc.expectedResult(t)
					if !reflect.DeepEqual(result, want) {
						failed++
						t.Errorf("expression %q: got %#v (%T), want %#v (%T)",
							tc.Expression, result, result, want, want)
					}
				})
			}
		})
	}
	t.Logf("TCK: %d cases", total)
	if failed > 0 {
		t.Fatalf("%d of %d TCK cases failed", failed, total)
	}
}

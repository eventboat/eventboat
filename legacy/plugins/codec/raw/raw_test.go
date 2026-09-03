package rawcodec

import (
	"testing"
)

func TestRawEncodeUnsupportedTypeReturnsError(t *testing.T) {
	r := &Raw{}
	for _, data := range []any{42, map[string]any{"a": 1}, []string{"x"}, nil} {
		if _, err := r.Encode(data); err == nil {
			t.Fatalf("expected error encoding %T", data)
		}
	}
}

func TestRawEncodeSupportedTypes(t *testing.T) {
	r := &Raw{}
	if b, err := r.Encode([]byte("abc")); err != nil || string(b) != "abc" {
		t.Fatalf("[]byte: %v %q", err, b)
	}
	if b, err := r.Encode("abc"); err != nil || string(b) != "abc" {
		t.Fatalf("string: %v %q", err, b)
	}
}

func TestRawValidateConfigRejectsUnknownFields(t *testing.T) {
	r := &Raw{}
	if err := r.ValidateConfig(map[string]any{"bogus": 1}); err == nil {
		t.Fatal("expected error for unknown config field")
	}
	if err := r.ValidateConfig(nil); err != nil {
		t.Fatalf("nil config should be valid: %v", err)
	}
	if err := r.ValidateConfig(map[string]any{}); err != nil {
		t.Fatalf("empty config should be valid: %v", err)
	}
}

package jsoncodec

import (
	"testing"
)

func TestJSONValidateConfigRejectsUnknownFields(t *testing.T) {
	j := &JSON{}
	if err := j.ValidateConfig(map[string]any{"bogus": 1}); err == nil {
		t.Fatal("expected error for unknown config field")
	}
	if err := j.ValidateConfig(nil); err != nil {
		t.Fatalf("nil config should be valid: %v", err)
	}
	if err := j.ValidateConfig(map[string]any{}); err != nil {
		t.Fatalf("empty config should be valid: %v", err)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	j := &JSON{}
	data, err := j.Decode([]byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Encode(data); err != nil {
		t.Fatal(err)
	}
}

package buffer

import (
	"bytes"
	"testing"

	"github.com/edgesets/edgestream/internal/message"
)

func TestWALRecordRoundTrip(t *testing.T) {
	orig := message.New([]byte(`{"a":1}`), map[string]any{"k": "v"})
	orig.ID = "msg-123"
	var buf bytes.Buffer
	if err := encodeWALRecord(&buf, orig); err != nil {
		t.Fatal(err)
	}
	got, err := decodeWALRecord(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != orig.ID {
		t.Fatalf("id = %q", got.ID)
	}
	if string(got.Payload) != string(orig.Payload) {
		t.Fatalf("payload mismatch")
	}
	if got.Metadata["k"] != "v" {
		t.Fatalf("metadata mismatch")
	}
}

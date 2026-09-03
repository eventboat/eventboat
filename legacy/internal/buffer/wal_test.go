package buffer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/riverpod/riverpod/internal/message"
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

func TestWALRecordCRCDetectsCorruption(t *testing.T) {
	orig := message.New([]byte("payload-bytes"), map[string]any{"k": "v"})
	orig.ID = "msg-crc"
	var buf bytes.Buffer
	if err := encodeWALRecord(&buf, orig); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	// flip a byte inside the payload region
	raw[len(raw)-10] ^= 0xff
	if _, err := decodeWALRecord(bytes.NewReader(raw)); !errors.Is(err, errWALCorrupt) {
		t.Fatalf("expected errWALCorrupt, got %v", err)
	}
}

func TestWALRecordTruncatedTailIsCorrupt(t *testing.T) {
	orig := message.New([]byte("payload-bytes"), nil)
	orig.ID = "msg-trunc"
	var buf bytes.Buffer
	if err := encodeWALRecord(&buf, orig); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()[:buf.Len()-3] // torn write: record cut short
	if _, err := decodeWALRecord(bytes.NewReader(raw)); !errors.Is(err, errWALCorrupt) {
		t.Fatalf("expected errWALCorrupt, got %v", err)
	}
}

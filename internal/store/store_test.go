package store

import (
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

func exerciseStore(t *testing.T, st Store) {
	t.Helper()
	defer st.Close()

	msg := registry.Message{
		ID:      "m-1",
		Codec:   "json",
		Raw:     []byte(`{"a":1}`),
		Meta:    map[string]any{"region": "eu"},
		SrcName: "in",
		SrcSeq:  1,
		Cursor:  "c1",
	}
	ingest := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	seq1, err := st.AppendSpool("p", msg, ingest)
	if err != nil || seq1 != 1 {
		t.Fatalf("append: seq=%d err=%v", seq1, err)
	}
	seq2, err := st.AppendSpool("p", msg, ingest)
	if err != nil || seq2 != 2 {
		t.Fatalf("append2: seq=%d err=%v", seq2, err)
	}

	// Nothing replayed before checkpoint moves.
	if cp, _ := st.Checkpoint("p"); cp != 0 {
		t.Fatalf("checkpoint = %d, want 0", cp)
	}
	var replayed []int64
	if err := st.ReplayFrom("p", 0, func(seq int64, m registry.Message, ts time.Time) error {
		replayed = append(replayed, seq)
		if m.ID != "m-1" || m.Meta["region"] != "eu" || m.Codec != "json" {
			t.Errorf("roundtrip lost fields: %+v", m)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %v", replayed)
	}

	if err := st.SetCheckpoint("p", 1); err != nil {
		t.Fatal(err)
	}
	cp, err := st.Checkpoint("p")
	if err != nil {
		t.Fatal(err)
	}
	replayed = nil
	_ = st.ReplayFrom("p", cp, func(seq int64, m registry.Message, ts time.Time) error {
		replayed = append(replayed, seq)
		return nil
	})
	if len(replayed) != 1 || replayed[0] != 2 {
		t.Fatalf("after checkpoint, replayed %v, want [2]", replayed)
	}

	if err := st.SetSourceState("p", "in", []byte(`{"watermark":"c1"}`), 1); err != nil {
		t.Fatal(err)
	}
	state, srcSeq, err := st.SourceState("p", "in")
	if err != nil || srcSeq != 1 || string(state) != `{"watermark":"c1"}` {
		t.Fatalf("source state: %s %d %v", state, srcSeq, err)
	}

	dl := DeadLetter{
		Pipeline: "p", MessageID: "m-1", Node: "out", Edge: "t -> out",
		Reason: "delivery: sink write failed after retries", Backtrace: "t.star:3:1",
		Raw: msg.Raw, Codec: msg.Codec, Meta: msg.Meta, SrcName: "in", SrcSeq: 1,
	}
	if err := st.WriteDeadLetter(dl); err != nil {
		t.Fatal(err)
	}
	dls, err := st.DeadLetters("p")
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters: %v %v", dls, err)
	}
	if dls[0].Reason != dl.Reason || dls[0].Backtrace != "t.star:3:1" {
		t.Errorf("dead letter roundtrip: %+v", dls[0])
	}
}

func TestMemoryStore(t *testing.T) {
	exerciseStore(t, NewMemory("p"))
}

func TestSQLiteStore(t *testing.T) {
	path := t.TempDir() + "/test.db"
	st, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	exerciseStore(t, st)
}

func TestSQLitePersistsAcrossReopen(t *testing.T) {
	path := t.TempDir() + "/persist.db"
	st, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.AppendSpool("p", registry.Message{ID: "m-1", Codec: "json", Raw: []byte(`{}`), Meta: map[string]any{}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetCheckpoint("p", 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if cp, _ := st2.Checkpoint("p"); cp != 1 {
		t.Fatalf("checkpoint lost across reopen: %d", cp)
	}
	count := 0
	_ = st2.ReplayFrom("p", 1, func(seq int64, m registry.Message, ts time.Time) error {
		count++
		return nil
	})
	if count != 0 {
		t.Fatalf("settled message replayed: %d", count)
	}
}

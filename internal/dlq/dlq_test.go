package dlq

import (
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

func letters() []store.DeadLetter {
	return []store.DeadLetter{
		{ID: 1, MessageID: "m1", Node: "t", Codec: "csv", Raw: []byte(`{"region":"eu"}`), Meta: map[string]any{"region": "eu"}},
		{ID: 2, MessageID: "m2", Node: "t2", Codec: "json", Raw: []byte(`{"region":"us"}`), Meta: map[string]any{"region": "us"}},
		{ID: 3, MessageID: "m3", Node: "t", Codec: "json", Raw: []byte(`{"region":"eu"}`), Meta: map[string]any{"region": "eu"}},
	}
}

// Selection order: ids → where → limit. The delete list derived from the
// SELECTED requests is what guards `--ids` + `--limit` (candidate 05): rows
// beyond the limit were never replayed, so they must never be deleted.
func TestSelectOrderAndDeleteSafety(t *testing.T) {
	reqs, err := Select(letters(), Filter{IDs: []int64{1, 2, 3}, Limit: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 1 || reqs[0].ID != 1 {
		t.Fatalf("selected %+v, want only id 1", reqs)
	}
	if got := IDs(reqs); len(got) != 1 || got[0] != 1 {
		t.Fatalf("delete list = %v, want [1] (ids 2,3 were never replayed)", got)
	}
}

// The where filter compiles with the pipeline's constants on every surface
// (the CLI used to compile with constants while the MCP path did not).
func TestFilterUsesPipelineConstants(t *testing.T) {
	constants := map[string]any{"region": "eu"}
	reqs, err := Select(letters(), Filter{Where: "meta.region == constants.region"}, constants)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 2 || reqs[0].ID != 1 || reqs[1].ID != 3 {
		t.Fatalf("constant-filtered select = %+v", reqs)
	}
	// Without the constant bound the predicate cannot pass any row — the old
	// MCP behavior, now gone.
	reqs, err = Select(letters(), Filter{Where: "meta.region == constants.region"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 0 {
		t.Fatalf("unbound constants should match nothing, got %+v", reqs)
	}
}

// The replay request carries identity and codec (candidate 01) and applies
// the target-node override.
func TestRequestCarriesCodecAndNode(t *testing.T) {
	reqs, err := Select(letters(), Filter{IDs: []int64{1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 1 {
		t.Fatalf("select = %+v", reqs)
	}
	msg := reqs[0].Message()
	if msg.Codec != "csv" || msg.ID != "m1" || string(msg.Raw) != `{"region":"eu"}` {
		t.Fatalf("message lost identity/codec: %+v", msg)
	}
	reqs, err = Select(letters(), Filter{IDs: []int64{1}, At: "sink-x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reqs[0].Node != "sink-x" {
		t.Fatalf("at override not applied: %q", reqs[0].Node)
	}
	if reqs[0].ID != 1 {
		t.Fatalf("at override must not lose the id: %+v", reqs[0])
	}
}

func TestSinceParsesDurations(t *testing.T) {
	now := time.Now()
	got, err := Since("2h", now)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now.Add(-2 * time.Hour)) {
		t.Fatalf("since = %v", got)
	}
	if zero, err := Since("", now); err != nil || !zero.IsZero() {
		t.Fatalf("empty since = %v, %v", zero, err)
	}
	if _, err := Since("nope", now); err == nil {
		t.Fatal("bad duration accepted")
	}
}

// A bad where expression is reported, never silently treated as no filter.
func TestBadWhereIsAnError(t *testing.T) {
	if _, err := FilterDeadLetters(letters(), Filter{Where: "meta.region =="}, nil); err == nil {
		t.Fatal("malformed where accepted")
	}
}

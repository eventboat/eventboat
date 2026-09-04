package convert

import (
	"os"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
)

// The M4 acceptance's manual spot-check, made executable and permanent:
// three fixtures whose converted semantics are verified message-by-message
// against the v2 behavior described by their own comments (redesign-v3.md
// §7.4 M4 / review-m4 §三).

func convertFixture(t *testing.T, path string) (*config.Pipeline, *ir.Pipeline) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Convert(path, data)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Report.VerifyOK {
		t.Fatalf("converted %s does not verify", path)
	}
	lr := config.LoadBytes("converted.yaml", res.YAML)
	if lr.HasErrors() {
		t.Fatalf("re-load failed: %v", lr.Diagnostics)
	}
	reg := convertRegistry()
	built, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	for _, d := range diags {
		if d.Severity == "error" {
			t.Fatalf("ir.Build error: %s", d.Error())
		}
	}
	return lr.Pipeline, built
}

func evalWhen(t *testing.T, e *ir.Edge, payload any) bool {
	t.Helper()
	if e.When == nil {
		return true
	}
	ok, err := e.When.Eval(payload, map[string]any{})
	if err != nil {
		t.Fatalf("when eval: %v", err)
	}
	return ok
}

// newPayload runs one compiled script against a sample payload and returns
// the post-script payload state (meta stays empty).
func newPayload(t *testing.T, prog *starhost.Program, payload map[string]any) *starhost.MsgState {
	t.Helper()
	ps := starhost.NewMsgState("payload", payload)
	ms := starhost.NewMsgState("meta", map[string]any{})
	if serr := prog.Run(ps, ms, starhost.FreezeConstants(nil)); serr != nil {
		t.Fatalf("script run: %v", serr)
	}
	return ps
}

// Case 1 — 01-linear-etl: the v2 filter `payload.total > 20` folded onto the
// sink edge must pass exactly the messages the v2 filter passed, with the
// same computed total (script identical in shape to the v2 dsl).
func TestEquivalence01LinearFilter(t *testing.T) {
	_, built := convertFixture(t, "../../legacy/_examples/01-linear-etl.yaml")
	sink := built.Nodes["drop-sink"]
	if sink == nil || len(sink.In) != 1 || sink.In[0].From != "enrich" {
		t.Fatalf("unexpected sink edges: %+v", sink.In)
	}
	// The script first (same as production would run it).
	enrich := built.Nodes["enrich"]
	if enrich.Script == nil {
		t.Fatal("enrich has no compiled script")
	}
	run := func(price, quantity float64) any {
		ps := newPayload(t, enrich.Script, map[string]any{"price": price, "quantity": quantity, "sku": "A"})
		return ps.GoValue()
	}
	// price*quantity = 50 > 20 → delivered (v2: filter pass).
	if !evalWhen(t, &sink.In[0], run(10, 5)) {
		t.Error("total=50 should reach the sink (v2 filter passed it)")
	}
	// price*quantity = 10 <= 20 → filtered (v2: silently dropped; v3:
	// settles as filtered + counted — same message fate, observable).
	if evalWhen(t, &sink.In[0], run(2, 5)) {
		t.Error("total=10 should be filtered")
	}
}

// Case 2 — 02-route-branching: the v2 route table (us, eu, _default with
// first-match semantics) folded into ordered edge guards must route each
// region to exactly the sink the v2 route transform chose.
func TestEquivalence02RouteFirstMatch(t *testing.T) {
	_, built := convertFixture(t, "../../legacy/_examples/02-route-branching.yaml")
	edgeFrom := func(node, from string) *ir.Edge {
		for i := range built.Nodes[node].In {
			if built.Nodes[node].In[i].From == from {
				return &built.Nodes[node].In[i]
			}
		}
		return nil
	}
	// tag-region copies payload.region into meta.region; evaluate the guards
	// with that state applied.
	eval := func(e *ir.Edge, region string) bool {
		if e == nil {
			t.Fatalf("missing edge for region %s", region)
		}
		if e.When == nil {
			return true
		}
		ok, err := e.When.Eval(map[string]any{"region": region}, map[string]any{"region": region})
		if err != nil {
			t.Fatalf("when eval: %v", err)
		}
		return ok
	}
	cases := []struct {
		region                    string
		wantUS, wantEU, wantDeflt bool
	}{
		{"us", true, false, false},
		{"eu", false, true, false},
		{"apac", false, false, true}, // no explicit route matched → _default
	}
	for _, tc := range cases {
		if got := eval(edgeFrom("us-sink", "tag-region"), tc.region); got != tc.wantUS {
			t.Errorf("region=%s us-sink matched=%v want %v", tc.region, got, tc.wantUS)
		}
		if got := eval(edgeFrom("eu-sink", "tag-region"), tc.region); got != tc.wantEU {
			t.Errorf("region=%s eu-sink matched=%v want %v", tc.region, got, tc.wantEU)
		}
		if got := eval(edgeFrom("default-sink", "tag-region"), tc.region); got != tc.wantDeflt {
			t.Errorf("region=%s default-sink matched=%v want %v", tc.region, got, tc.wantDeflt)
		}
	}
}

// Case 3 — 06-edge-delivery: the v2 per-edge delivery (retry 3 exponential,
// timeout 5s) and required:false best-effort edge map onto the v3 edge
// policy fields with the same numeric semantics; the dlq sink and section
// are gone (dead letters live in the store).
func TestEquivalence06EdgeDelivery(t *testing.T) {
	t.Setenv("PRIMARY_SINK_URL", "https://primary.example/api")
	cfg, built := convertFixture(t, "../../legacy/_examples/06-edge-delivery.yaml")
	primary := built.Nodes["primary-out"]
	if primary == nil {
		t.Fatal("primary-out missing")
	}
	var edge *ir.Edge
	for i := range primary.In {
		if primary.In[i].From == "enrich" {
			edge = &primary.In[i]
		}
	}
	if edge == nil {
		t.Fatal("primary-out has no edge from enrich")
	}
	if edge.Retries != 3 || edge.Backoff != "exponential" || edge.TimeoutMs != 5000 {
		t.Errorf("delivery policy = retries %d, backoff %s, timeout %dms; want 3/exponential/5000",
			edge.Retries, edge.Backoff, edge.TimeoutMs)
	}
	analytics := built.Nodes["analytics-out"]
	var aedge *ir.Edge
	for i := range analytics.In {
		if analytics.In[i].From == "enrich" {
			aedge = &analytics.In[i]
		}
	}
	if aedge == nil || aedge.Required {
		t.Errorf("analytics edge must be required:false (v2 optional), got %+v", aedge)
	}
	if _, exists := built.Nodes["dlq-sink"]; exists {
		t.Error("dlq-sink stage should be gone (v3 dead letters go to the store)")
	}
	if cfg.Sources == nil || cfg.Sources["cron-source"] == nil {
		t.Fatal("source lost in conversion")
	}
}

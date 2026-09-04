package starhost

import "testing"

// BenchmarkContainerReadOnly measures the engine-side cost model for scripts
// that READ through nested containers without writing: the script run, plus
// the engine's write-back gate (Dirty() ? GoValue() : keep the decoded value
// — nodes.go). Precise dirty tracking is what keeps the write-back off this
// path; the numbers live in redesign-v3-review-beta.md's appendix.
func BenchmarkContainerReadOnly(b *testing.B) {
	prog, err := Compile("bench_ro.star", `
x = payload.user.profile.email
y = payload.user.id
z = payload.region
`, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	payload := map[string]any{
		"user": map[string]any{
			"id":      "u-1",
			"profile": map[string]any{"email": "a@b.example", "lang": "en"},
		},
		"region": "eu",
	}
	meta := map[string]any{}
	constants := FreezeConstants(nil)
	var writeback any
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps := NewMsgState("payload", payload)
		ms := NewMsgState("meta", meta)
		if serr := prog.Run(ps, ms, constants); serr != nil {
			b.Fatal(serr)
		}
		if ps.Dirty() {
			writeback = ps.GoValue() // the engine's write-back gate
		}
	}
	_ = writeback
}

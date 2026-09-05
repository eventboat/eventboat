package engine

import "testing"

// The srcRefs FIFO (replacement for the per-advance full-map scan) must
// behave exactly like the map it replaced: entries beyond the committed
// frontier survive the sweep, and the rare out-of-order registration (two
// source goroutines interleaving AppendSpool and arrived in accept) splices
// into its sorted position instead of disordering the queue.

func TestCommitTrackerSrcRefsFIFO(t *testing.T) {
	var frontiers []int64
	tr := newCommitTracker("p", []string{"in"}, nil, func(through int64, f map[string]int64) {
		frontiers = append(frontiers, f["in"])
	})

	// Near-ordered arrival: 1, 2, then 4 before 3 — the preemption window in
	// accept between AppendSpool and arrived, crossed by another source.
	tr.arrived(1, "in", 1)
	tr.arrived(2, "in", 2)
	tr.arrived(4, "in", 4)
	if got := len(tr.srcRefs); got != 3 {
		t.Fatalf("queue holds %d entries, want 3", got)
	}

	// Commit 1: the sweep pops exactly the swept prefix; 2 and 4 survive.
	tr.done(1)
	if len(frontiers) != 1 || frontiers[0] != 1 {
		t.Fatalf("frontiers after seq 1 = %v, want [1]", frontiers)
	}
	if got := len(tr.srcRefs); got != 2 {
		t.Fatalf("queue holds %d entries after sweeping through 1, want 2", got)
	}

	// The inversion resolves: 3 registers late, ahead of 4 in the queue, and
	// commits in order.
	tr.arrived(3, "in", 3)
	tr.done(2)
	tr.done(3)
	if len(frontiers) != 3 || frontiers[2] != 3 {
		t.Fatalf("frontiers = %v, want frontier 3 after committing 1..3", frontiers)
	}
	if got := len(tr.srcRefs); got != 1 || tr.srcRefs[0].seq != 4 {
		t.Fatalf("queue = %+v, want only seq 4", tr.srcRefs)
	}

	// Drain: the surviving entry is still delivered to its tracker.
	tr.done(4)
	if len(frontiers) != 4 || frontiers[3] != 4 {
		t.Fatalf("frontiers = %v, want final frontier 4", frontiers)
	}
	if len(tr.srcRefs) != 0 {
		t.Fatalf("queue not drained: %+v", tr.srcRefs)
	}

	// An extreme straggler (seq below the already-swept prefix — the map
	// delivered those at the next advance too) must splice to the head and
	// ride the next sweep, not wedge the queue behind higher seqs.
	tr.arrived(5, "in", 5)
	tr.arrived(4, "in", 4) // duplicate seq of a popped entry, arriving late
	tr.done(5)
	if len(frontiers) != 5 || frontiers[4] != 5 {
		t.Fatalf("frontiers = %v, want frontier 5", frontiers)
	}
	if len(tr.srcRefs) != 0 {
		t.Fatalf("straggler not swept: %+v", tr.srcRefs)
	}
}

// BenchmarkCommitTrackerSweep measures one commit delta while the in-flight
// window is full: a hole pins the committed frontier, everything above it
// stays queued, and every terminal event still runs the advance scan — the
// regime the full-map srcRefs scan targeted (O(window) per message; the FIFO
// head check is O(1)).
func BenchmarkCommitTrackerSweep(b *testing.B) {
	const window = 10_000 // DefaultHighWatermark
	tr := newCommitTracker("p", []string{"in"}, nil, func(int64, map[string]int64) {})
	for seq := int64(1); seq <= window; seq++ {
		tr.arrived(seq, "in", seq)
	}
	// Commit everything below the hole; seq window/2 stays outstanding, so
	// the frontier freezes just below it and the upper half of the queue is
	// the permanent backlog each advance must look past.
	for seq := int64(1); seq < window/2; seq++ {
		tr.done(seq)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.add(window/2+1, 1)
		tr.add(window/2+1, -1)
	}
}

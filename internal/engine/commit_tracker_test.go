package engine

import (
	"math/rand"
	"testing"
)

// srcTracker memory contract: arrivedAt/committedAt may only hold the
// in-flight window (seqs above front), never the emission history. Both maps
// used to be write-only — a run that processed N messages left N entries in
// each, so a long-lived pipeline grew without bound until OOM.

// TestSrcTrackerPrunesSweptRange drives 100k arrived/committed seqs in
// 64-wide blocks committed as two out-of-order halves: the frontier stalls at
// the hole until the lower half lands, so the sweep must tolerate
// out-of-order commits above front while still deleting everything it passes.
// The per-block bounds fail from the second block on without the prune (the
// previous block's 64 entries would still be resident), and the final drain
// check fails with all 100k.
func TestSrcTrackerPrunesSweptRange(t *testing.T) {
	const n = 100_000
	const window = 64
	s := newSrcTracker()
	rnd := rand.New(rand.NewSource(1))

	for base := int64(0); base < n; base += window {
		hi := base + window
		if hi > n {
			hi = n
		}
		block := make([]int64, 0, hi-base)
		for seq := base + 1; seq <= hi; seq++ {
			s.arrived(seq)
			block = append(block, seq)
		}
		rnd.Shuffle(len(block), func(a, b int) { block[a], block[b] = block[b], block[a] })
		mid := len(block) / 2

		// Upper half first: the frontier cannot pass the block (at least one
		// seq below hi is still uncommitted) and the half must survive above
		// front until the frontier reaches it.
		s.committed(block[mid:])
		if got := s.frontier(); got < base || got >= hi {
			t.Fatalf("block %d..%d: front = %d after the upper half, want within [%d,%d)",
				base+1, hi, got, base, hi)
		}
		if got := len(s.arrivedAt); got > window {
			t.Fatalf("block %d..%d: arrivedAt holds %d entries, want <= %d", base+1, hi, got, window)
		}
		if got := len(s.committedAt); got > window {
			t.Fatalf("block %d..%d: committedAt holds %d entries, want <= %d", base+1, hi, got, window)
		}

		// Lower half closes the hole: the frontier sweeps the whole block and
		// must leave nothing behind.
		s.committed(block[:mid])
		if got := s.frontier(); got != hi {
			t.Fatalf("block %d..%d: front = %d after the full block committed, want %d", base+1, hi, got, hi)
		}
		if len(s.arrivedAt) != 0 || len(s.committedAt) != 0 {
			t.Fatalf("block %d..%d: maps hold %d/%d entries below front, want 0/0",
				base+1, hi, len(s.arrivedAt), len(s.committedAt))
		}
	}
	if got := s.frontier(); got != n {
		t.Fatalf("frontier = %d, want %d", got, n)
	}
}

// snapshot()'s outstanding total is a maintained counter (openBranches), not
// a map walk under the lock — it is polled from the hot path (WaitCommit's
// 2ms loop, Quiesced, ops status). Every mutation site must adjust it by
// exactly the change in the positive-value sum it replaced: this
// differential test drives all mutation APIs at random (arrived, fan-out
// add, done — including double-terminal — and forceTerminal) and compares
// the counter against a recomputed map sum after every step, then verifies
// the full-commit drain reaches zero with the contiguous prefix closed.
func TestCommitTrackerOpenCountMatchesMap(t *testing.T) {
	tr := newCommitTracker("p", nil, nil, nil)
	rnd := rand.New(rand.NewSource(7))
	recount := func() int {
		open := 0
		for _, v := range tr.outstanding {
			open += posBranches(v)
		}
		return open
	}
	for i := 0; i < 500; i++ {
		seq := int64(rnd.Intn(40) + 1)
		switch rnd.Intn(5) {
		case 0:
			tr.arrived(seq, "", 0)
		case 1:
			tr.add(seq, rnd.Intn(3)) // fan-out expansion (split, extra edges)
		case 2, 3:
			tr.done(seq)
		case 4:
			tr.forceTerminal(seq) // abandon path
		}
		outstanding, committedThrough, arrivedMax := tr.snapshot()
		if want := recount(); outstanding != want {
			t.Fatalf("step %d: snapshot outstanding = %d, want map sum %d", i, outstanding, want)
		}
		if committedThrough > arrivedMax {
			t.Fatalf("step %d: committedThrough %d > arrivedMax %d", i, committedThrough, arrivedMax)
		}
	}
	// Drain: every open branch terminal ⇒ outstanding must read exactly 0
	// (WaitCommit/Quiesced terminate on it) and the contiguous prefix must
	// have closed up to the highest arrival.
	for seq := int64(1); seq <= 40; seq++ {
		for tr.isOutstanding(seq) {
			tr.done(seq)
		}
	}
	outstanding, committedThrough, arrivedMax := tr.snapshot()
	if outstanding != 0 || recount() != 0 {
		t.Fatalf("after full drain: outstanding = %d (map sum %d), want 0", outstanding, recount())
	}
	if committedThrough != arrivedMax {
		t.Fatalf("after full drain: committedThrough = %d, want arrivedMax %d", committedThrough, arrivedMax)
	}
}

// TestCommitTrackerStragglerDeliversTerminalEvent: a seq whose arrived() lands
// after the contiguous-prefix sweep passed it — the AppendSpool→arrived window
// between two concurrent sources — must still get its per-message terminal
// event when its branches finish. Missing it leaks the admission slot (release
// runs only from onCommit), the accept-time entry and the commit count while
// the tracker still reads quiescent (2026-09-25: the group-commit writer made
// this window routine because a batch's waiters wake in completion order, not
// seq order; the engine benchmark stranded messages before the fix).
func TestCommitTrackerStragglerDeliversTerminalEvent(t *testing.T) {
	var commits []int64
	tr := newCommitTracker("p", []string{"in"}, func(seq int64) { commits = append(commits, seq) }, nil)

	// Seq 2 arrives and completes while seq 1 is still between AppendSpool
	// and arrived: the sweep treats the not-yet-registered 1 as committed and
	// moves the prefix past it (the documented tolerance for holes).
	tr.arrived(2, "in", 2)
	tr.done(2)
	if len(commits) != 1 || commits[0] != 2 {
		t.Fatalf("commits after seq 2 = %v, want [2]", commits)
	}
	if out, through, _ := tr.snapshot(); out != 0 || through != 2 {
		t.Fatalf("after seq 2: outstanding=%d through=%d, want 0/2", out, through)
	}

	// The straggler registers below the swept prefix and fans out: no
	// terminal event until every branch is done (arrived registers one unit;
	// fanOut adds len(matched)-1, so +1 makes two branches).
	tr.arrived(1, "in", 1)
	tr.add(1, 1)
	if len(commits) != 1 {
		t.Fatalf("straggler committed with open branches: %v", commits)
	}
	tr.done(1)
	if len(commits) != 1 {
		t.Fatalf("straggler committed with one branch still open: %v", commits)
	}
	tr.done(1)
	if len(commits) != 2 || commits[1] != 1 {
		t.Fatalf("commits = %v, want [2 1]: the straggler's terminal event was lost", commits)
	}
	if out, _, _ := tr.snapshot(); out != 0 {
		t.Fatalf("outstanding after the straggler drained = %d, want 0", out)
	}
	if _, open := tr.outstanding[1]; open {
		t.Fatal("straggler left in the outstanding map")
	}
}

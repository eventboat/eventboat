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
	// have closed up to the highest arrival. The random walk can leave a seq
	// it never arrived() (seq 14 under seed 7) — a permanent version of the
	// AppendSpool→arrived window, which the tracker now treats as a barrier
	// (BF1), so the drain registers each leftover hole before terminating it.
	for seq := int64(1); seq <= 40; seq++ {
		tr.arrived(seq, "", 0)
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

// TestCommitTrackerStragglerDeliversTerminalEvent: SEMANTICS CHANGED
// 2026-09-25 (adversarial review B, BF1). The sweep used to treat a missing
// outstanding entry as committed, so a seq whose arrived() had not landed yet
// (the AppendSpool→arrived window) was crossed and the durable checkpoint
// advanced over a durable-but-undelivered row; a crash then replayed from
// beyond it and a no-cursor source lost it for good. The hole is now a
// barrier: an unregistered seq pins the prefix (advanceLocked). This test
// drives the pre-BF1 scenario — seq 2 completes while seq 1 sits in the
// window — and asserts the new contract: nothing commits while the hole is
// open, and when seq 1 finally registers, 1 and 2 commit together, in order.
// The straggler branch survives as the forceTerminal race guard and has its
// own test (TestCommitTrackerStragglerGuardAfterForceTerminal).
func TestCommitTrackerStragglerDeliversTerminalEvent(t *testing.T) {
	var commits []int64
	var fronts []int64
	tr := newCommitTracker("p", []string{"in"},
		func(seq int64) { commits = append(commits, seq) },
		func(through int64, f map[string]int64) { fronts = append(fronts, f["in"]) })

	// Seq 2 arrives and completes while seq 1 is still between AppendSpool
	// and arrived. The sweep stops at 1 (a hole, not a commit): nothing may
	// commit, the prefix stays below the barrier, and 2's srcRef waits.
	tr.arrived(2, "in", 2)
	tr.done(2)
	if len(commits) != 0 {
		t.Fatalf("commits = %v, want none: the unregistered hole must be a barrier", commits)
	}
	if out, through, arrived := tr.snapshot(); out != 0 || through != 0 || arrived != 2 {
		t.Fatalf("after seq 2: outstanding=%d through=%d arrived=%d, want 0/0/2", out, through, arrived)
	}
	if len(tr.srcRefs) != 1 || tr.srcRefs[0].seq != 2 {
		t.Fatalf("srcRefs = %+v, want seq 2 still queued behind the barrier", tr.srcRefs)
	}

	// The straggler registers below the barrier and fans out: no terminal
	// event until every branch is done (arrived registers one unit; fanOut
	// adds len(matched)-1, so +1 makes two branches).
	tr.arrived(1, "in", 1)
	tr.add(1, 1)
	if len(commits) != 0 {
		t.Fatalf("straggler committed with open branches: %v", commits)
	}
	tr.done(1)
	if len(commits) != 0 {
		t.Fatalf("straggler committed with one branch still open: %v", commits)
	}
	tr.done(1)
	// The hole is closed: both rows commit together, in seq order.
	if len(commits) != 2 || commits[0] != 1 || commits[1] != 2 {
		t.Fatalf("commits = %v, want [1 2]", commits)
	}
	if out, through, _ := tr.snapshot(); out != 0 || through != 2 {
		t.Fatalf("after the hole closed: outstanding=%d through=%d, want 0/2", out, through)
	}
	if len(tr.srcRefs) != 0 {
		t.Fatalf("srcRefs not swept: %+v", tr.srcRefs)
	}
	if len(fronts) != 1 || fronts[0] != 2 {
		t.Fatalf("posted source frontiers = %v, want [2]", fronts)
	}
	if _, open := tr.outstanding[1]; open {
		t.Fatal("straggler left in the outstanding map")
	}
}

// TestCommitTrackerSourceFrontierAfterHole (BF3): once the hole closes, the
// per-source frontier must advance to the swept prefix and the srcTracker
// bookkeeping must be pruned. The BF3 defect was a straggler whose srcRefs
// were never popped, leaving committedThrough=2 with a source frontier of 0
// plus arrivedAt/srcRefs residue.
func TestCommitTrackerSourceFrontierAfterHole(t *testing.T) {
	var frontiers []int64
	tr := newCommitTracker("p", []string{"in"}, nil,
		func(through int64, f map[string]int64) { frontiers = append(frontiers, f["in"]) })

	tr.arrived(2, "in", 2)
	tr.done(2) // barrier at 1: no frontier advance
	if len(frontiers) != 0 {
		t.Fatalf("frontiers posted behind the barrier: %v", frontiers)
	}
	tr.arrived(1, "in", 1)
	tr.done(1) // hole closed: 1 and 2 commit and both srcRefs are swept

	if got := tr.srcs["in"].frontier(); got != 2 {
		t.Fatalf("source frontier = %d, want 2 (BF3: stalled below the checkpoint)", got)
	}
	if len(tr.srcRefs) != 0 {
		t.Fatalf("srcRefs = %+v, want empty", tr.srcRefs)
	}
	if n := len(tr.srcs["in"].arrivedAt); n != 0 {
		t.Fatalf("arrivedAt holds %d entries after the sweep, want 0", n)
	}
	if n := len(tr.srcs["in"].committedAt); n != 0 {
		t.Fatalf("committedAt holds %d entries after the sweep, want 0", n)
	}
	if len(frontiers) == 0 || frontiers[len(frontiers)-1] != 2 {
		t.Fatalf("posted frontiers = %v, want the last one 2", frontiers)
	}
	t.Logf("after the hole closed: source frontier=%d srcRefs=%d arrivedAt=%d committedAt=%d posted=%v",
		tr.srcs["in"].frontier(), len(tr.srcRefs), len(tr.srcs["in"].arrivedAt), len(tr.srcs["in"].committedAt), frontiers)
}

// TestCommitTrackerStragglerGuardAfterForceTerminal: the straggler branch is
// the guard for a forceTerminal racing an in-flight arrived(). The abandon
// path force-terminates an outstanding message (its durable dead letter is
// already written) while the admission's arrived() lands afterwards; the
// re-registered seq now sits below the cursor its removed mark pushed past,
// so only the straggler branch can deliver its terminal event. It must also
// sweep the re-registered source ref (BF3) or the ref and its arrivedAt entry
// leak.
func TestCommitTrackerStragglerGuardAfterForceTerminal(t *testing.T) {
	var commits []int64
	tr := newCommitTracker("p", []string{"in"},
		func(seq int64) { commits = append(commits, seq) }, nil)

	tr.arrived(1, "in", 1)
	if !tr.forceTerminal(1) {
		t.Fatal("forceTerminal(1) = false, want true")
	}
	if _, through, _ := tr.snapshot(); through != 1 {
		t.Fatalf("through after forceTerminal = %d, want 1 (the mark lets the prefix cross)", through)
	}
	// A late done() from a racing worker must be a no-op: the entry is gone
	// and must not be resurrected.
	tr.done(1)
	if len(commits) != 0 {
		t.Fatalf("late done after forceTerminal committed: %v", commits)
	}

	// The racing arrived() lands after the termination: the seq is below
	// committedPtr and only the straggler branch can deliver its event.
	tr.arrived(1, "in", 1)
	tr.done(1)
	if len(commits) != 1 || commits[0] != 1 {
		t.Fatalf("commits = %v, want [1]: the straggler guard lost the terminal event", commits)
	}
	if len(tr.srcRefs) != 0 {
		t.Fatalf("straggler source refs not swept: %+v", tr.srcRefs)
	}
	if _, open := tr.outstanding[1]; open {
		t.Fatal("straggler left in the outstanding map")
	}
	t.Logf("straggler guard: commits=%v srcRefs=%d after the forceTerminal race", commits, len(tr.srcRefs))
}

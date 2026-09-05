package engine

import (
	"sort"
	"sync"
)

// commitTracker counts outstanding execution branches per spooled message
// (redesign-v3.md §6.2). A message is committed when its outstanding count
// reaches zero; the checkpoint may only advance over the contiguous prefix of
// committed sequences (invariant 2).
type commitTracker struct {
	mu       sync.Mutex
	pipeline string

	outstanding  map[int64]int // spool seq -> outstanding branch count
	openBranches int           // running sum of positive outstanding values (keeps snapshot O(1))
	arrivedMax   int64         // highest seq appended (and pre-registered)
	committedPtr int64         // next candidate seq for the contiguous prefix

	// srcRefs is the FIFO of source emissions awaiting their commit sweep,
	// ordered by spool seq (this run only). Arrival is near-ordered: each
	// source goroutine appends and registers back-to-back, but two sources
	// racing through accept can register out of order — addSrcRef splices
	// those rare inversions into place, so the head is always the lowest
	// outstanding seq and the sweep pops a contiguous prefix.
	srcRefs []srcRefEntry

	srcs map[string]*srcTracker

	onCommit  func(seq int64) // per-message commit hook (backpressure release)
	onAdvance func(committedThrough int64, frontiers map[string]int64)
}

type srcRef struct {
	node   string
	srcSeq int64
}

// srcRefEntry is one source emission tagged with its spool seq (the FIFO
// ordering key; the payload is the srcRef it resolves to at sweep time).
type srcRefEntry struct {
	seq int64
	srcRef
}

func newCommitTracker(pipeline string, sources []string, onCommit func(int64), onAdvance func(int64, map[string]int64)) *commitTracker {
	t := &commitTracker{
		pipeline:    pipeline,
		outstanding: map[int64]int{},
		srcs:        map[string]*srcTracker{},
		onCommit:    onCommit,
		onAdvance:   onAdvance,
	}
	for _, s := range sources {
		t.srcs[s] = newSrcTracker()
	}
	return t
}

// arrived pre-registers one outstanding unit for a freshly spooled message
// and records its source emission (nil node = injected/replayed, not
// attributable to a live source emission of this run).
func (t *commitTracker) arrived(seq int64, node string, srcSeq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.openBranches += 1 - posBranches(t.outstanding[seq])
	t.outstanding[seq] = 1
	if seq > t.arrivedMax {
		t.arrivedMax = seq
	}
	if node != "" {
		if st, ok := t.srcs[node]; ok {
			st.arrived(srcSeq)
			t.addSrcRef(seq, node, srcSeq)
		}
	}
}

// addSrcRef records one source emission in the srcRefs FIFO. The common case
// is an append (spool seqs from a monotonic counter, registered per source
// goroutine back-to-back); a concurrent accept from another source can
// register a lower seq after a higher one, so that rare inversion splices
// into its sorted position — the queue stays ordered, which is what lets the
// commit sweep pop from the head instead of scanning the whole in-flight
// window on every advance (the old map scan was O(high watermark) per
// message; the pop is O(messages swept)).
func (t *commitTracker) addSrcRef(seq int64, node string, srcSeq int64) {
	entry := srcRefEntry{seq: seq, srcRef: srcRef{node: node, srcSeq: srcSeq}}
	n := len(t.srcRefs)
	if n == 0 || seq > t.srcRefs[n-1].seq {
		t.srcRefs = append(t.srcRefs, entry)
		return
	}
	i := sort.Search(n, func(i int) bool { return t.srcRefs[i].seq >= seq })
	t.srcRefs = append(t.srcRefs, srcRefEntry{})
	copy(t.srcRefs[i+1:], t.srcRefs[i:])
	t.srcRefs[i] = entry
}

// add applies a delta to the outstanding count of a message (fan-out adds,
// terminal events subtract). A delta for an unknown seq is a no-op: the
// message was already terminal (forceTerminal/Abandon removed it), and a
// late done() from a racing worker must not resurrect it.
func (t *commitTracker) add(seq int64, delta int) {
	t.mu.Lock()
	if _, open := t.outstanding[seq]; !open {
		t.mu.Unlock()
		return
	}
	before := t.outstanding[seq]
	t.outstanding[seq] += delta
	t.openBranches += posBranches(t.outstanding[seq]) - posBranches(before)
	justCommit, advanced, through, frontiers := t.advanceLocked()
	t.mu.Unlock()
	t.invoke(justCommit, advanced, through, frontiers)
}

// forceTerminal removes a message from the outstanding set without a terminal
// branch event (canceled runs dead-letter outstanding messages directly,
// M2 review R2). It reports whether the message was actually outstanding.
// The checkpoint prefix may then advance past it.
func (t *commitTracker) forceTerminal(seq int64) bool {
	t.mu.Lock()
	if _, open := t.outstanding[seq]; !open {
		t.mu.Unlock()
		return false
	}
	t.openBranches -= posBranches(t.outstanding[seq])
	delete(t.outstanding, seq)
	justCommit, advanced, through, frontiers := t.advanceLocked()
	t.mu.Unlock()
	t.invoke(justCommit, advanced, through, frontiers)
	return true
}

// done marks one branch terminal.
func (t *commitTracker) done(seq int64) { t.add(seq, -1) }

// advanceLocked runs the contiguous-prefix scan under t.mu and returns the
// callback payload; it must not itself invoke the callbacks.
func (t *commitTracker) advanceLocked() (justCommit []int64, advanced bool, through int64, frontiers map[string]int64) {
	for t.committedPtr <= t.arrivedMax {
		if n, open := t.outstanding[t.committedPtr]; !open || n <= 0 {
			if open {
				delete(t.outstanding, t.committedPtr)
				justCommit = append(justCommit, t.committedPtr)
			}
			t.committedPtr++
			advanced = true
			continue
		}
		break
	}
	if len(justCommit) == 0 && !advanced {
		return
	}
	through = t.committedPtr - 1
	committed := map[string][]int64{}
	// Pop the swept prefix off the srcRefs FIFO (head-first; addSrcRef keeps
	// the queue ordered). Re-slicing drops the entry without a second
	// structure, and append reclaims the consumed capacity on its next
	// reallocation, so the queue stays bounded by the in-flight window.
	for len(t.srcRefs) > 0 && t.srcRefs[0].seq <= through {
		ref := t.srcRefs[0].srcRef
		t.srcRefs = t.srcRefs[1:]
		committed[ref.node] = append(committed[ref.node], ref.srcSeq)
	}
	for node, seqs := range committed {
		if st, ok := t.srcs[node]; ok {
			st.committed(seqs)
		}
	}
	frontiers = make(map[string]int64, len(t.srcs))
	for node, st := range t.srcs {
		frontiers[node] = st.frontier()
	}
	return
}

// invoke runs the commit callbacks WITHOUT holding t.mu (beta hardening:
// the callbacks do store IO — checkpoint, source states — and the tracker
// lock must not convoy on fsync). Correctness is preserved engine-side:
// persistCheckpoint's monotonic guards (persistMu) make out-of-order flushes
// safe, and the observers that used to imply "committed ⇒ persisted" by
// polling snapshot() — WaitCommit, Quiesced — now wait on an explicit flush
// barrier (durableThrough). The callbacks still run on the goroutine that
// committed the message, so same-goroutine observers (a sink worker's next
// write, a test polling the durable store) keep the old ordering.
func (t *commitTracker) invoke(justCommit []int64, advanced bool, through int64, frontiers map[string]int64) {
	if len(justCommit) == 0 && !advanced {
		return
	}
	if t.onCommit != nil {
		for _, seq := range justCommit {
			t.onCommit(seq)
		}
	}
	if advanced && t.onAdvance != nil {
		t.onAdvance(through, frontiers)
	}
}

// isOutstanding reports whether a message still has open branches.
func (t *commitTracker) isOutstanding(seq int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, open := t.outstanding[seq]
	return open && n > 0
}

// posBranches is one seq's contribution to openBranches: only positive
// counts are open work (a 0 entry awaits its sweep; a negative value would
// be a double-terminal bug, and reads as closed either way). Every mutation
// of t.outstanding adjusts openBranches by the change in this quantity, all
// under t.mu.
func posBranches(v int) int {
	if v > 0 {
		return v
	}
	return 0
}

// snapshot reports counters for tests and status output. The outstanding
// total is maintained incrementally (openBranches): snapshot is polled from
// the hot path (WaitCommit's 2ms loop, Quiesced, ops status), so it must not
// walk the in-flight map under the lock. TestCommitTrackerOpenCountMatchesMap
// pins it to the map it summarizes.
func (t *commitTracker) snapshot() (outstanding int, committedThrough int64, arrivedMax int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.openBranches, t.committedPtr - 1, t.arrivedMax
}

// srcTracker tracks per-source commit frontiers for emissions of THIS run.
// Replayed spool rows are not attributable to this run's source counter and
// are ignored (redesign-v3-review.md: crash recovery replays from spool;
// sources resume from their committed watermark and may re-emit the uncommitted
// tail — duplicate delivery, never loss).
type srcTracker struct {
	mu          sync.Mutex
	arrivedAt   map[int64]bool // srcSeq emitted this run
	committedAt map[int64]bool
	front       int64
}

func newSrcTracker() *srcTracker {
	return &srcTracker{arrivedAt: map[int64]bool{}, committedAt: map[int64]bool{}}
}

func (s *srcTracker) arrived(seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arrivedAt[seq] = true
}

func (s *srcTracker) committed(seqs []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		s.committedAt[seq] = true
	}
	from := s.front + 1
	for {
		next := s.front + 1
		if s.committedAt[next] && s.arrivedAt[next] {
			s.front = next
			continue
		}
		break
	}
	// The swept range is dead state: front only ever passes seqs present in
	// both maps and never revisits them, so dropping them here keeps the maps
	// bounded by the in-flight window instead of growing with every message
	// the run processes. Emission seqs are monotonic within a run (sources
	// number them from a per-run counter; replayed spool rows never reach the
	// tracker), so no key at or below front can appear after the sweep —
	// commits landing above front simply wait for the frontier.
	for seq := from; seq <= s.front; seq++ {
		delete(s.arrivedAt, seq)
		delete(s.committedAt, seq)
	}
}

func (s *srcTracker) frontier() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.front
}

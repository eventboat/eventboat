package engine

import (
	"sync"
)

// settleTracker counts outstanding execution branches per spooled message
// (redesign-v3.md §6.2). A message is settled when its outstanding count
// reaches zero; the checkpoint may only advance over the contiguous prefix of
// settled sequences (invariant 2).
type settleTracker struct {
	mu       sync.Mutex
	pipeline string

	outstanding map[int64]int // spool seq -> outstanding branch count
	arrivedMax  int64         // highest seq appended (and pre-registered)
	settledPtr  int64         // next candidate seq for the contiguous prefix

	srcRefs map[int64]srcRef // spool seq -> source emission (this run only)
	srcs    map[string]*srcTracker

	onSettled func(seq int64) // per-message settle hook (backpressure release)
	onAdvance func(settledThrough int64, frontiers map[string]int64)
}

type srcRef struct {
	node   string
	srcSeq int64
}

func newSettleTracker(pipeline string, sources []string, onSettled func(int64), onAdvance func(int64, map[string]int64)) *settleTracker {
	t := &settleTracker{
		pipeline:    pipeline,
		outstanding: map[int64]int{},
		srcRefs:     map[int64]srcRef{},
		srcs:        map[string]*srcTracker{},
		onSettled:   onSettled,
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
func (t *settleTracker) arrived(seq int64, node string, srcSeq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outstanding[seq] = 1
	if seq > t.arrivedMax {
		t.arrivedMax = seq
	}
	if node != "" {
		if st, ok := t.srcs[node]; ok {
			st.arrived(srcSeq)
			t.srcRefs[seq] = srcRef{node: node, srcSeq: srcSeq}
		}
	}
}

// add applies a delta to the outstanding count of a message (fan-out adds,
// terminal events subtract).
func (t *settleTracker) add(seq int64, delta int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outstanding[seq] += delta
	t.maybeAdvanceLocked()
}

// done marks one branch terminal.
func (t *settleTracker) done(seq int64) { t.add(seq, -1) }

// maybeAdvanceLocked runs with t.mu held and keeps it through the callbacks:
// settle completion must imply that its persistence (checkpoint, source
// states) is visible — observers polling snapshot() only see zero after the
// durable writes finished. The callbacks are leaf operations (store writes,
// admission release) and never re-enter the tracker.
func (t *settleTracker) maybeAdvanceLocked() {
	advanced := false
	var justSettled []int64
	for t.settledPtr <= t.arrivedMax {
		if n, open := t.outstanding[t.settledPtr]; !open || n <= 0 {
			if open {
				delete(t.outstanding, t.settledPtr)
				justSettled = append(justSettled, t.settledPtr)
			}
			t.settledPtr++
			advanced = true
			continue
		}
		break
	}
	if len(justSettled) == 0 && !advanced {
		return
	}
	through := t.settledPtr - 1
	settled := map[string][]int64{}
	for seq := range t.srcRefs {
		if seq <= through {
			ref := t.srcRefs[seq]
			settled[ref.node] = append(settled[ref.node], ref.srcSeq)
			delete(t.srcRefs, seq)
		}
	}
	for node, seqs := range settled {
		if st, ok := t.srcs[node]; ok {
			st.settled(seqs)
		}
	}
	frontiers := make(map[string]int64, len(t.srcs))
	for node, st := range t.srcs {
		frontiers[node] = st.frontier()
	}
	if t.onSettled != nil {
		for _, seq := range justSettled {
			t.onSettled(seq)
		}
	}
	if advanced && t.onAdvance != nil {
		t.onAdvance(through, frontiers)
	}
}

// snapshot reports counters for tests and status output.
func (t *settleTracker) snapshot() (outstanding int, settledThrough int64, arrivedMax int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	open := 0
	for _, n := range t.outstanding {
		if n > 0 {
			open += n
		}
	}
	return open, t.settledPtr - 1, t.arrivedMax
}

// srcFrontier returns the contiguous settled srcSeq of one source.
func (t *settleTracker) srcFrontier(node string) int64 {
	t.mu.Lock()
	st := t.srcs[node]
	t.mu.Unlock()
	if st == nil {
		return 0
	}
	return st.frontier()
}

// srcTracker tracks per-source settle frontiers for emissions of THIS run.
// Replayed spool rows are not attributable to this run's source counter and
// are ignored (redesign-v3-review.md: crash recovery replays from spool;
// sources resume from their settled watermark and may re-emit the unsettled
// tail — duplicate delivery, never loss).
type srcTracker struct {
	mu        sync.Mutex
	arrivedAt map[int64]bool // srcSeq emitted this run
	settledAt map[int64]bool
	front     int64
}

func newSrcTracker() *srcTracker {
	return &srcTracker{arrivedAt: map[int64]bool{}, settledAt: map[int64]bool{}}
}

func (s *srcTracker) arrived(seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arrivedAt[seq] = true
}

func (s *srcTracker) settled(seqs []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		s.settledAt[seq] = true
	}
	for {
		next := s.front + 1
		if s.settledAt[next] && s.arrivedAt[next] {
			s.front = next
			continue
		}
		break
	}
}

func (s *srcTracker) frontier() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.front
}

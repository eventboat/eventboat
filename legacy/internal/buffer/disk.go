package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/riverpod/riverpod/internal/message"
)

const (
	DefaultSegmentSize  = 64 << 20 // 64MB
	DefaultDiskMaxSize  = 1 << 30  // 1GB
	DefaultSyncInterval = 500 * time.Millisecond
	defaultDiskRoot     = "buffers"
)

type offsetState struct {
	Segment string `json:"segment"`
	Offset  int64  `json:"offset"`
}

// pendingRead tracks a record that was read from the WAL but not yet acked.
// The consumer offset is only committed for the acked prefix of this queue.
type pendingRead struct {
	segment string
	end     int64 // offset just past this record within the segment
	acked   bool
}

// recordKey locates one WAL record within a segment, used to re-attach the
// in-process ack chain when the record is read back in the same run.
type recordKey struct {
	segment string
	end     int64
}

// DiskWAL persists edge messages in segmented append-only files.
type DiskWAL struct {
	dir            string
	segmentSize    int64
	maxSize        int64
	syncInterval   time.Duration
	mu             sync.Mutex
	currentSegment string
	currentFile    *os.File
	currentSize    int64
	totalSize      int64
	readSegment    string
	readFile       *os.File
	readOffset     int64
	offset         offsetState
	pending        []pendingRead
	pendingBase    uint64
	nextSeq        uint64
	deferred       map[string]int64 // fully read segments kept until their messages are acked
	ackFns         map[recordKey]func(error) // in-process ack chains of appended records, re-attached on read
	wakeCh         chan struct{}    // signalled on Append so idle readers re-poll the WAL
	dirty          bool
	closed         bool
	stopSync       chan struct{}
	syncWG         sync.WaitGroup
}

type DiskOptions struct {
	Dir          string
	SegmentSize  int64
	MaxSize      int64
	SyncInterval time.Duration
}

func NewDiskWAL(opts DiskOptions) (*DiskWAL, error) {
	dir := opts.Dir
	if dir == "" {
		dir = defaultDiskRoot
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segSize := opts.SegmentSize
	if segSize <= 0 {
		segSize = DefaultSegmentSize
	}
	maxSize := opts.MaxSize
	if maxSize <= 0 {
		maxSize = DefaultDiskMaxSize
	}
	syncInt := opts.SyncInterval
	if syncInt <= 0 {
		syncInt = DefaultSyncInterval
	}
	w := &DiskWAL{
		dir:          dir,
		segmentSize:  segSize,
		maxSize:      maxSize,
		syncInterval: syncInt,
		deferred:     make(map[string]int64),
		ackFns:       make(map[recordKey]func(error)),
		wakeCh:       make(chan struct{}, 1),
		stopSync:     make(chan struct{}),
	}
	if err := w.loadOffset(); err != nil {
		return nil, err
	}
	if err := w.scanTotalSize(); err != nil {
		return nil, err
	}
	if err := w.openReadSegment(); err != nil {
		return nil, err
	}
	w.syncWG.Add(1)
	go w.syncLoop()
	return w, nil
}

func (w *DiskWAL) Append(msg *message.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.totalSize >= w.maxSize {
		return fmt.Errorf("disk buffer full")
	}
	if err := w.ensureWriter(); err != nil {
		return err
	}
	before, _ := w.currentFile.Seek(0, io.SeekCurrent)
	if err := encodeWALRecord(w.currentFile, msg); err != nil {
		return err
	}
	after, _ := w.currentFile.Seek(0, io.SeekCurrent)
	written := after - before
	w.currentSize = after
	w.totalSize += written
	w.dirty = true
	// Stash the in-process ack chain (fan-out aggregator → source OnAck): the
	// WAL only serializes ID/Payload/Metadata, so the chain must be re-attached
	// when this record is read back during the same run. After a restart the
	// map is empty and replayed records only commit their WAL offset on ack —
	// a new source session must not ack offsets fetched by the dead one.
	if fn := msg.AckFn(); fn != nil {
		w.ackFns[recordKey{segment: w.currentSegment, end: after}] = fn
	}
	// wake a reader that went idle before this record landed on disk
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
	return nil
}

// WakeCh is signalled (non-blocking) whenever a record is appended, so a
// reader blocked on other inputs re-polls the WAL instead of sleeping past
// newly spilled messages.
func (w *DiskWAL) WakeCh() <-chan struct{} {
	return w.wakeCh
}

func (w *DiskWAL) ReadNext() (*message.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.readFile == nil {
			if err := w.openReadSegmentLocked(); err != nil {
				return nil, err
			}
			if w.readFile == nil {
				return nil, nil
			}
		}
		msg, err := decodeWALRecord(w.readFile)
		if err == nil {
			pos, _ := w.readFile.Seek(0, io.SeekCurrent)
			w.readOffset = pos
			// The record is consumed once the downstream reaches a terminal
			// ack: success, or a processing failure the engine already
			// disposed of (DLQ/drop). Shutdown nacks and crashes leave the
			// offset in place so the message is redelivered (at-least-once).
			seq := w.nextSeq
			w.nextSeq++
			w.pending = append(w.pending, pendingRead{segment: w.readSegment, end: pos})
			key := recordKey{segment: w.readSegment, end: pos}
			if fn, ok := w.ackFns[key]; ok {
				delete(w.ackFns, key)
				msg.SetAckFn(fn)
			}
			msg.WrapAckFn(func(ackErr error) {
				// Commit on success and on terminal processing failures:
				// once the engine has delivered the message to its final
				// disposition (DLQ or configured drop), the record is
				// consumed — holding the offset would stall the ack
				// watermark and block shutdown forever. Only shutdown nacks
				// (context cancellation) hold the offset, so an unprocessed
				// message is redelivered after a restart.
				if ackErr == nil || (!errors.Is(ackErr, context.Canceled) && !errors.Is(ackErr, context.DeadlineExceeded)) {
					w.commitPending(seq)
				}
			})
			return msg, nil
		}
		if errors.Is(err, errWALCorrupt) {
			// torn write or bit rot at the tail: drop the damaged bytes
			// and keep serving instead of stalling this edge forever.
			slog.Warn("discarding corrupt wal tail",
				"dir", w.dir, "segment", w.readSegment, "offset", w.readOffset, "err", err)
			if terr := w.truncateTailLocked(); terr != nil {
				return nil, terr
			}
			err = io.EOF
		}
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
		if w.readSegment == w.currentSegment {
			return nil, nil
		}
		_ = w.readFile.Close()
		w.readFile = nil
		seg := w.readSegment
		w.readSegment = ""
		if seg != "" {
			if w.hasUnackedForSegmentLocked(seg) {
				// Records from this segment are delivered but not yet
				// acked; keep the file so a crash can redeliver them.
				// It is removed once the ack watermark moves past it.
				info, statErr := os.Stat(filepath.Join(w.dir, seg))
				if statErr == nil {
					w.deferred[seg] = info.Size()
				}
			} else {
				w.removeSegmentLocked(seg)
			}
		}
		if err := w.openReadSegmentAfterLocked(seg); err != nil {
			return nil, err
		}
		if w.readFile == nil {
			return nil, nil
		}
	}
}

// commitPending marks a delivered record as acked and advances the committed
// consumer offset over the longest acked prefix. Deferred segments whose
// messages are all acked are removed once the watermark has passed them.
func (w *DiskWAL) commitPending(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := seq - w.pendingBase
	if idx >= uint64(len(w.pending)) {
		return
	}
	w.pending[idx].acked = true
	var last pendingRead
	advanced := false
	for len(w.pending) > 0 && w.pending[0].acked {
		last = w.pending[0]
		advanced = true
		w.pending = w.pending[1:]
		w.pendingBase++
	}
	if !advanced {
		return
	}
	if len(w.pending) > 0 && w.pending[0].segment != last.segment {
		// everything read so far from last.segment is acked; resume at
		// the start of the next segment after a crash.
		w.offset = offsetState{Segment: w.pending[0].segment, Offset: 0}
	} else {
		w.offset = offsetState{Segment: last.segment, Offset: last.end}
	}
	w.persistOffsetLocked()
	for seg, size := range w.deferred {
		if w.offset.Segment == seg || w.hasUnackedForSegmentLocked(seg) {
			continue
		}
		_ = os.Remove(filepath.Join(w.dir, seg))
		w.totalSize -= size
		if w.totalSize < 0 {
			w.totalSize = 0
		}
		delete(w.deferred, seg)
	}
}

func (w *DiskWAL) hasUnackedForSegmentLocked(seg string) bool {
	for i := range w.pending {
		if w.pending[i].segment == seg && !w.pending[i].acked {
			return true
		}
	}
	return false
}

// truncateTailLocked drops everything past the last intact record of the
// read segment, so a crash-torn or corrupted tail never stalls the reader.
func (w *DiskWAL) truncateTailLocked() error {
	seg := w.readSegment
	if seg == "" {
		return nil
	}
	path := filepath.Join(w.dir, seg)
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() <= w.readOffset {
		return nil
	}
	trimmed := info.Size() - w.readOffset
	if err := os.Truncate(path, w.readOffset); err != nil {
		return err
	}
	// records past the read offset are gone; drop their stashed ack chains
	for key := range w.ackFns {
		if key.segment == seg && key.end > w.readOffset {
			delete(w.ackFns, key)
		}
	}
	w.totalSize -= trimmed
	if w.totalSize < 0 {
		w.totalSize = 0
	}
	if seg == w.currentSegment {
		w.currentSize = w.readOffset
	}
	return nil
}

func (w *DiskWAL) removeSegmentLocked(seg string) {
	info, statErr := os.Stat(filepath.Join(w.dir, seg))
	if statErr == nil {
		w.totalSize -= info.Size()
		if w.totalSize < 0 {
			w.totalSize = 0
		}
	}
	_ = os.Remove(filepath.Join(w.dir, seg))
	if w.offset.Segment == seg {
		w.offset = offsetState{}
		w.persistOffsetLocked()
	}
}

// Pending reports whether the WAL still holds undelivered or unacked
// records. Being positioned at the EOF of the newest segment is caught up,
// not pending — otherwise a drained disk edge would block shutdown forever.
func (w *DiskWAL) Pending() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.pending {
		if !w.pending[i].acked {
			return true
		}
	}
	if w.readFile != nil {
		if w.readSegment != w.currentSegment {
			return true
		}
		return w.readOffset < w.currentSize
	}
	segs, err := w.listSegments()
	return err == nil && len(segs) > 0
}

func (w *DiskWAL) SizeBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalSize
}

func (w *DiskWAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	segs, err := w.listSegments()
	if err != nil {
		return 0
	}
	return len(segs)
}

func (w *DiskWAL) Fsync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fsyncLocked()
}

func (w *DiskWAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()
	close(w.stopSync)
	w.syncWG.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentFile != nil {
		_ = w.fsyncLocked()
		_ = w.currentFile.Close()
		w.currentFile = nil
	}
	if w.readFile != nil {
		_ = w.readFile.Close()
		w.readFile = nil
	}
	return nil
}

func (w *DiskWAL) syncLoop() {
	defer w.syncWG.Done()
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopSync:
			return
		case <-ticker.C:
			_ = w.Fsync()
		}
	}
}

func (w *DiskWAL) ensureWriter() error {
	if w.currentFile != nil && w.currentSize < w.segmentSize {
		return nil
	}
	if w.currentFile != nil {
		if err := w.fsyncLocked(); err != nil {
			return err
		}
		_ = w.currentFile.Close()
		w.currentFile = nil
	}
	name, err := w.nextSegmentName()
	if err != nil {
		return err
	}
	path := filepath.Join(w.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.currentSegment = name
	w.currentFile = f
	info, err := f.Stat()
	if err != nil {
		return err
	}
	w.currentSize = info.Size()
	return nil
}

func (w *DiskWAL) nextSegmentName() (string, error) {
	segs, err := w.listSegments()
	if err != nil {
		return "", err
	}
	next := 1
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		n, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(last, ".wal"), "seg-"))
		if err == nil {
			next = n + 1
		}
	}
	return fmt.Sprintf("seg-%06d.wal", next), nil
}

func (w *DiskWAL) listSegments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var segs []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".wal") {
			segs = append(segs, name)
		}
	}
	sort.Strings(segs)
	return segs, nil
}

func (w *DiskWAL) openReadSegment() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.openReadSegmentLocked()
}

func (w *DiskWAL) openReadSegmentLocked() error {
	if w.readFile != nil {
		return nil
	}
	segs, err := w.listSegments()
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}
	start := segs[0]
	offset := int64(0)
	if w.offset.Segment != "" {
		found := false
		for _, s := range segs {
			if s == w.offset.Segment {
				start = s
				offset = w.offset.Offset
				found = true
				break
			}
		}
		if !found {
			start = segs[0]
			offset = 0
		}
	}
	path := filepath.Join(w.dir, start)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
	}
	w.readSegment = start
	w.readFile = f
	w.readOffset = offset
	return nil
}

// openReadSegmentAfterLocked opens the first segment ordered after prev,
// used when the reader has finished a segment and moves to the next one.
func (w *DiskWAL) openReadSegmentAfterLocked(prev string) error {
	if w.readFile != nil {
		return nil
	}
	segs, err := w.listSegments()
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s > prev {
			path := filepath.Join(w.dir, s)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			w.readSegment = s
			w.readFile = f
			w.readOffset = 0
			return nil
		}
	}
	return nil
}

func (w *DiskWAL) loadOffset() error {
	path := filepath.Join(w.dir, "consumer.offset")
	// drop a stale temp file from a crashed atomic write
	if err := os.Remove(path + ".tmp"); err != nil && !os.IsNotExist(err) {
		return err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := json.Unmarshal(b, &w.offset); err != nil {
		// a torn offset file (legacy non-atomic write) must not kill the
		// edge: fall back to a full replay, which duplicates but never loses.
		slog.Warn("ignoring unreadable consumer offset, replaying from start",
			"dir", w.dir, "err", err)
		w.offset = offsetState{}
	}
	return nil
}

// persistOffsetLocked writes the consumer offset atomically: a temp file is
// fsynced first, then renamed over the target, so a crash can never leave a
// half-written offset behind.
func (w *DiskWAL) persistOffsetLocked() {
	path := filepath.Join(w.dir, "consumer.offset")
	b, err := json.Marshal(w.offset)
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return
	}
	if err := f.Close(); err != nil {
		return
	}
	// Windows rename cannot overwrite an existing file
	_ = os.Remove(path)
	_ = os.Rename(tmp, path)
}

func (w *DiskWAL) scanTotalSize() error {
	segs, err := w.listSegments()
	if err != nil {
		return err
	}
	var total int64
	for _, s := range segs {
		info, err := os.Stat(filepath.Join(w.dir, s))
		if err != nil {
			return err
		}
		total += info.Size()
	}
	w.totalSize = total
	return nil
}

func (w *DiskWAL) fsyncLocked() error {
	if w.currentFile == nil || !w.dirty {
		return nil
	}
	if err := w.currentFile.Sync(); err != nil {
		return err
	}
	w.dirty = false
	w.persistOffsetLocked()
	return nil
}

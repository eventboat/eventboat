package buffer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/edgesets/edgestream/internal/message"
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
	dirty          bool
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
	return nil
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
			w.offset = offsetState{Segment: w.readSegment, Offset: w.readOffset}
			w.persistOffsetLocked()
			return msg, nil
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
			info, statErr := os.Stat(filepath.Join(w.dir, seg))
			if statErr == nil {
				w.totalSize -= info.Size()
				if w.totalSize < 0 {
					w.totalSize = 0
				}
			}
			_ = os.Remove(filepath.Join(w.dir, seg))
		}
		w.offset = offsetState{}
		w.persistOffsetLocked()
		if err := w.openReadSegmentLocked(); err != nil {
			return nil, err
		}
		if w.readFile == nil {
			return nil, nil
		}
	}
}

func (w *DiskWAL) Pending() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.readFile != nil {
		return true
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

func (w *DiskWAL) loadOffset() error {
	path := filepath.Join(w.dir, "consumer.offset")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(b, &w.offset)
}

func (w *DiskWAL) persistOffsetLocked() {
	path := filepath.Join(w.dir, "consumer.offset")
	b, _ := json.Marshal(w.offset)
	_ = os.WriteFile(path, b, 0o644)
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

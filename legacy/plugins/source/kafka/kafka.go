package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
	"github.com/riverpod/riverpod/internal/stage"
	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// commitTimeout bounds offset commits so OnAck cannot hang indefinitely
// (CommitMessages has no internal deadline when given context.Background).
const commitTimeout = 10 * time.Second

func init() {
	registry.RegisterSource("kafka", newSource)
}

func newSource(id string, cfg map[string]any) (stage.Source, error) {
	brokers := basestage.ConfigStringSlice(cfg, "brokers")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka source: brokers is required")
	}
	topics := basestage.ConfigStringSlice(cfg, "topics")
	if len(topics) == 0 {
		topics = basestage.ConfigStringSlice(cfg, "topic")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka source: topics is required")
	}
	groupID := basestage.ConfigString(cfg, "group_id")
	if groupID == "" {
		groupID = id
	}
	minBytes := basestage.ConfigInt(cfg, "min_bytes", 1)
	maxBytes := basestage.ConfigInt(cfg, "max_bytes", 10e6)
	return &Source{
		Base:    basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: "kafka"},
		brokers: brokers,
		topics:  topics,
		groupID: groupID,
		minBytes: minBytes,
		maxBytes: maxBytes,
		commitTimeout: commitTimeout,
		pending: make(map[string]kafkago.Message),
	}, nil
}

type Source struct {
	basestage.Base
	brokers  []string
	topics   []string
	groupID  string
	minBytes int
	maxBytes int

	// commitTimeout bounds offset commits; a field so tests can shorten it.
	commitTimeout time.Duration

	mu      sync.Mutex
	reader  *kafkago.Reader
	closed  bool
	pending map[string]kafkago.Message
}

func (s *Source) Init(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = false
	s.reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  s.brokers,
		GroupID:  s.groupID,
		GroupTopics: s.topics,
		MinBytes: s.minBytes,
		MaxBytes: s.maxBytes,
	})
	return nil
}

// Stop marks the source closed and clears pending offsets before closing the
// reader, so engine error paths that never deliver acks cannot leak the
// pending map, and no commit can race with reader.Close.
func (s *Source) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.pending = make(map[string]kafkago.Message)
	if s.reader != nil {
		err := s.reader.Close()
		s.reader = nil
		return err
	}
	return nil
}

func (s *Source) Consume(ctx context.Context, out chan<- *message.Message) error {
	for {
		s.mu.Lock()
		reader := s.reader
		s.mu.Unlock()
		if reader == nil {
			return fmt.Errorf("kafka source: not initialized")
		}
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		msgID := uuid.NewString()
		meta := map[string]any{
			"kafka.topic":     km.Topic,
			"kafka.partition": km.Partition,
			"kafka.offset":    km.Offset,
			"kafka.key":       string(km.Key),
		}
		msg := message.New(append([]byte(nil), km.Value...), meta)
		msg.ID = msgID
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return fmt.Errorf("kafka source: stopped")
		}
		s.pending[msgID] = km
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- msg:
		}
	}
}

// OnAck commits the message offset on success. The lock is held across the
// commit so Stop cannot close the reader mid-commit; after Stop the pending
// map is empty and commits are skipped safely.
func (s *Source) OnAck(msg *message.Message, err error) {
	if msg == nil || err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	km, ok := s.pending[msg.ID]
	if ok {
		delete(s.pending, msg.ID)
	}
	if !ok || s.closed || s.reader == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.commitTimeout)
	defer cancel()
	_ = s.reader.CommitMessages(ctx, km)
}

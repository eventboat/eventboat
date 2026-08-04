package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

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

	mu      sync.Mutex
	reader  *kafkago.Reader
	pending map[string]kafkago.Message
}

func (s *Source) Init(ctx context.Context) error {
	s.reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  s.brokers,
		GroupID:  s.groupID,
		GroupTopics: s.topics,
		MinBytes: s.minBytes,
		MaxBytes: s.maxBytes,
	})
	return nil
}

func (s *Source) Stop(ctx context.Context) error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

func (s *Source) Consume(ctx context.Context, out chan<- *message.Message) error {
	for {
		km, err := s.reader.FetchMessage(ctx)
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
		s.pending[msgID] = km
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- msg:
		}
	}
}

func (s *Source) OnAck(msg *message.Message, err error) {
	if msg == nil || err != nil {
		return
	}
	s.mu.Lock()
	km, ok := s.pending[msg.ID]
	if ok {
		delete(s.pending, msg.ID)
	}
	s.mu.Unlock()
	if !ok || s.reader == nil {
		return
	}
	_ = s.reader.CommitMessages(context.Background(), km)
}

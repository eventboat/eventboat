package builtin

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/eventboat/eventboat/internal/registry"
)

type kafkaSourceConfig struct {
	Brokers []string `json:"brokers" schema:"minItems=1,desc=Kafka broker addresses"`
	Topics  []string `json:"topics" schema:"minItems=1"`
	GroupID string   `json:"group_id" schema:"default=eventboat,desc=Consumer group id"`
}

func registerKafkaSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "kafka", 1, nil, func(c kafkaSourceConfig) (registry.Source, error) {
		return &kafkaSource{brokers: c.Brokers, topics: c.Topics, group: c.GroupID}, nil
	})
}

// kafkaSource consumes Kafka topics with kafka-go. Offsets are committed only
// when the engine reports messages committed (Commit), which preserves
// at-least-once across crashes.
type kafkaSource struct {
	brokers []string
	topics  []string
	group   string

	mu      sync.Mutex
	reader  *kafka.Reader
	pending map[int64]kafka.Message // srcSeq -> uncommitted message
	nextSeq int64
}

func (s *kafkaSource) Init(state []byte) error { return nil } // offsets live in the consumer group

func (s *kafkaSource) Run(ctx context.Context, emit func(registry.Message)) {
	s.mu.Lock()
	s.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		GroupID:     s.group,
		GroupTopics: s.topics,
	})
	s.pending = map[int64]kafka.Message{}
	s.mu.Unlock()
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		s.mu.Lock()
		s.nextSeq++
		seq := s.nextSeq
		s.pending[seq] = msg
		s.mu.Unlock()
		meta := map[string]any{
			"kafka_topic":     msg.Topic,
			"kafka_partition": int64(msg.Partition),
			"kafka_offset":    msg.Offset,
			"kafka_key":       string(msg.Key),
		}
		emit(registry.Message{Raw: msg.Value, Meta: meta, SrcName: "kafka", SrcSeq: seq})
	}
}

func (s *kafkaSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	var toCommit []kafka.Message
	for seq := int64(1); seq <= throughSrcSeq; seq++ {
		if m, ok := s.pending[seq]; ok {
			toCommit = append(toCommit, m)
			delete(s.pending, seq)
		}
	}
	r := s.reader
	s.mu.Unlock()
	if r == nil || len(toCommit) == 0 {
		return nil, nil
	}
	if err := r.CommitMessages(ctx, toCommit...); err != nil {
		return nil, fmt.Errorf("kafka source: commit: %w", err)
	}
	return nil, nil
}

func (s *kafkaSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

type kafkaSinkConfig struct {
	Brokers []string `json:"brokers" schema:"minItems=1,desc=Kafka broker addresses"`
	Topic   string   `json:"topic"`
}

func registerKafkaSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "kafka", 1, func(c kafkaSinkConfig) (registry.Sink, error) {
		return &kafkaSink{writer: &kafka.Writer{
			Addr:         kafka.TCP(c.Brokers...),
			Topic:        c.Topic,
			Balancer:     &kafka.Hash{}, // stable partitioning by Key when present
			RequiredAcks: kafka.RequireOne,
			BatchSize:    1, // engine owns batching
		}}, nil
	})
}

type kafkaSink struct {
	writer *kafka.Writer
}

func (s *kafkaSink) Write(ctx context.Context, msgs []registry.Message) error {
	kmsgs := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		kmsgs = append(kmsgs, kafka.Message{Key: m.Key, Value: encodedBytes(m), Headers: []kafka.Header{
			{Key: "eventboat-message-id", Value: []byte(m.ID)},
		}})
	}
	if err := s.writer.WriteMessages(ctx, kmsgs...); err != nil {
		return fmt.Errorf("kafka sink: %w", err)
	}
	return nil
}

func (s *kafkaSink) Close() error { return s.writer.Close() }

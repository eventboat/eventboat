package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	kafkago "github.com/segmentio/kafka-go"
)

func init() {
	registry.RegisterSink("kafka", newSink)
}

func newSink(id string, cfg map[string]any) (stage.Sink, error) {
	brokers := basestage.ConfigStringSlice(cfg, "brokers")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka sink: brokers is required")
	}
	topic := basestage.ConfigString(cfg, "topic")
	if topic == "" {
		return nil, fmt.Errorf("kafka sink: topic is required")
	}
	balancerName := basestage.ConfigString(cfg, "balancer")
	var balancer kafkago.Balancer
	switch balancerName {
	case "", "least_bytes":
		balancer = &kafkago.LeastBytes{}
	case "hash":
		balancer = &kafkago.Hash{}
	default:
		return nil, fmt.Errorf("kafka sink: unknown balancer %q (want least_bytes or hash)", balancerName)
	}
	return &Sink{
		Base:     basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "kafka"},
		brokers:  brokers,
		topic:    topic,
		balancer: balancer,
	}, nil
}

type Sink struct {
	basestage.Base
	brokers  []string
	topic    string
	balancer kafkago.Balancer
	writer   *kafkago.Writer
}

func (s *Sink) Init(ctx context.Context) error {
	s.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(s.brokers...),
		Topic:        s.topic,
		Balancer:     s.balancer,
		RequiredAcks: kafkago.RequireOne,
		BatchTimeout: 10 * time.Millisecond,
	}
	return nil
}

func (s *Sink) Stop(ctx context.Context) error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

func (s *Sink) Write(ctx context.Context, msgs []*message.Message) error {
	if s.writer == nil {
		return fmt.Errorf("kafka sink: not initialized")
	}
	kmsgs := make([]kafkago.Message, 0, len(msgs))
	for _, msg := range msgs {
		kmsgs = append(kmsgs, kafkago.Message{
			Topic: s.topic,
			Key:   messageKey(msg.Metadata),
			Value: msg.Payload,
		})
	}
	return s.writer.WriteMessages(ctx, kmsgs...)
}

// messageKey converts the kafka.key metadata entry to bytes; non-string
// scalars (e.g. numbers decoded from JSON) are stringified instead of dropped.
func messageKey(meta map[string]any) []byte {
	if meta == nil {
		return nil
	}
	key, ok := meta["kafka.key"]
	if !ok || key == nil {
		return nil
	}
	if s, ok := key.(string); ok {
		return []byte(s)
	}
	return []byte(fmt.Sprint(key))
}

// Flush is a no-op: kafka-go's WriteMessages is synchronous (it returns only
// after the brokers ack), so there is no client-side buffer to flush.
func (s *Sink) Flush(context.Context) error { return nil }

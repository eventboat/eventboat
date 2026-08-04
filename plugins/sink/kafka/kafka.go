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
	balancer := kafkago.Balancer(&kafkago.LeastBytes{})
	if basestage.ConfigString(cfg, "balancer") == "hash" {
		balancer = &kafkago.Hash{}
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
		km := kafkago.Message{
			Topic: s.topic,
			Value: msg.Payload,
		}
		if msg.Metadata != nil {
			if key, ok := msg.Metadata["kafka.key"].(string); ok {
				km.Key = []byte(key)
			}
		}
		kmsgs = append(kmsgs, km)
	}
	return s.writer.WriteMessages(ctx, kmsgs...)
}

func (s *Sink) Flush(context.Context) error { return nil }

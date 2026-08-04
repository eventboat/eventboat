package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	"github.com/robfig/cron/v3"
)

func init() {
	registry.RegisterSource("cron", newSource)
}

func newSource(id string, cfg map[string]any) (stage.Source, error) {
	schedule := basestage.ConfigString(cfg, "schedule")
	if schedule == "" {
		schedule = basestage.ConfigString(cfg, "cron")
	}
	if schedule == "" {
		return nil, fmt.Errorf("cron source: schedule is required (6-field cron with seconds, e.g. \"0 */1 * * * *\")")
	}
	loc := time.UTC
	if tz := basestage.ConfigString(cfg, "timezone"); tz != "" {
		parsed, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("cron source: invalid timezone %q: %w", tz, err)
		}
		loc = parsed
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(schedule); err != nil {
		return nil, fmt.Errorf("cron source: invalid schedule %q: %w", schedule, err)
	}
	return &Source{
		Base:     basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: "cron"},
		schedule: schedule,
		loc:      loc,
		body:     basestage.ConfigString(cfg, "payload"),
	}, nil
}

type Source struct {
	basestage.Base
	schedule string
	loc      *time.Location
	body     string

	mu      sync.Mutex
	out     chan<- *message.Message
	stopped atomic.Bool
}

func (s *Source) Consume(ctx context.Context, out chan<- *message.Message) error {
	s.mu.Lock()
	s.out = out
	s.stopped.Store(false)
	s.mu.Unlock()

	c := cron.New(
		cron.WithParser(cron.NewParser(cron.Second|cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor)),
		cron.WithLocation(s.loc),
	)
	_, err := c.AddFunc(s.schedule, func() {
		s.emit(ctx)
	})
	if err != nil {
		return err
	}
	c.Start()
	<-ctx.Done()
	// Mark stopped before stopping cron to prevent emit from sending on closed channels
	s.stopped.Store(true)
	stopCtx := c.Stop()
	select {
	case <-stopCtx.Done():
	case <-time.After(5 * time.Second):
	}
	return ctx.Err()
}

func (s *Source) emit(ctx context.Context) {
	if s.stopped.Load() {
		return
	}
	payload := []byte(s.body)
	if len(payload) == 0 {
		payload = []byte(`{"tick":true}`)
	}
	meta := map[string]any{
		"source":          "cron",
		"cron.schedule":   s.schedule,
	}
	var parsed map[string]any
	_ = json.Unmarshal(payload, &parsed)
	msg := message.New(payload, meta)
	msg.SetParsedCodec("json")
	if parsed != nil {
		msg.SetParsedData(parsed)
	}
	s.mu.Lock()
	out := s.out
	s.mu.Unlock()
	if out == nil {
		return
	}
	select {
	case <-ctx.Done():
	case out <- msg:
	default: // backpressure — drop, don't block cron scheduler
	}
}

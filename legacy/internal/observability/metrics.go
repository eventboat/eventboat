package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Prometheus collectors for the riverpod runtime (§10.2 Sprint 1 subset).
type Metrics struct {
	reg prometheus.Registerer

	EventsTotal       *prometheus.CounterVec
	EventLatency      *prometheus.HistogramVec
	InflightEvents    *prometheus.GaugeVec
	StageDuration     *prometheus.HistogramVec
	StageErrors       *prometheus.CounterVec
	EdgeBufferSize    *prometheus.GaugeVec
	EdgeDropped       *prometheus.CounterVec
	DLQEnqueued       *prometheus.CounterVec
	EnginePipelines   prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{reg: reg}

	m.EventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "riverpod_events_total",
		Help: "Total events processed through a pipeline.",
	}, []string{"pipeline", "status"})

	m.EventLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "riverpod_event_latency_seconds",
		Help:    "End-to-end event latency from source dispatch to final ack.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline"})

	m.InflightEvents = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "riverpod_inflight_events",
		Help: "Number of events currently in flight within a pipeline.",
	}, []string{"pipeline"})

	m.StageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "riverpod_stage_duration_seconds",
		Help:    "Stage processing duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline", "stage_id", "stage_kind"})

	m.StageErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "riverpod_stage_errors_total",
		Help: "Stage processing errors.",
	}, []string{"pipeline", "stage_id", "error_type"})

	m.EdgeBufferSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "riverpod_edge_buffer_size",
		Help: "Current edge inbound buffer occupancy.",
	}, []string{"pipeline", "from", "to"})

	m.EdgeDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "riverpod_edge_dropped_total",
		Help: "Messages dropped due to edge buffer policy.",
	}, []string{"pipeline", "from", "to", "reason"})

	m.DLQEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "riverpod_dlq_enqueued_total",
		Help: "Messages enqueued to DLQ.",
	}, []string{"pipeline", "dlq_stage_id", "error_type"})

	m.EnginePipelines = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "riverpod_engine_pipelines",
		Help: "Number of loaded pipelines.",
	})

	reg.MustRegister(
		m.EventsTotal,
		m.EventLatency,
		m.InflightEvents,
		m.StageDuration,
		m.StageErrors,
		m.EdgeBufferSize,
		m.EdgeDropped,
		m.DLQEnqueued,
		m.EnginePipelines,
	)
	return m
}

func (m *Metrics) Gatherer() prometheus.Gatherer {
	if g, ok := m.reg.(prometheus.Gatherer); ok {
		return g
	}
	return prometheus.DefaultGatherer
}

func (m *Metrics) SetPipelineCount(n int) {
	m.EnginePipelines.Set(float64(n))
}

func (m *Metrics) IncInflight(pipeline string) {
	m.InflightEvents.WithLabelValues(pipeline).Inc()
}

func (m *Metrics) DecInflight(pipeline string) {
	m.InflightEvents.WithLabelValues(pipeline).Dec()
}

func (m *Metrics) RecordEvent(pipeline, status string, d time.Duration) {
	m.EventsTotal.WithLabelValues(pipeline, status).Inc()
	m.EventLatency.WithLabelValues(pipeline).Observe(d.Seconds())
}

func (m *Metrics) ObserveStage(pipeline, stageID, kind string, d time.Duration) {
	m.StageDuration.WithLabelValues(pipeline, stageID, kind).Observe(d.Seconds())
}

func (m *Metrics) IncStageError(pipeline, stageID, errorType string) {
	m.StageErrors.WithLabelValues(pipeline, stageID, errorType).Inc()
}

func (m *Metrics) SetEdgeBuffer(pipeline, from, to string, size int) {
	m.EdgeBufferSize.WithLabelValues(pipeline, from, to).Set(float64(size))
}

func (m *Metrics) IncEdgeDropped(pipeline, from, to, reason string) {
	m.EdgeDropped.WithLabelValues(pipeline, from, to, reason).Inc()
}

func (m *Metrics) IncDLQ(pipeline, dlqStageID, errorType string) {
	m.DLQEnqueued.WithLabelValues(pipeline, dlqStageID, errorType).Inc()
}

func EventStatus(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

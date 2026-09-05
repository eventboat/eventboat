// Package obs is the OpenTelemetry foundation (redesign-v3.md §6.6): one
// MeterProvider with TWO readers — Prometheus exposition (pull, served at
// /metrics) and optional OTLP/HTTP export (push, when an endpoint is
// configured) — plus a TracerProvider (OTLP when configured, noop
// otherwise). Metric names carry the eventboat_ prefix; the implemented set
// is the review's list of 25 ("写下的 = 实现的"). All helpers are nil-receiver
// safe: a nil *Obs means telemetry is disabled.
package obs

import (
	"context"
	"net/http"
	"time"

	promc "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config mirrors runtimecfg.Telemetry.
type Config struct {
	OTLPEndpoint string
	SampleRatio  float64
	Prometheus   bool
}

// Obs owns the providers and the instrument set.
type Obs struct {
	meter          metric.Meter
	tracer         trace.Tracer
	provider       *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	handler        http.Handler // prometheus exposition (nil when disabled)

	// The 25 instruments (review §六), created once.
	MessagesIn            metric.Int64Counter
	MessagesCommitted     metric.Int64Counter
	DeadLettered          metric.Int64Counter
	DlqWriteFailures      metric.Int64Counter
	CelEvalErrors         metric.Int64Counter
	FanoutNoMatch         metric.Int64Counter
	DeliveryRetries       metric.Int64Counter
	OptionalDrops         metric.Int64Counter
	DecodeErrors          metric.Int64Counter
	SpoolFailures         metric.Int64Counter
	BackpressureEvents    metric.Int64Counter
	ScriptBudgetExhausted metric.Int64Counter
	WasmTimeouts          metric.Int64Counter
	JobsStarted           metric.Int64Counter
	JobsOverlapSkipped    metric.Int64Counter
	JobsCatchupSkipped    metric.Int64Counter
	JobsCompleted         metric.Int64Counter
	JobRowsRead           metric.Int64Counter
	JobRowsDelivered      metric.Int64Counter
	PluginRestarts        metric.Int64Counter

	ScriptDuration    metric.Float64Histogram
	SinkWriteDuration metric.Float64Histogram
	JobDuration       metric.Float64Histogram
	CommitLatency     metric.Float64Histogram
	WasmDuration      metric.Float64Histogram

	InFlight       metric.Float64Gauge
	SpoolDepth     metric.Float64Gauge
	PipelinePaused metric.Float64Gauge
}

// Setup builds the providers. A disabled Prometheus and empty endpoint
// yields a fully noop Obs (zero export overhead).
func Setup(ctx context.Context, cfg Config) (*Obs, error) {
	o := &Obs{}
	var readers []sdkmetric.Reader
	if cfg.Prometheus {
		promReg := promc.NewRegistry()
		prom, err := otelprom.New(otelprom.WithRegisterer(promReg))
		if err != nil {
			return nil, err
		}
		readers = append(readers, prom)
		o.handler = promhttp.HandlerFor(promReg, promhttp.HandlerOpts{})
	}
	if cfg.OTLPEndpoint != "" {
		otlp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(cfg.OTLPEndpoint),
			otlpmetrichttp.WithTimeout(5*time.Second))
		if err != nil {
			return nil, err
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlp, sdkmetric.WithInterval(10*time.Second)))
	}
	if len(readers) == 0 {
		o.meter = otel.GetMeterProvider().Meter("eventboat") // global noop
		o.tracer = otel.Tracer("eventboat")
		if err := o.createInstruments(); err != nil {
			return nil, err
		}
		return o, nil
	}
	var opts []sdkmetric.Option
	for _, r := range readers {
		opts = append(opts, sdkmetric.WithReader(r))
	}
	provider := sdkmetric.NewMeterProvider(opts...)
	o.provider = provider
	o.meter = provider.Meter("eventboat")

	if cfg.OTLPEndpoint != "" {
		exp, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(cfg.OTLPEndpoint),
			otlptracehttp.WithTimeout(5*time.Second))
		if err != nil {
			return nil, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
			sdktrace.WithBatcher(exp),
		)
		o.tracerProvider = tp
		otel.SetTracerProvider(tp)
		o.tracer = tp.Tracer("eventboat")
	} else {
		o.tracer = otel.Tracer("eventboat")
	}
	if err := o.createInstruments(); err != nil {
		return nil, err
	}
	return o, nil
}

// Handler returns the Prometheus exposition handler (nil when disabled).
func (o *Obs) Handler() http.Handler {
	if o == nil {
		return nil
	}
	return o.handler
}

// NewWithTracer builds an Obs with noop metrics and the given provider's
// tracer — tests inject in-memory recorders to assert span emission
// (production builds providers through Setup).
func NewWithTracer(tp trace.TracerProvider) (*Obs, error) {
	o := &Obs{}
	o.meter = otel.GetMeterProvider().Meter("eventboat")
	o.tracer = tp.Tracer("eventboat")
	if err := o.createInstruments(); err != nil {
		return nil, err
	}
	return o, nil
}

// Tracer returns the trace provider's tracer (nil-safe: a disabled tracer
// is a noop through the global provider).
func (o *Obs) Tracer() trace.Tracer {
	if o == nil {
		return otel.Tracer("eventboat")
	}
	return o.tracer
}

// Shutdown flushes and stops the providers.
func (o *Obs) Shutdown(ctx context.Context) error {
	if o == nil {
		return nil
	}
	var err error
	if o.tracerProvider != nil {
		if e := o.tracerProvider.Shutdown(ctx); e != nil {
			err = e
		}
	}
	if o.provider != nil {
		if e := o.provider.Shutdown(ctx); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (o *Obs) createInstruments() error {
	m := o.meter
	var err error
	newCounter := func(name, desc string) metric.Int64Counter {
		if err != nil {
			return nil
		}
		var c metric.Int64Counter
		c, err = m.Int64Counter(name, metric.WithDescription(desc))
		return c
	}
	newHist := func(name, desc string) metric.Float64Histogram {
		if err != nil {
			return nil
		}
		var h metric.Float64Histogram
		h, err = m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"))
		return h
	}
	newGauge := func(name, desc string) metric.Float64Gauge {
		if err != nil {
			return nil
		}
		var g metric.Float64Gauge
		g, err = m.Float64Gauge(name, metric.WithDescription(desc))
		return g
	}

	o.MessagesIn = newCounter("eventboat_messages_in_total", "Messages accepted into the spool")
	o.MessagesCommitted = newCounter("eventboat_messages_committed_total", "Messages reached a terminal state")
	o.DeadLettered = newCounter("eventboat_dead_letter_total", "Messages dead-lettered")
	o.DlqWriteFailures = newCounter("eventboat_dlq_write_failures_total", "Dead letter writes that failed (commit blocked)")
	o.CelEvalErrors = newCounter("eventboat_cel_eval_errors_total", "CEL predicate evaluation errors (treated as not-passed)")
	o.FanoutNoMatch = newCounter("eventboat_fanout_no_match_total", "Messages filtered by zero matching edges")
	o.DeliveryRetries = newCounter("eventboat_delivery_retries_total", "Delivery retry attempts")
	o.OptionalDrops = newCounter("eventboat_optional_drops_total", "Drops on failed optional (required:false) edges")
	o.DecodeErrors = newCounter("eventboat_decode_errors_total", "Decode failures at entry")
	o.SpoolFailures = newCounter("eventboat_spool_failures_total", "Spool append failures (message not delivered)")
	o.BackpressureEvents = newCounter("eventboat_backpressure_events_total", "Source admissions blocked by the high watermark")
	o.ScriptBudgetExhausted = newCounter("eventboat_script_step_budget_exhausted_total", "Starlark executions that hit the step budget")
	o.WasmTimeouts = newCounter("eventboat_wasm_timeouts_total", "WASM transform invocations killed by the per-invoke wall-clock budget (review-m3 R1)")
	o.JobsStarted = newCounter("eventboat_jobs_started_total", "Job runs started")
	o.JobsOverlapSkipped = newCounter("eventboat_jobs_overlap_skipped_total", "Triggers rejected by overlap:skip")
	o.JobsCatchupSkipped = newCounter("eventboat_jobs_catchup_skipped_total", "Missed schedule ticks outside the catchup window")
	o.JobsCompleted = newCounter("eventboat_jobs_completed_total", "Job runs completed by terminal status")
	o.JobRowsRead = newCounter("eventboat_job_rows_read_total", "Rows read by job runs")
	o.JobRowsDelivered = newCounter("eventboat_job_rows_delivered_total", "Rows delivered by job runs")
	o.PluginRestarts = newCounter("eventboat_plugin_restarts_total", "Supervisor respawns of crashed gRPC plugin processes (grpc.restart: restart)")

	o.ScriptDuration = newHist("eventboat_script_duration_seconds", "Starlark script execution duration")
	o.WasmDuration = newHist("eventboat_wasm_transform_duration_seconds", "WASM transform invocation duration")
	o.SinkWriteDuration = newHist("eventboat_sink_write_duration_seconds", "Sink batch write duration")
	o.JobDuration = newHist("eventboat_job_duration_seconds", "Job run wall-clock duration")
	o.CommitLatency = newHist("eventboat_commit_latency_seconds", "Accept-to-commit latency")

	o.InFlight = newGauge("eventboat_in_flight_messages", "Uncommitted messages in execution")
	o.SpoolDepth = newGauge("eventboat_spool_depth", "Spooled messages beyond the checkpoint")
	o.PipelinePaused = newGauge("eventboat_pipeline_paused", "1 when the pipeline is paused, else 0")
	return err
}

// --- engine/jobs-facing helpers (all nil-receiver safe) ---

// RecordMessageIn counts one accepted message.
func (o *Obs) RecordMessageIn(pipeline, source string) {
	if o == nil || o.MessagesIn == nil {
		return
	}
	o.MessagesIn.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("pipeline", pipeline), attribute.String("source", source)))
}

// RecordPluginRestart counts one supervisor respawn of a crashed external
// plugin process.
func (o *Obs) RecordPluginRestart(plugin string) {
	if o == nil || o.PluginRestarts == nil {
		return
	}
	o.PluginRestarts.Add(context.Background(), 1, metric.WithAttributes(attribute.String("plugin", plugin)))
}

// ReasonClass maps a dead-letter reason to a coarse class label.
func ReasonClass(reason string) string {
	for _, prefix := range []string{"script", "decode", "codec", "delivery", "encoder", "canceled"} {
		if len(reason) >= len(prefix) && reason[:len(prefix)] == prefix {
			return prefix
		}
	}
	return "other"
}

// RecordDeadLetter counts one dead letter.
func (o *Obs) RecordDeadLetter(pipeline, node, class string) {
	if o == nil || o.DeadLettered == nil {
		return
	}
	o.DeadLettered.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("pipeline", pipeline), attribute.String("node", node), attribute.String("reason_class", class)))
}

// RecordCommit counts one committed message and its latency.
func (o *Obs) RecordCommit(pipeline string, latency time.Duration) {
	if o == nil {
		return
	}
	ctx := context.Background()
	if o.MessagesCommitted != nil {
		o.MessagesCommitted.Add(ctx, 1, metric.WithAttributes(attribute.String("pipeline", pipeline)))
	}
	if o.CommitLatency != nil && latency > 0 {
		o.CommitLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attribute.String("pipeline", pipeline)))
	}
}

// RecordScript observes one script execution (duration; budget exhausted →
// the budget counter).
func (o *Obs) RecordScript(pipeline, node string, d time.Duration, budgetExhausted bool) {
	if o == nil {
		return
	}
	ctx := context.Background()
	attrs := metric.WithAttributes(attribute.String("pipeline", pipeline), attribute.String("node", node))
	if o.ScriptDuration != nil {
		o.ScriptDuration.Record(ctx, d.Seconds(), attrs)
	}
	if budgetExhausted && o.ScriptBudgetExhausted != nil {
		o.ScriptBudgetExhausted.Add(ctx, 1, attrs)
	}
}

// RecordSinkWrite observes one sink batch write attempt.
func (o *Obs) RecordSinkWrite(pipeline, node string, d time.Duration) {
	if o == nil || o.SinkWriteDuration == nil {
		return
	}
	o.SinkWriteDuration.Record(context.Background(), d.Seconds(), metric.WithAttributes(
		attribute.String("pipeline", pipeline), attribute.String("node", node)))
}

// RecordWasm observes one WASM transform invocation; timedOut feeds the
// per-invoke budget counter (review-m3 R1).
func (o *Obs) RecordWasm(pipeline, node string, d time.Duration, timedOut bool) {
	if o == nil {
		return
	}
	ctx := context.Background()
	attrs := metric.WithAttributes(attribute.String("pipeline", pipeline), attribute.String("node", node))
	if o.WasmDuration != nil {
		o.WasmDuration.Record(ctx, d.Seconds(), attrs)
	}
	if timedOut && o.WasmTimeouts != nil {
		o.WasmTimeouts.Add(ctx, 1, attrs)
	}
}

// RecordJobStart counts a run start by trigger.
func (o *Obs) RecordJobStart(pipeline, trigger string) {
	if o == nil || o.JobsStarted == nil {
		return
	}
	o.JobsStarted.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("pipeline", pipeline), attribute.String("trigger", trigger)))
}

// RecordJobEnd counts a terminal run with duration and row counts.
func (o *Obs) RecordJobEnd(pipeline, status string, d time.Duration, rows, delivered, dead int64) {
	if o == nil {
		return
	}
	ctx := context.Background()
	pa := attribute.String("pipeline", pipeline)
	if o.JobsCompleted != nil {
		o.JobsCompleted.Add(ctx, 1, metric.WithAttributes(pa, attribute.String("status", status)))
	}
	if o.JobDuration != nil {
		o.JobDuration.Record(ctx, d.Seconds(), metric.WithAttributes(pa, attribute.String("status", status)))
	}
	if o.JobRowsRead != nil && rows > 0 {
		o.JobRowsRead.Add(ctx, rows, metric.WithAttributes(pa))
	}
	if o.JobRowsDelivered != nil && delivered > 0 {
		o.JobRowsDelivered.Add(ctx, delivered, metric.WithAttributes(pa))
	}
}

// RecordOverlapSkip counts one overlap:skip rejection.
func (o *Obs) RecordOverlapSkip(pipeline string) {
	if o == nil || o.JobsOverlapSkipped == nil {
		return
	}
	o.JobsOverlapSkipped.Add(context.Background(), 1, metric.WithAttributes(attribute.String("pipeline", pipeline)))
}

// RecordCatchupSkip counts one out-of-window missed tick.
func (o *Obs) RecordCatchupSkip(pipeline string) {
	if o == nil || o.JobsCatchupSkipped == nil {
		return
	}
	o.JobsCatchupSkipped.Add(context.Background(), 1, metric.WithAttributes(attribute.String("pipeline", pipeline)))
}

// SetGauges pushes the instantaneous pipeline-level gauges.
func (o *Obs) SetGauges(pipeline string, inFlight, spoolDepth int, paused bool) {
	if o == nil {
		return
	}
	ctx := context.Background()
	pa := metric.WithAttributes(attribute.String("pipeline", pipeline))
	if o.InFlight != nil {
		o.InFlight.Record(ctx, float64(inFlight), pa)
	}
	if o.SpoolDepth != nil {
		o.SpoolDepth.Record(ctx, float64(spoolDepth), pa)
	}
	if o.PipelinePaused != nil {
		v := 0.0
		if paused {
			v = 1
		}
		o.PipelinePaused.Record(ctx, v, pa)
	}
}

// count is the shared single-event counter shape.
func (o *Obs) count(c metric.Int64Counter, labels ...string) {
	if o == nil || c == nil || len(labels)%2 != 0 {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(labels)/2)
	for i := 0; i+1 < len(labels); i += 2 {
		attrs = append(attrs, attribute.String(labels[i], labels[i+1]))
	}
	c.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// RecordCelError counts one predicate evaluation error.
// RecordCelError counts one predicate evaluation error. lang distinguishes
// the dialect ("cel" default, "cesql" opt-in); the metric name keeps "cel"
// for M2 continuity (review-m3 R8: same position, same counter).
func (o *Obs) RecordCelError(pipeline, edge string, lang string) {
	if o == nil {
		return
	}
	if lang == "" {
		lang = "cel"
	}
	o.count(o.CelEvalErrors, "pipeline", pipeline, "edge", edge, "lang", lang)
}

// RecordNoMatch counts one filtered message.
func (o *Obs) RecordNoMatch(pipeline, node string) {
	if o == nil {
		return
	}
	o.count(o.FanoutNoMatch, "pipeline", pipeline, "node", node)
}

// RecordRetry counts one delivery retry.
func (o *Obs) RecordRetry(pipeline, node string) {
	if o == nil {
		return
	}
	o.count(o.DeliveryRetries, "pipeline", pipeline, "node", node)
}

// RecordOptionalDrop counts one optional-edge drop.
func (o *Obs) RecordOptionalDrop(pipeline, edge string) {
	if o == nil {
		return
	}
	o.count(o.OptionalDrops, "pipeline", pipeline, "edge", edge)
}

// RecordDecodeError counts one decode failure.
func (o *Obs) RecordDecodeError(pipeline, source string) {
	if o == nil {
		return
	}
	o.count(o.DecodeErrors, "pipeline", pipeline, "source", source)
}

// RecordSpoolFailure counts one spool append failure.
func (o *Obs) RecordSpoolFailure(pipeline string) {
	if o == nil {
		return
	}
	o.count(o.SpoolFailures, "pipeline", pipeline)
}

// RecordBackpressure counts one admission block.
func (o *Obs) RecordBackpressure(pipeline, source string) {
	if o == nil {
		return
	}
	o.count(o.BackpressureEvents, "pipeline", pipeline, "source", source)
}

// RecordDlqFailure counts one dead-letter write failure.
func (o *Obs) RecordDlqFailure(pipeline string) {
	if o == nil {
		return
	}
	o.count(o.DlqWriteFailures, "pipeline", pipeline)
}

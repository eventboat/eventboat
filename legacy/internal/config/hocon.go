package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	hoconlib "github.com/gurkankaymak/hocon"
)

func LoadHOCON(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// The upstream HOCON parser mishandles CRLF line endings when comments
	// contain multi-byte UTF-8 characters, causing it to skip the rest of the
	// file. Normalize line endings before parsing to avoid that defect while
	// keeping the file contents otherwise unchanged.
	data = []byte(strings.ReplaceAll(string(data), "\r\n", "\n"))
	conf, err := hoconlib.ParseString(string(data))
	if err != nil {
		return nil, fmt.Errorf("hocon parse: %w", err)
	}
	tree, err := ConfigToMap(conf)
	if err != nil {
		return nil, err
	}
	return mapToPipelineConfig(tree)
}

func ConfigToMap(conf *hoconlib.Config) (map[string]any, error) {
	root := conf.GetRoot()
	if root == nil {
		return nil, fmt.Errorf("empty config root")
	}
	out, ok := valueToAny(root).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("root is not an object")
	}
	return out, nil
}

func valueToAny(v hoconlib.Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case hoconlib.ObjectType:
		obj := v.(hoconlib.Object)
		m := make(map[string]any, len(obj))
		for k, child := range obj {
			m[k] = valueToAny(child)
		}
		return m
	case hoconlib.ArrayType:
		arr := v.(hoconlib.Array)
		slice := make([]any, len(arr))
		for i, child := range arr {
			slice[i] = valueToAny(child)
		}
		return slice
	case hoconlib.StringType:
		if d, ok := v.(hoconlib.Duration); ok {
			return time.Duration(d).String()
		}
		return string(v.(hoconlib.String))
	case hoconlib.BooleanType:
		return bool(v.(hoconlib.Boolean))
	case hoconlib.NumberType:
		switch n := v.(type) {
		case hoconlib.Int:
			return int(n)
		case hoconlib.Float32:
			return float32(n)
		case hoconlib.Float64:
			return float64(n)
		default:
			return v.String()
		}
	case hoconlib.NullType:
		return nil
	default:
		if d, ok := v.(hoconlib.Duration); ok {
			return time.Duration(d).String()
		}
		return v.String()
	}
}

func mapToPipelineConfig(tree map[string]any) (*PipelineConfig, error) {
	cfg := &PipelineConfig{}
	if v, ok := tree["apiVersion"].(string); ok {
		cfg.APIVersion = v
	}
	if v, ok := tree["kind"].(string); ok {
		cfg.Kind = v
	}
	if v, ok := tree["metadata"].(map[string]any); ok {
		cfg.Metadata = stringMap(v)
	}
	if v, ok := tree["engine"].(map[string]any); ok {
		e, err := mapEngine(v)
		if err != nil {
			return nil, err
		}
		cfg.Engine = e
	}
	if v, ok := tree["edgeDefaults"].(map[string]any); ok {
		e, err := mapEdgeAttrs(v)
		if err != nil {
			return nil, err
		}
		cfg.EdgeDefaults = e
	}
	if v, ok := tree["dlq"].(map[string]any); ok {
		d, err := mapDLQ(v)
		if err != nil {
			return nil, err
		}
		cfg.DLQ = d
	}
	if v, ok := tree["observability"].(map[string]any); ok {
		o, err := mapObservability(v)
		if err != nil {
			return nil, err
		}
		cfg.Observability = o
	}
	if v, ok := tree["steps"].(map[string]any); ok {
		steps, err := mapSteps(v)
		if err != nil {
			return nil, err
		}
		cfg.Steps = steps
	}
	if v, ok := tree["pipeline"].([]any); ok {
		stages, err := mapPipelineStages(v)
		if err != nil {
			return nil, err
		}
		cfg.Pipeline = stages
	}
	if v, ok := tree["codecs"].([]any); ok {
		codecs, err := mapCodecs(v)
		if err != nil {
			return nil, err
		}
		cfg.Codecs = codecs
	}
	if v, ok := tree["edges"].([]any); ok {
		edges, err := mapEdges(v)
		if err != nil {
			return nil, err
		}
		cfg.Edges = edges
	}
	return cfg, nil
}

func stringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func mapEngine(m map[string]any) (EngineConfig, error) {
	maxWorkers, err := intVal(m["max_workers"], "engine.max_workers")
	if err != nil {
		return EngineConfig{}, err
	}
	maxInflight, err := intVal(m["max_inflight"], "engine.max_inflight")
	if err != nil {
		return EngineConfig{}, err
	}
	errorMode, err := strVal(m["error_mode"], "engine.error_mode")
	if err != nil {
		return EngineConfig{}, err
	}
	drainTimeout, err := strVal(m["drain_timeout"], "engine.drain_timeout")
	if err != nil {
		return EngineConfig{}, err
	}
	return EngineConfig{
		MaxWorkers:   maxWorkers,
		MaxInflight:  maxInflight,
		ErrorMode:    errorMode,
		DrainTimeout: drainTimeout,
	}, nil
}

func mapDLQ(m map[string]any) (*DLQConfig, error) {
	sink, err := strVal(m["sink"], "dlq.sink")
	if err != nil {
		return nil, err
	}
	include, err := boolVal(m["include_current_payload"], "dlq.include_current_payload")
	if err != nil {
		return nil, err
	}
	return &DLQConfig{
		Sink:                  sink,
		IncludeCurrentPayload: include,
	}, nil
}

func mapObservability(m map[string]any) (ObservabilityConfig, error) {
	var out ObservabilityConfig
	if metrics, ok := m["metrics"].(map[string]any); ok {
		enabled, err := boolVal(metrics["enabled"], "observability.metrics.enabled")
		if err != nil {
			return out, err
		}
		port, err := intVal(metrics["port"], "observability.metrics.port")
		if err != nil {
			return out, err
		}
		path, err := strVal(metrics["path"], "observability.metrics.path")
		if err != nil {
			return out, err
		}
		out.Metrics = MetricsConfig{
			Enabled: enabled,
			Port:    port,
			Path:    path,
		}
	}
	if health, ok := m["health"].(map[string]any); ok {
		enabled, err := boolVal(health["enabled"], "observability.health.enabled")
		if err != nil {
			return out, err
		}
		port, err := intVal(health["port"], "observability.health.port")
		if err != nil {
			return out, err
		}
		liveness, err := strVal(health["liveness"], "observability.health.liveness")
		if err != nil {
			return out, err
		}
		readiness, err := strVal(health["readiness"], "observability.health.readiness")
		if err != nil {
			return out, err
		}
		out.Health = HealthConfig{
			Enabled:   enabled,
			Port:      port,
			Liveness:  liveness,
			Readiness: readiness,
		}
	}
	if logging, ok := m["logging"].(map[string]any); ok {
		level, err := strVal(logging["level"], "observability.logging.level")
		if err != nil {
			return out, err
		}
		format, err := strVal(logging["format"], "observability.logging.format")
		if err != nil {
			return out, err
		}
		out.Logging = LoggingConfig{
			Level:  level,
			Format: format,
		}
	}
	return out, nil
}

func mapSteps(steps map[string]any) (map[string]StepConfig, error) {
	out := make(map[string]StepConfig, len(steps))
	for name, raw := range steps {
		stepMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("step %q is not an object", name)
		}
		step, err := mapStep(stepMap)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", name, err)
		}
		out[name] = step
	}
	return out, nil
}

func mapStep(m map[string]any) (StepConfig, error) {
	stepType, err := strVal(m["step_type"], "step.step_type")
	if err != nil {
		return StepConfig{}, err
	}
	step := StepConfig{StepType: stepType}
	if deps, ok := m["depends_on"]; ok {
		parsed, err := parseDependsOn(deps)
		if err != nil {
			return step, err
		}
		step.DependsOn = parsed
	}
	if src, ok := m["source"].(map[string]any); ok {
		s, err := mapSourceBlock(src)
		if err != nil {
			return step, err
		}
		step.Source = s
	}
	if tr, ok := m["transform"].(map[string]any); ok {
		t, err := mapTransformBlock(tr)
		if err != nil {
			return step, err
		}
		step.Transform = t
	}
	if sk, ok := m["sink"].(map[string]any); ok {
		s, err := mapSinkBlock(sk)
		if err != nil {
			return step, err
		}
		step.Sink = s
	}
	return step, nil
}

func mapSourceBlock(m map[string]any) (*SourceBlock, error) {
	t, err := strVal(m["type"], "source.type")
	if err != nil {
		return nil, err
	}
	decoder, err := mapCodecRef(m["decoder"], "source.decoder")
	if err != nil {
		return nil, err
	}
	return &SourceBlock{
		Type:    t,
		Decoder: decoder,
		Config:  mapAny(m["config"]),
	}, nil
}

func mapTransformBlock(m map[string]any) (*TransformBlock, error) {
	t, err := strVal(m["type"], "transform.type")
	if err != nil {
		return nil, err
	}
	predicate, err := strVal(m["predicate"], "transform.predicate")
	if err != nil {
		return nil, err
	}
	workers, err := intVal(m["workers"], "transform.workers")
	if err != nil {
		return nil, err
	}
	errorMode, err := strVal(m["error_mode"], "transform.error_mode")
	if err != nil {
		return nil, err
	}
	return &TransformBlock{
		Type:      t,
		Predicate: predicate,
		Workers:   workers,
		ErrorMode: errorMode,
		Config:    mapAny(m["config"]),
	}, nil
}

func mapSinkBlock(m map[string]any) (*SinkBlock, error) {
	t, err := strVal(m["type"], "sink.type")
	if err != nil {
		return nil, err
	}
	encoder, err := mapCodecRef(m["encoder"], "sink.encoder")
	if err != nil {
		return nil, err
	}
	batch, err := mapBatch(m["batch"], "sink.batch")
	if err != nil {
		return nil, err
	}
	ordering, err := strVal(m["ordering"], "sink.ordering")
	if err != nil {
		return nil, err
	}
	maxInFlight, err := intVal(m["max_in_flight"], "sink.max_in_flight")
	if err != nil {
		return nil, err
	}
	return &SinkBlock{
		Type:        t,
		Encoder:     encoder,
		Batch:       batch,
		Ordering:    ordering,
		MaxInFlight: maxInFlight,
		Config:      mapAny(m["config"]),
	}, nil
}

func mapPipelineStages(items []any) ([]StageConfig, error) {
	out := make([]StageConfig, 0, len(items))
	for i, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("pipeline[%d] is not an object", i)
		}
		prefix := fmt.Sprintf("pipeline[%d]", i)
		id, err := strVal(m["id"], prefix+".id")
		if err != nil {
			return nil, err
		}
		kind, err := strVal(m["kind"], prefix+".kind")
		if err != nil {
			return nil, err
		}
		t, err := strVal(m["type"], prefix+".type")
		if err != nil {
			return nil, err
		}
		workers, err := intVal(m["workers"], prefix+".workers")
		if err != nil {
			return nil, err
		}
		predicate, err := strVal(m["predicate"], prefix+".predicate")
		if err != nil {
			return nil, err
		}
		errorMode, err := strVal(m["error_mode"], prefix+".error_mode")
		if err != nil {
			return nil, err
		}
		ordering, err := strVal(m["ordering"], prefix+".ordering")
		if err != nil {
			return nil, err
		}
		maxInFlight, err := intVal(m["max_in_flight"], prefix+".max_in_flight")
		if err != nil {
			return nil, err
		}
		decoder, err := mapCodecRef(m["decoder"], prefix+".decoder")
		if err != nil {
			return nil, err
		}
		encoder, err := mapCodecRef(m["encoder"], prefix+".encoder")
		if err != nil {
			return nil, err
		}
		batch, err := mapBatch(m["batch"], prefix+".batch")
		if err != nil {
			return nil, err
		}
		st := StageConfig{
			ID:          id,
			Kind:        kind,
			Type:        t,
			Workers:     workers,
			Predicate:   predicate,
			ErrorMode:   errorMode,
			Ordering:    ordering,
			MaxInFlight: maxInFlight,
			Decoder:     decoder,
			Encoder:     encoder,
			Batch:       batch,
			Config:      mapAny(m["config"]),
		}
		if deps, ok := m["depends_on"]; ok {
			parsed, err := parseDependsOn(deps)
			if err != nil {
				return nil, err
			}
			st.DependsOn = parsed
		}
		out = append(out, st)
	}
	return out, nil
}

func parseDependsOn(raw any) (DependsOnList, error) {
	switch v := raw.(type) {
	case []any:
		out := make(DependsOnList, 0, len(v))
		for _, item := range v {
			switch t := item.(type) {
			case string:
				out = append(out, DependsOnEntry{Upstream: t})
			case map[string]any:
				if len(t) != 1 {
					return nil, fmt.Errorf("depends_on sequence item must be single-key object")
				}
				for upstream, attrsRaw := range t {
					attrsMap, ok := attrsRaw.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("depends_on sequence item attributes must be an object")
					}
					attrs, err := mapEdgeAttrs(attrsMap)
					if err != nil {
						return nil, err
					}
					out = append(out, DependsOnEntry{Upstream: upstream, Edge: &attrs})
				}
			default:
				return nil, fmt.Errorf("unsupported depends_on list element %T", item)
			}
		}
		return out, nil
	case map[string]any:
		out := make(DependsOnList, 0, len(v))
		for upstream, attrsRaw := range v {
			attrsMap, ok := attrsRaw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("depends_on map value for %q must be an object", upstream)
			}
			attrs, err := mapEdgeAttrs(attrsMap)
			if err != nil {
				return nil, err
			}
			out = append(out, DependsOnEntry{Upstream: upstream, Edge: &attrs})
		}
		return out, nil
	default:
		return nil, fmt.Errorf("depends_on must be list or map, got %T", raw)
	}
}

func mapEdgeAttrs(m map[string]any) (EdgeAttrs, error) {
	if m == nil {
		return EdgeAttrs{}, nil
	}
	condition, err := strVal(m["condition"], "edge.condition")
	if err != nil {
		return EdgeAttrs{}, err
	}
	route, err := strVal(m["route"], "edge.route")
	if err != nil {
		return EdgeAttrs{}, err
	}
	buffer, err := mapEdgeBuffer(m["buffer"], "edge.buffer")
	if err != nil {
		return EdgeAttrs{}, err
	}
	delivery, err := mapDelivery(m["delivery"], "edge.delivery")
	if err != nil {
		return EdgeAttrs{}, err
	}
	var required *bool
	if v, ok := m["required"].(bool); ok {
		required = &v
	}
	return EdgeAttrs{
		Condition: condition,
		Route:     route,
		Buffer:    buffer,
		Delivery:  delivery,
		Required:  required,
	}, nil
}

func mapEdgeBuffer(raw any, path string) (*EdgeBufferConfig, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: expected object, got %T", path, raw)
	}
	t, err := strVal(m["type"], path+".type")
	if err != nil {
		return nil, err
	}
	size, err := intVal(m["size"], path+".size")
	if err != nil {
		return nil, err
	}
	strategy, err := strVal(m["strategy"], path+".strategy")
	if err != nil {
		return nil, err
	}
	key, err := stringSlice(m["key"], path+".key")
	if err != nil {
		return nil, err
	}
	diskPath, err := strVal(m["disk_path"], path+".disk_path")
	if err != nil {
		return nil, err
	}
	diskMaxSize, err := int64Val(m["disk_max_size"], path+".disk_max_size")
	if err != nil {
		return nil, err
	}
	diskSyncInterval, err := strVal(m["disk_sync_interval"], path+".disk_sync_interval")
	if err != nil {
		return nil, err
	}
	return &EdgeBufferConfig{
		Type:             t,
		Size:             size,
		Strategy:         strategy,
		Key:              key,
		DiskPath:         diskPath,
		DiskMaxSize:      diskMaxSize,
		DiskSyncInterval: diskSyncInterval,
	}, nil
}

func mapDelivery(raw any, path string) (*DeliverySpec, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: expected object, got %T", path, raw)
	}
	var retry *RetryConfig
	if r, ok := m["retry"].(map[string]any); ok {
		max, err := intVal(r["max"], path+".retry.max")
		if err != nil {
			return nil, err
		}
		backoff, err := strVal(r["backoff"], path+".retry.backoff")
		if err != nil {
			return nil, err
		}
		retry = &RetryConfig{
			Max:     max,
			Backoff: backoff,
		}
	}
	timeout, err := strVal(m["timeout"], path+".timeout")
	if err != nil {
		return nil, err
	}
	dlq, err := strVal(m["dlq"], path+".dlq")
	if err != nil {
		return nil, err
	}
	return &DeliverySpec{
		Retry:   retry,
		Timeout: timeout,
		DLQ:     dlq,
	}, nil
}

func mapCodecRef(raw any, path string) (*CodecRef, error) {
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case string:
		return &CodecRef{Type: v}, nil
	case map[string]any:
		t, err := strVal(v["type"], path+".type")
		if err != nil {
			return nil, err
		}
		ref, err := strVal(v["ref"], path+".ref")
		if err != nil {
			return nil, err
		}
		return &CodecRef{
			Type:   t,
			Ref:    ref,
			Config: mapAny(v["config"]),
		}, nil
	default:
		return nil, fmt.Errorf("%s: expected string or object, got %T", path, raw)
	}
}

func mapBatch(raw any, path string) (*BatchConfig, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: expected object, got %T", path, raw)
	}
	size, err := intVal(m["size"], path+".size")
	if err != nil {
		return nil, err
	}
	timeout, err := strVal(m["timeout"], path+".timeout")
	if err != nil {
		return nil, err
	}
	maxBytes, err := intVal(m["max_bytes"], path+".max_bytes")
	if err != nil {
		return nil, err
	}
	return &BatchConfig{
		Size:     size,
		Timeout:  timeout,
		MaxBytes: maxBytes,
	}, nil
}

func mapCodecs(items []any) ([]CodecConfig, error) {
	out := make([]CodecConfig, 0, len(items))
	for i, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("codecs[%d]: expected object, got %T", i, raw)
		}
		prefix := fmt.Sprintf("codecs[%d]", i)
		name, err := strVal(m["name"], prefix+".name")
		if err != nil {
			return nil, err
		}
		t, err := strVal(m["type"], prefix+".type")
		if err != nil {
			return nil, err
		}
		out = append(out, CodecConfig{
			Name:   name,
			Type:   t,
			Config: mapAny(m["config"]),
		})
	}
	return out, nil
}

func mapEdges(items []any) ([]EdgeConfig, error) {
	out := make([]EdgeConfig, 0, len(items))
	for i, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("edges[%d]: expected object, got %T", i, raw)
		}
		prefix := fmt.Sprintf("edges[%d]", i)
		var required *bool
		if v, ok := m["required"].(bool); ok {
			required = &v
		}
		from, err := strVal(m["from"], prefix+".from")
		if err != nil {
			return nil, err
		}
		to, err := strVal(m["to"], prefix+".to")
		if err != nil {
			return nil, err
		}
		condition, err := strVal(m["condition"], prefix+".condition")
		if err != nil {
			return nil, err
		}
		route, err := strVal(m["route"], prefix+".route")
		if err != nil {
			return nil, err
		}
		buffer, err := mapEdgeBuffer(m["buffer"], prefix+".buffer")
		if err != nil {
			return nil, err
		}
		delivery, err := mapDelivery(m["delivery"], prefix+".delivery")
		if err != nil {
			return nil, err
		}
		out = append(out, EdgeConfig{
			From:      from,
			To:        to,
			Condition: condition,
			Route:     route,
			Buffer:    buffer,
			Delivery:  delivery,
			Required:  required,
		})
	}
	return out, nil
}

func mapAny(m any) map[string]any {
	if m == nil {
		return nil
	}
	src, ok := m.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func strVal(v any, path string) (string, error) {
	if v == nil {
		return "", nil
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case int, int64, float64, float32:
		return fmt.Sprint(t), nil
	default:
		return "", fmt.Errorf("%s: expected string, got %T", path, v)
	}
}

func intVal(v any, path string) (int, error) {
	switch t := v.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case float64:
		return int(t), nil
	case float32:
		return int(t), nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("%s: expected int, got %T", path, v)
	}
}

func int64Val(v any, path string) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case float32:
		return int64(t), nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("%s: expected int64, got %T", path, v)
	}
}

func boolVal(v any, path string) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("%s: expected bool, got %T", path, v)
	}
}

func stringSlice(v any, path string) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		if s, ok := v.(string); ok && s != "" {
			return strings.Split(s, ","), nil
		}
		return nil, fmt.Errorf("%s: expected list or string, got %T", path, v)
	}
	out := make([]string, 0, len(arr))
	for i, item := range arr {
		s, err := strVal(item, fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

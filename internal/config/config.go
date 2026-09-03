// Package config implements the typed pipeline configuration of Eventboat v3:
// three sections (sources/transforms/sinks) joined by `from` edges,
// plugin-name-as-key nodes, a framework-field whitelist at node level, and
// full-field ${VAR} substitution (redesign-v3.md §5).
package config

// Section identifies which of the three topology sections a node belongs to.
type Section string

const (
	SectionSource    Section = "sources"
	SectionTransform Section = "transforms"
	SectionSink      Section = "sinks"
)

// Diagnostic is one verify finding. Errors abort build; warnings surface as
// lint (upgraded to errors under --strict).
type Diagnostic struct {
	Severity string `json:"severity"` // "error" | "warning"
	Code     string `json:"code"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Message  string `json:"message"`
	Hint     string `json:"hint"`
}

func (d Diagnostic) Error() string {
	return d.Severity + "[" + d.Code + "] " + d.File + ":" + itoa(d.Line) + ": " + d.Message
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// Pipeline is the typed form of a pipeline configuration file.
type Pipeline struct {
	File         string
	Name         string
	Constants    map[string]any
	EdgeDefaults EdgeAttrs
	Sources      map[string]*Node
	Transforms   map[string]*Node
	Sinks        map[string]*Node
	// Order preserves a deterministic listing of all node names.
	Order []string
}

// Node is one entry of one section.
type Node struct {
	Name    string
	Section Section
	Line    int // line of the node's key

	From     []Edge // incoming edges (empty for sources)
	Decoder  string // sources; "" means json
	Encoder  string // sinks; "" means json
	Workers  int    // default 1
	OrderKey string // sinks; CEL expression
	Batch    *Batch // sinks
	Script   string // transforms (Starlark)
	Split    *SplitConfig

	Plugin       string
	PluginConfig map[string]any
	PluginLine   int
}

// Edge is one incoming edge declared via `from`.
type Edge struct {
	From     string
	Line     int
	When     string // CEL predicate source ("" = unconditional)
	Route    string // named route sugar; compiled to When by ir
	Delivery *Delivery
	Required *bool
	Buffer   *BufferConfig
}

// EdgeAttrs is the attribute subset allowed in edge_defaults.
type EdgeAttrs struct {
	Delivery *Delivery
	Required *bool
	Buffer   *BufferConfig
}

// Delivery is the per-edge delivery policy.
type Delivery struct {
	Retries   int    // attempt count beyond the first try
	Backoff   string // "exponential" (default) | "constant"
	TimeoutMs int    // per-attempt timeout; 0 = engine default
}

// Batch configures the engine-owned sink batcher.
type Batch struct {
	Size      int
	TimeoutMs int // flush interval when the batch is not full
}

// SplitConfig marks a transform as a splitter. POC semantics: the payload must
// be a JSON array; each element becomes one message (redesign-v3-review R8).
type SplitConfig struct{}

// BufferConfig is the in-memory per-edge buffer sizing (surge absorption
// only; reliability comes from the spool, not from buffers).
type BufferConfig struct {
	Type      string // "memory"
	MaxEvents int
}

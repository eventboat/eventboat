// Package config implements the typed pipeline configuration of Eventboat v3:
// three sections (sources/transforms/sinks) joined by `from` edges,
// plugin-name-as-key nodes, a framework-field whitelist at node level, and
// full-field ${VAR} substitution (redesign-v3.md §5).
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
	File          string
	Name          string
	Constants     map[string]any
	ConstantsUsed map[string]bool // constants referenced via ${constants.x} (pre-substitution truth)
	EdgeDefaults  EdgeAttrs
	Limits        *Limits
	Run           *RunSpec
	Parameters    map[string]*ParameterSpec // declared job parameters (nil without a run block)
	Hooks         *HooksSpec
	Sources       map[string]*Node
	Transforms    map[string]*Node
	Sinks         map[string]*Node
	// Order preserves a deterministic listing of all node names.
	Order []string
}

// Limits is the optional per-pipeline resource ceiling (redesign-v3.md §5.10).
type Limits struct {
	MaxInFlight  int           // engine spool admission high watermark
	DrainTimeout time.Duration // graceful drain bound on shutdown
}

// RunSpec is the pipeline-level job policy (redesign-v3.md §5.8). A pipeline
// with a run block in mode job executes as a job: scheduled or triggered
// runs with catchup, overlap control and run history.
type RunSpec struct {
	Mode             string        // "continuous" (default) | "job"
	Schedule         string        // 5-field cron; empty = manual/trigger only
	Overlap          string        // skip (default) | all | latest
	CatchupWindow    time.Duration // missed-tick compensation window; 0 = no catchup
	SkipIfSuccessful bool
	Retention        time.Duration // run history retention (retention.history); 0 = keep forever
}

// IsJob reports whether the pipeline runs in job mode.
func (p *Pipeline) IsJob() bool { return p.Run != nil && p.Run.Mode == "job" }

// ParameterSpec is one typed job parameter declaration (§5.8/§5.9).
type ParameterSpec struct {
	Name     string
	Type     string // string | integer | number | boolean
	Default  any    // engine-bound sentinel strings allowed: "cursor", "now"
	Required bool
	Enum     []any
	Pattern  string
	Min      *float64
	Max      *float64
	Line     int
}

// HooksSpec holds lifecycle hooks (failure/success), each an inline sink
// (plugin name as key, reviewed R14).
type HooksSpec struct {
	Failure *HookSink
	Success *HookSink
}

// HookSink is one inline hook sink: plugin name + its config block.
type HookSink struct {
	Plugin       string
	PluginConfig map[string]any
	Line         int
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

// ParseDuration extends time.ParseDuration with a "d" (24h) unit so spec
// examples like "90d" (retention) parse. All other units are standard Go.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("config: empty duration")
	}
	// days suffix (possibly fractional, e.g. "1d12h")
	if i := strings.IndexByte(s, 'd'); i >= 0 {
		days, err := strconv.ParseFloat(s[:i], 64)
		if err != nil {
			return 0, fmt.Errorf("config: bad duration %q", s)
		}
		rest := s[i+1:]
		base := time.Duration(days * 24 * float64(time.Hour))
		if rest == "" {
			return base, nil
		}
		tail, err := time.ParseDuration(rest)
		if err != nil {
			return 0, fmt.Errorf("config: bad duration %q", s)
		}
		return base + tail, nil
	}
	return time.ParseDuration(s)
}

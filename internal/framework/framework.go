// Package framework is the single source of the framework vocabulary
// (candidate 06, design §3.5): the per-section framework fields, the
// top-level allowed keys, the edge attributes (per container), the reserved
// plugin names and the node-level default constants.
//
// It is a leaf: config, registry and the LSP read it, and no copy of these
// lists exists elsewhere. The registry's reserved-name set and config's node
// whitelists come from the same source, so a name like grpc or version can no
// longer register as a plugin that config would parse as a framework field.
package framework

// Sections are the three topology sections, in the parse/listing order.
var Sections = []string{"sources", "transforms", "sinks"}

// TopLevelKeys is the complete allow-list of top-level document keys, in the
// documented order (the loader's unknown-key hint and the LSP's completion
// both read it).
var TopLevelKeys = []string{
	"apiVersion", "kind", "metadata", "edge_defaults", "constants", "limits",
	"telemetry", "run", "parameters", "hooks", "codecs", "dlq",
	"sources", "transforms", "sinks",
}

// RunFields is the `run:` block's field allow-list (§5.8).
var RunFields = []string{"mode", "schedule", "overlap", "catchup_window", "skip_if_successful", "retention"}

// MetadataFields is the `metadata:` field allow-list. name is the only
// consumed field: it becomes the deployed file name and the store key.
var MetadataFields = []string{"name"}

// SectionFields lists the framework fields allowed at node level per section.
// Everything else at node level must be exactly one plugin key; script/split/
// wasm are registered transform plugins, not framework fields, so transforms
// carry only the shared node fields.
var sectionFields = map[string][]string{
	"sources":    {"decoder", "grpc", "version"},
	"transforms": {"depends_on", "workers", "version"},
	// sinks deliberately has no `workers`: sink concurrency is engine-owned
	// (batching + order_key semantics), and the field was accepted and
	// ignored before candidate 06 — a dead knob the loader now rejects.
	"sinks": {"depends_on", "encoder", "order_key", "batch", "grpc", "version"},
}

// SectionFields returns the node-level framework fields of one section (nil
// for an unknown section).
func SectionFields(section string) []string { return sectionFields[section] }

// NodeFields is the union of every node-level framework field.
var NodeFields = unionFields()

// EdgeDependsOnAttrs is the attribute allow-list of a depends_on element.
var EdgeDependsOnAttrs = []string{"when", "route", "delivery", "required", "buffer"}

// EdgeDefaultsAttrs is the attribute allow-list of edge_defaults: a global
// default predicate (when) is a footgun and a default route is meaningless,
// so the loader rejects them with cfg_edge_defaults_field.
var EdgeDefaultsAttrs = []string{"delivery", "required", "buffer"}

// FromKey stays reserved: the cfg_from_renamed migration diagnostic claims
// the key so old configs get a targeted message, not a plugin-name clash.
const FromKey = "from"

// Node-level default constants. The loader materializes these into the typed
// config, so downstream layers stop re-expressing them (the json default used
// to be copied ten times).
const (
	DecoderDefault         = "json"
	EncoderDefault         = "json"
	WorkersDefault         = 1
	DeliveryRetriesDefault = 3
	DeliveryBackoffDefault = "exponential"
	BufferMaxDefault       = 128
	BatchSizeDefault       = 1
)

// ReservedNames is the set of plugin names the framework owns: the union of
// every node-level framework field and every edge attribute, plus "from". A
// plugin registered under one of these names could never be used — its block
// key would parse as a framework field — so the registry refuses it. Test
// pins: reserved names == the union of the framework fields.
func ReservedNames() map[string]bool {
	out := make(map[string]bool, len(NodeFields)+len(EdgeDependsOnAttrs)+1)
	for _, f := range NodeFields {
		out[f] = true
	}
	for _, f := range EdgeDependsOnAttrs {
		out[f] = true
	}
	out[FromKey] = true
	return out
}

// Reserved reports whether a plugin name is reserved by the framework.
func Reserved(name string) bool {
	if _, ok := reservedSet[name]; ok {
		return true
	}
	return false
}

// reservedSet is ReservedNames built once (package-level, immutable).
var reservedSet = ReservedNames()

// Has reports whether list contains key.
func Has(list []string, key string) bool {
	for _, k := range list {
		if k == key {
			return true
		}
	}
	return false
}

func unionFields() []string {
	seen := map[string]bool{}
	var out []string
	for _, section := range Sections {
		for _, f := range sectionFields[section] {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	return out
}

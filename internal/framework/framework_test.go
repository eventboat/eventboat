package framework

import (
	"reflect"
	"sort"
	"testing"
)

// Candidate 06 acceptance 1: the reserved plugin names are exactly the union
// of the framework fields plus "from" — no hand-copied set can drift again
// (grpc/version used to register as plugins and could never load).
func TestReservedNamesCoverFrameworkFields(t *testing.T) {
	union := map[string]bool{FromKey: true}
	for _, section := range Sections {
		fields := SectionFields(section)
		if len(fields) == 0 {
			t.Fatalf("section %q has no framework fields", section)
		}
		for _, f := range fields {
			union[f] = true
		}
	}
	for _, f := range EdgeDependsOnAttrs {
		union[f] = true
	}
	got := ReservedNames()
	if !reflect.DeepEqual(union, got) {
		t.Fatalf("reserved names != union of framework fields:\nunion:    %v\nreserved: %v", sorted(union), sorted(got))
	}
	for _, name := range []string{"grpc", "version", "decoder", "workers", "order_key", "batch", "when", "route", "buffer", "delivery", "required", "depends_on", "from"} {
		if !Reserved(name) {
			t.Errorf("framework field %q is not reserved", name)
		}
	}
	// The built-in transform plugins stay ordinary, registrable names.
	for _, name := range []string{"script", "split", "wasm", "file", "json"} {
		if Reserved(name) {
			t.Errorf("plugin name %q must not be reserved", name)
		}
	}
}

// Sinks carry no workers field (dead knob, candidate 06); transforms do.
func TestSinkWorkersNotAFrameworkField(t *testing.T) {
	if Has(SectionFields("sinks"), "workers") {
		t.Fatal("sinks.workers must not be a framework field")
	}
	if !Has(SectionFields("transforms"), "workers") {
		t.Fatal("transforms.workers is still a framework field")
	}
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

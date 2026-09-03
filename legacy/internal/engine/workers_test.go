package engine

import "testing"

func TestAllocateTransformWorkersWithinBudget(t *testing.T) {
	nodes := []*runtimeNode{
		{id: "a", workers: 2},
		{id: "b", workers: 3},
	}
	got := allocateTransformWorkers(nodes, 8)
	if got["a"] != 2 || got["b"] != 3 {
		t.Fatalf("unexpected allocation: %#v", got)
	}
}

func TestAllocateTransformWorkersScalesDown(t *testing.T) {
	nodes := []*runtimeNode{
		{id: "a", workers: 4},
		{id: "b", workers: 4},
		{id: "c", workers: 4},
	}
	got := allocateTransformWorkers(nodes, 6)
	total := got["a"] + got["b"] + got["c"]
	if total != 6 {
		t.Fatalf("total workers = %d, want 6 (%#v)", total, got)
	}
	for id, w := range got {
		if w < 1 {
			t.Fatalf("stage %s got %d workers", id, w)
		}
	}
}

func TestAllocateTransformWorkersDefaultsMax(t *testing.T) {
	nodes := []*runtimeNode{{id: "a", workers: 0}}
	got := allocateTransformWorkers(nodes, 0)
	if got["a"] != 1 {
		t.Fatalf("got %#v", got)
	}
}

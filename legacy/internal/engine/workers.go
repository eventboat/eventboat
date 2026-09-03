package engine

// allocateTransformWorkers caps total transform goroutines at maxWorkers.
// When over budget, workers are distributed evenly with at least one per transform when possible.
func allocateTransformWorkers(transforms []*runtimeNode, maxWorkers int) map[string]int {
	if len(transforms) == 0 {
		return nil
	}
	if maxWorkers <= 0 {
		maxWorkers = 16
	}

	requested := make(map[string]int, len(transforms))
	total := 0
	for _, n := range transforms {
		w := n.workers
		if w < 1 {
			w = 1
		}
		requested[n.id] = w
		total += w
	}
	if total <= maxWorkers {
		return requested
	}

	out := make(map[string]int, len(transforms))
	base := maxWorkers / len(transforms)
	extra := maxWorkers % len(transforms)
	if base < 1 {
		base = 1
	}
	assigned := 0
	for i, n := range transforms {
		w := base
		if i < extra {
			w++
		}
		if assigned+w > maxWorkers {
			w = maxWorkers - assigned
		}
		if w < 1 && assigned < maxWorkers {
			w = 1
		}
		out[n.id] = w
		assigned += w
	}
	return out
}

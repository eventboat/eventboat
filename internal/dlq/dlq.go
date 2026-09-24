// Package dlq owns the dead-letter strategy surface (candidate 05, design
// §3.4): where-filter compilation (with the pipeline's constants — the
// CLI compiled with constants while the MCP path did not), selection order
// (select before delete: an id filter with a limit must never delete rows
// that were never replayed) and the codec-carrying replay request.
//
// The two transports stay explicit: ops injects into a live engine
// (Engine.InjectReplay) and the CLI replay builds a local engine, but both
// assemble the injection from the same Request — identity, codec and payload
// travel together (candidate 01).
package dlq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Filter is the selection contract shared by the query and replay surfaces.
// The zero value selects every dead letter. Time bounds are applied by the
// store query (DeadLettersSince) — the caller resolves Since once, both
// surfaces agree on the helper.
type Filter struct {
	Where string  // CEL predicate over {payload, meta} ("" = no predicate)
	IDs   []int64 // explicit dead-letter ids (empty = no id filter)
	Limit int     // max selected (<= 0 = unlimited)
	At    string  // target node override ("" = each letter's origin node)
}

// Request is one replay item: the codec-carrying message plus its target
// node. ID is 0 for non-dead-letter items (a spool-window row); only ids
// returned by IDs are ever eligible for deletion after a successful replay.
type Request struct {
	ID        int64
	Node      string
	MessageID string
	Codec     string
	Raw       []byte
	Meta      map[string]any
}

// Message assembles the injection message: identity and codec travel with the
// payload (candidate 01 — a csv dead letter replays as csv).
func (r Request) Message() registry.Message {
	return registry.Message{ID: r.MessageID, Codec: r.Codec, Raw: r.Raw, Meta: r.Meta}
}

// IDs returns the dead-letter ids of the selected requests — exactly the rows
// a successful replay may delete, in selection order.
func IDs(reqs []Request) []int64 {
	out := make([]int64, 0, len(reqs))
	for _, r := range reqs {
		if r.ID > 0 {
			out = append(out, r.ID)
		}
	}
	return out
}

// Since turns a --since duration (e.g. "2h") into the filter's lower bound.
func Since(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	d, err := config.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--since %q: %w", s, err)
	}
	return now.Add(-d), nil
}

// CompileWhere compiles the --where predicate over {payload, meta} with the
// pipeline's constants bound (so `constants.region` works on every surface).
func CompileWhere(where string, constants map[string]any) (*celhost.Predicate, error) {
	if where == "" {
		return nil, nil
	}
	env, err := celhost.NewEnv(constants, nil)
	if err != nil {
		return nil, err
	}
	pred, err := env.Compile(where)
	if err != nil {
		return nil, fmt.Errorf("--where: %w", err)
	}
	return pred, nil
}

// FilterDeadLetters applies the filter and returns the matching rows in store
// order, keeping every field (the query surface's shape). Selection order:
// ids → where → limit.
func FilterDeadLetters(dls []store.DeadLetter, f Filter, constants map[string]any) ([]store.DeadLetter, error) {
	pred, err := CompileWhere(f.Where, constants)
	if err != nil {
		return nil, err
	}
	var idSet map[int64]bool
	if len(f.IDs) > 0 {
		idSet = make(map[int64]bool, len(f.IDs))
		for _, id := range f.IDs {
			idSet[id] = true
		}
	}
	out := make([]store.DeadLetter, 0, len(dls))
	for _, dl := range dls {
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
		if idSet != nil && !idSet[dl.ID] {
			continue
		}
		if pred != nil {
			var payload any
			_ = json.Unmarshal(dl.Raw, &payload)
			ok, evalErr := pred.Eval(payload, dl.Meta)
			if evalErr != nil || !ok {
				continue
			}
		}
		out = append(out, dl)
	}
	return out, nil
}

// Select applies the filter and maps the survivors onto replay requests (the
// origin node stays the default; Filter.At overrides it). Delete only what
// this returned — never the raw filter input — so `--ids` with a limit
// cannot delete rows that were never replayed.
func Select(dls []store.DeadLetter, f Filter, constants map[string]any) ([]Request, error) {
	kept, err := FilterDeadLetters(dls, f, constants)
	if err != nil {
		return nil, err
	}
	out := make([]Request, 0, len(kept))
	for _, dl := range kept {
		node := dl.Node
		if f.At != "" {
			node = f.At
		}
		out = append(out, Request{
			ID:        dl.ID,
			Node:      node,
			MessageID: dl.MessageID,
			Codec:     dl.Codec,
			Raw:       dl.Raw,
			Meta:      dl.Meta,
		})
	}
	return out, nil
}

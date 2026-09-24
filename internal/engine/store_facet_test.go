package engine

import "github.com/eventboat/eventboat/internal/store"

// Candidate 04 acceptance 3: the engine's store dependency is the spool +
// dead-letter facets only. A value exposing exactly those two (and nothing
// from JobRunStore, and no Close) must satisfy the interface New accepts — if
// a job-history or handle-lifetime method ever leaks into the engine's
// contract, this assertion stops compiling.
type facetOnlyStore struct {
	store.SpoolStore
	store.DeadLetterStore
}

var _ Store = facetOnlyStore{}

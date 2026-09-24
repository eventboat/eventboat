package jobs

import "github.com/eventboat/eventboat/internal/store"

// Candidate 04 acceptance 3: the manager's store dependency is run history
// plus the engine's facets — and NOT the handle lifetime. A value exposing
// the three facets without Close must satisfy the interface New accepts; the
// owner owns closing, never the manager. If Close ever leaks back into the
// manager's contract, this assertion stops compiling.
type facetOnlyStore struct {
	store.SpoolStore
	store.DeadLetterStore
	store.JobRunStore
}

var _ Store = facetOnlyStore{}

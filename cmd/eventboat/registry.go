package main

import (
	"sync"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

var (
	regOnce sync.Once
	reg     *registry.Registry
	regErr  error
)

// commandRegistry registers builtins exactly once per process and shares the
// registry across subcommands (and repeated calls in tests).
func commandRegistry() (*registry.Registry, error) {
	regOnce.Do(func() {
		reg = registry.Default()
		regErr = builtin.RegisterAll(reg)
	})
	return reg, regErr
}

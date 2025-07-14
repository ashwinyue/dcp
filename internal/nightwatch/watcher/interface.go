package watcher

import (
	"github.com/onexstack/onexstack/pkg/watch/registry"

	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// WantsAggregateConfig defines a function which sets AggregateConfig for watcher plugins that need it.
type WantsAggregateConfig interface {
	registry.Watcher
	SetAggregateConfig(config *AggregateConfig)
}

// WantsStore defines a function which sets store for watcher plugins that need it.
type WantsStore interface {
	registry.Watcher
	SetStore(store store.IStore)
}

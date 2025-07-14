// Package watcher provides functions used by all watchers.
package watcher

import (
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// AggregateConfig aggregates the configurations of all watchers and serves as a configuration aggregator.
type AggregateConfig struct {
	Store store.IStore
	// Then maximum concurrency event of user watcher.
	UserWatcherMaxWorkers int64
}

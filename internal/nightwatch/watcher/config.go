// Package watcher provides functions used by all watchers.
package watcher

import (
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/client/minio"
)

// AggregateConfig aggregates the configurations of all watchers and serves as a configuration aggregator.
type AggregateConfig struct {
	Store store.IStore
	// Database connection for direct database operations
	DB *gorm.DB
	// MinIO client for object storage
	Minio minio.IMinio
	// Then maximum concurrency event of user watcher.
	UserWatcherMaxWorkers int64
}

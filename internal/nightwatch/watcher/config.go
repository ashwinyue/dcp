// Package watcher provides functions used by all watchers.
package watcher

import (
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/messaging"
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
	// Redis cache manager for high-performance caching
	Cache *cache.CacheManager
	// Kafka messaging helper for event publishing
	Messaging *messaging.KafkaHelper
	// Then maximum concurrency event of user watcher.
	UserWatcherMaxWorkers int64
}

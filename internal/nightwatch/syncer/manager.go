// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// RedisSyncerManager implements SyncerManager interface
type RedisSyncerManager struct {
	cache  *cache.CacheManager
	logger log.Logger

	// Syncers
	statusSyncer     StatusSyncer
	statisticsSyncer StatisticsSyncer
	partitionSyncer  PartitionSyncer
	heartbeatSyncer  HeartbeatSyncer

	// Initialization flags
	initialized bool
	mu          sync.RWMutex
}

// NewRedisSyncerManager creates a new Redis syncer manager
func NewRedisSyncerManager(cache *cache.CacheManager, logger log.Logger) *RedisSyncerManager {
	return &RedisSyncerManager{
		cache:  cache,
		logger: logger,
	}
}

// GetStatusSyncer returns status syncer
func (rsm *RedisSyncerManager) GetStatusSyncer() StatusSyncer {
	rsm.ensureInitialized()
	return rsm.statusSyncer
}

// GetStatisticsSyncer returns statistics syncer
func (rsm *RedisSyncerManager) GetStatisticsSyncer() StatisticsSyncer {
	rsm.ensureInitialized()
	return rsm.statisticsSyncer
}

// GetPartitionSyncer returns partition syncer
func (rsm *RedisSyncerManager) GetPartitionSyncer() PartitionSyncer {
	rsm.ensureInitialized()
	return rsm.partitionSyncer
}

// GetHeartbeatSyncer returns heartbeat syncer
func (rsm *RedisSyncerManager) GetHeartbeatSyncer() HeartbeatSyncer {
	rsm.ensureInitialized()
	return rsm.heartbeatSyncer
}

// Close closes all syncers
func (rsm *RedisSyncerManager) Close() error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	var errors []error

	if rsm.statusSyncer != nil {
		if err := rsm.statusSyncer.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if rsm.statisticsSyncer != nil {
		if err := rsm.statisticsSyncer.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if rsm.partitionSyncer != nil {
		if err := rsm.partitionSyncer.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if rsm.heartbeatSyncer != nil {
		if err := rsm.heartbeatSyncer.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors[0] // Return first error
	}

	rsm.logger.Infow("Redis syncer manager closed")
	return nil
}

// ensureInitialized ensures all syncers are initialized
func (rsm *RedisSyncerManager) ensureInitialized() {
	rsm.mu.RLock()
	if rsm.initialized {
		rsm.mu.RUnlock()
		return
	}
	rsm.mu.RUnlock()

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Double-check locking pattern
	if rsm.initialized {
		return
	}

	rsm.statusSyncer = NewRedisStatusSyncer(rsm.cache, rsm.logger)
	rsm.statisticsSyncer = NewRedisStatisticsSyncer(rsm.cache, rsm.logger)
	rsm.partitionSyncer = NewRedisPartitionSyncer(rsm.cache, rsm.logger)
	rsm.heartbeatSyncer = NewRedisHeartbeatSyncer(rsm.cache, rsm.logger)

	rsm.initialized = true
	rsm.logger.Infow("Redis syncer manager initialized")
}

// GetSyncerStatistics returns statistics for all syncers
func (rsm *RedisSyncerManager) GetSyncerStatistics() map[string]*SyncStatistics {
	rsm.ensureInitialized()

	return map[string]*SyncStatistics{
		"status":     rsm.statusSyncer.GetStatistics(),
		"statistics": rsm.statisticsSyncer.GetStatistics(),
		"partition":  rsm.partitionSyncer.GetStatistics(),
		"heartbeat":  rsm.heartbeatSyncer.GetStatistics(),
	}
}

// SyncerFactory provides factory methods for creating syncers
type SyncerFactory struct {
	cache  *cache.CacheManager
	logger log.Logger
}

// NewSyncerFactory creates a new syncer factory
func NewSyncerFactory(cache *cache.CacheManager, logger log.Logger) *SyncerFactory {
	return &SyncerFactory{
		cache:  cache,
		logger: logger,
	}
}

// CreateStatusSyncer creates a status syncer
func (sf *SyncerFactory) CreateStatusSyncer() StatusSyncer {
	return NewRedisStatusSyncer(sf.cache, sf.logger)
}

// CreateStatisticsSyncer creates a statistics syncer
func (sf *SyncerFactory) CreateStatisticsSyncer() StatisticsSyncer {
	return NewRedisStatisticsSyncer(sf.cache, sf.logger)
}

// CreatePartitionSyncer creates a partition syncer
func (sf *SyncerFactory) CreatePartitionSyncer() PartitionSyncer {
	return NewRedisPartitionSyncer(sf.cache, sf.logger)
}

// CreateHeartbeatSyncer creates a heartbeat syncer
func (sf *SyncerFactory) CreateHeartbeatSyncer() HeartbeatSyncer {
	return NewRedisHeartbeatSyncer(sf.cache, sf.logger)
}

// CreateSyncerManager creates a syncer manager
func (sf *SyncerFactory) CreateSyncerManager() SyncerManager {
	return NewRedisSyncerManager(sf.cache, sf.logger)
}

// SyncerMonitor provides monitoring capabilities for syncers
type SyncerMonitor struct {
	manager SyncerManager
	logger  log.Logger

	// Monitoring configuration
	checkInterval time.Duration

	// Monitoring state
	running bool
	stopCh  chan struct{}
	mu      sync.RWMutex
}

// NewSyncerMonitor creates a new syncer monitor
func NewSyncerMonitor(manager SyncerManager, logger log.Logger) *SyncerMonitor {
	return &SyncerMonitor{
		manager:       manager,
		logger:        logger,
		checkInterval: 30 * time.Second,
		stopCh:        make(chan struct{}),
	}
}

// Start starts the syncer monitor
func (sm *SyncerMonitor) Start(ctx context.Context) {
	sm.mu.Lock()
	if sm.running {
		sm.mu.Unlock()
		return
	}
	sm.running = true
	sm.mu.Unlock()

	sm.logger.Infow("Starting syncer monitor", "check_interval", sm.checkInterval)

	ticker := time.NewTicker(sm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sm.logger.Infow("Syncer monitor stopped due to context cancellation")
			return
		case <-sm.stopCh:
			sm.logger.Infow("Syncer monitor stopped")
			return
		case <-ticker.C:
			sm.checkSyncerHealth(ctx)
		}
	}
}

// Stop stops the syncer monitor
func (sm *SyncerMonitor) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.running {
		return
	}

	sm.running = false
	close(sm.stopCh)
}

// checkSyncerHealth checks the health of all syncers
func (sm *SyncerMonitor) checkSyncerHealth(ctx context.Context) {
	stats := sm.manager.(*RedisSyncerManager).GetSyncerStatistics()

	for syncerType, syncerStats := range stats {
		sm.logger.Debugw("Syncer statistics",
			"syncer_type", syncerType,
			"success_count", syncerStats.SuccessCount,
			"failure_count", syncerStats.FailureCount,
			"total_count", syncerStats.TotalCount,
			"average_latency", syncerStats.AverageLatency,
			"last_sync_time", syncerStats.LastSyncTime,
		)

		// Check for high failure rates
		if syncerStats.TotalCount > 0 {
			failureRate := float64(syncerStats.FailureCount) / float64(syncerStats.TotalCount)
			if failureRate > 0.1 { // 10% failure rate threshold
				sm.logger.Warnw("High failure rate detected",
					"syncer_type", syncerType,
					"failure_rate", failureRate,
					"failure_count", syncerStats.FailureCount,
					"total_count", syncerStats.TotalCount,
				)
			}
		}

		// Check for stale syncers (no activity in last 5 minutes)
		if !syncerStats.LastSyncTime.IsZero() {
			elapsed := time.Since(syncerStats.LastSyncTime)
			if elapsed > 5*time.Minute {
				sm.logger.Warnw("Syncer appears stale",
					"syncer_type", syncerType,
					"last_sync_time", syncerStats.LastSyncTime,
					"elapsed", elapsed,
				)
			}
		}
	}
}

// SetCheckInterval sets the monitoring check interval
func (sm *SyncerMonitor) SetCheckInterval(interval time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.checkInterval = interval
}

// IsRunning returns whether the monitor is running
func (sm *SyncerMonitor) IsRunning() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.running
}

// SyncerUtils provides utility functions for syncers
type SyncerUtils struct{}

// NewSyncerUtils creates a new syncer utils instance
func NewSyncerUtils() *SyncerUtils {
	return &SyncerUtils{}
}

// GenerateBatchKey generates a standard batch key
func (su *SyncerUtils) GenerateBatchKey(batchID string) string {
	return "batch:" + batchID
}

// GenerateJobKey generates a standard job key
func (su *SyncerUtils) GenerateJobKey(jobID string) string {
	return "job:" + jobID
}

// GenerateTaskKey generates a standard task key
func (su *SyncerUtils) GenerateTaskKey(taskID string) string {
	return "task:" + taskID
}

// GeneratePartitionKey generates a standard partition key
func (su *SyncerUtils) GeneratePartitionKey(batchID, partitionID string) string {
	return "batch:" + batchID + ":partition:" + partitionID
}

// GenerateComponentKey generates a standard component key
func (su *SyncerUtils) GenerateComponentKey(componentID string) string {
	return "component:" + componentID
}

// ValidateKey validates a sync key format
func (su *SyncerUtils) ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > 250 { // Redis key length limit
		return fmt.Errorf("key too long: %d characters (max 250)", len(key))
	}

	return nil
}

// ValidateTTL validates a TTL value
func (su *SyncerUtils) ValidateTTL(ttl time.Duration) error {
	if ttl < 0 {
		return fmt.Errorf("TTL cannot be negative")
	}

	if ttl > 365*24*time.Hour { // One year maximum
		return fmt.Errorf("TTL too long: %v (max 1 year)", ttl)
	}

	return nil
}

// SanitizeValue sanitizes a value for storage
func (su *SyncerUtils) SanitizeValue(value interface{}) interface{} {
	// Basic sanitization - can be extended based on requirements
	if value == nil {
		return ""
	}

	return value
}

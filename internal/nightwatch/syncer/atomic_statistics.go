// Package syncer provides atomic statistics operations for message batch processing
package syncer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// AtomicCounter represents a thread-safe counter
type AtomicCounter struct {
	value int64
}

// NewAtomicCounter creates a new atomic counter
func NewAtomicCounter(initial int64) *AtomicCounter {
	return &AtomicCounter{value: initial}
}

// Add atomically adds value to the counter and returns the new value
func (ac *AtomicCounter) Add(delta int64) int64 {
	return atomic.AddInt64(&ac.value, delta)
}

// Increment atomically increments the counter by 1 and returns the new value
func (ac *AtomicCounter) Increment() int64 {
	return atomic.AddInt64(&ac.value, 1)
}

// Decrement atomically decrements the counter by 1 and returns the new value
func (ac *AtomicCounter) Decrement() int64 {
	return atomic.AddInt64(&ac.value, -1)
}

// Get atomically loads and returns the current value
func (ac *AtomicCounter) Get() int64 {
	return atomic.LoadInt64(&ac.value)
}

// Set atomically stores the value
func (ac *AtomicCounter) Set(value int64) {
	atomic.StoreInt64(&ac.value, value)
}

// CompareAndSwap atomically compares current value with old and sets it to new if they match
func (ac *AtomicCounter) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&ac.value, old, new)
}

// StepStatistics represents atomic statistics for batch processing steps
type StepStatistics struct {
	Total   *AtomicCounter `json:"total"`
	Success *AtomicCounter `json:"success"`
	Failure *AtomicCounter `json:"failure"`

	// Additional metrics
	startTime  time.Time
	lastUpdate int64 // Unix timestamp
	mu         sync.RWMutex
}

// NewStepStatistics creates new step statistics with atomic counters
func NewStepStatistics() *StepStatistics {
	return &StepStatistics{
		Total:      NewAtomicCounter(0),
		Success:    NewAtomicCounter(0),
		Failure:    NewAtomicCounter(0),
		startTime:  time.Now(),
		lastUpdate: time.Now().Unix(),
	}
}

// NewStepStatisticsWithValues creates step statistics with initial values
func NewStepStatisticsWithValues(success, failure int64) *StepStatistics {
	total := success + failure
	return &StepStatistics{
		Total:      NewAtomicCounter(total),
		Success:    NewAtomicCounter(success),
		Failure:    NewAtomicCounter(failure),
		startTime:  time.Now(),
		lastUpdate: time.Now().Unix(),
	}
}

// IncreaseTotal atomically increases total count
func (ss *StepStatistics) IncreaseTotal(num int64) int64 {
	result := ss.Total.Add(num)
	atomic.StoreInt64(&ss.lastUpdate, time.Now().Unix())
	return result
}

// IncreaseSuccess atomically increases success count
func (ss *StepStatistics) IncreaseSuccess(num int64) int64 {
	result := ss.Success.Add(num)
	atomic.StoreInt64(&ss.lastUpdate, time.Now().Unix())
	return result
}

// IncreaseFailure atomically increases failure count
func (ss *StepStatistics) IncreaseFailure(num int64) int64 {
	result := ss.Failure.Add(num)
	atomic.StoreInt64(&ss.lastUpdate, time.Now().Unix())
	return result
}

// GetSnapshot returns a consistent snapshot of statistics
func (ss *StepStatistics) GetSnapshot() *StepStatisticsSnapshot {
	return &StepStatisticsSnapshot{
		Total:       ss.Total.Get(),
		Success:     ss.Success.Get(),
		Failure:     ss.Failure.Get(),
		StartTime:   ss.startTime,
		LastUpdate:  time.Unix(atomic.LoadInt64(&ss.lastUpdate), 0),
		SuccessRate: ss.calculateSuccessRate(),
		Duration:    time.Since(ss.startTime),
	}
}

// calculateSuccessRate calculates success rate atomically
func (ss *StepStatistics) calculateSuccessRate() float64 {
	total := ss.Total.Get()
	if total == 0 {
		return 0.0
	}
	success := ss.Success.Get()
	return float64(success) / float64(total) * 100.0
}

// Reset resets all counters to zero
func (ss *StepStatistics) Reset() {
	ss.Total.Set(0)
	ss.Success.Set(0)
	ss.Failure.Set(0)
	ss.mu.Lock()
	ss.startTime = time.Now()
	ss.mu.Unlock()
	atomic.StoreInt64(&ss.lastUpdate, time.Now().Unix())
}

// StepStatisticsSnapshot represents a point-in-time snapshot of statistics
type StepStatisticsSnapshot struct {
	Total       int64         `json:"total"`
	Success     int64         `json:"success"`
	Failure     int64         `json:"failure"`
	StartTime   time.Time     `json:"start_time"`
	LastUpdate  time.Time     `json:"last_update"`
	SuccessRate float64       `json:"success_rate"`
	Duration    time.Duration `json:"duration"`
}

// AtomicStatisticsSyncer provides distributed atomic operations using Redis
type AtomicStatisticsSyncer struct {
	cache     *cache.CacheManager
	logger    log.Logger
	keyPrefix string

	// Local statistics cache
	localStats map[string]*StepStatistics
	mu         sync.RWMutex
}

// NewAtomicStatisticsSyncer creates a new atomic statistics syncer
func NewAtomicStatisticsSyncer(cache *cache.CacheManager, logger log.Logger, keyPrefix string) *AtomicStatisticsSyncer {
	return &AtomicStatisticsSyncer{
		cache:      cache,
		logger:     logger,
		keyPrefix:  keyPrefix,
		localStats: make(map[string]*StepStatistics),
	}
}

// buildKey constructs cache key with prefix
func (ass *AtomicStatisticsSyncer) buildKey(batchID, key string) string {
	return fmt.Sprintf("%s%s:%s", ass.keyPrefix, batchID, key)
}

// InitSyncer initializes statistics for a batch (clears existing data)
func (ass *AtomicStatisticsSyncer) InitSyncer(ctx context.Context, batchID string) error {
	keys := []string{
		ass.buildKey(batchID, "SUCCESS"),
		ass.buildKey(batchID, "FAILURE"),
		ass.buildKey(batchID, "TOTAL"),
	}

	if err := ass.cache.Delete(ctx, keys...); err != nil {
		ass.logger.Errorw("Failed to initialize syncer",
			"batch_id", batchID,
			"error", err,
		)
		return fmt.Errorf("failed to initialize syncer for batch %s: %w", batchID, err)
	}

	// Initialize local statistics
	ass.mu.Lock()
	ass.localStats[batchID] = NewStepStatistics()
	ass.mu.Unlock()

	ass.logger.Infow("Syncer initialized successfully",
		"batch_id", batchID,
	)

	return nil
}

// SyncToStatistics syncs Redis counters to local statistics and returns snapshot
func (ass *AtomicStatisticsSyncer) SyncToStatistics(ctx context.Context, batchID string) (*StepStatisticsSnapshot, error) {
	// Get current values from Redis (using INCR with 0 for atomic read)
	successKey := ass.buildKey(batchID, "SUCCESS")
	failureKey := ass.buildKey(batchID, "FAILURE")

	successVal, err := ass.cache.IncrBy(ctx, successKey, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get success count: %w", err)
	}

	failureVal, err := ass.cache.IncrBy(ctx, failureKey, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get failure count: %w", err)
	}

	// Update or create local statistics
	ass.mu.Lock()
	stats, exists := ass.localStats[batchID]
	if !exists {
		stats = NewStepStatisticsWithValues(successVal, failureVal)
		ass.localStats[batchID] = stats
	} else {
		// Update local counters to match Redis values
		stats.Success.Set(successVal)
		stats.Failure.Set(failureVal)
		stats.Total.Set(successVal + failureVal)
	}
	ass.mu.Unlock()

	snapshot := stats.GetSnapshot()

	ass.logger.Debugw("Statistics synced successfully",
		"batch_id", batchID,
		"success", snapshot.Success,
		"failure", snapshot.Failure,
		"total", snapshot.Total,
		"success_rate", snapshot.SuccessRate,
	)

	return snapshot, nil
}

// SyncToCounter atomically updates Redis counters
func (ass *AtomicStatisticsSyncer) SyncToCounter(ctx context.Context, batchID string, successNum, failureNum int64, isRetry bool) error {
	var operations []func() error

	// Handle success count
	if successNum != 0 {
		successKey := ass.buildKey(batchID, "SUCCESS")
		operations = append(operations, func() error {
			_, err := ass.cache.IncrBy(ctx, successKey, successNum)
			return err
		})
	}

	// Handle failure count (special logic for retry)
	actualFailureNum := failureNum
	if isRetry {
		// For retry operations, decrease failure count by success amount
		actualFailureNum = -successNum
	}

	if actualFailureNum != 0 {
		failureKey := ass.buildKey(batchID, "FAILURE")
		operations = append(operations, func() error {
			_, err := ass.cache.IncrBy(ctx, failureKey, actualFailureNum)
			return err
		})
	}

	// Execute all operations
	for _, op := range operations {
		if err := op(); err != nil {
			ass.logger.Errorw("Failed to sync counter",
				"batch_id", batchID,
				"success_num", successNum,
				"failure_num", failureNum,
				"is_retry", isRetry,
				"error", err,
			)
			return fmt.Errorf("failed to sync counter: %w", err)
		}
	}

	// Update local statistics
	ass.mu.Lock()
	if stats, exists := ass.localStats[batchID]; exists {
		if successNum != 0 {
			stats.IncreaseSuccess(successNum)
		}
		if actualFailureNum != 0 {
			stats.IncreaseFailure(actualFailureNum)
		}
		// Update total
		stats.Total.Set(stats.Success.Get() + stats.Failure.Get())
	}
	ass.mu.Unlock()

	ass.logger.Debugw("Counter synced successfully",
		"batch_id", batchID,
		"success_added", successNum,
		"failure_added", actualFailureNum,
		"is_retry", isRetry,
	)

	return nil
}

// GetLocalStatistics returns local statistics snapshot without Redis sync
func (ass *AtomicStatisticsSyncer) GetLocalStatistics(batchID string) *StepStatisticsSnapshot {
	ass.mu.RLock()
	stats, exists := ass.localStats[batchID]
	ass.mu.RUnlock()

	if !exists {
		return &StepStatisticsSnapshot{}
	}

	return stats.GetSnapshot()
}

// ClearLocalStatistics removes local statistics for a batch
func (ass *AtomicStatisticsSyncer) ClearLocalStatistics(batchID string) {
	ass.mu.Lock()
	delete(ass.localStats, batchID)
	ass.mu.Unlock()

	ass.logger.Infow("Local statistics cleared",
		"batch_id", batchID,
	)
}

// GetAllBatchStatistics returns statistics for all tracked batches
func (ass *AtomicStatisticsSyncer) GetAllBatchStatistics() map[string]*StepStatisticsSnapshot {
	ass.mu.RLock()
	defer ass.mu.RUnlock()

	result := make(map[string]*StepStatisticsSnapshot)
	for batchID, stats := range ass.localStats {
		result[batchID] = stats.GetSnapshot()
	}

	return result
}

// GlobalCounter provides global atomic counter operations
type GlobalCounter struct {
	cache     *cache.CacheManager
	logger    log.Logger
	keyPrefix string
}

// NewGlobalCounter creates a new global counter
func NewGlobalCounter(cache *cache.CacheManager, logger log.Logger, keyPrefix string) *GlobalCounter {
	return &GlobalCounter{
		cache:     cache,
		logger:    logger,
		keyPrefix: keyPrefix,
	}
}

// GetExtCode generates a unique external code (0-99999)
func (gc *GlobalCounter) GetExtCode(ctx context.Context) (int32, error) {
	key := gc.keyPrefix + "EXT_CODE"
	val, err := gc.cache.Incr(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get ext code: %w", err)
	}

	extCode := int32(val % 100000)
	gc.logger.Debugw("Generated ext code",
		"ext_code", extCode,
		"raw_value", val,
	)

	return extCode, nil
}

// GetWorkID generates a work ID based on node configuration
func (gc *GlobalCounter) GetWorkID(ctx context.Context, nodeNum int32) (int32, error) {
	key := gc.keyPrefix + "WORK_CODE"
	val, err := gc.cache.Incr(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get work ID: %w", err)
	}

	workID := int32(val % int64(nodeNum))
	gc.logger.Debugw("Generated work ID",
		"work_id", workID,
		"raw_value", val,
		"node_num", nodeNum,
	)

	return workID, nil
}

// GetCustomCounter gets or increments a custom counter
func (gc *GlobalCounter) GetCustomCounter(ctx context.Context, counterName string) (int64, error) {
	key := gc.keyPrefix + counterName
	val, err := gc.cache.Incr(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get counter %s: %w", counterName, err)
	}

	gc.logger.Debugw("Counter incremented",
		"counter_name", counterName,
		"value", val,
	)

	return val, nil
}

// SetCustomCounter sets a custom counter to a specific value
func (gc *GlobalCounter) SetCustomCounter(ctx context.Context, counterName string, value int64) error {
	key := gc.keyPrefix + counterName
	if err := gc.cache.SetString(ctx, key, fmt.Sprintf("%d", value), 0); err != nil {
		return fmt.Errorf("failed to set counter %s: %w", counterName, err)
	}

	gc.logger.Debugw("Counter set",
		"counter_name", counterName,
		"value", value,
	)

	return nil
}

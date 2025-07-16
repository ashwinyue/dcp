// Package syncer provides distributed atomic counter service with Redis backend
package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// AtomicCounterService provides distributed atomic counter operations
type AtomicCounterService struct {
	cache     *cache.CacheManager
	logger    log.Logger
	keyPrefix string

	// Thread management
	threadCounters map[string]*ThreadCounter
	threadMutex    sync.RWMutex

	// Heartbeat management
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	// Partition management
	partitionCount int
	bitOperations  *BitOperations
}

// NewAtomicCounterService creates a new atomic counter service
func NewAtomicCounterService(cache *cache.CacheManager, logger log.Logger, keyPrefix string) *AtomicCounterService {
	return &AtomicCounterService{
		cache:             cache,
		logger:            logger,
		keyPrefix:         keyPrefix,
		threadCounters:    make(map[string]*ThreadCounter),
		heartbeatInterval: 30 * time.Second,
		heartbeatTimeout:  2 * time.Minute,
		partitionCount:    128, // 与Java项目保持一致
		bitOperations:     NewBitOperations(cache, logger),
	}
}

// ThreadCounter manages thread counting operations
type ThreadCounter struct {
	BatchID     string
	CounterType string
	mu          sync.RWMutex

	// Counter state
	activeThreads    int64
	startedThreads   int64
	completedThreads int64
	lastActivity     time.Time
}

// BitOperations provides Redis bit operations for partition management
type BitOperations struct {
	cache  *cache.CacheManager
	logger log.Logger
}

// NewBitOperations creates a new bit operations manager
func NewBitOperations(cache *cache.CacheManager, logger log.Logger) *BitOperations {
	return &BitOperations{
		cache:  cache,
		logger: logger,
	}
}

// buildKey constructs cache key with prefix
func (acs *AtomicCounterService) buildKey(batchID, suffix string) string {
	return fmt.Sprintf("%s%s:%s", acs.keyPrefix, batchID, suffix)
}

// buildBitKey constructs bit operation key
func (acs *AtomicCounterService) buildBitKey(batchID string) string {
	return fmt.Sprintf("%s%s:BIT", acs.keyPrefix, batchID)
}

// InitializeCounters initializes all counters for a batch (clears existing data)
func (acs *AtomicCounterService) InitializeCounters(ctx context.Context, batchID string) error {
	keys := []string{
		acs.buildKey(batchID, "SUCCESS"),
		acs.buildKey(batchID, "FAILURE"),
		acs.buildKey(batchID, "TOTAL"),
		acs.buildKey(batchID, "SEND"),
		acs.buildKey(batchID, "SAVE"),
		acs.buildKey(batchID, "READ_STATUS"),
		acs.buildKey(batchID, "HEART"),
		acs.buildBitKey(batchID),
	}

	if err := acs.cache.Delete(ctx, keys...); err != nil {
		acs.logger.Errorw("Failed to initialize counters",
			"batch_id", batchID,
			"error", err,
		)
		return fmt.Errorf("failed to initialize counters for batch %s: %w", batchID, err)
	}

	acs.logger.Infow("Counters initialized successfully",
		"batch_id", batchID,
		"cleared_keys", len(keys),
	)

	return nil
}

// InitializePartitionCounters initializes partition-specific counters
func (acs *AtomicCounterService) InitializePartitionCounters(ctx context.Context, batchID string) error {
	// Clear bit operations
	bitKey := acs.buildBitKey(batchID)
	if err := acs.cache.Delete(ctx, bitKey); err != nil {
		return fmt.Errorf("failed to clear partition bits: %w", err)
	}

	// Clear partition heartbeat keys
	for i := 0; i < acs.partitionCount; i++ {
		partitionKey := fmt.Sprintf("partition_%d", i)
		heartbeatKey := acs.buildKey(batchID, "PARTITION_HEART:"+partitionKey)
		if err := acs.cache.Delete(ctx, heartbeatKey); err != nil {
			acs.logger.Warnw("Failed to clear partition heartbeat",
				"batch_id", batchID,
				"partition", partitionKey,
				"error", err,
			)
		}
	}

	acs.logger.Infow("Partition counters initialized",
		"batch_id", batchID,
		"partition_count", acs.partitionCount,
	)

	return nil
}

// IncrementCounter atomically increments a counter
func (acs *AtomicCounterService) IncrementCounter(ctx context.Context, batchID, counterType string, delta int64) (int64, error) {
	key := acs.buildKey(batchID, counterType)

	newVal, err := acs.cache.IncrBy(ctx, key, delta)
	if err != nil {
		acs.logger.Errorw("Failed to increment counter",
			"batch_id", batchID,
			"counter_type", counterType,
			"delta", delta,
			"error", err,
		)
		return 0, fmt.Errorf("failed to increment counter %s: %w", counterType, err)
	}

	acs.logger.Debugw("Counter incremented",
		"batch_id", batchID,
		"counter_type", counterType,
		"delta", delta,
		"new_value", newVal,
	)

	return newVal, nil
}

// GetCounter gets current counter value
func (acs *AtomicCounterService) GetCounter(ctx context.Context, batchID, counterType string) (int64, error) {
	key := acs.buildKey(batchID, counterType)

	val, err := acs.cache.IncrBy(ctx, key, 0) // INCR with 0 for atomic read
	if err != nil {
		return 0, fmt.Errorf("failed to get counter %s: %w", counterType, err)
	}

	return val, nil
}

// Thread Management Operations

// StartSendThread marks the start of a send thread
func (acs *AtomicCounterService) StartSendThread(ctx context.Context, batchID string) (int64, error) {
	return acs.IncrementCounter(ctx, batchID, "SEND", 1)
}

// EndSendThread marks the end of a send thread
func (acs *AtomicCounterService) EndSendThread(ctx context.Context, batchID string) (int64, error) {
	return acs.IncrementCounter(ctx, batchID, "SEND", -1)
}

// IsAllSendThreadsComplete checks if all send threads are complete
func (acs *AtomicCounterService) IsAllSendThreadsComplete(ctx context.Context, batchID string) (bool, error) {
	count, err := acs.GetCounter(ctx, batchID, "SEND")
	if err != nil {
		return false, err
	}

	isComplete := count == 0
	acs.logger.Debugw("Send threads completion check",
		"batch_id", batchID,
		"active_threads", count,
		"all_complete", isComplete,
	)

	return isComplete, nil
}

// StartSaveThread marks the start of a save thread
func (acs *AtomicCounterService) StartSaveThread(ctx context.Context, batchID string) (int64, error) {
	return acs.IncrementCounter(ctx, batchID, "SAVE", 1)
}

// EndSaveThread marks the end of a save thread
func (acs *AtomicCounterService) EndSaveThread(ctx context.Context, batchID string) (int64, error) {
	return acs.IncrementCounter(ctx, batchID, "SAVE", -1)
}

// IsAllSaveThreadsComplete checks if all save threads are complete
func (acs *AtomicCounterService) IsAllSaveThreadsComplete(ctx context.Context, batchID string) (bool, error) {
	count, err := acs.GetCounter(ctx, batchID, "SAVE")
	if err != nil {
		return false, err
	}

	return count == 0, nil
}

// Read Thread Operations

// MarkReadThreadComplete marks the read thread as complete
func (acs *AtomicCounterService) MarkReadThreadComplete(ctx context.Context, batchID string) error {
	key := acs.buildKey(batchID, "READ_STATUS")
	if err := acs.cache.SetString(ctx, key, "completed", 24*time.Hour); err != nil {
		return fmt.Errorf("failed to mark read thread complete: %w", err)
	}

	acs.logger.Infow("Read thread marked as complete",
		"batch_id", batchID,
	)

	return nil
}

// IsReadThreadComplete checks if the read thread is complete
func (acs *AtomicCounterService) IsReadThreadComplete(ctx context.Context, batchID string) (bool, error) {
	key := acs.buildKey(batchID, "READ_STATUS")
	value, err := acs.cache.GetString(ctx, key)
	if err != nil {
		// Key doesn't exist means not complete
		return false, nil
	}

	return value == "completed", nil
}

// Heartbeat Operations

// SetHeartbeat sets heartbeat timestamp for a batch or partition
func (acs *AtomicCounterService) SetHeartbeat(ctx context.Context, batchID string, partitionKey string) error {
	var key string
	if partitionKey == "" {
		// Global heartbeat
		key = acs.buildKey(batchID, "HEART")
	} else {
		// Partition-specific heartbeat
		key = acs.buildKey(batchID, "PARTITION_HEART:"+partitionKey)
	}

	timestamp := time.Now().Unix()
	if err := acs.cache.SetString(ctx, key, fmt.Sprintf("%d", timestamp), acs.heartbeatTimeout); err != nil {
		return fmt.Errorf("failed to set heartbeat: %w", err)
	}

	acs.logger.Debugw("Heartbeat set",
		"batch_id", batchID,
		"partition_key", partitionKey,
		"timestamp", timestamp,
	)

	return nil
}

// GetHeartbeat gets heartbeat timestamp
func (acs *AtomicCounterService) GetHeartbeat(ctx context.Context, batchID string, partitionKey string) (int64, error) {
	var key string
	if partitionKey == "" {
		key = acs.buildKey(batchID, "HEART")
	} else {
		key = acs.buildKey(batchID, "PARTITION_HEART:"+partitionKey)
	}

	value, err := acs.cache.GetString(ctx, key)
	if err != nil {
		return 0, nil // Return 0 if not found
	}

	var timestamp int64
	if _, err := fmt.Sscanf(value, "%d", &timestamp); err != nil {
		return 0, fmt.Errorf("failed to parse heartbeat timestamp: %w", err)
	}

	return timestamp, nil
}

// IsHeartbeatAlive checks if heartbeat is still alive
func (acs *AtomicCounterService) IsHeartbeatAlive(ctx context.Context, batchID string, partitionKey string) (bool, error) {
	timestamp, err := acs.GetHeartbeat(ctx, batchID, partitionKey)
	if err != nil {
		return false, err
	}

	if timestamp == 0 {
		return false, nil // No heartbeat found
	}

	lastHeartbeat := time.Unix(timestamp, 0)
	isAlive := time.Since(lastHeartbeat) < acs.heartbeatTimeout

	acs.logger.Debugw("Heartbeat check",
		"batch_id", batchID,
		"partition_key", partitionKey,
		"last_heartbeat", lastHeartbeat,
		"is_alive", isAlive,
	)

	return isAlive, nil
}

// Partition Bit Operations

// MarkPartitionComplete marks a partition as complete using bit operations
func (acs *AtomicCounterService) MarkPartitionComplete(ctx context.Context, batchID string, partitionIndex int) error {
	if partitionIndex < 0 || partitionIndex >= acs.partitionCount {
		return fmt.Errorf("partition index %d out of range [0, %d)", partitionIndex, acs.partitionCount)
	}

	return acs.bitOperations.SetBit(ctx, acs.buildBitKey(batchID), partitionIndex, true)
}

// IsPartitionComplete checks if a partition is complete
func (acs *AtomicCounterService) IsPartitionComplete(ctx context.Context, batchID string, partitionIndex int) (bool, error) {
	if partitionIndex < 0 || partitionIndex >= acs.partitionCount {
		return false, fmt.Errorf("partition index %d out of range [0, %d)", partitionIndex, acs.partitionCount)
	}

	return acs.bitOperations.GetBit(ctx, acs.buildBitKey(batchID), partitionIndex)
}

// IsAllPartitionsComplete checks if all partitions are complete
func (acs *AtomicCounterService) IsAllPartitionsComplete(ctx context.Context, batchID string) (bool, error) {
	count, err := acs.bitOperations.CountBits(ctx, acs.buildBitKey(batchID))
	if err != nil {
		return false, err
	}

	isComplete := int(count) == acs.partitionCount
	acs.logger.Debugw("Partition completion check",
		"batch_id", batchID,
		"completed_partitions", count,
		"total_partitions", acs.partitionCount,
		"all_complete", isComplete,
	)

	return isComplete, nil
}

// GetCompletedPartitionsCount returns the number of completed partitions
func (acs *AtomicCounterService) GetCompletedPartitionsCount(ctx context.Context, batchID string) (int64, error) {
	return acs.bitOperations.CountBits(ctx, acs.buildBitKey(batchID))
}

// Batch Statistics Operations

// SyncStatisticsCounters syncs success/failure counters to local statistics
func (acs *AtomicCounterService) SyncStatisticsCounters(ctx context.Context, batchID string) (*StepStatisticsSnapshot, error) {
	successCount, err := acs.GetCounter(ctx, batchID, "SUCCESS")
	if err != nil {
		return nil, fmt.Errorf("failed to get success count: %w", err)
	}

	failureCount, err := acs.GetCounter(ctx, batchID, "FAILURE")
	if err != nil {
		return nil, fmt.Errorf("failed to get failure count: %w", err)
	}

	totalCount := successCount + failureCount
	var successRate float64
	if totalCount > 0 {
		successRate = float64(successCount) / float64(totalCount) * 100.0
	}

	snapshot := &StepStatisticsSnapshot{
		Total:       totalCount,
		Success:     successCount,
		Failure:     failureCount,
		SuccessRate: successRate,
		LastUpdate:  time.Now(),
	}

	acs.logger.Debugw("Statistics synced",
		"batch_id", batchID,
		"success", successCount,
		"failure", failureCount,
		"total", totalCount,
		"success_rate", successRate,
	)

	return snapshot, nil
}

// UpdateStatisticsCounters updates success/failure counters with retry logic
func (acs *AtomicCounterService) UpdateStatisticsCounters(ctx context.Context, batchID string, successDelta, failureDelta int64, isRetry bool) error {
	var operations []func() error

	// Handle success count
	if successDelta != 0 {
		operations = append(operations, func() error {
			_, err := acs.IncrementCounter(ctx, batchID, "SUCCESS", successDelta)
			return err
		})
	}

	// Handle failure count (special logic for retry)
	actualFailureDelta := failureDelta
	if isRetry {
		// For retry operations, decrease failure count by success amount
		actualFailureDelta = -successDelta
	}

	if actualFailureDelta != 0 {
		operations = append(operations, func() error {
			_, err := acs.IncrementCounter(ctx, batchID, "FAILURE", actualFailureDelta)
			return err
		})
	}

	// Execute all operations
	for _, op := range operations {
		if err := op(); err != nil {
			acs.logger.Errorw("Failed to update statistics counters",
				"batch_id", batchID,
				"success_delta", successDelta,
				"failure_delta", failureDelta,
				"is_retry", isRetry,
				"error", err,
			)
			return fmt.Errorf("failed to update statistics counters: %w", err)
		}
	}

	acs.logger.Debugw("Statistics counters updated",
		"batch_id", batchID,
		"success_delta", successDelta,
		"failure_delta", actualFailureDelta,
		"is_retry", isRetry,
	)

	return nil
}

// Service Management

// SetHeartbeatInterval sets the heartbeat check interval
func (acs *AtomicCounterService) SetHeartbeatInterval(interval time.Duration) {
	acs.heartbeatInterval = interval
	acs.logger.Infow("Heartbeat interval updated",
		"interval", interval,
	)
}

// SetHeartbeatTimeout sets the heartbeat timeout duration
func (acs *AtomicCounterService) SetHeartbeatTimeout(timeout time.Duration) {
	acs.heartbeatTimeout = timeout
	acs.logger.Infow("Heartbeat timeout updated",
		"timeout", timeout,
	)
}

// GetServiceStats returns service statistics
func (acs *AtomicCounterService) GetServiceStats() map[string]interface{} {
	acs.threadMutex.RLock()
	threadCount := len(acs.threadCounters)
	acs.threadMutex.RUnlock()

	return map[string]interface{}{
		"key_prefix":         acs.keyPrefix,
		"partition_count":    acs.partitionCount,
		"heartbeat_interval": acs.heartbeatInterval,
		"heartbeat_timeout":  acs.heartbeatTimeout,
		"thread_counters":    threadCount,
		"service_status":     "active",
	}
}

// BitOperations implementation

// SetBit sets a bit at the specified offset
func (bo *BitOperations) SetBit(ctx context.Context, key string, offset int, value bool) error {
	// Note: Redis SETBIT is atomic
	var bitValue int
	if value {
		bitValue = 1
	}

	// Use Redis string operation since our cache doesn't have SETBIT
	// We'll simulate with a hash field approach for now
	fieldKey := fmt.Sprintf("bit_%d", offset)
	cacheKey := key + "_bits"

	if err := bo.cache.SetHash(ctx, cacheKey, fieldKey, bitValue); err != nil {
		bo.logger.Errorw("Failed to set bit",
			"key", key,
			"offset", offset,
			"value", value,
			"error", err,
		)
		return fmt.Errorf("failed to set bit: %w", err)
	}

	bo.logger.Debugw("Bit set successfully",
		"key", key,
		"offset", offset,
		"value", value,
	)

	return nil
}

// GetBit gets a bit value at the specified offset
func (bo *BitOperations) GetBit(ctx context.Context, key string, offset int) (bool, error) {
	fieldKey := fmt.Sprintf("bit_%d", offset)
	cacheKey := key + "_bits"

	var bitValue int
	if err := bo.cache.GetHash(ctx, cacheKey, fieldKey, &bitValue); err != nil {
		// Bit not set, return false
		return false, nil
	}

	return bitValue == 1, nil
}

// CountBits counts the number of set bits
func (bo *BitOperations) CountBits(ctx context.Context, key string) (int64, error) {
	// This is a simplified implementation
	// In a real Redis implementation, you would use BITCOUNT

	// For our simulation, we'll need to iterate through possible bits
	// This is not as efficient as Redis BITCOUNT but works for demonstration
	var count int64

	// Check up to 128 bits (partition count)
	for i := 0; i < 128; i++ {
		if isSet, err := bo.GetBit(ctx, key, i); err == nil && isSet {
			count++
		}
	}

	bo.logger.Debugw("Bit count calculated",
		"key", key,
		"count", count,
	)

	return count, nil
}

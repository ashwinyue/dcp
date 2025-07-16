// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

const (
	// Key prefixes for different syncer types
	statusSyncerPrefix     = "syncer:status:"
	statisticsSyncerPrefix = "syncer:stats:"
	partitionSyncerPrefix  = "syncer:partition:"
	heartbeatSyncerPrefix  = "syncer:heartbeat:"
	counterPrefix          = "syncer:counter:"

	// Default TTL values
	defaultStatusTTL     = 24 * time.Hour
	defaultStatisticsTTL = 1 * time.Hour
	defaultPartitionTTL  = 12 * time.Hour
	defaultHeartbeatTTL  = 5 * time.Minute
)

// RedisSyncer implements the Syncer interface using Redis
type RedisSyncer struct {
	cache  *cache.CacheManager
	logger log.Logger
	prefix string

	// Statistics tracking
	stats *SyncStatistics
	mu    sync.RWMutex
}

// NewRedisSyncer creates a new Redis-based syncer
func NewRedisSyncer(cache *cache.CacheManager, logger log.Logger, prefix string) *RedisSyncer {
	return &RedisSyncer{
		cache:  cache,
		logger: logger,
		prefix: prefix,
		stats: &SyncStatistics{
			LastSyncTime: time.Now(),
		},
	}
}

// buildKey builds a cache key with prefix
func (rs *RedisSyncer) buildKey(key string) string {
	return rs.prefix + key
}

// Sync synchronizes data
func (rs *RedisSyncer) Sync(ctx context.Context, data *SyncData) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{
		Timestamp: start,
	}

	rs.mu.Lock()
	rs.stats.TotalCount++
	rs.mu.Unlock()

	key := rs.buildKey(data.Key)

	switch data.Operation {
	case SyncOperationSet:
		err := rs.cache.Set(ctx, key, data.Value, data.TTL)
		if err != nil {
			rs.recordError(err)
			result.Error = err
			return result, err
		}
		result.Success = true

	case SyncOperationGet:
		var value interface{}
		err := rs.cache.Get(ctx, key, &value)
		if err != nil {
			rs.recordError(err)
			result.Error = err
			return result, err
		}
		result.Success = true
		result.Value = value

	case SyncOperationIncrement:
		if val, ok := data.Value.(int64); ok {
			newVal, err := rs.cache.IncrBy(ctx, key, val)
			if err != nil {
				rs.recordError(err)
				result.Error = err
				return result, err
			}
			result.Success = true
			result.Value = newVal
		} else {
			err := fmt.Errorf("increment operation requires int64 value")
			rs.recordError(err)
			result.Error = err
			return result, err
		}

	case SyncOperationDecrement:
		if val, ok := data.Value.(int64); ok {
			newVal, err := rs.cache.IncrBy(ctx, key, -val)
			if err != nil {
				rs.recordError(err)
				result.Error = err
				return result, err
			}
			result.Success = true
			result.Value = newVal
		} else {
			err := fmt.Errorf("decrement operation requires int64 value")
			rs.recordError(err)
			result.Error = err
			return result, err
		}

	case SyncOperationDelete:
		err := rs.cache.Delete(ctx, key)
		if err != nil {
			rs.recordError(err)
			result.Error = err
			return result, err
		}
		result.Success = true

	default:
		err := fmt.Errorf("unsupported operation: %s", data.Operation)
		rs.recordError(err)
		result.Error = err
		return result, err
	}

	rs.recordSuccess(time.Since(start))
	return result, nil
}

// BatchSync synchronizes multiple data items
func (rs *RedisSyncer) BatchSync(ctx context.Context, data []*SyncData) ([]*SyncResult, error) {
	results := make([]*SyncResult, len(data))

	for i, item := range data {
		result, err := rs.Sync(ctx, item)
		results[i] = result
		if err != nil {
			rs.logger.Errorw("Batch sync item failed",
				"index", i,
				"key", item.Key,
				"operation", item.Operation,
				"error", err,
			)
		}
	}

	return results, nil
}

// Get retrieves synchronized data
func (rs *RedisSyncer) Get(ctx context.Context, key string) (*SyncResult, error) {
	data := &SyncData{
		Key:       key,
		Operation: SyncOperationGet,
	}
	return rs.Sync(ctx, data)
}

// Delete removes synchronized data
func (rs *RedisSyncer) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = rs.buildKey(key)
	}

	return rs.cache.Delete(ctx, prefixedKeys...)
}

// GetStatistics returns sync statistics
func (rs *RedisSyncer) GetStatistics() *SyncStatistics {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// Create a copy to avoid data race
	stats := *rs.stats
	return &stats
}

// Close closes the syncer
func (rs *RedisSyncer) Close() error {
	rs.logger.Infow("Redis syncer closed", "prefix", rs.prefix)
	return nil
}

// recordSuccess updates success statistics
func (rs *RedisSyncer) recordSuccess(latency time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.stats.SuccessCount++
	rs.stats.LastSyncTime = time.Now()

	// Update average latency
	if rs.stats.SuccessCount == 1 {
		rs.stats.AverageLatency = latency
	} else {
		rs.stats.AverageLatency = time.Duration(
			(int64(rs.stats.AverageLatency)*(rs.stats.SuccessCount-1) + int64(latency)) / rs.stats.SuccessCount,
		)
	}
}

// recordError updates error statistics
func (rs *RedisSyncer) recordError(err error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.stats.FailureCount++
	rs.stats.FailureTotal++
	rs.stats.LastSyncTime = time.Now()

	rs.logger.Errorw("Sync operation failed",
		"prefix", rs.prefix,
		"error", err,
		"failure_count", rs.stats.FailureCount,
	)
}

// RedisStatusSyncer implements StatusSyncer interface
type RedisStatusSyncer struct {
	*RedisSyncer
}

// NewRedisStatusSyncer creates a new Redis status syncer
func NewRedisStatusSyncer(cache *cache.CacheManager, logger log.Logger) *RedisStatusSyncer {
	return &RedisStatusSyncer{
		RedisSyncer: NewRedisSyncer(cache, logger, statusSyncerPrefix),
	}
}

// SetJobStatus sets job status
func (rss *RedisStatusSyncer) SetJobStatus(ctx context.Context, jobID, status string) error {
	data := &SyncData{
		Key:       "job:" + jobID,
		Value:     status,
		Operation: SyncOperationSet,
		TTL:       defaultStatusTTL,
		Timestamp: time.Now(),
	}

	_, err := rss.Sync(ctx, data)
	return err
}

// GetJobStatus gets job status
func (rss *RedisStatusSyncer) GetJobStatus(ctx context.Context, jobID string) (string, error) {
	result, err := rss.Get(ctx, "job:"+jobID)
	if err != nil {
		return "", err
	}

	if status, ok := result.Value.(string); ok {
		return status, nil
	}

	return "", fmt.Errorf("invalid status type for job %s", jobID)
}

// SetTaskStatus sets task status
func (rss *RedisStatusSyncer) SetTaskStatus(ctx context.Context, taskID, status string) error {
	data := &SyncData{
		Key:       "task:" + taskID,
		Value:     status,
		Operation: SyncOperationSet,
		TTL:       defaultStatusTTL,
		Timestamp: time.Now(),
	}

	_, err := rss.Sync(ctx, data)
	return err
}

// GetTaskStatus gets task status
func (rss *RedisStatusSyncer) GetTaskStatus(ctx context.Context, taskID string) (string, error) {
	result, err := rss.Get(ctx, "task:"+taskID)
	if err != nil {
		return "", err
	}

	if status, ok := result.Value.(string); ok {
		return status, nil
	}

	return "", fmt.Errorf("invalid status type for task %s", taskID)
}

// BatchSetStatus sets multiple statuses
func (rss *RedisStatusSyncer) BatchSetStatus(ctx context.Context, statuses map[string]string) error {
	data := make([]*SyncData, 0, len(statuses))

	for key, status := range statuses {
		data = append(data, &SyncData{
			Key:       key,
			Value:     status,
			Operation: SyncOperationSet,
			TTL:       defaultStatusTTL,
			Timestamp: time.Now(),
		})
	}

	results, err := rss.BatchSync(ctx, data)
	if err != nil {
		return err
	}

	// Check for any failures
	for i, result := range results {
		if !result.Success {
			return fmt.Errorf("failed to set status for key %s: %v", data[i].Key, result.Error)
		}
	}

	return nil
}

// RedisStatisticsSyncer implements StatisticsSyncer interface
type RedisStatisticsSyncer struct {
	*RedisSyncer
}

// NewRedisStatisticsSyncer creates a new Redis statistics syncer
func NewRedisStatisticsSyncer(cache *cache.CacheManager, logger log.Logger) *RedisStatisticsSyncer {
	return &RedisStatisticsSyncer{
		RedisSyncer: NewRedisSyncer(cache, logger, statisticsSyncerPrefix),
	}
}

// IncrementCounter increments a counter
func (rss *RedisStatisticsSyncer) IncrementCounter(ctx context.Context, key string, value int64) (int64, error) {
	data := &SyncData{
		Key:       counterPrefix + key,
		Value:     value,
		Operation: SyncOperationIncrement,
		Timestamp: time.Now(),
	}

	result, err := rss.Sync(ctx, data)
	if err != nil {
		return 0, err
	}

	if val, ok := result.Value.(int64); ok {
		return val, nil
	}

	return 0, fmt.Errorf("invalid counter value type for key %s", key)
}

// DecrementCounter decrements a counter
func (rss *RedisStatisticsSyncer) DecrementCounter(ctx context.Context, key string, value int64) (int64, error) {
	data := &SyncData{
		Key:       counterPrefix + key,
		Value:     value,
		Operation: SyncOperationDecrement,
		Timestamp: time.Now(),
	}

	result, err := rss.Sync(ctx, data)
	if err != nil {
		return 0, err
	}

	if val, ok := result.Value.(int64); ok {
		return val, nil
	}

	return 0, fmt.Errorf("invalid counter value type for key %s", key)
}

// GetCounter gets counter value
func (rss *RedisStatisticsSyncer) GetCounter(ctx context.Context, key string) (int64, error) {
	result, err := rss.Get(ctx, counterPrefix+key)
	if err != nil {
		return 0, err
	}

	if val, ok := result.Value.(int64); ok {
		return val, nil
	}

	// Try to parse as string
	if str, ok := result.Value.(string); ok {
		return ParseInt64(str), nil
	}

	return 0, nil // Return 0 for missing counters
}

// SetStatistics sets statistics data
func (rss *RedisStatisticsSyncer) SetStatistics(ctx context.Context, key string, stats *SyncStatistics) error {
	data := &SyncData{
		Key:       "stats:" + key,
		Value:     stats,
		Operation: SyncOperationSet,
		TTL:       defaultStatisticsTTL,
		Timestamp: time.Now(),
	}

	_, err := rss.Sync(ctx, data)
	return err
}

// GetStatisticsData gets statistics data
func (rss *RedisStatisticsSyncer) GetStatisticsData(ctx context.Context, key string) (*SyncStatistics, error) {
	result, err := rss.Get(ctx, "stats:"+key)
	if err != nil {
		return nil, err
	}

	if result.Value == nil {
		return &SyncStatistics{}, nil
	}

	// Try to unmarshal JSON
	var stats SyncStatistics
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &stats); err != nil {
			return nil, fmt.Errorf("failed to unmarshal statistics: %w", err)
		}
		return &stats, nil
	}

	if statsPtr, ok := result.Value.(*SyncStatistics); ok {
		return statsPtr, nil
	}

	return nil, fmt.Errorf("invalid statistics type for key %s", key)
}

// ResetCounter resets a counter to zero
func (rss *RedisStatisticsSyncer) ResetCounter(ctx context.Context, key string) error {
	return rss.Delete(ctx, counterPrefix+key)
}

// ParseInt64 safely parses string to int64
func ParseInt64(s string) int64 {
	if s == "" {
		return 0
	}

	// Try to parse as JSON number first
	var val int64
	if err := json.Unmarshal([]byte(s), &val); err == nil {
		return val
	}

	// Fallback to direct string conversion
	if strings.Contains(s, ".") {
		// Handle float strings
		var f float64
		if err := json.Unmarshal([]byte(s), &f); err == nil {
			return int64(f)
		}
	}

	return 0
}

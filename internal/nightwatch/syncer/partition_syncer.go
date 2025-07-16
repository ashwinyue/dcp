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
	"strconv"
	"strings"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

const (
	// Partition specific constants
	partitionBitPrefix = "syncer:partition:bit:"
	partitionDataKey   = "data"
	partitionCountKey  = "count"
	partitionListKey   = "list"

	// Default partition count (consistent with message batch processing)
	defaultPartitionCount = 128
)

// RedisPartitionSyncer implements PartitionSyncer interface
type RedisPartitionSyncer struct {
	*RedisSyncer
}

// NewRedisPartitionSyncer creates a new Redis partition syncer
func NewRedisPartitionSyncer(cache *cache.CacheManager, logger log.Logger) *RedisPartitionSyncer {
	return &RedisPartitionSyncer{
		RedisSyncer: NewRedisSyncer(cache, logger, partitionSyncerPrefix),
	}
}

// SetPartitionStatus sets partition status
func (rps *RedisPartitionSyncer) SetPartitionStatus(ctx context.Context, batchID, partitionID, status string) error {
	key := fmt.Sprintf("batch:%s:partition:%s:status", batchID, partitionID)
	data := &SyncData{
		Key:       key,
		Value:     status,
		Operation: SyncOperationSet,
		TTL:       defaultPartitionTTL,
		Timestamp: time.Now(),
	}

	_, err := rps.Sync(ctx, data)
	return err
}

// GetPartitionStatus gets partition status
func (rps *RedisPartitionSyncer) GetPartitionStatus(ctx context.Context, batchID, partitionID string) (string, error) {
	key := fmt.Sprintf("batch:%s:partition:%s:status", batchID, partitionID)
	result, err := rps.Get(ctx, key)
	if err != nil {
		return "", err
	}

	if status, ok := result.Value.(string); ok {
		return status, nil
	}

	return "", fmt.Errorf("invalid status type for partition %s in batch %s", partitionID, batchID)
}

// SetPartitionData sets partition data
func (rps *RedisPartitionSyncer) SetPartitionData(ctx context.Context, data *PartitionSyncData) error {
	key := fmt.Sprintf("batch:%s:partition:%s:%s", data.BatchID, data.PartitionID, partitionDataKey)
	syncData := &SyncData{
		Key:       key,
		Value:     data,
		Operation: SyncOperationSet,
		TTL:       defaultPartitionTTL,
		Timestamp: time.Now(),
	}

	_, err := rps.Sync(ctx, syncData)
	return err
}

// GetPartitionData gets partition data
func (rps *RedisPartitionSyncer) GetPartitionData(ctx context.Context, batchID, partitionID string) (*PartitionSyncData, error) {
	key := fmt.Sprintf("batch:%s:partition:%s:%s", batchID, partitionID, partitionDataKey)
	result, err := rps.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if result.Value == nil {
		return nil, fmt.Errorf("partition data not found for batch %s partition %s", batchID, partitionID)
	}

	// Try to unmarshal JSON
	var partitionData PartitionSyncData
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &partitionData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal partition data: %w", err)
		}
		return &partitionData, nil
	}

	if data, ok := result.Value.(*PartitionSyncData); ok {
		return data, nil
	}

	return nil, fmt.Errorf("invalid partition data type for batch %s partition %s", batchID, partitionID)
}

// IsPartitionComplete checks if partition is complete using bit operations
func (rps *RedisPartitionSyncer) IsPartitionComplete(ctx context.Context, batchID, partitionID string) (bool, error) {
	// For this implementation, we'll use a simple approach with hash fields
	// since the cache manager doesn't directly expose bit operations

	// For this implementation, we'll use a simple approach with hash fields
	// since the cache manager doesn't directly expose bit operations
	completeKey := fmt.Sprintf("batch:%s:complete_partitions", batchID)
	result, err := rps.Get(ctx, completeKey)
	if err != nil {
		return false, nil // Key doesn't exist, partition not complete
	}

	if result.Value == nil {
		return false, nil
	}

	// Parse completed partitions list
	var completedPartitions map[string]bool
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &completedPartitions); err != nil {
			return false, fmt.Errorf("failed to unmarshal completed partitions: %w", err)
		}
	} else if partitionsPtr, ok := result.Value.(map[string]bool); ok {
		completedPartitions = partitionsPtr
	} else {
		return false, fmt.Errorf("invalid completed partitions type")
	}

	return completedPartitions[partitionID], nil
}

// MarkPartitionComplete marks a partition as complete
func (rps *RedisPartitionSyncer) MarkPartitionComplete(ctx context.Context, batchID, partitionID string) error {
	completeKey := fmt.Sprintf("batch:%s:complete_partitions", batchID)

	// Get current completed partitions
	var completedPartitions map[string]bool
	result, err := rps.Get(ctx, completeKey)
	if err != nil || result.Value == nil {
		completedPartitions = make(map[string]bool)
	} else {
		if str, ok := result.Value.(string); ok {
			if err := json.Unmarshal([]byte(str), &completedPartitions); err != nil {
				completedPartitions = make(map[string]bool)
			}
		} else if partitionsPtr, ok := result.Value.(map[string]bool); ok {
			completedPartitions = partitionsPtr
		} else {
			completedPartitions = make(map[string]bool)
		}
	}

	// Mark partition as complete
	completedPartitions[partitionID] = true

	// Save updated map
	data := &SyncData{
		Key:       completeKey,
		Value:     completedPartitions,
		Operation: SyncOperationSet,
		TTL:       defaultPartitionTTL,
		Timestamp: time.Now(),
	}

	_, err = rps.Sync(ctx, data)
	return err
}

// IsAllPartitionsComplete checks if all partitions are complete
func (rps *RedisPartitionSyncer) IsAllPartitionsComplete(ctx context.Context, batchID string) (bool, error) {
	completeKey := fmt.Sprintf("batch:%s:complete_partitions", batchID)
	result, err := rps.Get(ctx, completeKey)
	if err != nil || result.Value == nil {
		return false, nil // No partitions completed yet
	}

	var completedPartitions map[string]bool
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &completedPartitions); err != nil {
			return false, fmt.Errorf("failed to unmarshal completed partitions: %w", err)
		}
	} else if partitionsPtr, ok := result.Value.(map[string]bool); ok {
		completedPartitions = partitionsPtr
	} else {
		return false, fmt.Errorf("invalid completed partitions type")
	}

	// Get total partition count for this batch
	expectedCount, err := rps.getExpectedPartitionCount(ctx, batchID)
	if err != nil {
		return false, err
	}

	// Count completed partitions
	completedCount := 0
	for _, completed := range completedPartitions {
		if completed {
			completedCount++
		}
	}

	return completedCount >= expectedCount, nil
}

// GetActivePartitions gets list of active partitions
func (rps *RedisPartitionSyncer) GetActivePartitions(ctx context.Context, batchID string) ([]string, error) {
	listKey := fmt.Sprintf("batch:%s:active_partitions", batchID)
	result, err := rps.Get(ctx, listKey)
	if err != nil || result.Value == nil {
		return []string{}, nil // No active partitions
	}

	var activePartitions []string
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &activePartitions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal active partitions: %w", err)
		}
	} else if partitionsPtr, ok := result.Value.([]string); ok {
		activePartitions = partitionsPtr
	} else {
		return nil, fmt.Errorf("invalid active partitions type")
	}

	return activePartitions, nil
}

// SetActivePartitions sets the list of active partitions
func (rps *RedisPartitionSyncer) SetActivePartitions(ctx context.Context, batchID string, partitions []string) error {
	listKey := fmt.Sprintf("batch:%s:active_partitions", batchID)
	data := &SyncData{
		Key:       listKey,
		Value:     partitions,
		Operation: SyncOperationSet,
		TTL:       defaultPartitionTTL,
		Timestamp: time.Now(),
	}

	_, err := rps.Sync(ctx, data)
	return err
}

// getPartitionIndex converts partition ID to index
func (rps *RedisPartitionSyncer) getPartitionIndex(partitionID string) (int, error) {
	// Try to parse as integer
	if index, err := strconv.Atoi(partitionID); err == nil {
		return index, nil
	}

	// For string partition IDs, use a consistent hash
	hash := 0
	for _, char := range partitionID {
		hash = int(char) + (hash << 6) + (hash << 16) - hash
	}

	// Ensure positive index within partition count
	return (hash%defaultPartitionCount + defaultPartitionCount) % defaultPartitionCount, nil
}

// getExpectedPartitionCount gets the expected partition count for a batch
func (rps *RedisPartitionSyncer) getExpectedPartitionCount(ctx context.Context, batchID string) (int, error) {
	countKey := fmt.Sprintf("batch:%s:partition_%s", batchID, partitionCountKey)
	result, err := rps.Get(ctx, countKey)
	if err != nil || result.Value == nil {
		return defaultPartitionCount, nil // Use default if not set
	}

	if count, ok := result.Value.(int); ok {
		return count, nil
	}

	if str, ok := result.Value.(string); ok {
		if count, err := strconv.Atoi(str); err == nil {
			return count, nil
		}
	}

	return defaultPartitionCount, nil
}

// SetExpectedPartitionCount sets the expected partition count for a batch
func (rps *RedisPartitionSyncer) SetExpectedPartitionCount(ctx context.Context, batchID string, count int) error {
	countKey := fmt.Sprintf("batch:%s:partition_%s", batchID, partitionCountKey)
	data := &SyncData{
		Key:       countKey,
		Value:     count,
		Operation: SyncOperationSet,
		TTL:       defaultPartitionTTL,
		Timestamp: time.Now(),
	}

	_, err := rps.Sync(ctx, data)
	return err
}

// RedisHeartbeatSyncer implements HeartbeatSyncer interface
type RedisHeartbeatSyncer struct {
	*RedisSyncer
}

// NewRedisHeartbeatSyncer creates a new Redis heartbeat syncer
func NewRedisHeartbeatSyncer(cache *cache.CacheManager, logger log.Logger) *RedisHeartbeatSyncer {
	return &RedisHeartbeatSyncer{
		RedisSyncer: NewRedisSyncer(cache, logger, heartbeatSyncerPrefix),
	}
}

// SetHeartbeat sets heartbeat data
func (rhs *RedisHeartbeatSyncer) SetHeartbeat(ctx context.Context, data *HeartbeatData) error {
	key := fmt.Sprintf("component:%s", data.ID)
	syncData := &SyncData{
		Key:       key,
		Value:     data,
		Operation: SyncOperationSet,
		TTL:       defaultHeartbeatTTL,
		Timestamp: time.Now(),
	}

	_, err := rhs.Sync(ctx, syncData)
	return err
}

// GetHeartbeat gets heartbeat data
func (rhs *RedisHeartbeatSyncer) GetHeartbeat(ctx context.Context, id string) (*HeartbeatData, error) {
	key := fmt.Sprintf("component:%s", id)
	result, err := rhs.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if result.Value == nil {
		return nil, fmt.Errorf("heartbeat not found for component %s", id)
	}

	// Try to unmarshal JSON
	var heartbeatData HeartbeatData
	if str, ok := result.Value.(string); ok {
		if err := json.Unmarshal([]byte(str), &heartbeatData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal heartbeat data: %w", err)
		}
		return &heartbeatData, nil
	}

	if data, ok := result.Value.(*HeartbeatData); ok {
		return data, nil
	}

	return nil, fmt.Errorf("invalid heartbeat data type for component %s", id)
}

// IsAlive checks if component is alive based on heartbeat
func (rhs *RedisHeartbeatSyncer) IsAlive(ctx context.Context, id string, timeout time.Duration) (bool, error) {
	heartbeat, err := rhs.GetHeartbeat(ctx, id)
	if err != nil {
		return false, nil // Component not found or error, consider as not alive
	}

	// Check if heartbeat is within timeout
	elapsed := time.Since(heartbeat.Timestamp)
	return elapsed <= timeout, nil
}

// CleanupExpiredHeartbeats removes expired heartbeat data
func (rhs *RedisHeartbeatSyncer) CleanupExpiredHeartbeats(ctx context.Context, maxAge time.Duration) error {
	// For simplicity, we rely on Redis TTL for cleanup
	// In a more sophisticated implementation, we could:
	// 1. Scan for all heartbeat keys
	// 2. Check their timestamps
	// 3. Delete expired ones

	rhs.logger.Infow("Heartbeat cleanup relies on Redis TTL",
		"max_age", maxAge,
		"default_ttl", defaultHeartbeatTTL,
	)

	return nil
}

// UpdateHeartbeatStatus updates the status of a heartbeat
func (rhs *RedisHeartbeatSyncer) UpdateHeartbeatStatus(ctx context.Context, id, status string) error {
	// Get current heartbeat
	heartbeat, err := rhs.GetHeartbeat(ctx, id)
	if err != nil {
		// Create new heartbeat if not exists
		heartbeat = &HeartbeatData{
			ID:        id,
			Component: id,
			Status:    status,
			Timestamp: time.Now(),
		}
	} else {
		// Update existing heartbeat
		heartbeat.Status = status
		heartbeat.Timestamp = time.Now()
	}

	return rhs.SetHeartbeat(ctx, heartbeat)
}

// GetComponentsByStatus gets all components with a specific status
func (rhs *RedisHeartbeatSyncer) GetComponentsByStatus(ctx context.Context, status string) ([]*HeartbeatData, error) {
	// This is a simplified implementation
	// In a real-world scenario, you might want to:
	// 1. Use Redis patterns to scan for heartbeat keys
	// 2. Filter by status
	// 3. Return matching components

	rhs.logger.Warnw("GetComponentsByStatus not fully implemented - requires Redis pattern scanning",
		"status", status,
	)

	return []*HeartbeatData{}, nil
}

// GeneratePartitionKeys generates standard partition keys
func GeneratePartitionKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("partition_%d", i)
	}
	return keys
}

// GeneratePartitionKeysWithPrefix generates partition keys with a custom prefix
func GeneratePartitionKeysWithPrefix(prefix string, count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return keys
}

// ParsePartitionKey parses a partition key to extract the index
func ParsePartitionKey(key string) (int, error) {
	// Handle standard format "partition_N"
	if strings.HasPrefix(key, "partition_") {
		indexStr := strings.TrimPrefix(key, "partition_")
		return strconv.Atoi(indexStr)
	}

	// Handle custom format "prefix_N"
	parts := strings.Split(key, "_")
	if len(parts) >= 2 {
		indexStr := parts[len(parts)-1]
		return strconv.Atoi(indexStr)
	}

	return 0, fmt.Errorf("invalid partition key format: %s", key)
}

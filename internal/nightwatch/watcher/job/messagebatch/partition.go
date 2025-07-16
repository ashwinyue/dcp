// Package messagebatch provides partition management for distributed message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// DefaultPartitionManager implements PartitionManager interface
type DefaultPartitionManager struct {
	partitions     map[int]*PartitionProcessor
	consistentHash *ConsistentHashRing
	logger         log.Logger
	mu             sync.RWMutex

	// Configuration
	maxConcurrency int
	retryCount     int
	timeout        time.Duration

	// Statistics
	stats *PartitionManagerStats
}

// PartitionManagerStats holds statistics for partition processing
type PartitionManagerStats struct {
	TotalPartitions       int
	ActivePartitions      int
	CompletedPartitions   int
	FailedPartitions      int
	ProcessedMessages     int64
	FailedMessages        int64
	AverageProcessingTime time.Duration
	mu                    sync.RWMutex
}

// PartitionProcessor handles processing for a single partition
type PartitionProcessor struct {
	ID             int
	Status         string
	Messages       []MessageData
	ProcessedCount int64
	FailedCount    int64
	StartTime      time.Time
	EndTime        *time.Time
	LastError      error
	RetryCount     int
	mu             sync.RWMutex
}

// ConsistentHashRing implements consistent hashing for partition distribution
type ConsistentHashRing struct {
	nodes      map[uint32]int // hash -> partition ID
	sortedKeys []uint32
	replicas   int
	mu         sync.RWMutex
}

// NewDefaultPartitionManager creates a new default partition manager
func NewDefaultPartitionManager(logger log.Logger, maxConcurrency int, timeout time.Duration) *DefaultPartitionManager {
	pm := &DefaultPartitionManager{
		partitions:     make(map[int]*PartitionProcessor),
		consistentHash: NewConsistentHashRing(256), // Use hardcoded value for virtual nodes
		logger:         logger,
		maxConcurrency: maxConcurrency,
		retryCount:     3, // Use hardcoded value for max retries
		timeout:        timeout,
		stats: &PartitionManagerStats{
			TotalPartitions: 128, // Use hardcoded value instead of known constant
		},
	}

	// Initialize all partitions
	for i := 0; i < 128; i++ { // Use hardcoded value
		pm.partitions[i] = &PartitionProcessor{
			ID:     i,
			Status: "pending",
		}
		pm.consistentHash.AddNode(i)
	}

	return pm
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(replicas int) *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:    make(map[uint32]int),
		replicas: replicas,
	}
}

// AddNode adds a partition node to the hash ring
func (ring *ConsistentHashRing) AddNode(partitionID int) {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	for i := 0; i < ring.replicas; i++ {
		key := ring.hash(fmt.Sprintf("%d:%d", partitionID, i))
		ring.nodes[key] = partitionID
		ring.sortedKeys = append(ring.sortedKeys, key)
	}

	// Keep keys sorted for binary search
	for i := 1; i < len(ring.sortedKeys); i++ {
		key := ring.sortedKeys[i]
		j := i - 1
		for j >= 0 && ring.sortedKeys[j] > key {
			ring.sortedKeys[j+1] = ring.sortedKeys[j]
			j--
		}
		ring.sortedKeys[j+1] = key
	}
}

// GetPartition returns the partition ID for a given key
func (ring *ConsistentHashRing) GetPartition(key string) int {
	ring.mu.RLock()
	defer ring.mu.RUnlock()

	if len(ring.sortedKeys) == 0 {
		return 0
	}

	hash := ring.hash(key)

	// Binary search for the smallest key >= hash
	left, right := 0, len(ring.sortedKeys)
	for left < right {
		mid := (left + right) / 2
		if ring.sortedKeys[mid] < hash {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// If hash is greater than all keys, wrap around to first key
	if left == len(ring.sortedKeys) {
		left = 0
	}

	return ring.nodes[ring.sortedKeys[left]]
}

// hash generates a hash value for a key
func (ring *ConsistentHashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// DistributeMessages distributes messages across partitions
func (pm *DefaultPartitionManager) DistributeMessages(ctx context.Context, messages []MessageData) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Clear existing messages in partitions
	for _, partition := range pm.partitions {
		partition.mu.Lock()
		partition.Messages = nil
		partition.Status = "pending"
		partition.mu.Unlock()
	}

	// Distribute messages to partitions
	for _, msg := range messages {
		partitionID := pm.consistentHash.GetPartition(msg.Recipient)

		if partition, exists := pm.partitions[partitionID]; exists {
			partition.mu.Lock()
			partition.Messages = append(partition.Messages, msg)
			partition.mu.Unlock()
		}
	}

	pm.logger.Infow("Messages distributed to partitions",
		"totalMessages", len(messages),
		"partitionsUsed", len(pm.partitions),
	)

	return nil
}

// ProcessPartitions processes all partitions concurrently
func (pm *DefaultPartitionManager) ProcessPartitions(ctx context.Context) error {
	// Create a semaphore to limit concurrency
	semaphore := make(chan struct{}, pm.maxConcurrency)

	// Create a wait group to wait for all partitions
	var wg sync.WaitGroup

	// Process each partition
	for partitionID, partition := range pm.partitions {
		partition.mu.RLock()
		hasMessages := len(partition.Messages) > 0
		partition.mu.RUnlock()

		if !hasMessages {
			continue
		}

		wg.Add(1)
		go func(id int, p *PartitionProcessor) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := pm.processPartition(ctx, id, p); err != nil {
				pm.logger.Errorw("Failed to process partition",
					"partitionID", id,
					"error", err,
				)
			}
		}(partitionID, partition)
	}

	// Wait for all partitions to complete
	wg.Wait()

	pm.updateStats()

	return nil
}

// processPartition processes a single partition
func (pm *DefaultPartitionManager) processPartition(ctx context.Context, partitionID int, partition *PartitionProcessor) error {
	partition.mu.Lock()
	partition.Status = "running"
	partition.StartTime = time.Now()
	messages := make([]MessageData, len(partition.Messages))
	copy(messages, partition.Messages)
	partition.mu.Unlock()

	pm.logger.Infow("Processing partition",
		"partitionID", partitionID,
		"messageCount", len(messages),
	)

	// Create context with timeout
	processCtx, cancel := context.WithTimeout(ctx, pm.timeout)
	defer cancel()

	// Process messages in this partition
	processedCount := int64(0)
	failedCount := int64(0)

	for _, message := range messages {
		select {
		case <-processCtx.Done():
			partition.mu.Lock()
			partition.Status = "timeout"
			partition.LastError = processCtx.Err()
			endTime := time.Now()
			partition.EndTime = &endTime
			partition.mu.Unlock()
			return processCtx.Err()

		default:
			// Simulate message processing
			if err := pm.processMessage(processCtx, message); err != nil {
				failedCount++
				pm.logger.Errorw("Failed to process message",
					"partitionID", partitionID,
					"messageID", message.ID,
					"error", err,
				)
			} else {
				processedCount++
			}
		}
	}

	// Update partition status
	partition.mu.Lock()
	partition.ProcessedCount = processedCount
	partition.FailedCount = failedCount
	endTime := time.Now()
	partition.EndTime = &endTime

	if failedCount > 0 {
		partition.Status = "completed_with_errors"
	} else {
		partition.Status = "completed"
	}
	partition.mu.Unlock()

	pm.logger.Infow("Partition processing completed",
		"partitionID", partitionID,
		"processed", processedCount,
		"failed", failedCount,
		"duration", time.Since(partition.StartTime),
	)

	return nil
}

// processMessage simulates processing a single message
func (pm *DefaultPartitionManager) processMessage(ctx context.Context, message MessageData) error {
	// Simulate processing time
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond * 10):
		// Message processed successfully
		return nil
	}
}

// GetPartitionStatus returns the status of a specific partition
func (pm *DefaultPartitionManager) GetPartitionStatus(partitionID int) (*PartitionProcessor, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if partitionID < 0 || partitionID >= 128 { // Use hardcoded value
		return nil, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// Return a copy to avoid race conditions
	partition.mu.RLock()
	defer partition.mu.RUnlock()

	return &PartitionProcessor{
		ID:             partition.ID,
		Status:         partition.Status,
		ProcessedCount: partition.ProcessedCount,
		FailedCount:    partition.FailedCount,
		StartTime:      partition.StartTime,
		EndTime:        partition.EndTime,
		LastError:      partition.LastError,
		RetryCount:     partition.RetryCount,
	}, nil
}

// GetAllPartitionStatuses returns the status of all partitions
func (pm *DefaultPartitionManager) GetAllPartitionStatuses() map[int]*PartitionProcessor {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	statuses := make(map[int]*PartitionProcessor)

	for id, partition := range pm.partitions {
		partition.mu.RLock()
		statuses[id] = &PartitionProcessor{
			ID:             partition.ID,
			Status:         partition.Status,
			ProcessedCount: partition.ProcessedCount,
			FailedCount:    partition.FailedCount,
			StartTime:      partition.StartTime,
			EndTime:        partition.EndTime,
			LastError:      partition.LastError,
			RetryCount:     partition.RetryCount,
		}
		partition.mu.RUnlock()
	}

	return statuses
}

// updateStats updates the partition manager statistics
func (pm *DefaultPartitionManager) updateStats() {
	pm.stats.mu.Lock()
	defer pm.stats.mu.Unlock()

	activeCount := 0
	completedCount := 0
	failedCount := 0
	var totalProcessed, totalFailed int64

	pm.mu.RLock()
	for _, partition := range pm.partitions {
		partition.mu.RLock()
		switch partition.Status {
		case "running":
			activeCount++
		case "completed", "completed_with_errors":
			completedCount++
		case "failed", "timeout":
			failedCount++
		}
		totalProcessed += partition.ProcessedCount
		totalFailed += partition.FailedCount
		partition.mu.RUnlock()
	}
	pm.mu.RUnlock()

	pm.stats.ActivePartitions = activeCount
	pm.stats.CompletedPartitions = completedCount
	pm.stats.FailedPartitions = failedCount
	pm.stats.ProcessedMessages = totalProcessed
	pm.stats.FailedMessages = totalFailed
}

// GetStats returns the current partition manager statistics
func (pm *DefaultPartitionManager) GetStats() *PartitionManagerStats {
	pm.stats.mu.RLock()
	defer pm.stats.mu.RUnlock()

	return &PartitionManagerStats{
		TotalPartitions:       pm.stats.TotalPartitions,
		ActivePartitions:      pm.stats.ActivePartitions,
		CompletedPartitions:   pm.stats.CompletedPartitions,
		FailedPartitions:      pm.stats.FailedPartitions,
		ProcessedMessages:     pm.stats.ProcessedMessages,
		FailedMessages:        pm.stats.FailedMessages,
		AverageProcessingTime: pm.stats.AverageProcessingTime,
	}
}

// Reset resets all partitions to their initial state
func (pm *DefaultPartitionManager) Reset() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, partition := range pm.partitions {
		partition.mu.Lock()
		partition.Status = "pending"
		partition.Messages = nil
		partition.ProcessedCount = 0
		partition.FailedCount = 0
		partition.StartTime = time.Time{}
		partition.EndTime = nil
		partition.LastError = nil
		partition.RetryCount = 0
		partition.mu.Unlock()
	}

	pm.stats.mu.Lock()
	pm.stats.ActivePartitions = 0
	pm.stats.CompletedPartitions = 0
	pm.stats.FailedPartitions = 0
	pm.stats.ProcessedMessages = 0
	pm.stats.FailedMessages = 0
	pm.stats.mu.Unlock()
}

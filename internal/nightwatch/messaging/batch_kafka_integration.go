// Package messaging provides batch processing integration with Kafka messaging
package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// BatchKafkaIntegration integrates message batch processing with Kafka
type BatchKafkaIntegration struct {
	kafkaHelper      *KafkaHelper
	messageProcessor *MessageProcessor
	atomicCounter    *syncer.AtomicCounterService
	cache            *cache.CacheManager
	logger           log.Logger

	// Configuration
	config *BatchKafkaConfig

	// State management
	activeBatches map[string]*BatchProcessingContext
	batchesMutex  sync.RWMutex
	isRunning     bool
	stopChan      chan struct{}
	mu            sync.RWMutex
}

// BatchKafkaConfig defines configuration for batch Kafka integration
type BatchKafkaConfig struct {
	DefaultBatchSize         int           `json:"default_batch_size"`
	MaxBatchSize             int           `json:"max_batch_size"`
	BatchTimeout             time.Duration `json:"batch_timeout"`
	MaxRetries               int           `json:"max_retries"`
	RetryInterval            time.Duration `json:"retry_interval"`
	EnableDeadLetter         bool          `json:"enable_dead_letter"`
	EnableMessagePersistence bool          `json:"enable_message_persistence"`
	PartitionCount           int           `json:"partition_count"`

	// Kafka specific
	ProducerFlushTimeout   time.Duration `json:"producer_flush_timeout"`
	ConsumerCommitInterval time.Duration `json:"consumer_commit_interval"`

	// Statistics
	StatsUpdateInterval time.Duration `json:"stats_update_interval"`
}

// DefaultBatchKafkaConfig returns default configuration
func DefaultBatchKafkaConfig() *BatchKafkaConfig {
	return &BatchKafkaConfig{
		DefaultBatchSize:         100,
		MaxBatchSize:             1000,
		BatchTimeout:             30 * time.Second,
		MaxRetries:               3,
		RetryInterval:            5 * time.Second,
		EnableDeadLetter:         true,
		EnableMessagePersistence: true,
		PartitionCount:           128,
		ProducerFlushTimeout:     10 * time.Second,
		ConsumerCommitInterval:   5 * time.Second,
		StatsUpdateInterval:      10 * time.Second,
	}
}

// BatchProcessingContext represents the context for processing a message batch
type BatchProcessingContext struct {
	BatchID string                `json:"batch_id"`
	JobID   string                `json:"job_id"`
	Phase   string                `json:"phase"`
	Status  BatchProcessingStatus `json:"status"`

	// Message data
	Messages       []*ProcessedMessage `json:"messages"`
	TotalMessages  int64               `json:"total_messages"`
	ProcessedCount int64               `json:"processed_count"`
	SuccessCount   int64               `json:"success_count"`
	FailureCount   int64               `json:"failure_count"`

	// Timing
	StartTime      time.Time     `json:"start_time"`
	EndTime        *time.Time    `json:"end_time,omitempty"`
	ProcessingTime time.Duration `json:"processing_time"`

	// Partition management
	PartitionStates map[int]*PartitionState `json:"partition_states"`

	// Error handling
	LastError  string `json:"last_error,omitempty"`
	RetryCount int    `json:"retry_count"`

	// Synchronization
	mu sync.RWMutex
}

// BatchProcessingStatus represents the status of batch processing
type BatchProcessingStatus string

const (
	BatchStatusPending    BatchProcessingStatus = "pending"
	BatchStatusProcessing BatchProcessingStatus = "processing"
	BatchStatusCompleted  BatchProcessingStatus = "completed"
	BatchStatusFailed     BatchProcessingStatus = "failed"
	BatchStatusRetrying   BatchProcessingStatus = "retrying"
	BatchStatusCancelled  BatchProcessingStatus = "cancelled"
)

// PartitionState represents the state of a partition in batch processing
type PartitionState struct {
	PartitionID    int        `json:"partition_id"`
	Status         string     `json:"status"`
	MessageCount   int64      `json:"message_count"`
	ProcessedCount int64      `json:"processed_count"`
	SuccessCount   int64      `json:"success_count"`
	FailureCount   int64      `json:"failure_count"`
	StartTime      time.Time  `json:"start_time"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	LastHeartbeat  time.Time  `json:"last_heartbeat"`
	WorkerID       string     `json:"worker_id,omitempty"`
}

// NewBatchKafkaIntegration creates a new batch Kafka integration
func NewBatchKafkaIntegration(
	kafkaHelper *KafkaHelper,
	messageProcessor *MessageProcessor,
	cache *cache.CacheManager,
	logger log.Logger,
	config *BatchKafkaConfig,
) *BatchKafkaIntegration {
	if config == nil {
		config = DefaultBatchKafkaConfig()
	}

	// Create atomic counter service
	atomicCounter := syncer.NewAtomicCounterService(
		cache,
		logger,
		"batch_kafka:",
	)

	return &BatchKafkaIntegration{
		kafkaHelper:      kafkaHelper,
		messageProcessor: messageProcessor,
		atomicCounter:    atomicCounter,
		cache:            cache,
		logger:           logger,
		config:           config,
		activeBatches:    make(map[string]*BatchProcessingContext),
		stopChan:         make(chan struct{}),
	}
}

// Start starts the batch Kafka integration
func (bki *BatchKafkaIntegration) Start(ctx context.Context) error {
	bki.mu.Lock()
	if bki.isRunning {
		bki.mu.Unlock()
		return fmt.Errorf("batch Kafka integration is already running")
	}
	bki.isRunning = true
	bki.mu.Unlock()

	bki.logger.Infow("Starting batch Kafka integration",
		"default_batch_size", bki.config.DefaultBatchSize,
		"partition_count", bki.config.PartitionCount,
	)

	// Start message processor
	if err := bki.messageProcessor.StartProcessing(ctx); err != nil {
		return fmt.Errorf("failed to start message processor: %w", err)
	}

	// Start statistics updater
	go bki.runStatisticsUpdater(ctx)

	// Start batch monitor
	go bki.runBatchMonitor(ctx)

	return nil
}

// Stop stops the batch Kafka integration
func (bki *BatchKafkaIntegration) Stop() error {
	bki.mu.Lock()
	if !bki.isRunning {
		bki.mu.Unlock()
		return fmt.Errorf("batch Kafka integration is not running")
	}
	bki.isRunning = false
	bki.mu.Unlock()

	close(bki.stopChan)

	// Stop message processor
	if err := bki.messageProcessor.StopProcessing(); err != nil {
		bki.logger.Warnw("Failed to stop message processor", "error", err)
	}

	bki.logger.Infow("Batch Kafka integration stopped")
	return nil
}

// CreateBatch creates a new message batch for processing
func (bki *BatchKafkaIntegration) CreateBatch(ctx context.Context, batchID, jobID, phase string, messages []*ProcessedMessage) (*BatchProcessingContext, error) {
	if batchID == "" {
		return nil, fmt.Errorf("batch ID cannot be empty")
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("messages cannot be empty")
	}

	if len(messages) > bki.config.MaxBatchSize {
		return nil, fmt.Errorf("batch size %d exceeds maximum %d", len(messages), bki.config.MaxBatchSize)
	}

	// Initialize atomic counters for the batch
	if err := bki.atomicCounter.InitializeCounters(ctx, batchID); err != nil {
		return nil, fmt.Errorf("failed to initialize counters: %w", err)
	}

	if err := bki.atomicCounter.InitializePartitionCounters(ctx, batchID); err != nil {
		return nil, fmt.Errorf("failed to initialize partition counters: %w", err)
	}

	// Create partition states
	partitionStates := make(map[int]*PartitionState)
	messagesPerPartition := bki.distributeMessagesToPartitions(messages)

	for partitionID, partitionMessages := range messagesPerPartition {
		partitionStates[partitionID] = &PartitionState{
			PartitionID:   partitionID,
			Status:        "pending",
			MessageCount:  int64(len(partitionMessages)),
			StartTime:     time.Now(),
			LastHeartbeat: time.Now(),
		}
	}

	// Create batch context
	batchContext := &BatchProcessingContext{
		BatchID:         batchID,
		JobID:           jobID,
		Phase:           phase,
		Status:          BatchStatusPending,
		Messages:        messages,
		TotalMessages:   int64(len(messages)),
		StartTime:       time.Now(),
		PartitionStates: partitionStates,
	}

	// Store batch context
	bki.batchesMutex.Lock()
	bki.activeBatches[batchID] = batchContext
	bki.batchesMutex.Unlock()

	// Persist batch context
	if bki.config.EnableMessagePersistence {
		if err := bki.persistBatchContext(ctx, batchContext); err != nil {
			bki.logger.Warnw("Failed to persist batch context",
				"batch_id", batchID,
				"error", err,
			)
		}
	}

	bki.logger.Infow("Batch created successfully",
		"batch_id", batchID,
		"job_id", jobID,
		"phase", phase,
		"message_count", len(messages),
		"partition_count", len(partitionStates),
	)

	return batchContext, nil
}

// ProcessBatch processes a message batch by publishing to Kafka
func (bki *BatchKafkaIntegration) ProcessBatch(ctx context.Context, batchID string) error {
	// Get batch context
	bki.batchesMutex.RLock()
	batchContext, exists := bki.activeBatches[batchID]
	bki.batchesMutex.RUnlock()

	if !exists {
		return fmt.Errorf("batch %s not found", batchID)
	}

	batchContext.mu.Lock()
	if batchContext.Status != BatchStatusPending {
		batchContext.mu.Unlock()
		return fmt.Errorf("batch %s is not in pending status: %s", batchID, batchContext.Status)
	}
	batchContext.Status = BatchStatusProcessing
	batchContext.mu.Unlock()

	bki.logger.Infow("Starting batch processing",
		"batch_id", batchID,
		"message_count", batchContext.TotalMessages,
	)

	// Process messages in parallel by partition
	var wg sync.WaitGroup
	errorChan := make(chan error, len(batchContext.PartitionStates))

	for partitionID, partitionState := range batchContext.PartitionStates {
		wg.Add(1)
		go func(pID int, pState *PartitionState) {
			defer wg.Done()

			if err := bki.processPartition(ctx, batchContext, pID, pState); err != nil {
				errorChan <- fmt.Errorf("partition %d failed: %w", pID, err)
			}
		}(partitionID, partitionState)
	}

	// Wait for all partitions to complete
	wg.Wait()
	close(errorChan)

	// Check for errors
	var processingErrors []error
	for err := range errorChan {
		processingErrors = append(processingErrors, err)
	}

	// Update batch status
	batchContext.mu.Lock()
	if len(processingErrors) > 0 {
		batchContext.Status = BatchStatusFailed
		batchContext.LastError = fmt.Sprintf("processing errors: %v", processingErrors)
		bki.logger.Errorw("Batch processing failed",
			"batch_id", batchID,
			"errors", processingErrors,
		)
	} else {
		batchContext.Status = BatchStatusCompleted
		endTime := time.Now()
		batchContext.EndTime = &endTime
		batchContext.ProcessingTime = endTime.Sub(batchContext.StartTime)

		bki.logger.Infow("Batch processing completed successfully",
			"batch_id", batchID,
			"processing_time", batchContext.ProcessingTime,
			"success_count", batchContext.SuccessCount,
			"failure_count", batchContext.FailureCount,
		)
	}
	batchContext.mu.Unlock()

	// Update final statistics
	if err := bki.updateBatchStatistics(ctx, batchContext); err != nil {
		bki.logger.Warnw("Failed to update final batch statistics",
			"batch_id", batchID,
			"error", err,
		)
	}

	// Persist final state
	if bki.config.EnableMessagePersistence {
		if err := bki.persistBatchContext(ctx, batchContext); err != nil {
			bki.logger.Warnw("Failed to persist final batch context",
				"batch_id", batchID,
				"error", err,
			)
		}
	}

	if len(processingErrors) > 0 {
		return fmt.Errorf("batch processing completed with errors: %v", processingErrors)
	}

	return nil
}

// processPartition processes messages for a specific partition
func (bki *BatchKafkaIntegration) processPartition(ctx context.Context, batchContext *BatchProcessingContext, partitionID int, partitionState *PartitionState) error {
	partitionState.Status = "processing"
	partitionState.StartTime = time.Now()

	// Get messages for this partition
	partitionMessages := bki.getMessagesForPartition(batchContext.Messages, partitionID)

	bki.logger.Debugw("Processing partition",
		"batch_id", batchContext.BatchID,
		"partition_id", partitionID,
		"message_count", len(partitionMessages),
	)

	// Set heartbeat
	if err := bki.atomicCounter.SetHeartbeat(ctx, batchContext.BatchID, fmt.Sprintf("partition_%d", partitionID)); err != nil {
		bki.logger.Warnw("Failed to set partition heartbeat",
			"batch_id", batchContext.BatchID,
			"partition_id", partitionID,
			"error", err,
		)
	}

	// Process each message in the partition
	var successCount, failureCount int64

	for _, message := range partitionMessages {
		// Set batch ID for message
		message.BatchID = batchContext.BatchID
		message.PartitionKey = fmt.Sprintf("partition_%d", partitionID)

		// Publish message to Kafka
		if err := bki.messageProcessor.PublishMessage(ctx, message); err != nil {
			failureCount++
			bki.logger.Errorw("Failed to publish message",
				"batch_id", batchContext.BatchID,
				"partition_id", partitionID,
				"message_id", message.ID,
				"error", err,
			)
		} else {
			successCount++
		}

		// Update partition heartbeat periodically
		partitionState.LastHeartbeat = time.Now()
	}

	// Update partition statistics
	partitionState.ProcessedCount = int64(len(partitionMessages))
	partitionState.SuccessCount = successCount
	partitionState.FailureCount = failureCount
	endTime := time.Now()
	partitionState.EndTime = &endTime
	partitionState.Status = "completed"

	// Mark partition as complete
	if err := bki.atomicCounter.MarkPartitionComplete(ctx, batchContext.BatchID, partitionID); err != nil {
		bki.logger.Errorw("Failed to mark partition complete",
			"batch_id", batchContext.BatchID,
			"partition_id", partitionID,
			"error", err,
		)
	}

	// Update batch counters
	batchContext.mu.Lock()
	batchContext.ProcessedCount += int64(len(partitionMessages))
	batchContext.SuccessCount += successCount
	batchContext.FailureCount += failureCount
	batchContext.mu.Unlock()

	// Update atomic counters
	if err := bki.atomicCounter.UpdateStatisticsCounters(ctx, batchContext.BatchID, successCount, failureCount, false); err != nil {
		bki.logger.Errorw("Failed to update partition statistics",
			"batch_id", batchContext.BatchID,
			"partition_id", partitionID,
			"error", err,
		)
	}

	bki.logger.Debugw("Partition processing completed",
		"batch_id", batchContext.BatchID,
		"partition_id", partitionID,
		"success_count", successCount,
		"failure_count", failureCount,
	)

	return nil
}

// GetBatchStatus returns the status of a batch
func (bki *BatchKafkaIntegration) GetBatchStatus(batchID string) (*BatchProcessingContext, error) {
	bki.batchesMutex.RLock()
	batchContext, exists := bki.activeBatches[batchID]
	bki.batchesMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("batch %s not found", batchID)
	}

	// Return a copy to avoid race conditions
	batchContext.mu.RLock()
	defer batchContext.mu.RUnlock()

	result := &BatchProcessingContext{
		BatchID:        batchContext.BatchID,
		JobID:          batchContext.JobID,
		Phase:          batchContext.Phase,
		Status:         batchContext.Status,
		TotalMessages:  batchContext.TotalMessages,
		ProcessedCount: batchContext.ProcessedCount,
		SuccessCount:   batchContext.SuccessCount,
		FailureCount:   batchContext.FailureCount,
		StartTime:      batchContext.StartTime,
		EndTime:        batchContext.EndTime,
		ProcessingTime: batchContext.ProcessingTime,
		LastError:      batchContext.LastError,
		RetryCount:     batchContext.RetryCount,
	}

	return result, nil
}

// ListActiveBatches returns all active batches
func (bki *BatchKafkaIntegration) ListActiveBatches() []*BatchProcessingContext {
	bki.batchesMutex.RLock()
	defer bki.batchesMutex.RUnlock()

	var batches []*BatchProcessingContext
	for _, batchContext := range bki.activeBatches {
		batchContext.mu.RLock()
		if batchContext.Status == BatchStatusProcessing || batchContext.Status == BatchStatusPending {
			batches = append(batches, &BatchProcessingContext{
				BatchID:        batchContext.BatchID,
				JobID:          batchContext.JobID,
				Phase:          batchContext.Phase,
				Status:         batchContext.Status,
				TotalMessages:  batchContext.TotalMessages,
				ProcessedCount: batchContext.ProcessedCount,
				SuccessCount:   batchContext.SuccessCount,
				FailureCount:   batchContext.FailureCount,
				StartTime:      batchContext.StartTime,
			})
		}
		batchContext.mu.RUnlock()
	}

	return batches
}

// RemoveBatch removes a completed or failed batch from active tracking
func (bki *BatchKafkaIntegration) RemoveBatch(batchID string) error {
	bki.batchesMutex.Lock()
	defer bki.batchesMutex.Unlock()

	batchContext, exists := bki.activeBatches[batchID]
	if !exists {
		return fmt.Errorf("batch %s not found", batchID)
	}

	batchContext.mu.RLock()
	status := batchContext.Status
	batchContext.mu.RUnlock()

	if status == BatchStatusProcessing || status == BatchStatusPending {
		return fmt.Errorf("cannot remove active batch %s with status %s", batchID, status)
	}

	delete(bki.activeBatches, batchID)

	bki.logger.Infow("Batch removed from active tracking",
		"batch_id", batchID,
		"final_status", status,
	)

	return nil
}

// Helper methods

// distributeMessagesToPartitions distributes messages across partitions
func (bki *BatchKafkaIntegration) distributeMessagesToPartitions(messages []*ProcessedMessage) map[int][]*ProcessedMessage {
	partitionMessages := make(map[int][]*ProcessedMessage)

	for i, message := range messages {
		// Simple round-robin distribution
		partitionID := i % bki.config.PartitionCount
		partitionMessages[partitionID] = append(partitionMessages[partitionID], message)
	}

	return partitionMessages
}

// getMessagesForPartition gets messages assigned to a specific partition
func (bki *BatchKafkaIntegration) getMessagesForPartition(messages []*ProcessedMessage, partitionID int) []*ProcessedMessage {
	var partitionMessages []*ProcessedMessage

	for i, message := range messages {
		if i%bki.config.PartitionCount == partitionID {
			partitionMessages = append(partitionMessages, message)
		}
	}

	return partitionMessages
}

// persistBatchContext persists batch context to cache
func (bki *BatchKafkaIntegration) persistBatchContext(ctx context.Context, batchContext *BatchProcessingContext) error {
	key := "batch_context:" + batchContext.BatchID
	return bki.cache.Set(ctx, key, batchContext, 24*time.Hour)
}

// updateBatchStatistics updates batch statistics
func (bki *BatchKafkaIntegration) updateBatchStatistics(ctx context.Context, batchContext *BatchProcessingContext) error {
	return bki.atomicCounter.UpdateStatisticsCounters(
		ctx,
		batchContext.BatchID,
		batchContext.SuccessCount,
		batchContext.FailureCount,
		false,
	)
}

// Background processes

// runStatisticsUpdater runs periodic statistics updates
func (bki *BatchKafkaIntegration) runStatisticsUpdater(ctx context.Context) {
	ticker := time.NewTicker(bki.config.StatsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-bki.stopChan:
			return
		case <-ticker.C:
			bki.updateAllBatchStatistics(ctx)
		}
	}
}

// runBatchMonitor monitors batch health and handles timeouts
func (bki *BatchKafkaIntegration) runBatchMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-bki.stopChan:
			return
		case <-ticker.C:
			bki.monitorBatchHealth(ctx)
		}
	}
}

// updateAllBatchStatistics updates statistics for all active batches
func (bki *BatchKafkaIntegration) updateAllBatchStatistics(ctx context.Context) {
	bki.batchesMutex.RLock()
	activeBatches := make(map[string]*BatchProcessingContext)
	for k, v := range bki.activeBatches {
		activeBatches[k] = v
	}
	bki.batchesMutex.RUnlock()

	for batchID := range activeBatches {
		if _, err := bki.atomicCounter.SyncStatisticsCounters(ctx, batchID); err != nil {
			bki.logger.Debugw("Failed to sync batch statistics",
				"batch_id", batchID,
				"error", err,
			)
		}
	}
}

// monitorBatchHealth monitors batch health and handles stalled batches
func (bki *BatchKafkaIntegration) monitorBatchHealth(ctx context.Context) {
	bki.batchesMutex.RLock()
	activeBatches := make(map[string]*BatchProcessingContext)
	for k, v := range bki.activeBatches {
		activeBatches[k] = v
	}
	bki.batchesMutex.RUnlock()

	for batchID, batchContext := range activeBatches {
		batchContext.mu.RLock()
		if batchContext.Status == BatchStatusProcessing {
			// Check if batch has been processing too long
			if time.Since(batchContext.StartTime) > bki.config.BatchTimeout {
				bki.logger.Warnw("Batch processing timeout detected",
					"batch_id", batchID,
					"processing_time", time.Since(batchContext.StartTime),
					"timeout", bki.config.BatchTimeout,
				)
			}
		}
		batchContext.mu.RUnlock()

		// Check partition heartbeats
		for partitionID := range batchContext.PartitionStates {
			partitionKey := fmt.Sprintf("partition_%d", partitionID)
			isAlive, err := bki.atomicCounter.IsHeartbeatAlive(ctx, batchID, partitionKey)
			if err != nil {
				bki.logger.Debugw("Failed to check partition heartbeat",
					"batch_id", batchID,
					"partition_id", partitionID,
					"error", err,
				)
				continue
			}

			if !isAlive {
				bki.logger.Warnw("Partition heartbeat expired",
					"batch_id", batchID,
					"partition_id", partitionID,
				)
			}
		}
	}
}

// GetIntegrationStats returns integration statistics
func (bki *BatchKafkaIntegration) GetIntegrationStats() map[string]interface{} {
	bki.batchesMutex.RLock()
	activeBatchCount := len(bki.activeBatches)
	bki.batchesMutex.RUnlock()

	bki.mu.RLock()
	isRunning := bki.isRunning
	bki.mu.RUnlock()

	return map[string]interface{}{
		"is_running":         isRunning,
		"active_batches":     activeBatchCount,
		"default_batch_size": bki.config.DefaultBatchSize,
		"max_batch_size":     bki.config.MaxBatchSize,
		"partition_count":    bki.config.PartitionCount,
		"batch_timeout":      bki.config.BatchTimeout,
		"kafka_stats":        bki.kafkaHelper.GetStats(),
		"message_processor":  bki.messageProcessor.GetAllStatistics(),
		"atomic_counter":     bki.atomicCounter.GetServiceStats(),
	}
}

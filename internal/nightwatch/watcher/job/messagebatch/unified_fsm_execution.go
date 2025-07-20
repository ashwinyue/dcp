package messagebatch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// Preparation Phase Execution

// initializePreparation initializes the preparation phase
func (usm *UnifiedStateMachine) initializePreparation(ctx context.Context) error {
	usm.logger.Infow("Initializing preparation phase", "job_id", usm.job.JobID)

	// Initialize job results if needed
	if usm.job.Results == nil {
		usm.job.Results = &model.JobResults{}
	}
	if usm.job.Results.MessageBatch == nil {
		usm.job.Results.MessageBatch = &v1.MessageBatchResults{}
	}

	// Initialize preparation statistics
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		stats.StartTime = time.Now()
		// Extract total count from job params
		if usm.job.Params != nil && usm.job.Params.MessageBatch != nil {
			stats.Total = int64(len(usm.job.Params.MessageBatch.Recipients))
		} else {
			// Default for demo purposes
			stats.Total = 10000
		}
		stats.Processed = 0
		stats.Success = 0
		stats.Failed = 0
		stats.Percent = 0
		stats.RetryCount = 0
		stats.Partitions = known.MessageBatchPartitionCount
	})

	return nil
}

// executePreparation executes the main preparation logic
func (usm *UnifiedStateMachine) executePreparation(ctx context.Context) error {
	usm.logger.Infow("Executing preparation phase", "job_id", usm.job.JobID)

	prepStats := usm.GetStatistics("PREPARATION")
	if prepStats == nil {
		return fmt.Errorf("preparation statistics not initialized")
	}

	// Simulate batch processing in chunks
	batchSize := known.MessageBatchDefaultBatchSize
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Process data in parallel chunks
	for processed := int64(0); processed < prepStats.Total; processed += int64(batchSize) {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()

			// Calculate chunk size
			remaining := prepStats.Total - offset
			chunkSize := int64(batchSize)
			if remaining < chunkSize {
				chunkSize = remaining
			}

			// Simulate processing delay
			time.Sleep(time.Duration(chunkSize/100) * time.Millisecond)

			// Simulate random failures (5% failure rate)
			successCount := chunkSize
			failedCount := int64(0)
			if rand.Intn(20) == 0 { // 5% chance of partial failure
				failedCount = chunkSize / 10 // 10% of chunk fails
				successCount = chunkSize - failedCount
			}

			// Update statistics atomically
			mu.Lock()
			usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
				stats.Processed += chunkSize
				stats.Success += successCount
				stats.Failed += failedCount
				stats.Percent = float32(stats.Processed) / float32(stats.Total) * 100
			})
			mu.Unlock()

			usm.logger.Infow("Preparation chunk processed",
				"job_id", usm.job.JobID,
				"offset", offset,
				"chunk_size", chunkSize,
				"success", successCount,
				"failed", failedCount,
				"progress", fmt.Sprintf("%.2f%%", float32(offset+chunkSize)/float32(prepStats.Total)*100),
			)

			// Check for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}
		}(processed)
	}

	// Wait for all chunks to complete
	wg.Wait()

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Finalize preparation statistics
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := int64(endTime.Sub(stats.StartTime) / time.Second)
		stats.Duration = &duration
	})

	usm.logger.Infow("Preparation phase execution completed",
		"job_id", usm.job.JobID,
		"total", prepStats.Total,
		"processed", prepStats.Processed,
		"success", prepStats.Success,
		"failed", prepStats.Failed,
		"success_rate", fmt.Sprintf("%.2f%%", float32(prepStats.Success)/float32(prepStats.Total)*100),
	)

	return nil
}

// isPreparationComplete checks if preparation is complete
func (usm *UnifiedStateMachine) isPreparationComplete(ctx context.Context) bool {
	prepStats := usm.GetStatistics("PREPARATION")
	if prepStats == nil {
		return false
	}
	return prepStats.Processed >= prepStats.Total
}

// savePreparationResults saves preparation results
func (usm *UnifiedStateMachine) savePreparationResults(ctx context.Context) error {
	usm.logger.Infow("Saving preparation results", "job_id", usm.job.JobID)

	// Create batch in the business service
	if err := usm.watcher.MessageBatchService.CreateBatch(ctx, usm.job.JobID, 0); err != nil {
		usm.logger.Errorw("Failed to create batch in service", "job_id", usm.job.JobID, "error", err)
		// Don't fail the entire process for this
	}

	// Save statistics
	return usm.saveStatistics(ctx)
}

// Delivery Phase Execution

// initializeDelivery initializes the delivery phase
func (usm *UnifiedStateMachine) initializeDelivery(ctx context.Context) error {
	usm.logger.Infow("Initializing delivery phase", "job_id", usm.job.JobID)

	// Get preparation results to determine delivery total
	prepStats := usm.GetStatistics("PREPARATION")
	deliveryTotal := int64(10000) // Default
	if prepStats != nil {
		deliveryTotal = prepStats.Success // Only deliver successfully prepared messages
	}

	// Initialize delivery statistics
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		stats.StartTime = time.Now()
		stats.Total = deliveryTotal
		stats.Processed = 0
		stats.Success = 0
		stats.Failed = 0
		stats.Percent = 0
		stats.RetryCount = 0
		stats.Partitions = known.MessageBatchPartitionCount
	})

	return nil
}

// executeDelivery executes the main delivery logic
func (usm *UnifiedStateMachine) executeDelivery(ctx context.Context) error {
	usm.logger.Infow("Executing delivery phase", "job_id", usm.job.JobID)

	deliveryStats := usm.GetStatistics("DELIVERY")
	if deliveryStats == nil {
		return fmt.Errorf("delivery statistics not initialized")
	}

	// Initialize all partitions
	partitionTasks := make([]*PartitionTask, known.MessageBatchPartitionCount)
	messagesPerPartition := deliveryStats.Total / int64(known.MessageBatchPartitionCount)
	remainder := deliveryStats.Total % int64(known.MessageBatchPartitionCount)

	for i := 0; i < known.MessageBatchPartitionCount; i++ {
		messageCount := messagesPerPartition
		if i < int(remainder) {
			messageCount++ // Distribute remainder across first few partitions
		}

		partitionTasks[i] = &PartitionTask{
			ID:           fmt.Sprintf("partition_%d_%s", i, usm.job.JobID),
			BatchID:      usm.job.JobID,
			PartitionKey: fmt.Sprintf("partition_%d", i),
			Status:       known.MessageBatchDeliveryReady,
			MessageCount: messageCount,
			RetryCount:   0,
			TaskCode:     fmt.Sprintf("DELIVERY_%d", i),
		}
	}

	usm.logger.Infow("Initialized delivery partitions",
		"job_id", usm.job.JobID,
		"partition_count", len(partitionTasks),
		"total_messages", deliveryStats.Total,
		"messages_per_partition", messagesPerPartition,
	)

	// Process partitions concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, task := range partitionTasks {
		if task.MessageCount == 0 {
			continue // Skip empty partitions
		}

		wg.Add(1)
		go func(partitionID int, partitionTask *PartitionTask) {
			defer wg.Done()

			// Process partition
			if err := usm.processDeliveryPartition(ctx, partitionID, partitionTask); err != nil {
				mu.Lock()
				usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
					stats.Failed += partitionTask.MessageCount
					stats.Processed += partitionTask.MessageCount
					stats.Percent = float32(stats.Processed) / float32(stats.Total) * 100
				})
				mu.Unlock()

				usm.logger.Errorw("Partition delivery failed",
					"job_id", usm.job.JobID,
					"partition_id", partitionID,
					"error", err,
				)
			} else {
				mu.Lock()
				usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
					stats.Success += partitionTask.MessageCount
					stats.Processed += partitionTask.MessageCount
					stats.Percent = float32(stats.Processed) / float32(stats.Total) * 100
				})
				mu.Unlock()

				usm.logger.Infow("Partition delivery completed",
					"job_id", usm.job.JobID,
					"partition_id", partitionID,
					"message_count", partitionTask.MessageCount,
				)
			}

			// Check for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}
		}(i, task)
	}

	// Wait for all partitions to complete
	wg.Wait()

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Finalize delivery statistics
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := int64(endTime.Sub(stats.StartTime) / time.Second)
		stats.Duration = &duration
	})

	deliveryStats = usm.GetStatistics("DELIVERY") // Refresh stats
	usm.logger.Infow("Delivery phase execution completed",
		"job_id", usm.job.JobID,
		"total", deliveryStats.Total,
		"processed", deliveryStats.Processed,
		"success", deliveryStats.Success,
		"failed", deliveryStats.Failed,
		"success_rate", fmt.Sprintf("%.2f%%", float32(deliveryStats.Success)/float32(deliveryStats.Total)*100),
	)

	// Check if there were any failures
	if deliveryStats.Failed > 0 {
		return fmt.Errorf("delivery completed with %d failures out of %d total messages", deliveryStats.Failed, deliveryStats.Total)
	}

	return nil
}

// processDeliveryPartition processes a single delivery partition
func (usm *UnifiedStateMachine) processDeliveryPartition(ctx context.Context, partitionID int, task *PartitionTask) error {
	usm.logger.Infow("Processing delivery partition",
		"job_id", usm.job.JobID,
		"partition_id", partitionID,
		"message_count", task.MessageCount,
	)

	// Simulate message sending for this partition
	// In a real implementation, this would:
	// 1. Retrieve messages for this partition from prepared data
	// 2. Send messages via SMS gateway
	// 3. Handle delivery status and retries
	// 4. Update delivery status in database

	// Generate sample messages for this partition
	messages := make([]messagebatch.MessageItem, task.MessageCount)
	for i := int64(0); i < task.MessageCount; i++ {
		messages[i] = messagebatch.MessageItem{
			MessageID: fmt.Sprintf("%s_p%d_msg_%d", usm.job.JobID, partitionID, i),
			Content: map[string]interface{}{
				"partition_id": partitionID,
				"index":        i,
				"batch_id":     usm.job.JobID,
				"data":         fmt.Sprintf("Message %d from partition %d", i, partitionID),
			},
			Timestamp: time.Now(),
			Status:    "pending",
		}
	}

	// Process messages through the business service
	if err := usm.watcher.MessageBatchService.ProcessBatch(ctx, usm.job.JobID, partitionID, messages); err != nil {
		usm.logger.Errorw("Failed to process partition through service",
			"job_id", usm.job.JobID,
			"partition_id", partitionID,
			"error", err,
		)
		// Don't fail the entire partition for service errors
	}

	// Simulate processing time (proportional to message count)
	processingTime := time.Duration(task.MessageCount/100) * time.Millisecond
	if processingTime < 100*time.Millisecond {
		processingTime = 100 * time.Millisecond // Minimum processing time
	}
	time.Sleep(processingTime)

	// Simulate random failures (10% failure rate for demo)
	if rand.Intn(10) == 0 {
		task.Status = known.MessageBatchDeliveryFailed
		task.ErrorMessage = fmt.Sprintf("Simulated delivery failure for partition %d", partitionID)
		return fmt.Errorf("partition %d delivery failed: %s", partitionID, task.ErrorMessage)
	}

	// Mark as completed
	now := time.Now()
	task.Status = known.MessageBatchDeliveryCompleted
	task.CompletedAt = &now

	return nil
}

// isDeliveryComplete checks if delivery is complete
func (usm *UnifiedStateMachine) isDeliveryComplete(ctx context.Context) bool {
	deliveryStats := usm.GetStatistics("DELIVERY")
	if deliveryStats == nil {
		return false
	}
	return deliveryStats.Processed >= deliveryStats.Total
}

// saveDeliveryResults saves delivery results
func (usm *UnifiedStateMachine) saveDeliveryResults(ctx context.Context) error {
	usm.logger.Infow("Saving delivery results", "job_id", usm.job.JobID)

	// Save statistics
	return usm.saveStatistics(ctx)
}
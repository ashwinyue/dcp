package messagebatch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
)

// Preparation Phase Execution

// initializePreparation initializes the preparation phase
func (usm *StateMachine) initializePreparation(ctx context.Context) error {
	usm.logger.Infow("Initializing preparation phase", "job_id", usm.job.JobID)

	// Initialize job results if needed
	if usm.job.Results == nil {
		usm.job.Results = &model.JobResults{}
	}

	// Initialize preparation statistics
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		stats.StartTime = time.Now()
		// Extract total count from job params
		if usm.job.Params != nil && usm.job.Params.MessageBatch != nil {
			params := usm.job.Params.MessageBatch
			stats.Total = int64(len(params.Recipients))
		} else {
			// Default for demo purposes
			stats.Total = 10000
		}
		stats.Processed = 0
		stats.Success = 0
		stats.Failed = 0
		stats.Percent = 0
		stats.RetryCount = 0
		stats.Partitions = 10 // Default partition count
	})

	return nil
}

// executePreparation executes the main preparation logic
func (usm *StateMachine) executePreparation(ctx context.Context) error {
	usm.logger.Infow("Executing preparation phase", "job_id", usm.job.JobID)

	prepStats := usm.GetStatistics("PREPARATION")
	if prepStats == nil {
		return fmt.Errorf("preparation statistics not initialized")
	}

	// Simulate batch processing in chunks
	batchSize := 1000 // Default batch size
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
				stats.Percent = float64(stats.Processed) / float64(stats.Total) * 100
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
		duration := float64(endTime.Sub(stats.StartTime) / time.Second)
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
func (usm *StateMachine) isPreparationComplete(ctx context.Context) bool {
	prepStats := usm.GetStatistics("PREPARATION")
	if prepStats == nil {
		return false
	}
	return prepStats.Processed >= prepStats.Total
}

// savePreparationResults saves preparation results
func (usm *StateMachine) savePreparationResults(ctx context.Context) error {
	usm.logger.Infow("Saving preparation results", "job_id", usm.job.JobID)

	// Create batch in the business service
	// Note: CreateBatch method not available in current service interface
	usm.logger.Infow("Batch creation skipped - method not available", "job_id", usm.job.JobID)

	// Save statistics
	return usm.saveStatistics(ctx)
}

// Delivery Phase Execution

// initializeDelivery initializes the delivery phase
func (usm *StateMachine) initializeDelivery(ctx context.Context) error {
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
		stats.Partitions = 10 // Default partition count
	})

	return nil
}

// executeDelivery executes the main delivery logic
func (usm *StateMachine) executeDelivery(ctx context.Context) error {
	usm.logger.Infow("Executing delivery phase", "job_id", usm.job.JobID)

	deliveryStats := usm.GetStatistics("DELIVERY")
	if deliveryStats == nil {
		return fmt.Errorf("delivery statistics not initialized")
	}

	// Initialize all partitions
	partitionCount := 10 // Default partition count
	partitionTasks := make([]*PartitionTask, partitionCount)
	messagesPerPartition := deliveryStats.Total / int64(partitionCount)
	remainder := deliveryStats.Total % int64(partitionCount)

	for i := 0; i < partitionCount; i++ {
		messageCount := messagesPerPartition
		if i < int(remainder) {
			messageCount++ // Distribute remainder across first few partitions
		}

		partitionTasks[i] = &PartitionTask{
			ID:           fmt.Sprintf("partition_%d_%s", i, usm.job.JobID),
			BatchID:      usm.job.JobID,
			PartitionKey: fmt.Sprintf("partition_%d", i),
			Status:       "READY", // Default status
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
					stats.Percent = float64(stats.Processed) / float64(stats.Total) * 100
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
					stats.Percent = float64(stats.Processed) / float64(stats.Total) * 100
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
		duration := float64(endTime.Sub(stats.StartTime) / time.Second)
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
func (usm *StateMachine) processDeliveryPartition(ctx context.Context, partitionID int, task *PartitionTask) error {
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
	messages := make([]MessageData, task.MessageCount)
	for i := int64(0); i < task.MessageCount; i++ {
		messages[i] = MessageData{
			ID:        fmt.Sprintf("%s_p%d_msg_%d", usm.job.JobID, partitionID, i),
			Recipient: fmt.Sprintf("user_%d", i),
			Content:   fmt.Sprintf("Message %d from partition %d", i, partitionID),
			Type:      "SMS",
			PartitionKey: fmt.Sprintf("partition_%d", partitionID),
			CreatedAt: time.Now(),
		}
	}

	// Process messages through the business service
	// Note: ProcessBatch method not available in current service interface
	usm.logger.Infow("Batch processing skipped - method not available",
		"job_id", usm.job.JobID,
		"partition_id", partitionID,
		"message_count", len(messages),
	)

	// Simulate processing time (proportional to message count)
	processingTime := time.Duration(task.MessageCount/100) * time.Millisecond
	if processingTime < 100*time.Millisecond {
		processingTime = 100 * time.Millisecond // Minimum processing time
	}
	time.Sleep(processingTime)

	// Simulate random failures (10% failure rate for demo)
	if rand.Intn(10) == 0 {
		task.Status = "FAILED" // Default failed status
		task.ErrorMessage = fmt.Sprintf("Simulated delivery failure for partition %d", partitionID)
		return fmt.Errorf("partition %d delivery failed: %s", partitionID, task.ErrorMessage)
	}

	// Mark as completed
	now := time.Now()
	task.Status = "COMPLETED" // Default completed status
	task.CompletedAt = &now

	return nil
}

// isDeliveryComplete checks if delivery is complete
func (usm *StateMachine) isDeliveryComplete(ctx context.Context) bool {
	deliveryStats := usm.GetStatistics("DELIVERY")
	if deliveryStats == nil {
		return false
	}
	return deliveryStats.Processed >= deliveryStats.Total
}

// saveDeliveryResults saves delivery results
func (usm *StateMachine) saveDeliveryResults(ctx context.Context) error {
	usm.logger.Infow("Saving delivery results", "job_id", usm.job.JobID)

	// Save statistics
	return usm.saveStatistics(ctx)
}

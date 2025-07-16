// Package messagebatch provides an example of how to use the message batch processing system
package messagebatch

import (
	"context"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// ExampleMessageBatchUsage demonstrates how to create and process a message batch job
func ExampleMessageBatchUsage() {
	// Create a sample job for message batch processing
	job := &model.JobM{
		JobID:       "batch_001",
		UserID:      "user123",
		Scope:       known.MessageBatchJobScope,
		Name:        "SMS Marketing Campaign",
		Description: "Send promotional SMS to customer list",
		Watcher:     known.MessageBatchWatcher,
		Status:      known.MessageBatchPending,
		Suspend:     known.JobNonSuspended,
		Params: &model.JobParams{
			MessageBatch: &v1.MessageBatchParams{
				Recipients: []string{
					"13800138001",
					"13800138002",
					"13800138003",
					// ... more recipients
				},
				BatchSize:  known.MessageBatchDefaultBatchSize,
				MaxRetries: known.MessageBatchMaxRetries,
			},
		},
		Results: &model.JobResults{
			MessageBatch: &v1.MessageBatchResults{},
		},
	}

	log.Infow("Created message batch job",
		"job_id", job.JobID,
		"recipient_count", len(job.Params.MessageBatch.Recipients),
		"batch_size", job.Params.MessageBatch.BatchSize,
	)

	// Example: Processing through the phases

	// Phase 1: Preparation
	log.Infow("=== PREPARATION PHASE ===")
	preparationFSM := NewPreparationFSM(job.Status, nil, job)

	ctx := context.Background()

	// Start preparation
	if err := preparationFSM.Transition(ctx, known.MessageBatchEventPrepareStart); err != nil {
		log.Errorw("Failed to start preparation", "error", err)
		return
	}

	// Begin preparation processing
	if err := preparationFSM.Transition(ctx, known.MessageBatchEventPrepareBegin); err != nil {
		log.Errorw("Failed to begin preparation", "error", err)
		return
	}

	// Simulate preparation completion
	time.Sleep(2 * time.Second)

	// Complete preparation
	if err := preparationFSM.Transition(ctx, known.MessageBatchEventPrepareComplete); err != nil {
		log.Errorw("Failed to complete preparation", "error", err)
		return
	}

	log.Infow("Preparation phase completed successfully")

	// Phase 2: Delivery
	log.Infow("=== DELIVERY PHASE ===")
	deliveryFSM := NewDeliveryFSM(job.Status, nil, job)

	// Start delivery
	if err := deliveryFSM.Transition(ctx, known.MessageBatchEventDeliveryStart); err != nil {
		log.Errorw("Failed to start delivery", "error", err)
		return
	}

	// Begin delivery processing
	if err := deliveryFSM.Transition(ctx, known.MessageBatchEventDeliveryBegin); err != nil {
		log.Errorw("Failed to begin delivery", "error", err)
		return
	}

	// Delivery will complete automatically via processDelivery goroutine
	log.Infow("Delivery phase started, processing in background...")

	// Wait a bit to see the delivery progress
	time.Sleep(5 * time.Second)

	// Check final statistics
	prepStats := preparationFSM.statistics
	delivStats := deliveryFSM.GetStatistics()

	log.Infow("=== FINAL STATISTICS ===")
	log.Infow("Preparation Stats",
		"total", prepStats.Total,
		"processed", prepStats.Processed,
		"success", prepStats.Success,
		"failed", prepStats.Failed,
		"duration", prepStats.Duration,
	)

	log.Infow("Delivery Stats",
		"total", delivStats.Total,
		"processed", delivStats.Processed,
		"success", delivStats.Success,
		"failed", delivStats.Failed,
		"partitions", delivStats.Partitions,
		"duration", delivStats.Duration,
	)
}

// ExamplePartitionProcessing demonstrates partition-level processing
func ExamplePartitionProcessing() {
	log.Infow("=== PARTITION PROCESSING EXAMPLE ===")

	// Create sample partition tasks
	tasks := []*PartitionTask{
		{
			ID:           "task_001",
			BatchID:      "batch_001",
			PartitionKey: "partition_0",
			Status:       known.MessageBatchDeliveryReady,
			MessageCount: 100,
			TaskCode:     "DELIVERY_0",
		},
		{
			ID:           "task_002",
			BatchID:      "batch_001",
			PartitionKey: "partition_1",
			Status:       known.MessageBatchDeliveryReady,
			MessageCount: 150,
			TaskCode:     "DELIVERY_1",
		},
	}

	for i, task := range tasks {
		log.Infow("Processing partition task",
			"partition_id", i,
			"task_id", task.ID,
			"message_count", task.MessageCount,
		)

		// Simulate processing
		time.Sleep(200 * time.Millisecond)

		// Mark as completed
		now := time.Now()
		task.Status = known.MessageBatchDeliveryCompleted
		task.CompletedAt = &now

		log.Infow("Partition task completed",
			"partition_id", i,
			"task_id", task.ID,
			"status", task.Status,
		)
	}

	log.Infow("All partition tasks completed successfully")
}

// ExampleStateMachineTransitions demonstrates FSM state transitions
func ExampleStateMachineTransitions() {
	log.Infow("=== FSM TRANSITIONS EXAMPLE ===")

	job := &model.JobM{
		JobID:  "fsm_demo",
		Status: known.MessageBatchPending,
	}

	ctx := context.Background()

	// Create and demo preparation FSM
	prepFSM := NewPreparationFSM(job.Status, nil, job)

	transitions := []string{
		known.MessageBatchEventPrepareStart,
		known.MessageBatchEventPrepareBegin,
		known.MessageBatchEventPrepareComplete,
	}

	for _, event := range transitions {
		log.Infow("Triggering FSM event",
			"current_state", prepFSM.fsm.Current(),
			"event", event,
		)

		if err := prepFSM.Transition(ctx, event); err != nil {
			log.Errorw("FSM transition failed", "error", err)
			break
		}

		log.Infow("FSM transition successful",
			"new_state", prepFSM.fsm.Current(),
		)

		time.Sleep(500 * time.Millisecond)
	}

	log.Infow("FSM transitions demonstration completed")
}

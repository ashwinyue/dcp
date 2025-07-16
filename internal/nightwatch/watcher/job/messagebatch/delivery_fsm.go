// Package messagebatch provides the finite state machine implementation for
// the DELIVERY phase of message batch processing.
package messagebatch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
)

// DeliveryFSM represents the finite state machine for the delivery phase
type DeliveryFSM struct {
	watcher    *Watcher
	job        *model.JobM
	fsm        *fsm.FSM
	statistics *PhaseStatistics
	retryCount int
	startTime  time.Time
}

// NewDeliveryFSM creates a new delivery phase FSM
func NewDeliveryFSM(initial string, watcher *Watcher, job *model.JobM) *DeliveryFSM {
	dfsm := &DeliveryFSM{
		watcher: watcher,
		job:     job,
		statistics: &PhaseStatistics{
			StartTime:  time.Now(),
			RetryCount: 0,
			Partitions: known.MessageBatchPartitionCount,
		},
		retryCount: 0,
		startTime:  time.Now(),
	}

	dfsm.fsm = fsm.NewFSM(
		initial,
		fsm.Events{
			// State transitions for delivery phase
			{Name: known.MessageBatchEventDeliveryStart, Src: []string{known.MessageBatchPreparationCompleted}, Dst: known.MessageBatchDeliveryReady},
			{Name: known.MessageBatchEventDeliveryBegin, Src: []string{known.MessageBatchDeliveryReady, known.MessageBatchDeliveryPaused}, Dst: known.MessageBatchDeliveryRunning},
			{Name: known.MessageBatchEventDeliveryPause, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryPausing},
			{Name: known.MessageBatchEventDeliveryPaused, Src: []string{known.MessageBatchDeliveryPausing}, Dst: known.MessageBatchDeliveryPaused},
			{Name: known.MessageBatchEventDeliveryResume, Src: []string{known.MessageBatchDeliveryPaused}, Dst: known.MessageBatchDeliveryReady},
			{Name: known.MessageBatchEventDeliveryComplete, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryCompleted},
			{Name: known.MessageBatchEventDeliveryFail, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryFailed},
			{Name: known.MessageBatchEventDeliveryRetry, Src: []string{known.MessageBatchDeliveryFailed}, Dst: known.MessageBatchDeliveryReady},
		},
		fsm.Callbacks{
			"enter_state": fsmutil.WrapEvent(dfsm.EnterState),
			// State-specific callbacks
			"enter_" + known.MessageBatchDeliveryReady:     fsmutil.WrapEvent(dfsm.OnReady),
			"enter_" + known.MessageBatchDeliveryRunning:   fsmutil.WrapEvent(dfsm.OnRunning),
			"enter_" + known.MessageBatchDeliveryPausing:   fsmutil.WrapEvent(dfsm.OnPausing),
			"enter_" + known.MessageBatchDeliveryPaused:    fsmutil.WrapEvent(dfsm.OnPaused),
			"enter_" + known.MessageBatchDeliveryCompleted: fsmutil.WrapEvent(dfsm.OnCompleted),
			"enter_" + known.MessageBatchDeliveryFailed:    fsmutil.WrapEvent(dfsm.OnFailed),
		},
	)

	return dfsm
}

// CanTransition checks if a state transition is valid
func (dfsm *DeliveryFSM) CanTransition(event string) bool {
	return dfsm.fsm.Can(event)
}

// Transition attempts to transition to a new state
func (dfsm *DeliveryFSM) Transition(ctx context.Context, event string) error {
	return dfsm.fsm.Event(ctx, event)
}

// AvailableTransitions returns the list of available transitions
func (dfsm *DeliveryFSM) AvailableTransitions() []string {
	return dfsm.fsm.AvailableTransitions()
}

// Current returns the current state
func (dfsm *DeliveryFSM) Current() string {
	return dfsm.fsm.Current()
}

// EnterState handles the state transition and updates job status
func (dfsm *DeliveryFSM) EnterState(ctx context.Context, event *fsm.Event) error {
	dfsm.job.Status = event.Dst

	log.Infow("Delivery FSM state transition",
		"from", event.Src,
		"to", event.Dst,
		"event", event.Event,
		"job_id", dfsm.job.JobID,
	)

	return dfsm.watcher.Store.Job().Update(ctx, dfsm.job)
}

// State-specific handlers
func (dfsm *DeliveryFSM) OnReady(ctx context.Context, event *fsm.Event) error {
	log.Infow("Delivery phase ready", "job_id", dfsm.job.JobID)
	return nil
}

func (dfsm *DeliveryFSM) OnRunning(ctx context.Context, event *fsm.Event) error {
	log.Infow("Delivery phase running", "job_id", dfsm.job.JobID)

	// Start delivery processing
	go dfsm.processDelivery(ctx)

	return nil
}

func (dfsm *DeliveryFSM) OnPausing(ctx context.Context, event *fsm.Event) error {
	log.Infow("Delivery phase pausing", "job_id", dfsm.job.JobID)

	// Set pausing signal - implementation depends on delivery processor
	// This would typically signal workers to pause gracefully

	return nil
}

func (dfsm *DeliveryFSM) OnPaused(ctx context.Context, event *fsm.Event) error {
	log.Infow("Delivery phase paused", "job_id", dfsm.job.JobID)
	return nil
}

func (dfsm *DeliveryFSM) OnCompleted(ctx context.Context, event *fsm.Event) error {
	log.Infow("Delivery phase completed", "job_id", dfsm.job.JobID)

	// Mark the entire job as completed
	dfsm.job.Status = known.MessageBatchSucceeded

	return nil
}

func (dfsm *DeliveryFSM) OnFailed(ctx context.Context, event *fsm.Event) error {
	log.Errorw("Delivery phase failed", "job_id", dfsm.job.JobID, "error", event.Err)

	dfsm.retryCount++

	// Check if we should retry
	if dfsm.retryCount < known.MessageBatchMaxRetries {
		log.Infow("Scheduling delivery retry", "job_id", dfsm.job.JobID, "retry_count", dfsm.retryCount)
		// Schedule retry after a delay
		time.AfterFunc(30*time.Second, func() {
			if err := dfsm.Transition(context.Background(), known.MessageBatchEventDeliveryRetry); err != nil {
				log.Errorw("Failed to retry delivery", "job_id", dfsm.job.JobID, "error", err)
			}
		})
	} else {
		// Max retries reached, mark job as permanently failed
		dfsm.job.Status = known.MessageBatchFailed
		log.Errorw("Delivery phase permanently failed", "job_id", dfsm.job.JobID, "retry_count", dfsm.retryCount)
	}

	return nil
}

// processDelivery handles the actual delivery processing
func (dfsm *DeliveryFSM) processDelivery(ctx context.Context) {
	log.Infow("Starting delivery processing", "job_id", dfsm.job.JobID)

	// Initialize delivery statistics
	dfsm.statistics.StartTime = time.Now()
	dfsm.statistics.Total = 10000 // Use the same total as preparation
	dfsm.statistics.Processed = 0
	dfsm.statistics.Partitions = known.MessageBatchPartitionCount

	// Initialize all partitions
	partitionTasks := make([]*PartitionTask, known.MessageBatchPartitionCount)
	for i := 0; i < known.MessageBatchPartitionCount; i++ {
		partitionTasks[i] = &PartitionTask{
			ID:           fmt.Sprintf("partition_%d_%s", i, dfsm.job.JobID),
			BatchID:      dfsm.job.JobID,
			PartitionKey: fmt.Sprintf("partition_%d", i),
			Status:       known.MessageBatchDeliveryReady,
			MessageCount: dfsm.statistics.Total / int64(known.MessageBatchPartitionCount),
			RetryCount:   0,
			TaskCode:     fmt.Sprintf("DELIVERY_%d", i),
		}
	}

	log.Infow("Initialized delivery partitions",
		"job_id", dfsm.job.JobID,
		"partition_count", len(partitionTasks),
		"messages_per_partition", partitionTasks[0].MessageCount,
	)

	// Process partitions concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := int64(0)
	failedCount := int64(0)

	for i, task := range partitionTasks {
		wg.Add(1)
		go func(partitionID int, partitionTask *PartitionTask) {
			defer wg.Done()

			// Simulate partition processing
			if err := dfsm.processPartition(ctx, partitionID, partitionTask); err != nil {
				mu.Lock()
				failedCount += partitionTask.MessageCount
				dfsm.statistics.Failed += partitionTask.MessageCount
				mu.Unlock()

				log.Errorw("Partition processing failed",
					"job_id", dfsm.job.JobID,
					"partition_id", partitionID,
					"error", err,
				)
			} else {
				mu.Lock()
				successCount += partitionTask.MessageCount
				dfsm.statistics.Success += partitionTask.MessageCount
				mu.Unlock()

				log.Infow("Partition processing completed",
					"job_id", dfsm.job.JobID,
					"partition_id", partitionID,
					"message_count", partitionTask.MessageCount,
				)
			}

			// Update overall progress
			mu.Lock()
			dfsm.statistics.Processed += partitionTask.MessageCount
			dfsm.statistics.Percent = float32(dfsm.statistics.Processed) / float32(dfsm.statistics.Total) * 100
			mu.Unlock()

		}(i, task)
	}

	// Wait for all partitions to complete
	wg.Wait()

	// Finalize statistics
	endTime := time.Now()
	dfsm.statistics.EndTime = &endTime
	duration := int64(endTime.Sub(dfsm.statistics.StartTime) / time.Second)
	dfsm.statistics.Duration = &duration

	log.Infow("Delivery processing completed",
		"job_id", dfsm.job.JobID,
		"duration", duration,
		"total", dfsm.statistics.Total,
		"processed", dfsm.statistics.Processed,
		"success", dfsm.statistics.Success,
		"failed", dfsm.statistics.Failed,
		"success_rate", float32(dfsm.statistics.Success)/float32(dfsm.statistics.Total)*100,
	)

	// Determine final result
	if dfsm.statistics.Failed > 0 {
		// If there are failures, trigger failure event
		if err := dfsm.Transition(ctx, known.MessageBatchEventDeliveryFail); err != nil {
			log.Errorw("Failed to transition to failed state", "job_id", dfsm.job.JobID, "error", err)
		}
	} else {
		// All successful, complete the delivery
		if err := dfsm.Transition(ctx, known.MessageBatchEventDeliveryComplete); err != nil {
			log.Errorw("Failed to complete delivery", "job_id", dfsm.job.JobID, "error", err)
		}
	}
}

// processPartition processes a single partition
func (dfsm *DeliveryFSM) processPartition(ctx context.Context, partitionID int, task *PartitionTask) error {
	log.Infow("Processing partition",
		"job_id", dfsm.job.JobID,
		"partition_id", partitionID,
		"message_count", task.MessageCount,
	)

	// Simulate message sending for this partition
	// In a real implementation, this would:
	// 1. Retrieve messages for this partition from prepared data
	// 2. Send messages via SMS gateway
	// 3. Handle delivery status and retries
	// 4. Update delivery status in database

	// Simulate processing time (proportional to message count)
	processingTime := time.Duration(task.MessageCount/100) * time.Millisecond
	time.Sleep(processingTime)

	// Simulate random failures (10% failure rate for demo)
	if rand.Intn(10) == 0 {
		task.Status = known.MessageBatchDeliveryFailed
		task.ErrorMessage = "Simulated delivery failure"
		return fmt.Errorf("partition %d delivery failed: %s", partitionID, task.ErrorMessage)
	}

	// Mark as completed
	now := time.Now()
	task.Status = known.MessageBatchDeliveryCompleted
	task.CompletedAt = &now

	return nil
}

// canRetry checks if the FSM can be retried
func (dfsm *DeliveryFSM) canRetry() bool {
	return dfsm.retryCount < known.MessageBatchMaxRetries
}

// GetStatistics returns the current statistics
func (dfsm *DeliveryFSM) GetStatistics() *PhaseStatistics {
	return dfsm.statistics
}

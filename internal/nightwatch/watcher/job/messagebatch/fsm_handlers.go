package messagebatch

import (
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"

	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
)

// Preparation Phase Handlers

// OnPreparationReady handles the preparation ready state
func (usm *StateMachine) OnPreparationReady(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Preparation phase ready", "job_id", usm.job.JobID)

	// Initialize preparation phase
	if err := usm.initializePreparation(ctx); err != nil {
		return fmt.Errorf("failed to initialize preparation: %w", err)
	}

	// Mark job condition as ready
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationReady)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Auto-transition to running
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventPrepareBegin); err != nil {
			usm.logger.Errorw("Failed to auto-transition to preparation running", "job_id", usm.job.JobID, "error", err)
		}
	}()

	return nil
}

// OnPreparationRunning handles the preparation running state
func (usm *StateMachine) OnPreparationRunning(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Preparation phase running", "job_id", usm.job.JobID)

	// Apply rate limiting
	_ = usm.watcher.Limiter.Processing.Take()

	// Execute preparation logic asynchronously
	go func() {
		if err := usm.executePreparation(ctx); err != nil {
			usm.logger.Errorw("Preparation execution failed", "job_id", usm.job.JobID, "error", err)
			if transErr := usm.fsm.Event(ctx, known.MessageBatchEventPrepareFail); transErr != nil {
				usm.logger.Errorw("Failed to transition to preparation failed", "job_id", usm.job.JobID, "error", transErr)
			}
			return
		}

		// Check if preparation is complete
		if usm.isPreparationComplete(ctx) {
			if err := usm.fsm.Event(ctx, known.MessageBatchEventPrepareComplete); err != nil {
				usm.logger.Errorw("Failed to complete preparation", "job_id", usm.job.JobID, "error", err)
			}
		}
	}()

	return nil
}

// OnPreparationPausing handles the preparation pausing state
func (usm *StateMachine) OnPreparationPausing(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Preparation phase pausing", "job_id", usm.job.JobID)

	// Signal all workers to pause
	if err := usm.pausePreparationWorkers(ctx); err != nil {
		return fmt.Errorf("failed to pause preparation workers: %w", err)
	}

	// Transition to paused state once all workers are paused
	return usm.fsm.Event(ctx, known.MessageBatchEventPreparePaused)
}

// OnPreparationPaused handles the preparation paused state
func (usm *StateMachine) OnPreparationPaused(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Preparation phase paused", "job_id", usm.job.JobID)

	// Save current statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save statistics", "job_id", usm.job.JobID, "error", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationPaused)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	return nil
}

// OnPreparationCompleted handles the preparation completed state
func (usm *StateMachine) OnPreparationCompleted(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Preparation phase completed", "job_id", usm.job.JobID)

	// Finalize preparation statistics
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := endTime.Sub(stats.StartTime).Seconds()
		durationInt := int64(duration)
		stats.Duration = &durationInt
	})

	// Save preparation results
	if err := usm.savePreparationResults(ctx); err != nil {
		return fmt.Errorf("failed to save preparation results: %w", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationCompleted)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Auto-transition to delivery phase
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventDeliveryStart); err != nil {
			usm.logger.Errorw("Failed to start delivery phase", "job_id", usm.job.JobID, "error", err)
		}
	}()

	return nil
}

// OnPreparationFailed handles the preparation failed state
func (usm *StateMachine) OnPreparationFailed(ctx context.Context, event *fsm.Event) error {
	usm.logger.Errorw("Preparation phase failed", "job_id", usm.job.JobID, "retry_count", usm.retryCount)

	usm.retryCount++
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		stats.RetryCount = usm.retryCount
	})

	// Check if we should retry
	if usm.canRetry() {
		usm.logger.Infow("Retrying preparation phase", "job_id", usm.job.JobID, "retry_count", usm.retryCount)

		// Add delay before retry
		go func() {
			time.Sleep(time.Duration(usm.retryCount) * time.Second)
			if err := usm.fsm.Event(context.Background(), known.MessageBatchEventPrepareRetry); err != nil {
				usm.logger.Errorw("Failed to retry preparation", "job_id", usm.job.JobID, "error", err)
			}
		}()

		return nil
	}

	// Mark as permanently failed
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
	})
	usm.job.EndedAt = time.Now()

	cond := jobconditionsutil.FalseCondition(known.MessageBatchPreparationFailed, "Preparation failed after max retries")
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Transition to final failed state
	return usm.fsm.Event(ctx, known.MessageBatchEventFail)
}

// Delivery Phase Handlers

// OnDeliveryReady handles the delivery ready state
func (usm *StateMachine) OnDeliveryReady(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Delivery phase ready", "job_id", usm.job.JobID)

	// Initialize delivery phase
	if err := usm.initializeDelivery(ctx); err != nil {
		return fmt.Errorf("failed to initialize delivery: %w", err)
	}

	// Mark job condition as ready
	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryReady)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Auto-transition to running
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventDeliveryBegin); err != nil {
			usm.logger.Errorw("Failed to auto-transition to delivery running", "job_id", usm.job.JobID, "error", err)
		}
	}()

	return nil
}

// OnDeliveryRunning handles the delivery running state
func (usm *StateMachine) OnDeliveryRunning(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Delivery phase running", "job_id", usm.job.JobID)

	// Apply rate limiting
	_ = usm.watcher.Limiter.Processing.Take()

	// Execute delivery logic asynchronously
	go func() {
		if err := usm.executeDelivery(ctx); err != nil {
			usm.logger.Errorw("Delivery execution failed", "job_id", usm.job.JobID, "error", err)
			if transErr := usm.fsm.Event(ctx, known.MessageBatchEventDeliveryFail); transErr != nil {
				usm.logger.Errorw("Failed to transition to delivery failed", "job_id", usm.job.JobID, "error", transErr)
			}
			return
		}

		// Check if delivery is complete
		if usm.isDeliveryComplete(ctx) {
			if err := usm.fsm.Event(ctx, known.MessageBatchEventDeliveryComplete); err != nil {
				usm.logger.Errorw("Failed to complete delivery", "job_id", usm.job.JobID, "error", err)
			}
		}
	}()

	return nil
}

// OnDeliveryPausing handles the delivery pausing state
func (usm *StateMachine) OnDeliveryPausing(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Delivery phase pausing", "job_id", usm.job.JobID)

	// Signal all workers to pause
	if err := usm.pauseDeliveryWorkers(ctx); err != nil {
		return fmt.Errorf("failed to pause delivery workers: %w", err)
	}

	// Transition to paused state once all workers are paused
	return usm.fsm.Event(ctx, known.MessageBatchEventDeliveryPaused)
}

// OnDeliveryPaused handles the delivery paused state
func (usm *StateMachine) OnDeliveryPaused(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Delivery phase paused", "job_id", usm.job.JobID)

	// Save current statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save statistics", "job_id", usm.job.JobID, "error", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryPaused)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	return nil
}

// OnDeliveryCompleted handles the delivery completed state
func (usm *StateMachine) OnDeliveryCompleted(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Delivery phase completed", "job_id", usm.job.JobID)

	// Finalize delivery statistics
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := endTime.Sub(stats.StartTime).Seconds()
		durationInt := int64(duration)
		stats.Duration = &durationInt
	})

	// Save delivery results
	if err := usm.saveDeliveryResults(ctx); err != nil {
		return fmt.Errorf("failed to save delivery results: %w", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryCompleted)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Auto-transition to final success state
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventComplete); err != nil {
			usm.logger.Errorw("Failed to transition to success state", "job_id", usm.job.JobID, "error", err)
		}
	}()

	return nil
}

// OnDeliveryFailed handles the delivery failed state
func (usm *StateMachine) OnDeliveryFailed(ctx context.Context, event *fsm.Event) error {
	usm.logger.Errorw("Delivery phase failed", "job_id", usm.job.JobID, "retry_count", usm.retryCount)

	usm.retryCount++
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		stats.RetryCount = usm.retryCount
	})

	// Check if we should retry
	if usm.canRetry() {
		usm.logger.Infow("Retrying delivery phase", "job_id", usm.job.JobID, "retry_count", usm.retryCount)

		// Add delay before retry
		go func() {
			time.Sleep(time.Duration(usm.retryCount) * 30 * time.Second) // Longer delay for delivery retries
			if err := usm.fsm.Event(context.Background(), known.MessageBatchEventDeliveryRetry); err != nil {
				usm.logger.Errorw("Failed to retry delivery", "job_id", usm.job.JobID, "error", err)
			}
		}()

		return nil
	}

	// Mark as permanently failed
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
	})
	usm.job.EndedAt = time.Now()

	cond := jobconditionsutil.FalseCondition(known.MessageBatchDeliveryFailed, "Delivery failed after max retries")
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	// Transition to final failed state
	return usm.fsm.Event(ctx, known.MessageBatchEventFail)
}

// Final State Handlers

// OnSucceeded handles the final success state
func (usm *StateMachine) OnSucceeded(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Message batch processing succeeded", "job_id", usm.job.JobID)

	// Finalize all statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.job.JobID, "error", err)
	}

	// Mark job as completed
	usm.job.EndedAt = time.Now()
	cond := jobconditionsutil.TrueCondition(known.MessageBatchSucceeded)
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	return nil
}

// OnFailed handles the final failed state
func (usm *StateMachine) OnFailed(ctx context.Context, event *fsm.Event) error {
	usm.logger.Errorw("Message batch processing failed", "job_id", usm.job.JobID)

	// Finalize all statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.job.JobID, "error", err)
	}

	// Mark job as failed
	usm.job.EndedAt = time.Now()
	cond := jobconditionsutil.FalseCondition(known.MessageBatchFailed, "Message batch processing failed")
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	return nil
}

// OnCancelled handles the cancelled state
func (usm *StateMachine) OnCancelled(ctx context.Context, event *fsm.Event) error {
	usm.logger.Infow("Message batch processing cancelled", "job_id", usm.job.JobID)

	// Stop all ongoing operations
	if err := usm.cancelAllOperations(ctx); err != nil {
		usm.logger.Errorw("Failed to cancel operations", "job_id", usm.job.JobID, "error", err)
	}

	// Finalize all statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.job.JobID, "error", err)
	}

	// Mark job as cancelled
	usm.job.EndedAt = time.Now()
	cond := jobconditionsutil.FalseCondition(known.MessageBatchCancelled, "Message batch processing cancelled")
	usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)

	return nil
}

// Helper methods

func (usm *StateMachine) canRetry() bool {
	return usm.retryCount < known.MessageBatchMaxRetries
}

func (usm *StateMachine) pausePreparationWorkers(ctx context.Context) error {
	usm.logger.Infow("Pausing preparation workers", "job_id", usm.job.JobID)
	// Implementation would signal workers to pause
	return nil
}

func (usm *StateMachine) pauseDeliveryWorkers(ctx context.Context) error {
	usm.logger.Infow("Pausing delivery workers", "job_id", usm.job.JobID)
	// Implementation would signal workers to pause
	return nil
}

func (usm *StateMachine) cancelAllOperations(ctx context.Context) error {
	usm.logger.Infow("Cancelling all operations", "job_id", usm.job.JobID)
	// Implementation would cancel all ongoing operations
	return nil
}

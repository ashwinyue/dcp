package messagebatch

import (
	"context"
	"fmt"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/known"

	"github.com/looplab/fsm"

	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
)

// EnterState is called when entering any state.
func (sm *StateMachine) EnterState(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("State transition", "from", e.Src, "to", e.Dst, "event", e.Event, "job_id", sm.Job.JobID, "step", sm.CurrentStep)

	// Update current step based on state
	switch e.Dst {
	case StatePreparationReady, StatePreparationRunning, StatePreparationPausing, StatePreparationPaused, StatePreparationCompleted, StatePreparationFailed:
		sm.CurrentStep = StepPreparation
	case StateDeliveryReady, StateDeliveryRunning, StateDeliveryPausing, StateDeliveryPaused, StateDeliveryCompleted, StateDeliveryFailed:
		sm.CurrentStep = StepDelivery
	}

	// Update job status
	sm.Job.Status = e.Dst
	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job status", "error", err, "job_id", sm.Job.JobID, "status", e.Dst)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	return nil
}

// EnterPreparationReady handles entering the preparation ready state.
func (sm *StateMachine) EnterPreparationReady(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Entering preparation ready state", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationReady)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

// StartPreparation handles the preparation phase of message batch.
func (sm *StateMachine) StartPreparation(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Starting message batch preparation", "job_id", sm.Job.JobID)

	// Apply rate limiting for preparation
	sm.Watcher.Limiter.Preparation.Take()

	// Simulate preparation logic
	// In a real implementation, this would involve:
	// 1. Validating message batch data
	// 2. Preparing messages for delivery
	// 3. Setting up delivery channels
	// 4. Creating delivery packs

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationRunning)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	sm.logger.Infow("Message batch preparation completed", "job_id", sm.Job.JobID)
	return nil
}

// CompletePreparation handles the completion of preparation phase.
func (sm *StateMachine) CompletePreparation(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Preparation phase completed", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationCompleted)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	// Automatically trigger delivery start
	if err := sm.FSM.Event(ctx, EventDeliveryStart); err != nil {
		sm.logger.Errorw("Failed to start delivery phase", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to start delivery phase: %w", err)
	}

	return nil
}

// FailPreparation handles preparation failure.
func (sm *StateMachine) FailPreparation(ctx context.Context, e *fsm.Event) error {
	sm.logger.Errorw("Preparation phase failed", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.FalseCondition(known.MessageBatchPreparationFailed, "Message batch preparation failed")
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

// EnterDeliveryReady handles entering the delivery ready state.
func (sm *StateMachine) EnterDeliveryReady(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Entering delivery ready state", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryReady)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

// StartDelivery handles the delivery phase of message batch.
func (sm *StateMachine) StartDelivery(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Starting message batch delivery", "job_id", sm.Job.JobID)

	// Apply rate limiting for delivery
	sm.Watcher.Limiter.Delivery.Take()

	// Simulate delivery logic
	// In a real implementation, this would involve:
	// 1. Sending messages to target channels
	// 2. Monitoring delivery status
	// 3. Handling delivery failures and retries
	// 4. Sending partition tasks to message queues

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryRunning)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	sm.logger.Infow("Message batch delivery completed", "job_id", sm.Job.JobID)
	return nil
}

// CompleteDelivery handles the completion of delivery phase.
func (sm *StateMachine) CompleteDelivery(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Delivery phase completed", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryCompleted)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	// Automatically trigger completion
	if err := sm.FSM.Event(ctx, EventComplete); err != nil {
		sm.logger.Errorw("Failed to complete job", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to complete job: %w", err)
	}

	return nil
}

// FailDelivery handles delivery failure.
func (sm *StateMachine) FailDelivery(ctx context.Context, e *fsm.Event) error {
	sm.logger.Errorw("Delivery phase failed", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.FalseCondition(known.MessageBatchDeliveryFailed, "Message batch delivery failed")
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

// Complete handles the completion of message batch processing.
func (sm *StateMachine) Complete(ctx context.Context, e *fsm.Event) error {
	sm.logger.Infow("Message batch processing completed successfully", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.TrueCondition(known.MessageBatchSucceeded)
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

// Fail handles the failure of message batch processing.
func (sm *StateMachine) Fail(ctx context.Context, e *fsm.Event) error {
	sm.logger.Errorw("Message batch processing failed", "job_id", sm.Job.JobID)

	// Update job conditions
	cond := jobconditionsutil.FalseCondition(known.MessageBatchFailed, "Message batch processing failed")
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)

	if err := sm.Watcher.store.Job().Update(ctx, sm.Job); err != nil {
		sm.logger.Errorw("Failed to update job conditions", "error", err, "job_id", sm.Job.JobID)
		return fmt.Errorf("failed to update job conditions: %w", err)
	}

	return nil
}

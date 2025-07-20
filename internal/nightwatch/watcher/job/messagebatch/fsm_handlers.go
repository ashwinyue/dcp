package messagebatch

import (
	"context"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
)

// Preparation Phase Handlers

// OnPreparationReady handles the PREPARATION_READY state
// Equivalent to AbstractStep.doReady() and SmsBatch.entryProcess()
func (usm *StateMachine) OnPreparationReady(ctx context.Context, e *fsm.Event) {
	usm.logger.Infow("Entering PREPARATION_READY state", "job_id", usm.Job.JobID)

	// Check if batch has provider (equivalent to SmsBatch.hasProvider())
	if usm.Job.Params == nil || usm.Job.Params.MessageBatch == nil {
		usm.logger.Errorw("Batch provider not configured", "job_id", usm.Job.JobID)
		if triggerErr := usm.TriggerEvent(ctx, EventFail); triggerErr != nil {
			usm.logger.Errorw("Failed to trigger failure event", "job_id", usm.Job.JobID, "error", triggerErr)
		}
		return
	}

	// Initialize preparation work (equivalent to SmsPreparationStep.doReady())
	if err := usm.initializePreparation(ctx); err != nil {
		usm.logger.Errorw("Failed to initialize preparation", "job_id", usm.Job.JobID, "error", err)
		if triggerErr := usm.TriggerEvent(ctx, EventFail); triggerErr != nil {
			usm.logger.Errorw("Failed to trigger failure event", "job_id", usm.Job.JobID, "error", triggerErr)
		}
		return
	}

	// Call phase start callback (equivalent to business callback)
	if usm.CallbackHandler != nil {
		if err := usm.CallbackHandler.OnPhaseStart(ctx, usm.Job.JobID, "PREPARATION"); err != nil {
			usm.logger.Errorw("Phase start callback failed", "job_id", usm.Job.JobID, "error", err)
		}
	}

	// Update job status and current step
	usm.Job.Status = "PREPARATION_READY"
	usm.Job.UpdatedAt = time.Now()
	if usm.Job.Results == nil {
		usm.Job.Results = &model.JobResults{}
	}
	usm.Job.Results.CurrentStep = "PREPARATION"

	// Save job state
	if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
		usm.logger.Errorw("Failed to save job state", "job_id", usm.Job.JobID, "error", err)
	}

	// Automatically transition to PREPARATION_RUNNING (equivalent to auto-start)
	if err := usm.TriggerEvent(ctx, EventPrepareStart); err != nil {
		usm.logger.Errorw("Failed to trigger prepare start event", "job_id", usm.Job.JobID, "error", err)
	}
}

// OnPreparationRunning handles the PREPARATION_RUNNING state
// Equivalent to AbstractStep.doExecute() and SmsPreparationStep.execute()
func (usm *StateMachine) OnPreparationRunning(ctx context.Context, e *fsm.Event) {
	usm.logger.Infow("Entering PREPARATION_RUNNING state", "job_id", usm.Job.JobID)

	// Update job status and current step (equivalent to SmsBatch.setCurrentStep)
	usm.Job.Status = "PREPARATION_RUNNING"
	usm.Job.UpdatedAt = time.Now()
	if usm.Job.Results != nil {
		usm.Job.Results.CurrentStep = "PREPARATION"
	}

	// Save job state
	if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
		usm.logger.Errorw("Failed to save job state", "job_id", usm.Job.JobID, "error", err)
	}

	// Execute preparation logic asynchronously (equivalent to preparePoolExecutor.submit)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				usm.logger.Errorw("Preparation execution panic", "job_id", usm.Job.JobID, "panic", r)
				if triggerErr := usm.TriggerEvent(ctx, EventPrepareFail); triggerErr != nil {
					usm.logger.Errorw("Failed to trigger failure event", "job_id", usm.Job.JobID, "error", triggerErr)
				}
			}
		}()

		if err := usm.executePreparation(ctx); err != nil {
			usm.logger.Errorw("Preparation execution failed", "job_id", usm.Job.JobID, "error", err)
			
			// Trigger phase failed callback
			if usm.CallbackHandler != nil {
				if callbackErr := usm.CallbackHandler.OnPhaseFailed(ctx, usm.Job.JobID, "PREPARATION", err, usm.GetStatistics("PREPARATION")); callbackErr != nil {
					usm.logger.Errorw("Phase failed callback error", "job_id", usm.Job.JobID, "error", callbackErr)
				}
			}
			
			// Trigger failure event (equivalent to executedFailed)
			if triggerErr := usm.TriggerEvent(ctx, EventPrepareFail); triggerErr != nil {
					usm.logger.Errorw("Failed to trigger failure event", "job_id", usm.Job.JobID, "error", triggerErr)
				}
			return
		}

		// Trigger progress callback
		if usm.CallbackHandler != nil {
			if err := usm.CallbackHandler.OnPhaseProgress(ctx, usm.Job.JobID, "PREPARATION", usm.GetStatistics("PREPARATION")); err != nil {
				usm.logger.Errorw("Phase progress callback failed", "job_id", usm.Job.JobID, "error", err)
			}
		}

		// Check if preparation is complete (equivalent to checking completion status)
		if usm.isPreparationComplete(ctx) {
			// Trigger completion event (equivalent to executedSuccess)
			if err := usm.TriggerEvent(ctx, EventPrepareComplete); err != nil {
					usm.logger.Errorw("Failed to trigger completion event", "job_id", usm.Job.JobID, "error", err)
			}
		}
	}()
}

// OnPreparationPausing handles the PREPARATION_PAUSING state
// Equivalent to AbstractStep.doPausing()
func (usm *StateMachine) OnPreparationPausing(ctx context.Context, e *fsm.Event) {
	usm.logger.Infow("Entering PREPARATION_PAUSING state", "job_id", usm.Job.JobID)

	// Update job status
	usm.Job.Status = "PREPARATION_PAUSING"
	usm.Job.UpdatedAt = time.Now()

	// Save job state (equivalent to smsBatchRepository.save)
	if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
		usm.logger.Errorw("Failed to save job state", "job_id", usm.Job.JobID, "error", err)
	}

	// Signal all workers to pause (equivalent to stopping preparation workers)
	if err := usm.pausePreparationWorkers(ctx); err != nil {
		usm.logger.Errorw("Failed to pause preparation workers", "job_id", usm.Job.JobID, "error", err)
		return
	}

	// Automatically transition to paused state once all workers are paused
	if err := usm.TriggerEvent(ctx, EventPreparePaused); err != nil {
		usm.logger.Errorw("Failed to trigger paused event", "job_id", usm.Job.JobID, "error", err)
	}
}

// OnPreparationPaused handles the PREPARATION_PAUSED state
// Equivalent to AbstractStep.doPause()
func (usm *StateMachine) OnPreparationPaused(ctx context.Context, e *fsm.Event) {
	usm.logger.Infow("Entering PREPARATION_PAUSED state", "job_id", usm.Job.JobID)

	// Update job status
	usm.Job.Status = "PREPARATION_PAUSED"
	usm.Job.UpdatedAt = time.Now()

	// Sync and save current statistics (equivalent to prepareSyncer.sync2Statistics)
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		// Update pause time and other metrics
		stats.RetryCount = usm.retryCount
	})

	// Save current statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save statistics", "job_id", usm.Job.JobID, "error", err)
	}

	// Save job state (equivalent to smsBatchRepository.save)
	if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
		usm.logger.Errorw("Failed to save job state", "job_id", usm.Job.JobID, "error", err)
	}

	// Mark job condition as paused
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationPaused)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))
}

// OnPreparationCompleted handles the PREPARATION_COMPLETED state
// Equivalent to AbstractStep.doComplete() and SmsPreparationStep.doComplete()
func (usm *StateMachine) OnPreparationCompleted(ctx context.Context, e *fsm.Event) {
	usm.logger.Infow("Entering PREPARATION_COMPLETED state", "job_id", usm.Job.JobID)

	// Finalize preparation statistics (equivalent to prepareSyncer.sync2Statistics)
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := endTime.Sub(stats.StartTime).Seconds()
		stats.Duration = &duration
		stats.Percent = 100.0 // Mark as 100% complete
	})

	// Update job status and prepare for next step
	usm.Job.Status = "PREPARATION_COMPLETED"
	usm.Job.UpdatedAt = time.Now()
	if usm.Job.Results != nil {
		usm.Job.Results.CurrentStep = "DELIVERY" // Transition to delivery step
	}

	// Save preparation results
	if err := usm.savePreparationResults(ctx); err != nil {
		usm.logger.Errorw("Failed to save preparation results", "job_id", usm.Job.JobID, "error", err)
		return
	}

	// Trigger phase completed callback
	if usm.CallbackHandler != nil {
		if err := usm.CallbackHandler.OnPhaseComplete(ctx, usm.Job.JobID, "PREPARATION", usm.GetStatistics("PREPARATION")); err != nil {
			usm.logger.Errorw("Phase completed callback failed", "job_id", usm.Job.JobID, "error", err)
		}
	}

	// Mark job condition as completed
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationCompleted)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Save job state
	if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
		usm.logger.Errorw("Failed to save job state", "job_id", usm.Job.JobID, "error", err)
	}

	// Auto-transition to delivery phase (equivalent to SmsBatch.setCurrentStep(DELIVERY))
	if err := usm.TriggerEvent(ctx, EventDeliveryStart); err != nil {
		usm.logger.Errorw("Failed to start delivery phase", "job_id", usm.Job.JobID, "error", err)
	}
}

// OnPreparationFailed handles the PREPARATION_FAILED state
// Equivalent to AbstractStep.doRetry() and AbstractStep.executedFailed()
func (usm *StateMachine) OnPreparationFailed(ctx context.Context, e *fsm.Event) {
	usm.logger.Errorw("Entering PREPARATION_FAILED state", "job_id", usm.Job.JobID)

	// Update job status
	usm.Job.Status = "PREPARATION_FAILED"
	usm.Job.UpdatedAt = time.Now()

	// Update statistics with failure
	usm.updateStatistics("PREPARATION", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := endTime.Sub(stats.StartTime).Seconds()
		stats.Duration = &duration
		stats.RetryCount = usm.retryCount
	})

	// Save current statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save statistics", "job_id", usm.Job.JobID, "error", err)
	}

	// Check if retry is possible (equivalent to AbstractStep.canRetry())
	if usm.canRetry() {
		usm.retryCount++
		usm.logger.Infow("Retrying preparation phase", "job_id", usm.Job.JobID, "retry_count", usm.retryCount)
		
		// Update retry count in job
		if usm.Job.Results != nil {
			usm.Job.Results.RetryCount = usm.retryCount
		}

		// Save job state before retry
		if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
			usm.logger.Errorw("Failed to save job state before retry", "job_id", usm.Job.JobID, "error", err)
		}
		
		// Trigger retry (equivalent to AbstractStep.doRetry())
		if err := usm.TriggerEvent(ctx, EventPrepareStart); err != nil {
			usm.logger.Errorw("Failed to retry preparation", "job_id", usm.Job.JobID, "error", err)
		}
	} else {
		// Mark as permanently failed
		cond := jobconditionsutil.FalseCondition(known.MessageBatchPreparationFailed, "Preparation failed after max retries")
		usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))
		
		// Save final job state
		if err := usm.Store.SaveJob(ctx, usm.Job); err != nil {
			usm.logger.Errorw("Failed to save final job state", "job_id", usm.Job.JobID, "error", err)
		}
		
		// Trigger error callback
		if usm.CallbackHandler != nil {
			if err := usm.CallbackHandler.OnError(ctx, usm.Job.JobID, "PREPARATION", fmt.Errorf("preparation failed after %d retries", usm.retryCount)); err != nil {
				usm.logger.Errorw("Error callback failed", "job_id", usm.Job.JobID, "error", err)
			}
		}
	}
}

// Delivery Phase Handlers

// OnDeliveryReady handles the delivery ready state
func (usm *StateMachine) OnDeliveryReady(event *fsm.Event) {
	usm.logger.Infow("Delivery phase ready", "job_id", usm.Job.JobID)

	// Trigger phase start callback
	ctx := context.Background()
	if usm.callbackHandler != nil {
		if err := usm.callbackHandler.OnPhaseStart(ctx, usm.Job.JobID, "DELIVERY"); err != nil {
			usm.logger.Errorw("Phase start callback failed", "error", err)
		}
	}

	// Initialize delivery phase
	if err := usm.initializeDelivery(ctx); err != nil {
		usm.logger.Errorw("Failed to initialize delivery", "job_id", usm.Job.JobID, "error", err)
		
		// Trigger phase failed callback
		if usm.callbackHandler != nil {
			if callbackErr := usm.callbackHandler.OnPhaseFailed(ctx, usm.Job.JobID, "DELIVERY", err, usm.stats["DELIVERY"]); callbackErr != nil {
				usm.logger.Errorw("Phase failed callback error", "error", callbackErr)
			}
		}
		return
	}

	// Mark job condition as ready
	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryReady)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Auto-transition to running
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventDeliveryBegin); err != nil {
			usm.logger.Errorw("Failed to auto-transition to delivery running", "job_id", usm.Job.JobID, "error", err)
		}
	}()
}

// OnDeliveryRunning handles the delivery running state
func (usm *StateMachine) OnDeliveryRunning(event *fsm.Event) {
	usm.logger.Infow("Delivery phase running", "job_id", usm.Job.JobID)

	// Apply rate limiting
	// TODO: Implement rate limiting when Limiter is available
	// _ = usm.watcher.Limiter.Processing.Take()

	// Execute delivery logic asynchronously
	go func() {
		ctx := context.Background()
		if err := usm.executeDelivery(ctx); err != nil {
			usm.logger.Errorw("Delivery execution failed", "job_id", usm.Job.JobID, "error", err)
			
			// Trigger progress callback with error
			if usm.callbackHandler != nil {
				if callbackErr := usm.callbackHandler.OnPhaseProgress(ctx, usm.Job.JobID, "DELIVERY", usm.stats["DELIVERY"]); callbackErr != nil {
					usm.logger.Errorw("Phase progress callback error", "error", callbackErr)
				}
			}
			
			if transErr := usm.FSM.Event(ctx, known.MessageBatchEventDeliveryFail); transErr != nil {
				usm.logger.Errorw("Failed to transition to delivery failed", "job_id", usm.Job.JobID, "error", transErr)
			}
			return
		}

		// Trigger progress callback
		if usm.callbackHandler != nil {
			if err := usm.callbackHandler.OnPhaseProgress(ctx, usm.Job.JobID, "DELIVERY", usm.stats["DELIVERY"]); err != nil {
				usm.logger.Errorw("Phase progress callback failed", "error", err)
			}
		}

		// Check if delivery is complete
		if usm.isDeliveryComplete(ctx) {
			if err := usm.FSM.Event(ctx, known.MessageBatchEventDeliveryComplete); err != nil {
				usm.logger.Errorw("Failed to complete delivery", "job_id", usm.Job.JobID, "error", err)
			}
		}
	}()
}

// OnDeliveryPausing handles the delivery pausing state
func (usm *StateMachine) OnDeliveryPausing(event *fsm.Event) {
	usm.logger.Infow("Delivery phase pausing", "job_id", usm.Job.JobID)

	// Signal all workers to pause
	ctx := context.Background()
	if err := usm.pauseDeliveryWorkers(ctx); err != nil {
		usm.logger.Errorw("Failed to pause delivery workers", "job_id", usm.Job.JobID, "error", err)
		return
	}

	// Transition to paused state once all workers are paused
	usm.FSM.Event(ctx, known.MessageBatchEventDeliveryPaused)
}

// OnDeliveryPaused handles the delivery paused state
func (usm *StateMachine) OnDeliveryPaused(event *fsm.Event) {
	usm.logger.Infow("Delivery phase paused", "job_id", usm.Job.JobID)

	// Save current statistics
	ctx := context.Background()
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save statistics", "job_id", usm.Job.JobID, "error", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryPaused)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))
}

// OnDeliveryCompleted handles the delivery completed state
func (usm *StateMachine) OnDeliveryCompleted(event *fsm.Event) {
	usm.logger.Infow("Delivery phase completed", "job_id", usm.Job.JobID)

	// Finalize delivery statistics
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
		duration := endTime.Sub(stats.StartTime).Seconds()
		stats.Duration = &duration
	})

	// Save delivery results
	ctx := context.Background()
	if err := usm.saveDeliveryResults(ctx); err != nil {
		usm.logger.Errorw("Failed to save delivery results", "job_id", usm.Job.JobID, "error", err)
		return
	}

	// Trigger phase completed callback
	if usm.callbackHandler != nil {
		if err := usm.callbackHandler.OnPhaseComplete(ctx, usm.Job.JobID, "DELIVERY", usm.stats["DELIVERY"]); err != nil {
			usm.logger.Errorw("Phase completed callback failed", "error", err)
		}
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchDeliveryCompleted)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Auto-transition to final success state
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure state is saved
		if err := usm.Transition(context.Background(), known.MessageBatchEventDeliveryComplete); err != nil {
			usm.logger.Errorw("Failed to transition to success state", "job_id", usm.Job.JobID, "error", err)
		}
	}()
}

// OnDeliveryFailed handles the delivery failed state
func (usm *StateMachine) OnDeliveryFailed(event *fsm.Event) {
	usm.logger.Errorw("Delivery phase failed", "job_id", usm.Job.JobID, "retry_count", usm.retryCount)

	usm.retryCount++
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		stats.RetryCount = usm.retryCount
	})

	// Check if we should retry
	if usm.canRetry() {
		usm.logger.Infow("Retrying delivery phase", "job_id", usm.Job.JobID, "retry_count", usm.retryCount)

		// Add delay before retry
		go func() {
			time.Sleep(time.Duration(usm.retryCount) * 30 * time.Second) // Longer delay for delivery retries
			if err := usm.FSM.Event(context.Background(), known.MessageBatchEventDeliveryRetry); err != nil {
				usm.logger.Errorw("Failed to retry delivery", "job_id", usm.Job.JobID, "error", err)
			}
		}()

		return
	}

	// Mark as permanently failed
	usm.updateStatistics("DELIVERY", func(stats *PhaseStatistics) {
		endTime := time.Now()
		stats.EndTime = &endTime
	})
	usm.Job.EndedAt = time.Now()

	cond := jobconditionsutil.FalseCondition(known.MessageBatchDeliveryFailed, "Delivery failed after max retries")
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Transition to final failed state
	ctx := context.Background()
	usm.FSM.Event(ctx, known.MessageBatchEventDeliveryFail)
}

// Final State Handlers

// OnSucceeded handles the final success state
func (usm *StateMachine) OnSucceeded(event *fsm.Event) {
	usm.logger.Infow("Message batch processing succeeded", "job_id", usm.Job.JobID)

	// Finalize all statistics
	ctx := context.Background()
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.Job.JobID, "error", err)
	}

	// Mark job as completed
	usm.Job.EndedAt = time.Now()
	cond := jobconditionsutil.TrueCondition(known.MessageBatchSucceeded)
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Trigger batch completion callback
	if usm.callbackHandler != nil {
		finalStats := map[string]*PhaseStatistics{
			"PREPARATION": usm.stats["PREPARATION"],
			"DELIVERY":    usm.stats["DELIVERY"],
		}
		if err := usm.callbackHandler.OnBatchComplete(ctx, usm.Job.JobID, finalStats); err != nil {
			usm.logger.Errorw("Batch completion callback failed", "error", err)
		}
	}
}

// OnFailed handles the final failed state
func (usm *StateMachine) OnFailed(event *fsm.Event) {
	usm.logger.Errorw("Message batch processing failed", "job_id", usm.Job.JobID)

	// Finalize all statistics
	ctx := context.Background()
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.Job.JobID, "error", err)
	}

	// Mark job as failed
	usm.Job.EndedAt = time.Now()
	cond := jobconditionsutil.FalseCondition(known.MessageBatchFailed, "Message batch processing failed")
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))

	// Trigger batch failure callback
	if usm.callbackHandler != nil {
		finalStats := map[string]*PhaseStatistics{
			"PREPARATION": usm.stats["PREPARATION"],
			"DELIVERY":    usm.stats["DELIVERY"],
		}
		if err := usm.callbackHandler.OnBatchFailed(ctx, usm.Job.JobID, nil, finalStats); err != nil {
			usm.logger.Errorw("Batch failure callback failed", "error", err)
		}
	}
}

// OnCancelled handles the cancelled state
func (usm *StateMachine) OnCancelled(event *fsm.Event) {
	usm.logger.Infow("Message batch processing cancelled", "job_id", usm.Job.JobID)

	// Stop all ongoing operations
	ctx := context.Background()
	if err := usm.cancelAllOperations(ctx); err != nil {
		usm.logger.Errorw("Failed to cancel operations", "job_id", usm.Job.JobID, "error", err)
	}

	// Finalize all statistics
	if err := usm.saveStatistics(ctx); err != nil {
		usm.logger.Errorw("Failed to save final statistics", "job_id", usm.Job.JobID, "error", err)
	}

	// Mark job as cancelled
	usm.Job.EndedAt = time.Now()
	cond := jobconditionsutil.FalseCondition(known.MessageBatchFailed, "Message batch processing cancelled")
	usm.Job.Conditions = (*model.JobConditions)(jobconditionsutil.Set((*model.JobConditions)(usm.Job.Conditions), cond))
}

// Helper methods

func (usm *StateMachine) canRetry() bool {
	return usm.retryCount < known.MessageBatchMaxRetries
}

func (usm *StateMachine) pausePreparationWorkers(ctx context.Context) error {
	usm.logger.Infow("Pausing preparation workers", "job_id", usm.Job.JobID)
	// Implementation would signal workers to pause
	return nil
}

func (usm *StateMachine) pauseDeliveryWorkers(ctx context.Context) error {
	usm.logger.Infow("Pausing delivery workers", "job_id", usm.Job.JobID)
	// Implementation would signal workers to pause
	return nil
}

func (usm *StateMachine) cancelAllOperations(ctx context.Context) error {
	usm.logger.Infow("Cancelling all operations", "job_id", usm.Job.JobID)
	// Implementation would cancel all ongoing operations
	return nil
}

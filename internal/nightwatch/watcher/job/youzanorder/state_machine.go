package youzanorder

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// StateMachine represents the state machine for YouZan order processing.
type StateMachine struct {
	ctx     context.Context
	store   store.IStore
	job     *model.JobM
	watcher *Watcher
	fsm     *fsm.FSM
}

// NewStateMachine creates a new state machine for YouZan order processing.
func NewStateMachine(currentState string, watcher *Watcher, job *model.JobM) *StateMachine {
	sm := &StateMachine{
		ctx:     context.Background(),
		store:   watcher.Store,
		job:     job,
		watcher: watcher,
	}

	// Define the finite state machine
	sm.fsm = fsm.NewFSM(
		job.Status,
		fsm.Events{
			{Name: "fetch", Src: []string{known.YouZanOrderPending}, Dst: known.YouZanOrderFetching},
			{Name: "fetched", Src: []string{known.YouZanOrderFetching}, Dst: known.YouZanOrderFetched},
			{Name: "validate", Src: []string{known.YouZanOrderFetched}, Dst: known.YouZanOrderValidating},
			{Name: "validated", Src: []string{known.YouZanOrderValidating}, Dst: known.YouZanOrderValidated},
			{Name: "enrich", Src: []string{known.YouZanOrderValidated}, Dst: known.YouZanOrderEnriching},
			{Name: "enriched", Src: []string{known.YouZanOrderEnriching}, Dst: known.YouZanOrderEnriched},
			{Name: "process", Src: []string{known.YouZanOrderEnriched}, Dst: known.YouZanOrderProcessing},
			{Name: "processed", Src: []string{known.YouZanOrderProcessing}, Dst: known.YouZanOrderProcessed},
			{Name: "succeed", Src: []string{known.YouZanOrderProcessed}, Dst: known.YouZanOrderSucceeded},
			{Name: "fail", Src: []string{known.YouZanOrderPending, known.YouZanOrderFetching, known.YouZanOrderFetched, known.YouZanOrderValidating, known.YouZanOrderValidated, known.YouZanOrderEnriching, known.YouZanOrderEnriched, known.YouZanOrderProcessing, known.YouZanOrderProcessed}, Dst: known.YouZanOrderFailed},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				sm.EnterState(e.Dst)
			},
		},
	)

	return sm
}

// Fetch handles the fetch event (moves from Pending to Fetching to Fetched).
func (sm *StateMachine) Fetch() error {
	log.Infow("StateMachine: Processing fetch event", "jobID", sm.job.JobID, "currentStatus", sm.job.Status)

	// Check if job should be skipped based on idempotency
	if ShouldSkipOnIdempotency(sm.ctx, sm.store, sm.job) {
		log.Infow("Skipping job due to idempotency", "jobID", sm.job.JobID)
		return nil
	}

	// Check for timeout
	if isJobTimeout(sm.job, known.YouZanOrderTimeout) {
		log.Warnw("Job timeout detected", "jobID", sm.job.JobID)
		return sm.handleJobFailure("Job timeout during fetch")
	}

	// Set default job parameters
	SetDefaultJobParams(sm.job)

	switch sm.job.Status {
	case known.YouZanOrderPending:
		// Update status to Fetching
		sm.job.Status = known.YouZanOrderFetching
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to fetching: %w", err)
		}

		// Execute actual fetch logic
		if err := sm.watcher.FetchOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("fetch failed: %v", err))
		}

		// Update status to Fetched
		sm.job.Status = known.YouZanOrderFetched
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to fetched: %w", err)
		}

		// Log summary
		logJobSummary(sm.job, "fetch")
		log.Infow("StateMachine: Fetch completed successfully", "jobID", sm.job.JobID)
		return nil

	case known.YouZanOrderFetching:
		// Continue with fetch logic if already in fetching state
		if err := sm.watcher.FetchOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("fetch failed: %v", err))
		}

		sm.job.Status = known.YouZanOrderFetched
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to fetched: %w", err)
		}

		logJobSummary(sm.job, "fetch")
		return nil

	default:
		return fmt.Errorf("invalid state transition: cannot fetch from state %s", sm.job.Status)
	}
}

// Validate handles the validation event (moves from Fetched to Validating to Validated).
func (sm *StateMachine) Validate() error {
	log.Infow("StateMachine: Processing validate event", "jobID", sm.job.JobID, "currentStatus", sm.job.Status)

	// Check for timeout
	if isJobTimeout(sm.job, known.YouZanOrderTimeout) {
		log.Warnw("Job timeout detected", "jobID", sm.job.JobID)
		return sm.handleJobFailure("Job timeout during validation")
	}

	switch sm.job.Status {
	case known.YouZanOrderFetched:
		// Update status to Validating
		sm.job.Status = known.YouZanOrderValidating
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to validating: %w", err)
		}

		// Execute actual validation logic
		if err := sm.watcher.ValidateOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("validation failed: %v", err))
		}

		// Update status to Validated
		sm.job.Status = known.YouZanOrderValidated
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to validated: %w", err)
		}

		logJobSummary(sm.job, "validate")
		log.Infow("StateMachine: Validation completed successfully", "jobID", sm.job.JobID)
		return nil

	case known.YouZanOrderValidating:
		// Continue with validation logic if already in validating state
		if err := sm.watcher.ValidateOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("validation failed: %v", err))
		}

		sm.job.Status = known.YouZanOrderValidated
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to validated: %w", err)
		}

		logJobSummary(sm.job, "validate")
		return nil

	default:
		return fmt.Errorf("invalid state transition: cannot validate from state %s", sm.job.Status)
	}
}

// Enrich handles the enrichment event (moves from Validated to Enriching to Enriched).
func (sm *StateMachine) Enrich() error {
	log.Infow("StateMachine: Processing enrich event", "jobID", sm.job.JobID, "currentStatus", sm.job.Status)

	// Check for timeout
	if isJobTimeout(sm.job, known.YouZanOrderTimeout) {
		log.Warnw("Job timeout detected", "jobID", sm.job.JobID)
		return sm.handleJobFailure("Job timeout during enrichment")
	}

	switch sm.job.Status {
	case known.YouZanOrderValidated:
		// Update status to Enriching
		sm.job.Status = known.YouZanOrderEnriching
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to enriching: %w", err)
		}

		// Execute actual enrichment logic
		if err := sm.watcher.EnrichOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("enrichment failed: %v", err))
		}

		// Update status to Enriched
		sm.job.Status = known.YouZanOrderEnriched
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to enriched: %w", err)
		}

		logJobSummary(sm.job, "enrich")
		log.Infow("StateMachine: Enrichment completed successfully", "jobID", sm.job.JobID)
		return nil

	case known.YouZanOrderEnriching:
		// Continue with enrichment logic if already in enriching state
		if err := sm.watcher.EnrichOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("enrichment failed: %v", err))
		}

		sm.job.Status = known.YouZanOrderEnriched
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to enriched: %w", err)
		}

		logJobSummary(sm.job, "enrich")
		return nil

	default:
		return fmt.Errorf("invalid state transition: cannot enrich from state %s", sm.job.Status)
	}
}

// Process handles the processing event (moves from Enriched to Processing to Processed).
func (sm *StateMachine) Process() error {
	log.Infow("StateMachine: Processing process event", "jobID", sm.job.JobID, "currentStatus", sm.job.Status)

	// Check for timeout
	if isJobTimeout(sm.job, known.YouZanOrderTimeout) {
		log.Warnw("Job timeout detected", "jobID", sm.job.JobID)
		return sm.handleJobFailure("Job timeout during processing")
	}

	switch sm.job.Status {
	case known.YouZanOrderEnriched:
		// Update status to Processing
		sm.job.Status = known.YouZanOrderProcessing
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to processing: %w", err)
		}

		// Execute actual processing logic
		if err := sm.watcher.ProcessOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("processing failed: %v", err))
		}

		// Update status to Processed
		sm.job.Status = known.YouZanOrderProcessed
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to processed: %w", err)
		}

		// Finally, mark as succeeded
		sm.job.Status = known.YouZanOrderSucceeded
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to succeeded: %w", err)
		}

		logJobSummary(sm.job, "process")
		log.Infow("StateMachine: Processing completed successfully", "jobID", sm.job.JobID)
		return nil

	case known.YouZanOrderProcessing:
		// Continue with processing logic if already in processing state
		if err := sm.watcher.ProcessOrder(sm.ctx, sm.job); err != nil {
			return sm.handleJobFailure(fmt.Sprintf("processing failed: %v", err))
		}

		sm.job.Status = known.YouZanOrderProcessed
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to processed: %w", err)
		}

		// Finally, mark as succeeded
		sm.job.Status = known.YouZanOrderSucceeded
		if err := sm.updateJobStatus(); err != nil {
			return fmt.Errorf("failed to update job status to succeeded: %w", err)
		}

		logJobSummary(sm.job, "process")
		return nil

	default:
		return fmt.Errorf("invalid state transition: cannot process from state %s", sm.job.Status)
	}
}

// updateJobStatus updates the job status in the store.
func (sm *StateMachine) updateJobStatus() error {
	return sm.store.Job().Update(sm.ctx, sm.job)
}

// handleJobFailure handles job failure scenarios with retry logic.
func (sm *StateMachine) handleJobFailure(reason string) error {
	log.Errorw("StateMachine: Job failed", "jobID", sm.job.JobID, "reason", reason)

	// Create error for retry check
	err := fmt.Errorf("job failed: %s", reason)

	// Check if we should retry
	if shouldRetryJob(sm.job, err) {
		log.Infow("Retrying job", "jobID", sm.job.JobID, "reason", reason)

		// Update job with retry information and keep current status for retry
		if updateErr := sm.updateJobStatus(); updateErr != nil {
			return fmt.Errorf("failed to update job for retry: %w", updateErr)
		}

		// Return the original error but don't mark as failed yet
		return err
	}

	// No more retries, mark as failed
	sm.job.Status = known.YouZanOrderFailed
	if err := sm.updateJobStatus(); err != nil {
		return fmt.Errorf("failed to update job status to failed: %w", err)
	}

	// Log final failure summary
	logJobSummary(sm.job, "failed")

	return fmt.Errorf("job failed permanently: %s", reason)
}

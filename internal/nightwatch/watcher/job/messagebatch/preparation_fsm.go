package messagebatch

import (
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// PreparationFSM represents the finite state machine for the preparation phase
type PreparationFSM struct {
	watcher    *Watcher
	job        *model.JobM
	fsm        *fsm.FSM
	statistics *PhaseStatistics
	retryCount int
	startTime  time.Time
}

// NewPreparationFSM creates a new preparation phase FSM
func NewPreparationFSM(initial string, watcher *Watcher, job *model.JobM) *PreparationFSM {
	pfsm := &PreparationFSM{
		watcher: watcher,
		job:     job,
		statistics: &PhaseStatistics{
			StartTime:  time.Now(),
			RetryCount: 0,
		},
		retryCount: 0,
		startTime:  time.Now(),
	}

	pfsm.fsm = fsm.NewFSM(
		initial,
		fsm.Events{
			// State transitions for preparation phase
			{Name: known.MessageBatchEventPrepareStart, Src: []string{known.MessageBatchPending}, Dst: known.MessageBatchPreparationReady},
			{Name: known.MessageBatchEventPrepareBegin, Src: []string{known.MessageBatchPreparationReady, known.MessageBatchPreparationPaused}, Dst: known.MessageBatchPreparationRunning},
			{Name: known.MessageBatchEventPreparePause, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationPausing},
			{Name: known.MessageBatchEventPreparePaused, Src: []string{known.MessageBatchPreparationPausing}, Dst: known.MessageBatchPreparationPaused},
			{Name: known.MessageBatchEventPrepareResume, Src: []string{known.MessageBatchPreparationPaused}, Dst: known.MessageBatchPreparationReady},
			{Name: known.MessageBatchEventPrepareComplete, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationCompleted},
			{Name: known.MessageBatchEventPrepareFail, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationFailed},
			{Name: known.MessageBatchEventPrepareRetry, Src: []string{known.MessageBatchPreparationFailed}, Dst: known.MessageBatchPreparationReady},
		},
		fsm.Callbacks{
			"enter_state": fsmutil.WrapEvent(pfsm.EnterState),
			// State-specific callbacks
			"enter_" + known.MessageBatchPreparationReady:     fsmutil.WrapEvent(pfsm.OnReady),
			"enter_" + known.MessageBatchPreparationRunning:   fsmutil.WrapEvent(pfsm.OnRunning),
			"enter_" + known.MessageBatchPreparationPausing:   fsmutil.WrapEvent(pfsm.OnPausing),
			"enter_" + known.MessageBatchPreparationPaused:    fsmutil.WrapEvent(pfsm.OnPaused),
			"enter_" + known.MessageBatchPreparationCompleted: fsmutil.WrapEvent(pfsm.OnCompleted),
			"enter_" + known.MessageBatchPreparationFailed:    fsmutil.WrapEvent(pfsm.OnFailed),
		},
	)

	return pfsm
}

// GetCurrentState returns the current state
func (pfsm *PreparationFSM) GetCurrentState() string {
	return pfsm.fsm.Current()
}

// CanTransition checks if a state transition is valid
func (pfsm *PreparationFSM) CanTransition(event string) bool {
	return pfsm.fsm.Can(event)
}

// Transition attempts to transition to a new state
func (pfsm *PreparationFSM) Transition(ctx context.Context, event string) error {
	return pfsm.fsm.Event(ctx, event)
}

// GetValidEvents returns valid events for the current state
func (pfsm *PreparationFSM) GetValidEvents() []string {
	return pfsm.fsm.AvailableTransitions()
}

// GetStatistics returns the current statistics
func (pfsm *PreparationFSM) GetStatistics() *PhaseStatistics {
	return pfsm.statistics
}

// EnterState handles the state transition and updates job status
func (pfsm *PreparationFSM) EnterState(ctx context.Context, event *fsm.Event) error {
	currentState := event.FSM.Current()

	log.Infow("Preparation FSM state transition",
		"job_id", pfsm.job.JobID,
		"event", event.Event,
		"from", event.Src,
		"to", currentState)

	// Update job status
	pfsm.job.Status = currentState

	// Handle timeout check
	if pfsm.isTimeout() {
		pfsm.job.Status = known.MessageBatchPreparationFailed
		endTime := time.Now()
		pfsm.statistics.EndTime = &endTime
		pfsm.job.EndedAt = endTime

		cond := jobconditionsutil.FalseCondition(currentState, "Preparation phase timeout")
		pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)
	}

	// Handle error cases
	if event.Err != nil {
		pfsm.job.Status = known.MessageBatchPreparationFailed
		endTime := time.Now()
		pfsm.statistics.EndTime = &endTime
		pfsm.job.EndedAt = endTime

		cond := jobconditionsutil.FalseCondition(currentState, event.Err.Error())
		pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)
	}

	// Update job in store
	if err := pfsm.watcher.Store.Job().Update(ctx, pfsm.job); err != nil {
		log.Errorw("Failed to update job", "job_id", pfsm.job.JobID, "error", err)
		return err
	}

	return nil
}

// OnReady handles the ready state
func (pfsm *PreparationFSM) OnReady(ctx context.Context, event *fsm.Event) error {
	log.Infow("Preparation phase ready", "job_id", pfsm.job.JobID)

	// Initialize preparation phase
	if err := pfsm.initializePreparation(ctx); err != nil {
		return fmt.Errorf("failed to initialize preparation: %w", err)
	}

	// Mark job condition as ready
	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationReady)
	pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)

	return nil
}

// OnRunning handles the running state
func (pfsm *PreparationFSM) OnRunning(ctx context.Context, event *fsm.Event) error {
	log.Infow("Preparation phase running", "job_id", pfsm.job.JobID)

	// Apply rate limiting
	_ = pfsm.watcher.Limiter.Processing.Take()

	// Execute preparation logic
	if err := pfsm.executePreparation(ctx); err != nil {
		log.Errorw("Preparation execution failed", "job_id", pfsm.job.JobID, "error", err)
		return pfsm.fsm.Event(ctx, known.MessageBatchEventPrepareFail)
	}

	// Check if preparation is complete
	if pfsm.isPreparationComplete(ctx) {
		return pfsm.fsm.Event(ctx, known.MessageBatchEventPrepareComplete)
	}

	return nil
}

// OnPausing handles the pausing state
func (pfsm *PreparationFSM) OnPausing(ctx context.Context, event *fsm.Event) error {
	log.Infow("Preparation phase pausing", "job_id", pfsm.job.JobID)

	// Signal all workers to pause
	if err := pfsm.pauseWorkers(ctx); err != nil {
		return fmt.Errorf("failed to pause workers: %w", err)
	}

	// Transition to paused state once all workers are paused
	return pfsm.fsm.Event(ctx, known.MessageBatchEventPreparePaused)
}

// OnPaused handles the paused state
func (pfsm *PreparationFSM) OnPaused(ctx context.Context, event *fsm.Event) error {
	log.Infow("Preparation phase paused", "job_id", pfsm.job.JobID)

	// Save current statistics
	if err := pfsm.saveStatistics(ctx); err != nil {
		log.Errorw("Failed to save statistics", "job_id", pfsm.job.JobID, "error", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationPaused)
	pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)

	return nil
}

// OnCompleted handles the completed state
func (pfsm *PreparationFSM) OnCompleted(ctx context.Context, event *fsm.Event) error {
	log.Infow("Preparation phase completed", "job_id", pfsm.job.JobID)

	// Finalize statistics
	endTime := time.Now()
	pfsm.statistics.EndTime = &endTime
	duration := endTime.Sub(pfsm.statistics.StartTime).Seconds()
	durationInt := int64(duration)
	pfsm.statistics.Duration = &durationInt

	// Save final results
	if err := pfsm.saveResults(ctx); err != nil {
		return fmt.Errorf("failed to save results: %w", err)
	}

	cond := jobconditionsutil.TrueCondition(known.MessageBatchPreparationCompleted)
	pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)

	return nil
}

// OnFailed handles the failed state
func (pfsm *PreparationFSM) OnFailed(ctx context.Context, event *fsm.Event) error {
	log.Errorw("Preparation phase failed", "job_id", pfsm.job.JobID, "retry_count", pfsm.retryCount)

	pfsm.retryCount++
	pfsm.statistics.RetryCount = pfsm.retryCount

	// Check if we should retry
	if pfsm.canRetry() {
		log.Infow("Retrying preparation phase", "job_id", pfsm.job.JobID, "retry_count", pfsm.retryCount)

		// Add delay before retry
		time.Sleep(time.Duration(pfsm.retryCount) * time.Second)

		return pfsm.fsm.Event(ctx, known.MessageBatchEventPrepareRetry)
	}

	// Mark as permanently failed
	endTime := time.Now()
	pfsm.statistics.EndTime = &endTime
	pfsm.job.EndedAt = endTime

	cond := jobconditionsutil.FalseCondition(known.MessageBatchPreparationFailed, "Preparation failed after max retries")
	pfsm.job.Conditions = jobconditionsutil.Set(pfsm.job.Conditions, cond)

	return nil
}

// initializePreparation initializes the preparation phase
func (pfsm *PreparationFSM) initializePreparation(ctx context.Context) error {
	log.Infow("Initializing preparation phase", "job_id", pfsm.job.JobID)

	// Initialize job results if needed
	if pfsm.job.Results == nil {
		pfsm.job.Results = &model.JobResults{}
	}
	if pfsm.job.Results.MessageBatch == nil {
		pfsm.job.Results.MessageBatch = &v1.MessageBatchResults{}
	}

	// Initialize statistics
	pfsm.statistics.StartTime = time.Now()
	// Note: This assumes Recipients is available in job params, needs to be defined
	if pfsm.job.Params != nil && pfsm.job.Params.MessageBatch != nil {
		pfsm.statistics.Total = int64(len(pfsm.job.Params.MessageBatch.Recipients))
	}

	return nil
}

// executePreparation executes the main preparation logic
func (pfsm *PreparationFSM) executePreparation(ctx context.Context) error {
	log.Infow("Executing preparation phase", "job_id", pfsm.job.JobID)

	// Simple preparation logic - simulate data processing
	// In a real implementation, this would:
	// 1. Read message batch configuration from job params
	// 2. Validate and prepare message data
	// 3. Split data into partitions
	// 4. Store prepared data for delivery phase

	// Simulate processing
	startTime := time.Now()

	// Update statistics
	pfsm.statistics.Total = 10000 // Simulate 10k messages
	pfsm.statistics.Processed = 0

	// Simulate batch processing in chunks
	batchSize := known.MessageBatchDefaultBatchSize
	for processed := int64(0); processed < pfsm.statistics.Total; processed += int64(batchSize) {
		// Simulate processing delay
		time.Sleep(100 * time.Millisecond)

		// Update progress
		remaining := pfsm.statistics.Total - processed
		if remaining < int64(batchSize) {
			batchSize = int(remaining)
		}

		pfsm.statistics.Processed += int64(batchSize)
		pfsm.statistics.Success += int64(batchSize)

		// Calculate percentage
		pfsm.statistics.Percent = float32(pfsm.statistics.Processed) / float32(pfsm.statistics.Total) * 100

		log.Infow("Preparation progress",
			"job_id", pfsm.job.JobID,
			"processed", pfsm.statistics.Processed,
			"total", pfsm.statistics.Total,
			"percent", pfsm.statistics.Percent,
		)

		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Finalize statistics
	endTime := time.Now()
	pfsm.statistics.EndTime = &endTime
	duration := int64(endTime.Sub(startTime) / time.Second)
	pfsm.statistics.Duration = &duration

	log.Infow("Preparation phase execution completed",
		"job_id", pfsm.job.JobID,
		"duration", duration,
		"processed", pfsm.statistics.Processed,
	)

	return nil
}

// isPreparationComplete checks if preparation is complete
func (pfsm *PreparationFSM) isPreparationComplete(ctx context.Context) bool {
	// Simple check: if processed count equals total count
	return pfsm.statistics.Processed >= pfsm.statistics.Total
}

// pauseWorkers pauses all preparation workers
func (pfsm *PreparationFSM) pauseWorkers(ctx context.Context) error {
	log.Infow("Pausing preparation workers", "job_id", pfsm.job.JobID)
	// Implementation would signal workers to pause
	// This is a placeholder for the actual implementation
	return nil
}

// saveStatistics saves current statistics
func (pfsm *PreparationFSM) saveStatistics(ctx context.Context) error {
	// Update job results with current statistics
	if pfsm.job.Results != nil && pfsm.job.Results.MessageBatch != nil {
		results := pfsm.job.Results.MessageBatch

		// Create temporary variables for pointer fields
		startTime := pfsm.statistics.StartTime.Unix()
		retryCount := int64(pfsm.statistics.RetryCount)
		partitions := int64(pfsm.statistics.Partitions)
		currentPhase := "PREPARATION"

		results.PreparationStats = &v1.MessageBatchPhaseStats{
			Total:      &pfsm.statistics.Total,
			Processed:  &pfsm.statistics.Processed,
			Success:    &pfsm.statistics.Success,
			Failed:     &pfsm.statistics.Failed,
			Percent:    &pfsm.statistics.Percent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if pfsm.statistics.EndTime != nil {
			endTime := pfsm.statistics.EndTime.Unix()
			results.PreparationStats.EndTime = &endTime
		}
		if pfsm.statistics.Duration != nil {
			results.PreparationStats.DurationSeconds = pfsm.statistics.Duration
		}

		// Update overall batch status
		results.CurrentPhase = &currentPhase
		results.CurrentState = &pfsm.job.Status
		batchRetryCount := int64(pfsm.retryCount)
		results.RetryCount = &batchRetryCount
	}

	return nil
}

// saveResults saves final preparation results
func (pfsm *PreparationFSM) saveResults(ctx context.Context) error {
	log.Infow("Saving preparation results", "job_id", pfsm.job.JobID)

	// Save final statistics
	if err := pfsm.saveStatistics(ctx); err != nil {
		return err
	}

	// Additional result saving logic can be added here

	return nil
}

// canRetry checks if we can retry the preparation
func (pfsm *PreparationFSM) canRetry() bool {
	return pfsm.retryCount < known.MessageBatchMaxRetries
}

// isTimeout checks if the preparation has timed out
func (pfsm *PreparationFSM) isTimeout() bool {
	timeout := time.Duration(known.MessageBatchPreparationTimeout) * time.Second
	return time.Since(pfsm.startTime) > timeout
}

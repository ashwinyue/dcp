package messagebatch

import (
	"context"
	"sync"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// StateMachine represents the state machine for SMS batch processing
type StateMachine struct {
	watcher    *Watcher
	job        *model.JobM
	fsm        *fsm.FSM
	statistics map[string]*PhaseStatistics
	retryCount int
	startTime  time.Time
	mu         sync.RWMutex
	logger     log.Logger
}

// NewStateMachine creates a new state machine
func NewStateMachine(initial string, watcher *Watcher, job *model.JobM) *StateMachine {
	usm := &StateMachine{
		watcher: watcher,
		job:     job,
		statistics: map[string]*PhaseStatistics{
			"PREPARATION": {
				StartTime:  time.Now(),
				RetryCount: 0,
			},
			"DELIVERY": {
				StartTime:  time.Now(),
				RetryCount: 0,
				Partitions: known.MessageBatchPartitionCount,
			},
		},
		retryCount: 0,
		startTime:  time.Now(),
		logger:     log.New(nil),
	}

	// Define all possible state transitions
	usm.fsm = fsm.NewFSM(
		initial,
		fsm.Events{
			// Preparation phase transitions
			{Name: known.MessageBatchEventPrepareStart, Src: []string{known.MessageBatchPending}, Dst: known.MessageBatchPreparationReady},
			{Name: known.MessageBatchEventPrepareBegin, Src: []string{known.MessageBatchPreparationReady, known.MessageBatchPreparationPaused}, Dst: known.MessageBatchPreparationRunning},
			{Name: known.MessageBatchEventPreparePause, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationPausing},
			{Name: known.MessageBatchEventPreparePaused, Src: []string{known.MessageBatchPreparationPausing}, Dst: known.MessageBatchPreparationPaused},
			{Name: known.MessageBatchEventPrepareResume, Src: []string{known.MessageBatchPreparationPaused}, Dst: known.MessageBatchPreparationReady},
			{Name: known.MessageBatchEventPrepareComplete, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationCompleted},
			{Name: known.MessageBatchEventPrepareFail, Src: []string{known.MessageBatchPreparationRunning}, Dst: known.MessageBatchPreparationFailed},
			{Name: known.MessageBatchEventPrepareRetry, Src: []string{known.MessageBatchPreparationFailed}, Dst: known.MessageBatchPreparationReady},

			// Delivery phase transitions
			{Name: known.MessageBatchEventDeliveryStart, Src: []string{known.MessageBatchPreparationCompleted}, Dst: known.MessageBatchDeliveryReady},
			{Name: known.MessageBatchEventDeliveryBegin, Src: []string{known.MessageBatchDeliveryReady, known.MessageBatchDeliveryPaused}, Dst: known.MessageBatchDeliveryRunning},
			{Name: known.MessageBatchEventDeliveryPause, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryPausing},
			{Name: known.MessageBatchEventDeliveryPaused, Src: []string{known.MessageBatchDeliveryPausing}, Dst: known.MessageBatchDeliveryPaused},
			{Name: known.MessageBatchEventDeliveryResume, Src: []string{known.MessageBatchDeliveryPaused}, Dst: known.MessageBatchDeliveryReady},
			{Name: known.MessageBatchEventDeliveryComplete, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryCompleted},
			{Name: known.MessageBatchEventDeliveryFail, Src: []string{known.MessageBatchDeliveryRunning}, Dst: known.MessageBatchDeliveryFailed},
			{Name: known.MessageBatchEventDeliveryRetry, Src: []string{known.MessageBatchDeliveryFailed}, Dst: known.MessageBatchDeliveryReady},

			// Final state transitions
			{Name: known.MessageBatchEventComplete, Src: []string{known.MessageBatchDeliveryCompleted}, Dst: known.MessageBatchSucceeded},
			{Name: known.MessageBatchEventFail, Src: []string{known.MessageBatchPreparationFailed, known.MessageBatchDeliveryFailed}, Dst: known.MessageBatchFailed},
			{Name: known.MessageBatchEventCancel, Src: []string{"*"}, Dst: known.MessageBatchCancelled},
		},
		fsm.Callbacks{
			"enter_state": fsmutil.WrapEvent(usm.EnterState),

			// Preparation phase callbacks
			"enter_" + known.MessageBatchPreparationReady:     fsmutil.WrapEvent(usm.OnPreparationReady),
			"enter_" + known.MessageBatchPreparationRunning:   fsmutil.WrapEvent(usm.OnPreparationRunning),
			"enter_" + known.MessageBatchPreparationPausing:   fsmutil.WrapEvent(usm.OnPreparationPausing),
			"enter_" + known.MessageBatchPreparationPaused:    fsmutil.WrapEvent(usm.OnPreparationPaused),
			"enter_" + known.MessageBatchPreparationCompleted: fsmutil.WrapEvent(usm.OnPreparationCompleted),
			"enter_" + known.MessageBatchPreparationFailed:    fsmutil.WrapEvent(usm.OnPreparationFailed),

			// Delivery phase callbacks
			"enter_" + known.MessageBatchDeliveryReady:     fsmutil.WrapEvent(usm.OnDeliveryReady),
			"enter_" + known.MessageBatchDeliveryRunning:   fsmutil.WrapEvent(usm.OnDeliveryRunning),
			"enter_" + known.MessageBatchDeliveryPausing:   fsmutil.WrapEvent(usm.OnDeliveryPausing),
			"enter_" + known.MessageBatchDeliveryPaused:    fsmutil.WrapEvent(usm.OnDeliveryPaused),
			"enter_" + known.MessageBatchDeliveryCompleted: fsmutil.WrapEvent(usm.OnDeliveryCompleted),
			"enter_" + known.MessageBatchDeliveryFailed:    fsmutil.WrapEvent(usm.OnDeliveryFailed),

			// Final state callbacks
			"enter_" + known.MessageBatchSucceeded: fsmutil.WrapEvent(usm.OnSucceeded),
			"enter_" + known.MessageBatchFailed:    fsmutil.WrapEvent(usm.OnFailed),
			"enter_" + known.MessageBatchCancelled: fsmutil.WrapEvent(usm.OnCancelled),
		},
	)

	return usm
}

// StateManager interface implementation
func (usm *StateMachine) GetCurrentState() string {
	return usm.fsm.Current()
}

func (usm *StateMachine) CanTransition(event string) bool {
	return usm.fsm.Can(event)
}

func (usm *StateMachine) Transition(ctx context.Context, event string) error {
	return usm.fsm.Event(ctx, event)
}

func (usm *StateMachine) GetValidEvents() []string {
	return usm.fsm.AvailableTransitions()
}

// GetStatistics returns statistics for a specific phase
func (usm *StateMachine) GetStatistics(phase string) *PhaseStatistics {
	usm.mu.RLock()
	defer usm.mu.RUnlock()
	return usm.statistics[phase]
}

// GetAllStatistics returns all phase statistics
func (usm *StateMachine) GetAllStatistics() map[string]*PhaseStatistics {
	usm.mu.RLock()
	defer usm.mu.RUnlock()
	stats := make(map[string]*PhaseStatistics)
	for k, v := range usm.statistics {
		stats[k] = v
	}
	return stats
}

// EnterState handles state transitions and updates job status
func (usm *StateMachine) EnterState(ctx context.Context, event *fsm.Event) error {
	currentState := event.FSM.Current()

	usm.logger.Infow("Unified FSM state transition",
		"job_id", usm.job.JobID,
		"event", event.Event,
		"from", event.Src,
		"to", currentState)

	// Update job status
	usm.job.Status = currentState

	// Handle timeout check
	if usm.isTimeout() {
		usm.job.Status = known.MessageBatchFailed
		endTime := time.Now()
		usm.job.EndedAt = endTime

		cond := jobconditionsutil.FalseCondition(currentState, "Batch processing timeout")
		usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)
	}

	// Handle error cases
	if event.Err != nil {
		usm.job.Status = known.MessageBatchFailed
		endTime := time.Now()
		usm.job.EndedAt = endTime

		cond := jobconditionsutil.FalseCondition(currentState, event.Err.Error())
		usm.job.Conditions = jobconditionsutil.Set(usm.job.Conditions, cond)
	}

	// Update job in store
	if err := usm.watcher.Store.Job().Update(ctx, usm.job); err != nil {
		usm.logger.Errorw("Failed to update job", "job_id", usm.job.JobID, "error", err)
		return err
	}

	return nil
}

// isTimeout checks if the batch processing has timed out
func (usm *StateMachine) isTimeout() bool {
	timeout := time.Duration(known.MessageBatchPreparationTimeout+known.MessageBatchDeliveryTimeout) * time.Second
	return time.Since(usm.startTime) > timeout
}

// updateStatistics updates statistics for a specific phase
func (usm *StateMachine) updateStatistics(phase string, updater func(*PhaseStatistics)) {
	usm.mu.Lock()
	defer usm.mu.Unlock()
	if stats, exists := usm.statistics[phase]; exists {
		updater(stats)
	}
}

// saveStatistics saves current statistics to job results
func (usm *StateMachine) saveStatistics(ctx context.Context) error {
	// Initialize job results if needed
	if usm.job.Results == nil {
		usm.job.Results = &model.JobResults{}
	}
	if usm.job.Results.MessageBatch == nil {
		usm.job.Results.MessageBatch = &v1.MessageBatchResults{}
	}

	results := usm.job.Results.MessageBatch

	// Save preparation statistics
	if prepStats := usm.GetStatistics("PREPARATION"); prepStats != nil {
		startTime := prepStats.StartTime.Unix()
		retryCount := int64(prepStats.RetryCount)
		partitions := int64(prepStats.Partitions)

		results.PreparationStats = &v1.MessageBatchPhaseStats{
			Total:      &prepStats.Total,
			Processed:  &prepStats.Processed,
			Success:    &prepStats.Success,
			Failed:     &prepStats.Failed,
			Percent:    &prepStats.Percent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if prepStats.EndTime != nil {
			endTime := prepStats.EndTime.Unix()
			results.PreparationStats.EndTime = &endTime
		}
		if prepStats.Duration != nil {
			results.PreparationStats.DurationSeconds = prepStats.Duration
		}
	}

	// Save delivery statistics
	if deliveryStats := usm.GetStatistics("DELIVERY"); deliveryStats != nil {
		startTime := deliveryStats.StartTime.Unix()
		retryCount := int64(deliveryStats.RetryCount)
		partitions := int64(deliveryStats.Partitions)

		results.DeliveryStats = &v1.MessageBatchPhaseStats{
			Total:      &deliveryStats.Total,
			Processed:  &deliveryStats.Processed,
			Success:    &deliveryStats.Success,
			Failed:     &deliveryStats.Failed,
			Percent:    &deliveryStats.Percent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if deliveryStats.EndTime != nil {
			endTime := deliveryStats.EndTime.Unix()
			results.DeliveryStats.EndTime = &endTime
		}
		if deliveryStats.Duration != nil {
			results.DeliveryStats.DurationSeconds = deliveryStats.Duration
		}
	}

	// Update overall batch status
	currentPhase := usm.getCurrentPhase()
	currentState := usm.job.Status
	batchRetryCount := int64(usm.retryCount)

	results.CurrentPhase = &currentPhase
	results.CurrentState = &currentState
	results.RetryCount = &batchRetryCount

	return nil
}

// getCurrentPhase determines the current phase based on state
func (usm *StateMachine) getCurrentPhase() string {
	state := usm.GetCurrentState()
	switch {
	case state == known.MessageBatchPending ||
		state == known.MessageBatchPreparationReady ||
		state == known.MessageBatchPreparationRunning ||
		state == known.MessageBatchPreparationPausing ||
		state == known.MessageBatchPreparationPaused ||
		state == known.MessageBatchPreparationCompleted ||
		state == known.MessageBatchPreparationFailed:
		return "PREPARATION"
	case state == known.MessageBatchDeliveryReady ||
		state == known.MessageBatchDeliveryRunning ||
		state == known.MessageBatchDeliveryPausing ||
		state == known.MessageBatchDeliveryPaused ||
		state == known.MessageBatchDeliveryCompleted ||
		state == known.MessageBatchDeliveryFailed:
		return "DELIVERY"
	default:
		return "UNKNOWN"
	}
}

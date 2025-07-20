package messagebatch

import (
	"context"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// Step represents the current processing step
type Step string

const (
	StepPreparation Step = "PREPARATION"
	StepDelivery    Step = "DELIVERY"
)

// State constants for each step
const (
	// Initial state
	StateInitial = "INITIAL"

	// Preparation states
	StatePreparationReady     = "PREPARATION_READY"
	StatePreparationRunning   = "PREPARATION_RUNNING"
	StatePreparationPausing   = "PREPARATION_PAUSING"
	StatePreparationPaused    = "PREPARATION_PAUSED"
	StatePreparationCompleted = "PREPARATION_COMPLETED"
	StatePreparationFailed    = "PREPARATION_FAILED"

	// Delivery states
	StateDeliveryReady     = "DELIVERY_READY"
	StateDeliveryRunning   = "DELIVERY_RUNNING"
	StateDeliveryPausing   = "DELIVERY_PAUSING"
	StateDeliveryPaused    = "DELIVERY_PAUSED"
	StateDeliveryCompleted = "DELIVERY_COMPLETED"
	StateDeliveryFailed    = "DELIVERY_FAILED"

	// Final states
	StateSucceeded = "SUCCEEDED"
	StateFailed    = "FAILED"
	StateCancelled = "CANCELLED"
)

// Event constants
const (
	// Preparation events
	EventPrepareStart    = "PREPARE_START"
	EventPrepareBegin    = "PREPARE_BEGIN"
	EventPreparePause    = "PREPARE_PAUSE"
	EventPreparePaused   = "PREPARE_PAUSED"
	EventPrepareResume   = "PREPARE_RESUME"
	EventPrepareComplete = "PREPARE_COMPLETE"
	EventPrepareFail     = "PREPARE_FAIL"
	EventPrepareRetry    = "PREPARE_RETRY"

	// Delivery events
	EventDeliveryStart    = "DELIVERY_START"
	EventDeliveryBegin    = "DELIVERY_BEGIN"
	EventDeliveryPause    = "DELIVERY_PAUSE"
	EventDeliveryPaused   = "DELIVERY_PAUSED"
	EventDeliveryResume   = "DELIVERY_RESUME"
	EventDeliveryComplete = "DELIVERY_COMPLETE"
	EventDeliveryFail     = "DELIVERY_FAIL"
	EventDeliveryRetry    = "DELIVERY_RETRY"

	// Final events
	EventComplete = "COMPLETE"
	EventFail     = "FAIL"
	EventCancel   = "CANCEL"
)

// StateMachine represents a finite state machine for managing message batch jobs.
type StateMachine struct {
	Watcher         *Watcher
	Job             *model.JobM
	FSM             *fsm.FSM
	CurrentStep     Step
	logger          log.Logger
	retryCount      int
	callbackHandler CallbackHandler
	stats           map[string]*PhaseStatistics
}

// NewStateMachine initializes a new StateMachine with the given initial state, watcher, and job.
// It configures the FSM with defined events and their corresponding state transitions,
// as well as callbacks for entering specific states.
func NewStateMachine(initial string, watcher *Watcher, job *model.JobM) *StateMachine {
	sm := &StateMachine{
		Watcher:     watcher,
		Job:         job,
		CurrentStep: StepPreparation, // Start with preparation step
		logger:      log.New(nil),
		stats:       make(map[string]*PhaseStatistics),
	}

	// Initialize job-specific statistics
	if job.Scope == known.SMSJobScope {
		sm.stats["sms_preparation"] = &PhaseStatistics{StartTime: time.Now()}
		sm.stats["sms_delivery"] = &PhaseStatistics{}
	} else {
		sm.stats["message_preparation"] = &PhaseStatistics{StartTime: time.Now()}
		sm.stats["message_delivery"] = &PhaseStatistics{}
	}

	sm.FSM = fsm.NewFSM(
		initial,
		fsm.Events{
			// Preparation phase transitions
			{Name: EventPrepareStart, Src: []string{StateInitial}, Dst: StatePreparationReady},
			{Name: EventPrepareBegin, Src: []string{StatePreparationReady, StatePreparationPaused}, Dst: StatePreparationRunning},
			{Name: EventPreparePause, Src: []string{StatePreparationRunning}, Dst: StatePreparationPausing},
			{Name: EventPreparePaused, Src: []string{StatePreparationPausing}, Dst: StatePreparationPaused},
			{Name: EventPrepareResume, Src: []string{StatePreparationPaused}, Dst: StatePreparationReady},
			{Name: EventPrepareComplete, Src: []string{StatePreparationRunning}, Dst: StatePreparationCompleted},
			{Name: EventPrepareFail, Src: []string{StatePreparationRunning}, Dst: StatePreparationFailed},
			{Name: EventPrepareRetry, Src: []string{StatePreparationFailed}, Dst: StatePreparationReady},

			// Delivery phase transitions
			{Name: EventDeliveryStart, Src: []string{StatePreparationCompleted}, Dst: StateDeliveryReady},
			{Name: EventDeliveryBegin, Src: []string{StateDeliveryReady, StateDeliveryPaused}, Dst: StateDeliveryRunning},
			{Name: EventDeliveryPause, Src: []string{StateDeliveryRunning}, Dst: StateDeliveryPausing},
			{Name: EventDeliveryPaused, Src: []string{StateDeliveryPausing}, Dst: StateDeliveryPaused},
			{Name: EventDeliveryResume, Src: []string{StateDeliveryPaused}, Dst: StateDeliveryReady},
			{Name: EventDeliveryComplete, Src: []string{StateDeliveryRunning}, Dst: StateDeliveryCompleted},
			{Name: EventDeliveryFail, Src: []string{StateDeliveryRunning}, Dst: StateDeliveryFailed},
			{Name: EventDeliveryRetry, Src: []string{StateDeliveryFailed}, Dst: StateDeliveryReady},

			// Final state transitions
			{Name: EventComplete, Src: []string{StateDeliveryCompleted}, Dst: StateSucceeded},
			{Name: EventFail, Src: []string{StatePreparationFailed, StateDeliveryFailed}, Dst: StateFailed},
			{Name: EventCancel, Src: []string{"*"}, Dst: StateCancelled},
		},
		fsm.Callbacks{
			// enter_state 先于 enter_xxx 执行。
			"enter_state": fsmutil.WrapEvent(sm.EnterState),

			// Preparation state callbacks
			"enter_" + StatePreparationReady:     fsmutil.WrapEvent(sm.EnterPreparationReady),
			"enter_" + StatePreparationRunning:   fsmutil.WrapEvent(sm.StartPreparation),
			"enter_" + StatePreparationCompleted: fsmutil.WrapEvent(sm.CompletePreparation),
			"enter_" + StatePreparationFailed:    fsmutil.WrapEvent(sm.FailPreparation),

			// Delivery state callbacks
			"enter_" + StateDeliveryReady:     fsmutil.WrapEvent(sm.EnterDeliveryReady),
			"enter_" + StateDeliveryRunning:   fsmutil.WrapEvent(sm.StartDelivery),
			"enter_" + StateDeliveryCompleted: fsmutil.WrapEvent(sm.CompleteDelivery),
			"enter_" + StateDeliveryFailed:    fsmutil.WrapEvent(sm.FailDelivery),

			// Final state callbacks
			"enter_" + StateSucceeded: fsmutil.WrapEvent(sm.Complete),
			"enter_" + StateFailed:    fsmutil.WrapEvent(sm.Fail),
		},
	)

	return sm
}

// StateManager interface implementation
func (usm *StateMachine) GetCurrentState() string {
	return usm.FSM.Current()
}

func (usm *StateMachine) CanTransition(event string) bool {
	return usm.FSM.Can(event)
}

func (usm *StateMachine) Transition(ctx context.Context, event string) error {
	return usm.FSM.Event(ctx, event)
}

// TriggerEvent triggers an event on the state machine
func (usm *StateMachine) TriggerEvent(event string) error {
	oldState := usm.FSM.Current()
	err := usm.FSM.Event(context.Background(), event)
	if err != nil {
		return err
	}

	// Trigger status change callback
	newState := usm.FSM.Current()
	if oldState != newState {
		if usm.Watcher != nil {
			// Handle status change through watcher
		}
	}

	return nil
}

func (usm *StateMachine) GetValidEvents() []string {
	return usm.FSM.AvailableTransitions()
}

// GetStatistics returns statistics for a specific phase
func (usm *StateMachine) GetStatistics(phase string) *PhaseStatistics {
	// TODO: Implement statistics tracking
	return nil
}

// GetAllStatistics returns all phase statistics
func (usm *StateMachine) GetAllStatistics() map[string]*PhaseStatistics {
	// TODO: Implement statistics tracking
	return make(map[string]*PhaseStatistics)
}

// isTimeout checks if the batch processing has timed out
func (usm *StateMachine) isTimeout() bool {
	timeout := time.Duration(3600+3600) * time.Second // 1 hour for each phase
	if usm.Job.CreatedAt.IsZero() {
		return false
	}
	return time.Since(usm.Job.CreatedAt) > timeout
}

// Stop stops the state machine
func (usm *StateMachine) Stop() {
	// Implementation for stopping the state machine
	// TODO: Add proper logging
}

// updateStatistics updates statistics for a specific phase
func (usm *StateMachine) updateStatistics(phase string, updater func(*PhaseStatistics)) {
	// TODO: Implement statistics tracking
}

// saveStatistics saves current statistics to job results
func (usm *StateMachine) saveStatistics(ctx context.Context) error {
	// Initialize job results if needed
	if usm.Job.Results == nil {
		usm.Job.Results = &model.JobResults{}
	}

	results := (*v1.JobResults)(usm.Job.Results)

	// Handle different job scopes
	if usm.Job.Scope == known.SMSJobScope {
		// Initialize SmsBatch if needed
		if results.SmsBatch == nil {
			results.SmsBatch = &v1.SmsBatchResults{}
		}
		return usm.saveSMSStatistics(ctx, results)
	} else {
		// Initialize MessageBatch if needed
		if results.MessageBatch == nil {
			results.MessageBatch = &v1.MessageBatchResults{}
		}
		return usm.saveMessageStatistics(ctx, results)
	}
}

// saveMessageStatistics saves message batch statistics
func (usm *StateMachine) saveMessageStatistics(ctx context.Context, results *v1.JobResults) error {
	// Save preparation statistics
	if prepStats := usm.GetStatistics("PREPARATION"); prepStats != nil {
		startTime := prepStats.StartTime.Unix()
		retryCount := int64(prepStats.RetryCount)
		partitions := int64(prepStats.Partitions)

		percent := float32(prepStats.Percent)
		results.MessageBatch.PreparationStats = &v1.MessageBatchPhaseStats{
			Total:      &prepStats.Total,
			Processed:  &prepStats.Processed,
			Success:    &prepStats.Success,
			Failed:     &prepStats.Failed,
			Percent:    &percent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if prepStats.EndTime != nil {
			endTime := prepStats.EndTime.Unix()
			results.MessageBatch.PreparationStats.EndTime = &endTime
		}
		if prepStats.Duration != nil {
			durationSeconds := int64(*prepStats.Duration)
			results.MessageBatch.PreparationStats.DurationSeconds = &durationSeconds
		}
	}

	// Save delivery statistics
	if deliveryStats := usm.GetStatistics("DELIVERY"); deliveryStats != nil {
		startTime := deliveryStats.StartTime.Unix()
		retryCount := int64(deliveryStats.RetryCount)
		partitions := int64(deliveryStats.Partitions)

		deliveryPercent := float32(deliveryStats.Percent)
		results.MessageBatch.DeliveryStats = &v1.MessageBatchPhaseStats{
			Total:      &deliveryStats.Total,
			Processed:  &deliveryStats.Processed,
			Success:    &deliveryStats.Success,
			Failed:     &deliveryStats.Failed,
			Percent:    &deliveryPercent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if deliveryStats.EndTime != nil {
			endTime := deliveryStats.EndTime.Unix()
			results.MessageBatch.DeliveryStats.EndTime = &endTime
		}
		if deliveryStats.Duration != nil {
			deliveryDurationSeconds := int64(*deliveryStats.Duration)
			results.MessageBatch.DeliveryStats.DurationSeconds = &deliveryDurationSeconds
		}
	}

	// Update overall batch status
	currentPhase := usm.getCurrentPhase()
	currentState := usm.Job.Status
	batchRetryCount := int64(usm.retryCount)

	results.MessageBatch.CurrentPhase = &currentPhase
	results.MessageBatch.CurrentState = &currentState
	results.MessageBatch.RetryCount = &batchRetryCount

	return nil
}

// saveSMSStatistics saves SMS batch statistics
func (usm *StateMachine) saveSMSStatistics(ctx context.Context, results *v1.JobResults) error {
	// Save preparation statistics
	if prepStats := usm.GetStatistics("PREPARATION"); prepStats != nil {
		startTime := prepStats.StartTime.Unix()
		retryCount := int64(prepStats.RetryCount)
		partitions := int64(prepStats.Partitions)

		percent := float32(prepStats.Percent)
		results.SmsBatch.PreparationStats = &v1.SmsBatchPhaseStats{
			Total:      &prepStats.Total,
			Processed:  &prepStats.Processed,
			Success:    &prepStats.Success,
			Failed:     &prepStats.Failed,
			Percent:    &percent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if prepStats.EndTime != nil {
			endTime := prepStats.EndTime.Unix()
			results.SmsBatch.PreparationStats.EndTime = &endTime
		}
		if prepStats.Duration != nil {
			durationSeconds := int64(*prepStats.Duration)
			results.SmsBatch.PreparationStats.DurationSeconds = &durationSeconds
		}
	}

	// Save delivery statistics
	if deliveryStats := usm.GetStatistics("DELIVERY"); deliveryStats != nil {
		startTime := deliveryStats.StartTime.Unix()
		retryCount := int64(deliveryStats.RetryCount)
		partitions := int64(deliveryStats.Partitions)

		deliveryPercent := float32(deliveryStats.Percent)
		results.SmsBatch.DeliveryStats = &v1.SmsBatchPhaseStats{
			Total:      &deliveryStats.Total,
			Processed:  &deliveryStats.Processed,
			Success:    &deliveryStats.Success,
			Failed:     &deliveryStats.Failed,
			Percent:    &deliveryPercent,
			StartTime:  &startTime,
			RetryCount: &retryCount,
			Partitions: &partitions,
		}

		if deliveryStats.EndTime != nil {
			endTime := deliveryStats.EndTime.Unix()
			results.SmsBatch.DeliveryStats.EndTime = &endTime
		}
		if deliveryStats.Duration != nil {
			deliveryDurationSeconds := int64(*deliveryStats.Duration)
			results.SmsBatch.DeliveryStats.DurationSeconds = &deliveryDurationSeconds
		}
	}

	// Update overall batch status
	currentPhase := usm.getCurrentPhase()
	currentState := usm.Job.Status
	batchRetryCount := int64(usm.retryCount)

	results.SmsBatch.CurrentPhase = &currentPhase
	results.SmsBatch.CurrentState = &currentState
	results.SmsBatch.RetryCount = &batchRetryCount

	return nil
}

// getCurrentPhase determines the current phase based on state
func (usm *StateMachine) getCurrentPhase() string {
	state := usm.GetCurrentState()
	switch {
	case state == StateInitial ||
		state == StatePreparationReady ||
		state == StatePreparationRunning ||
		state == StatePreparationPausing ||
		state == StatePreparationPaused ||
		state == StatePreparationCompleted ||
		state == StatePreparationFailed:
		return "PREPARATION"
	case state == StateDeliveryReady ||
		state == StateDeliveryRunning ||
		state == StateDeliveryPausing ||
		state == StateDeliveryPaused ||
		state == StateDeliveryCompleted ||
		state == StateDeliveryFailed:
		return "DELIVERY"
	default:
		return "UNKNOWN"
	}
}

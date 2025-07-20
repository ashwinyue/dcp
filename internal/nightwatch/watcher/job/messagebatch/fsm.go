package messagebatch

import (
	"context"
	"sync"
	"time"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// State constants
const (
	// Initial states
	Pending = "PENDING"
	
	// Preparation states
	PreparationReady     = "PREPARATION_READY"
	PreparationRunning   = "PREPARATION_RUNNING"
	PreparationPausing   = "PREPARATION_PAUSING"
	PreparationPaused    = "PREPARATION_PAUSED"
	PreparationCompleted = "PREPARATION_COMPLETED"
	PreparationFailed    = "PREPARATION_FAILED"
	
	// Delivery states
	DeliveryReady     = "DELIVERY_READY"
	DeliveryRunning   = "DELIVERY_RUNNING"
	DeliveryPausing   = "DELIVERY_PAUSING"
	DeliveryPaused    = "DELIVERY_PAUSED"
	DeliveryCompleted = "DELIVERY_COMPLETED"
	DeliveryFailed    = "DELIVERY_FAILED"
	
	// Final states
	Succeeded = "SUCCEEDED"
	Failed    = "FAILED"
	Cancelled = "CANCELLED"
)

// Event constants
const (
	// Preparation events
	PrepareStart    = "PREPARE_START"
	PrepareBegin    = "PREPARE_BEGIN"
	PreparePause    = "PREPARE_PAUSE"
	PreparePaused   = "PREPARE_PAUSED"
	PrepareResume   = "PREPARE_RESUME"
	PrepareComplete = "PREPARE_COMPLETE"
	PrepareFail     = "PREPARE_FAIL"
	PrepareRetry    = "PREPARE_RETRY"
	
	// Delivery events
	DeliveryStart    = "DELIVERY_START"
	DeliveryBegin    = "DELIVERY_BEGIN"
	DeliveryPause    = "DELIVERY_PAUSE"
	DeliveryPaused   = "DELIVERY_PAUSED"
	DeliveryResume   = "DELIVERY_RESUME"
	DeliveryComplete = "DELIVERY_COMPLETE"
	DeliveryFail     = "DELIVERY_FAIL"
	DeliveryRetry    = "DELIVERY_RETRY"
	
	// Final events
	Complete = "COMPLETE"
	Fail     = "FAIL"
	Cancel   = "CANCEL"
)

// StateMachine represents the state machine for message batch processing
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
				Partitions: 10, // Default partition count
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
			{Name: PrepareStart, Src: []string{Pending}, Dst: PreparationReady},
			{Name: PrepareBegin, Src: []string{PreparationReady, PreparationPaused}, Dst: PreparationRunning},
			{Name: PreparePause, Src: []string{PreparationRunning}, Dst: PreparationPausing},
			{Name: PreparePaused, Src: []string{PreparationPausing}, Dst: PreparationPaused},
			{Name: PrepareResume, Src: []string{PreparationPaused}, Dst: PreparationReady},
			{Name: PrepareComplete, Src: []string{PreparationRunning}, Dst: PreparationCompleted},
			{Name: PrepareFail, Src: []string{PreparationRunning}, Dst: PreparationFailed},
			{Name: PrepareRetry, Src: []string{PreparationFailed}, Dst: PreparationReady},

			// Delivery phase transitions
			{Name: DeliveryStart, Src: []string{PreparationCompleted}, Dst: DeliveryReady},
			{Name: DeliveryBegin, Src: []string{DeliveryReady, DeliveryPaused}, Dst: DeliveryRunning},
			{Name: DeliveryPause, Src: []string{DeliveryRunning}, Dst: DeliveryPausing},
			{Name: DeliveryPaused, Src: []string{DeliveryPausing}, Dst: DeliveryPaused},
			{Name: DeliveryResume, Src: []string{DeliveryPaused}, Dst: DeliveryReady},
			{Name: DeliveryComplete, Src: []string{DeliveryRunning}, Dst: DeliveryCompleted},
			{Name: DeliveryFail, Src: []string{DeliveryRunning}, Dst: DeliveryFailed},
			{Name: DeliveryRetry, Src: []string{DeliveryFailed}, Dst: DeliveryReady},

			// Final state transitions
			{Name: Complete, Src: []string{DeliveryCompleted}, Dst: Succeeded},
			{Name: Fail, Src: []string{PreparationFailed, DeliveryFailed}, Dst: Failed},
			{Name: Cancel, Src: []string{"*"}, Dst: Cancelled},
		},
		fsm.Callbacks{
			"enter_state": usm.EnterState,

			// Preparation phase callbacks
			"enter_" + PreparationReady:     usm.OnPreparationReady,
			"enter_" + PreparationRunning:   usm.OnPreparationRunning,
			"enter_" + PreparationPausing:   usm.OnPreparationPausing,
			"enter_" + PreparationPaused:    usm.OnPreparationPaused,
			"enter_" + PreparationCompleted: usm.OnPreparationCompleted,
			"enter_" + PreparationFailed:    usm.OnPreparationFailed,

			// Delivery phase callbacks
			"enter_" + DeliveryReady:     usm.OnDeliveryReady,
			"enter_" + DeliveryRunning:   usm.OnDeliveryRunning,
			"enter_" + DeliveryPausing:   usm.OnDeliveryPausing,
			"enter_" + DeliveryPaused:    usm.OnDeliveryPaused,
			"enter_" + DeliveryCompleted: usm.OnDeliveryCompleted,
			"enter_" + DeliveryFailed:    usm.OnDeliveryFailed,

			// Final state callbacks
			"enter_" + Succeeded: usm.OnSucceeded,
			"enter_" + Failed:    usm.OnFailed,
			"enter_" + Cancelled: usm.OnCancelled,
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

// TriggerEvent triggers an event on the state machine
func (usm *StateMachine) TriggerEvent(event string) error {
	return usm.fsm.Event(context.Background(), event)
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
func (usm *StateMachine) EnterState(event *fsm.Event) {
	currentState := event.FSM.Current()

	usm.logger.Infow("FSM state transition",
		"job_id", usm.job.JobID,
		"event", event.Event,
		"from", event.Src,
		"to", currentState)

	// Update job status
	usm.job.Status = currentState

	// Handle timeout check
	if usm.isTimeout() {
		usm.job.Status = Failed
		endTime := time.Now()
		usm.job.EndedAt = endTime
	}

	// Handle error cases
	if event.Err != nil {
		usm.job.Status = Failed
		endTime := time.Now()
		usm.job.EndedAt = endTime
	}

	// Update job in store
	ctx := context.Background()
	if err := usm.watcher.store.Job().Update(ctx, usm.job); err != nil {
		usm.logger.Errorw("Failed to update job", "job_id", usm.job.JobID, "error", err)
	}
}

// isTimeout checks if the batch processing has timed out
func (usm *StateMachine) isTimeout() bool {
	timeout := time.Duration(3600+3600) * time.Second // 1 hour for each phase
	return time.Since(usm.startTime) > timeout
}

// Stop stops the state machine
func (usm *StateMachine) Stop() {
	// Implementation for stopping the state machine
	usm.logger.Infow("Stopping state machine", "job_id", usm.job.JobID)
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

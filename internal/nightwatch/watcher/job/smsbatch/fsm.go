package smsbatch

import (
	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
)

// StateMachine represents a finite state machine for managing SMS batch processing jobs.
type StateMachine struct {
	Watcher *Watcher
	Job     *model.JobM
	FSM     *fsm.FSM
}

// NewStateMachine initializes a new StateMachine with the given initial state, watcher, and job.
// It configures the FSM with defined events and their corresponding state transitions,
// as well as callbacks for entering specific states.
func NewStateMachine(initial string, watcher *Watcher, job *model.JobM) *StateMachine {
	sm := &StateMachine{Watcher: watcher, Job: job}

	sm.FSM = fsm.NewFSM(
		initial,
		fsm.Events{
			// Define state transitions for the SMS batch process.
			{Name: SmsBatchInitial, Src: []string{SmsBatchInitial}, Dst: SmsBatchPreparationReady},
			{Name: SmsBatchPreparationReady, Src: []string{SmsBatchPreparationReady}, Dst: SmsBatchPreparationRunning},
			{Name: SmsBatchPreparationRunning, Src: []string{SmsBatchPreparationRunning}, Dst: SmsBatchPreparationCompleted},
			{Name: SmsBatchPreparationCompleted, Src: []string{SmsBatchPreparationCompleted}, Dst: SmsBatchDeliveryReady},
			{Name: SmsBatchDeliveryReady, Src: []string{SmsBatchDeliveryReady}, Dst: SmsBatchDeliveryRunning},
			{Name: SmsBatchDeliveryRunning, Src: []string{SmsBatchDeliveryRunning}, Dst: SmsBatchDeliveryCompleted},
			{Name: SmsBatchDeliveryCompleted, Src: []string{SmsBatchDeliveryCompleted}, Dst: SmsBatchSucceeded},
			
			// Pause and resume transitions
			{Name: SmsBatchPausePreparation, Src: []string{SmsBatchPreparationRunning}, Dst: SmsBatchPreparationPaused},
			{Name: SmsBatchResumePreparation, Src: []string{SmsBatchPreparationPaused}, Dst: SmsBatchPreparationRunning},
			{Name: SmsBatchPauseDelivery, Src: []string{SmsBatchDeliveryRunning}, Dst: SmsBatchDeliveryPaused},
			{Name: SmsBatchResumeDelivery, Src: []string{SmsBatchDeliveryPaused}, Dst: SmsBatchDeliveryRunning},
			
			// Retry transitions
			{Name: SmsBatchRetryPreparation, Src: []string{SmsBatchPreparationCompleted}, Dst: SmsBatchPreparationReady},
			{Name: SmsBatchRetryDelivery, Src: []string{SmsBatchDeliveryCompleted}, Dst: SmsBatchDeliveryReady},
			
			// Failure transitions
			{Name: SmsBatchPreparationFailed, Src: []string{SmsBatchPreparationRunning, SmsBatchPreparationReady}, Dst: SmsBatchFailed},
			{Name: SmsBatchDeliveryFailed, Src: []string{SmsBatchDeliveryRunning, SmsBatchDeliveryReady}, Dst: SmsBatchFailed},
			
			// Abort transition
			{Name: SmsBatchAbort, Src: []string{SmsBatchPreparationRunning, SmsBatchDeliveryRunning}, Dst: SmsBatchAborted},
		},
		fsm.Callbacks{
			// enter_state 先于 enter_xxx 执行
			"enter_state": fsmutil.WrapEvent(sm.EnterState),
			
			// Preparation phase callbacks
			"enter_" + SmsBatchPreparationCompleted: fsmutil.WrapEvent(sm.PreparationExecute),
			
			// Delivery phase callbacks
			"enter_" + SmsBatchDeliveryCompleted: fsmutil.WrapEvent(sm.DeliveryExecute),
			
			// Pause callbacks
			"enter_" + SmsBatchPreparationPaused: fsmutil.WrapEvent(sm.PreparationPause),
			"enter_" + SmsBatchDeliveryPaused:    fsmutil.WrapEvent(sm.DeliveryPause),
			
			// Resume callbacks
			"enter_" + SmsBatchPreparationRunning: fsmutil.WrapEvent(sm.PreparationResume),
			"enter_" + SmsBatchDeliveryRunning:    fsmutil.WrapEvent(sm.DeliveryResume),
		},
	)

	return sm
}
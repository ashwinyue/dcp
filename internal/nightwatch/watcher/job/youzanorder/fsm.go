package youzanorder

import (
	"context"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
)

// StateMachine represents a finite state machine for managing YouZan order jobs.
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
			// Define state transitions for the YouZan order process.
			{Name: known.YouZanOrderPending, Src: []string{known.YouZanOrderPending}, Dst: known.YouZanOrderFetching},
			{Name: known.YouZanOrderFetching, Src: []string{known.YouZanOrderFetching}, Dst: known.YouZanOrderFetched},
			{Name: known.YouZanOrderFetched, Src: []string{known.YouZanOrderFetched}, Dst: known.YouZanOrderValidating},
			{Name: known.YouZanOrderValidating, Src: []string{known.YouZanOrderValidating}, Dst: known.YouZanOrderValidated},
			{Name: known.YouZanOrderValidated, Src: []string{known.YouZanOrderValidated}, Dst: known.YouZanOrderEnriching},
			{Name: known.YouZanOrderEnriching, Src: []string{known.YouZanOrderEnriching}, Dst: known.YouZanOrderEnriched},
			{Name: known.YouZanOrderEnriched, Src: []string{known.YouZanOrderEnriched}, Dst: known.YouZanOrderProcessing},
			{Name: known.YouZanOrderProcessing, Src: []string{known.YouZanOrderProcessing}, Dst: known.YouZanOrderProcessed},
			{Name: known.YouZanOrderProcessed, Src: []string{known.YouZanOrderProcessed}, Dst: known.YouZanOrderSucceeded},
		},
		fsm.Callbacks{
			// enter_state 先于 enter_xxx 执行。此时：event=Pending, current=Fetching
			"enter_state": fsmutil.WrapEvent(sm.EnterState),
			// 此时 event = Fetching，current = Fetched
			"enter_" + known.YouZanOrderFetched:   fsmutil.WrapEvent(sm.Fetch),
			"enter_" + known.YouZanOrderValidated: fsmutil.WrapEvent(sm.Validate),
			"enter_" + known.YouZanOrderEnriched:  fsmutil.WrapEvent(sm.Enrich),
			"enter_" + known.YouZanOrderProcessed: fsmutil.WrapEvent(sm.Process),
		},
	)

	return sm
}

// EnterState is called when entering any state.
func (sm *StateMachine) EnterState(ctx context.Context, e *fsm.Event) error {
	// Update job state in the database
	return sm.Watcher.UpdateJobState(ctx, sm.Job.ID, e.Dst)
}

// Fetch handles the fetching logic when entering the fetched state.
func (sm *StateMachine) Fetch(ctx context.Context, e *fsm.Event) error {
	// Implement actual fetching logic here
	return sm.Watcher.FetchOrder(ctx, sm.Job)
}

// Validate handles the validation logic when entering the validated state.
func (sm *StateMachine) Validate(ctx context.Context, e *fsm.Event) error {
	// Implement actual validation logic here
	return sm.Watcher.ValidateOrder(ctx, sm.Job)
}

// Enrich handles the enrichment logic when entering the enriched state.
func (sm *StateMachine) Enrich(ctx context.Context, e *fsm.Event) error {
	// Implement actual enrichment logic here
	return sm.Watcher.EnrichOrder(ctx, sm.Job)
}

// Process handles the processing logic when entering the processed state.
func (sm *StateMachine) Process(ctx context.Context, e *fsm.Event) error {
	// Implement actual processing logic here
	return sm.Watcher.ProcessOrder(ctx, sm.Job)
}

// Current returns the current state of the state machine.
func (sm *StateMachine) Current() string {
	return sm.FSM.Current()
}

// CanTransition checks if a transition is possible from the current state.
func (sm *StateMachine) CanTransition(event string) bool {
	return sm.FSM.Can(event)
}

// Event triggers a state transition.
func (sm *StateMachine) Event(ctx context.Context, event string) error {
	return sm.FSM.Event(ctx, event)
}

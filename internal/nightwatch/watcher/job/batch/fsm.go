package batch

import (
	"context"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	fsmutil "github.com/ashwinyue/dcp/internal/pkg/util/fsm"
)

// StateMachine represents a finite state machine for managing data layer transformations.
type StateMachine struct {
	Processor *DataLayerProcessor
	Job       *model.JobM
	FSM       *fsm.FSM
}

// NewStateMachine initializes a new StateMachine with the given processor.
// It configures the FSM with defined events and their corresponding state transitions,
// as well as callbacks for entering specific states.
func NewStateMachine(processor *DataLayerProcessor) *StateMachine {
	sm := &StateMachine{
		Processor: processor,
		Job:       processor.GetJob(),
	}

	sm.FSM = fsm.NewFSM(
		known.DataLayerPending,
		fsm.Events{
			// Define state transitions for the data layer processing pipeline.
			{Name: known.DataLayerEventStart, Src: []string{known.DataLayerPending}, Dst: known.DataLayerLandingToODS},
			{Name: known.DataLayerEventLandingToODSComplete, Src: []string{known.DataLayerLandingToODS}, Dst: known.DataLayerODSToDWD},
			{Name: known.DataLayerEventODSToDWDComplete, Src: []string{known.DataLayerODSToDWD}, Dst: known.DataLayerDWDToDWS},
			{Name: known.DataLayerEventDWDToDWSComplete, Src: []string{known.DataLayerDWDToDWS}, Dst: known.DataLayerDWSToDS},
			{Name: known.DataLayerEventDWSToDS, Src: []string{known.DataLayerDWSToDS}, Dst: known.DataLayerCompleted},
			{Name: known.DataLayerEventComplete, Src: []string{known.DataLayerCompleted}, Dst: known.DataLayerSucceeded},
			{Name: known.DataLayerEventError, Src: []string{known.DataLayerPending, known.DataLayerLandingToODS, known.DataLayerODSToDWD, known.DataLayerDWDToDWS, known.DataLayerDWSToDS, known.DataLayerCompleted}, Dst: known.DataLayerFailed},
		},
		fsm.Callbacks{
			// State entry callbacks
			"enter_" + known.DataLayerLandingToODS: fsmutil.WrapEvent(sm.Processor.transformLandingToODS),
			"enter_" + known.DataLayerODSToDWD:     fsmutil.WrapEvent(sm.Processor.transformODSToDWD),
			"enter_" + known.DataLayerDWDToDWS:     fsmutil.WrapEvent(sm.Processor.transformDWDToDWS),
			"enter_" + known.DataLayerDWSToDS:      fsmutil.WrapEvent(sm.Processor.transformDWSToDS),
			"enter_" + known.DataLayerCompleted:    fsmutil.WrapEvent(sm.Processor.handleComplete),
			"enter_" + known.DataLayerFailed:       fsmutil.WrapEvent(sm.Processor.handleError),
		},
	)

	return sm
}

// Event triggers an event in the FSM
func (sm *StateMachine) Event(ctx context.Context, event string) error {
	return sm.FSM.Event(ctx, event)
}

// Current returns the current state of the FSM
func (sm *StateMachine) Current() string {
	return sm.FSM.Current()
}

// Can checks if an event can be triggered in the current state
func (sm *StateMachine) Can(event string) bool {
	return sm.FSM.Can(event)
}

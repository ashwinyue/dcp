package llmtrain

import (
	"context"

	"github.com/looplab/fsm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
)

// StateMachine represents the finite state machine for LLM training jobs.
type StateMachine struct {
	ctx   context.Context
	store store.IStore
	job   *model.JobM
	fsm   *fsm.FSM
}

// NewStateMachine creates a new state machine for LLM training jobs.
func NewStateMachine(ctx context.Context, store store.IStore, job *model.JobM) *StateMachine {
	sm := &StateMachine{
		ctx:   ctx,
		store: store,
		job:   job,
	}

	// Define the finite state machine
	sm.fsm = fsm.NewFSM(
		job.Status,
		fsm.Events{
			{Name: "download", Src: []string{known.LLMTrainPending}, Dst: known.LLMTrainDownloading},
			{Name: "downloaded", Src: []string{known.LLMTrainDownloading}, Dst: known.LLMTrainDownloaded},
			{Name: "embedding", Src: []string{known.LLMTrainDownloaded}, Dst: known.LLMTrainEmbedding},
			{Name: "embedded", Src: []string{known.LLMTrainEmbedding}, Dst: known.LLMTrainEmbedded},
			{Name: "training", Src: []string{known.LLMTrainEmbedded}, Dst: known.LLMTrainTraining},
			{Name: "trained", Src: []string{known.LLMTrainTraining}, Dst: known.LLMTrainTrained},
			{Name: "succeed", Src: []string{known.LLMTrainTrained}, Dst: known.LLMTrainSucceeded},
			{Name: "fail", Src: []string{known.LLMTrainPending, known.LLMTrainDownloading, known.LLMTrainDownloaded, known.LLMTrainEmbedding, known.LLMTrainEmbedded, known.LLMTrainTraining, known.LLMTrainTrained}, Dst: known.LLMTrainFailed},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				sm.EnterState(e.Dst)
			},
		},
	)

	return sm
}
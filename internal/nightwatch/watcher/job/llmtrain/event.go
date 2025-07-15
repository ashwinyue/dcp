package llmtrain

import (
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"
	"k8s.io/utils/ptr"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// Download retrieves feedback data from VOC and saves it to TOS.
func (sm *StateMachine) Download(ctx context.Context, event *fsm.Event) error {
	// Set default job params.
	SetDefaultJobParams(sm.Job)

	// Skip the download if the operation has already been performed (idempotency check)
	if ShouldSkipOnIdempotency(sm.Job, event.FSM.Current()) {
		return nil
	}

	time.Sleep(2 * time.Second)

	// Initialize job results if they are not already set
	if sm.Job.Results == nil || sm.Job.Results.Train == nil {
		sm.Job.Results = &model.JobResults{Train: &v1.TrainResults{}}
	}

	// TODO: Implement actual download logic here
	// This would typically involve:
	// 1. Downloading training data from specified sources
	// 2. Validating downloaded data
	// 3. Storing data in appropriate location

	// Simulate data download
	dataPath := fmt.Sprintf("job-%s-data.json", sm.Job.JobID)
	sm.Job.Results.Train.DataPath = &dataPath

	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// Embedding embedding daily estimation data.
func (sm *StateMachine) Embedding(ctx context.Context, event *fsm.Event) error {
	if ShouldSkipOnIdempotency(sm.Job, event.FSM.Current()) {
		return nil
	}

	results := sm.Job.Results.Train

	// Simulate embedding process with rate limiting
	_ = sm.Watcher.Limiter.Embedding.Take()

	// TODO: Implement actual embedding logic here
	// This would typically involve:
	// 1. Processing downloaded data through embedding models
	// 2. Generating vector embeddings
	// 3. Storing embeddings for training use

	// Simulate embedding process
	time.Sleep(time.Second)

	// Update results and write the embedded data
	embeddedDataPath := fmt.Sprintf("job-%s-embedded.json", sm.Job.JobID)
	results.EmbeddedDataPath = ptr.To(embeddedDataPath)

	results.TaskID = nil
	jobconditionsutil.Delete(sm.Job.Conditions, known.LLMTrainTrained)

	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

func (sm *StateMachine) Train(ctx context.Context, event *fsm.Event) error {
	if ShouldSkipOnIdempotency(sm.Job, event.FSM.Current()) {
		return nil
	}

	results := sm.Job.Results.Train
	_ = sm.Watcher.Limiter.Train.Take() // Rate limiting

	// Function to create the training task
	createTrainTaskFunc := func() error {
		resultPath := fmt.Sprintf("job-%s-result.json", sm.Job.JobID)
		taskID := fmt.Sprintf("task-%s", sm.Job.JobID)
		results.TaskID = &taskID
		results.ResultPath = &resultPath
		return nil
	}

	// Create task if it hasn't been created yet
	if results.TaskID == nil {
		if err := createTrainTaskFunc(); err != nil {
			return err
		}
	}

	// TODO: Implement actual training logic here
	// This would typically involve:
	// 1. Loading embedded data
	// 2. Configuring training parameters
	// 3. Running the training process
	// 4. Monitoring training progress
	// 5. Saving trained model

	// Simulate training process
	time.Sleep(time.Second)

	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// EnterState handles the state transition of the state machine
// and updates the Job's status and conditions based on the event.
func (sm *StateMachine) EnterState(ctx context.Context, event *fsm.Event) error {
	sm.Job.Status = event.FSM.Current()

	// Record the start time of the job
	if sm.Job.Status == known.LLMTrainDownloading {
		sm.Job.StartedAt = time.Now()
	}

	// Unified handling logic for Job failure
	if event.Err != nil || isJobTimeout(sm.Job) {
		sm.Job.Status = known.LLMTrainFailed
		sm.Job.EndedAt = time.Now()

		var cond *v1.JobCondition
		if isJobTimeout(sm.Job) {
			log.Infow("LLM train task timeout")
			cond = jobconditionsutil.FalseCondition(event.FSM.Current(), fmt.Sprintf("LLM train task exceeded timeout seconds"))
		} else {
			cond = jobconditionsutil.FalseCondition(event.FSM.Current(), event.Err.Error())
		}

		sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)
	}

	if err := sm.Watcher.Store.Job().Update(ctx, sm.Job); err != nil {
		return err
	}

	return nil
}

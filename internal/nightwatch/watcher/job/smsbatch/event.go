package smsbatch

import (
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"
	"k8s.io/utils/ptr"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/path"
	fakeminio "github.com/ashwinyue/dcp/internal/pkg/client/minio/fake"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// PreparationExecute handles the SMS preparation phase.
func (sm *StateMachine) PreparationExecute(ctx context.Context, event *fsm.Event) error {
	// Set default job params.
	SetDefaultJobParams(sm.Job)

	// Skip the preparation if the operation has already been performed (idempotency check)
	if ShouldSkipOnIdempotency(sm.Job, event.FSM.Current()) {
		return nil
	}

	time.Sleep(2 * time.Second)

	// Initialize job results if they are not already set
	if sm.Job.Results == nil {
		sm.Job.Results = &model.JobResults{}
	}
	
	// Initialize SMS batch results using MessageBatch field
	if (*v1.JobResults)(sm.Job.Results).MessageBatch == nil {
		(*v1.JobResults)(sm.Job.Results).MessageBatch = &v1.MessageBatchResults{}
	}

	// Simulate SMS preparation process
	// 1. Read SMS parameters from table storage
	data, err := sm.Watcher.Minio.Read(ctx, fakeminio.FakeObjectName)
	if err != nil {
		return err
	}

	// 2. Process and pack SMS data
	packedDataPath := path.Job.Path(sm.Job.JobID, "sms_packed_data")
	if err := sm.Watcher.Minio.Write(ctx, packedDataPath, data); err != nil {
		return err
	}
	
	// Update SMS batch results
	msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch
	
	// Store packed data path in current phase (preparation)
	msgBatchResults.CurrentPhase = ptr.To("preparation")
	
	// 3. Update statistics
	msgBatchResults.PreparationStats = &v1.MessageBatchPhaseStats{
		Processed: ptr.To(int64(len(data))),
		Success:   ptr.To(int64(len(data))),
		Failed:    ptr.To(int64(0)),
		StartTime: ptr.To(time.Now().Unix()),
		EndTime:   ptr.To(time.Now().Unix()),
	}
	msgBatchResults.ProcessedMessages = ptr.To(int64(len(data)))
	msgBatchResults.SuccessMessages = ptr.To(int64(len(data)))
	msgBatchResults.FailedMessages = ptr.To(int64(0))

	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// DeliveryExecute handles the SMS delivery phase.
func (sm *StateMachine) DeliveryExecute(ctx context.Context, event *fsm.Event) error {
	if ShouldSkipOnIdempotency(sm.Job, event.FSM.Current()) {
		return nil
	}

	// Get SMS batch results
	msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch

	// For delivery, we simulate reading packed data
	// In real implementation, you would store the path in preparation phase
	packedDataPath := path.Job.Path(sm.Job.JobID, "sms_packed_data")
	packedData, err := sm.Watcher.Minio.Read(ctx, packedDataPath)
	if err != nil {
		return err
	}

	// Simulate SMS delivery process
	// 1. Create delivery tasks for each partition
	deliveryResults := make([]string, len(packedData))
	for i, data := range packedData {
		// Simulate delivery to SMS provider
		deliveryResults[i] = fmt.Sprintf("delivered_%d: %s", i, data)
	}

	// 2. Save delivery results
	deliveryResultPath := path.Job.Path(sm.Job.JobID, "sms_delivery_results")
	if err := sm.Watcher.Minio.Write(ctx, deliveryResultPath, deliveryResults); err != nil {
		return err
	}
	
	// Update current phase to delivery
	msgBatchResults.CurrentPhase = ptr.To("delivery")

	// 3. Update delivery statistics
	msgBatchResults.DeliveryStats = &v1.MessageBatchPhaseStats{
		Processed: ptr.To(int64(len(packedData))),
		Success:   ptr.To(int64(len(packedData))),
		Failed:    ptr.To(int64(0)),
		StartTime: ptr.To(time.Now().Unix()),
		EndTime:   ptr.To(time.Now().Unix()),
	}
	
	// Update total message counts
	totalProcessed := int64(0)
	if msgBatchResults.PreparationStats != nil && msgBatchResults.PreparationStats.Processed != nil {
		totalProcessed += *msgBatchResults.PreparationStats.Processed
	}
	totalProcessed += int64(len(packedData))
	msgBatchResults.ProcessedMessages = ptr.To(totalProcessed)
	msgBatchResults.TotalMessages = ptr.To(totalProcessed)

	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// PreparationPause handles pausing the preparation phase.
func (sm *StateMachine) PreparationPause(ctx context.Context, event *fsm.Event) error {
	log.Infow("Pausing SMS preparation", "job_id", sm.Job.JobID)
	
	// Save current state and progress
	if sm.Job.Results != nil {
		msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch
		if msgBatchResults != nil {
			msgBatchResults.CurrentState = ptr.To("paused")
		}
	}
	
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// DeliveryPause handles pausing the delivery phase.
func (sm *StateMachine) DeliveryPause(ctx context.Context, event *fsm.Event) error {
	log.Infow("Pausing SMS delivery", "job_id", sm.Job.JobID)
	
	// Save current state and progress
	if sm.Job.Results != nil {
		msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch
		if msgBatchResults != nil {
			msgBatchResults.CurrentState = ptr.To("paused")
		}
	}
	
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// PreparationResume handles resuming the preparation phase.
func (sm *StateMachine) PreparationResume(ctx context.Context, event *fsm.Event) error {
	log.Infow("Resuming SMS preparation", "job_id", sm.Job.JobID)
	
	// Clear paused state
	if sm.Job.Results != nil {
		msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch
		if msgBatchResults != nil {
			msgBatchResults.CurrentState = ptr.To("running")
		}
	}
	
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// DeliveryResume handles resuming the delivery phase.
func (sm *StateMachine) DeliveryResume(ctx context.Context, event *fsm.Event) error {
	log.Infow("Resuming SMS delivery", "job_id", sm.Job.JobID)
	
	// Clear paused state
	if sm.Job.Results != nil {
		msgBatchResults := (*v1.JobResults)(sm.Job.Results).MessageBatch
		if msgBatchResults != nil {
			msgBatchResults.CurrentState = ptr.To("running")
		}
	}
	
	sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, jobconditionsutil.TrueCondition(event.FSM.Current()))
	return nil
}

// EnterState handles the state transition of the state machine
// and updates the Job's status and conditions based on the event.
func (sm *StateMachine) EnterState(ctx context.Context, event *fsm.Event) error {
	sm.Job.Status = event.FSM.Current()

	// Record the start time of the job
	if sm.Job.Status == SmsBatchPreparationReady {
		sm.Job.StartedAt = time.Now()
	}

	// Unified handling logic for Job failure
	if event.Err != nil || isJobTimeout(sm.Job) {
		sm.Job.Status = SmsBatchFailed
		sm.Job.EndedAt = time.Now()

		var cond *v1.JobCondition
		if isJobTimeout(sm.Job) {
			log.Infow("SMS batch task timeout")
			cond = jobconditionsutil.FalseCondition(event.FSM.Current(), "SMS batch task exceeded timeout seconds")
		} else {
			cond = jobconditionsutil.FalseCondition(event.FSM.Current(), event.Err.Error())
		}

		sm.Job.Conditions = jobconditionsutil.Set(sm.Job.Conditions, cond)
	}

	// Record completion time for final states
	if sm.Job.Status == SmsBatchSucceeded || sm.Job.Status == SmsBatchFailed || sm.Job.Status == SmsBatchAborted {
		sm.Job.EndedAt = time.Now()
	}

	if err := sm.Watcher.Store.Job().Update(ctx, sm.Job); err != nil {
		return err
	}

	return nil
}
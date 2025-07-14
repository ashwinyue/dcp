package youzanorder

import (
	"context"
	"fmt"
	"time"

	"github.com/onexstack/onexstack/pkg/store/where"

	"github.com/ashwinyue/dcp/internal/nightwatch/batch"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// EventHandler handles YouZan order processing events.
type EventHandler struct {
	store       store.IStore
	taskManager *batch.TaskManager
}

// NewEventHandler creates a new event handler for YouZan order processing.
func NewEventHandler(store store.IStore, taskManager *batch.TaskManager) *EventHandler {
	return &EventHandler{
		store:       store,
		taskManager: taskManager,
	}
}

// Fetch handles the order fetching event.
func (eh *EventHandler) Fetch(ctx context.Context, jobID string) error {
	log.Infow("Starting YouZan order fetching", "job_id", jobID)

	// Update job status to fetching
	if err := eh.updateJobStatus(ctx, jobID, known.YouZanOrderFetching); err != nil {
		return fmt.Errorf("failed to update job status to fetching: %w", err)
	}

	// Check for timeout
	if err := eh.checkTimeout(ctx, jobID); err != nil {
		return fmt.Errorf("job timeout check failed: %w", err)
	}

	// Simulate fetching process
	time.Sleep(2 * time.Second)

	// Create YouZan order task for batch processing
	taskResp, err := eh.taskManager.CreateTask(&batch.CreateTaskRequest{
		Type: batch.TaskTypeYouZanOrder,
		Config: &batch.BatchConfig{
			BatchSize: known.YouZanOrderBatchSize,
		},
		Metadata: map[string]interface{}{
			"job_id": jobID,
			"source": "youzan_api",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create YouZan order task: %w", err)
	}

	log.Infow("YouZan order fetching completed", "job_id", jobID, "task_id", taskResp.TaskID)

	return nil
}

// Validate handles the order validation event.
func (eh *EventHandler) Validate(ctx context.Context, jobID string) error {
	log.Infow("Starting YouZan order validation", "job_id", jobID)

	// Update job status to validating
	if err := eh.updateJobStatus(ctx, jobID, known.YouZanOrderValidating); err != nil {
		return fmt.Errorf("failed to update job status to validating: %w", err)
	}

	// Check for timeout
	if err := eh.checkTimeout(ctx, jobID); err != nil {
		return fmt.Errorf("job timeout check failed: %w", err)
	}

	// Simulate validation process
	time.Sleep(1 * time.Second)

	log.Infow("YouZan order validation completed", "job_id", jobID)
	return nil
}

// Enrich handles the order enrichment event.
func (eh *EventHandler) Enrich(ctx context.Context, jobID string) error {
	log.Infow("Starting YouZan order enrichment", "job_id", jobID)

	// Update job status to enriching
	if err := eh.updateJobStatus(ctx, jobID, known.YouZanOrderEnriching); err != nil {
		return fmt.Errorf("failed to update job status to enriching: %w", err)
	}

	// Check for timeout
	if err := eh.checkTimeout(ctx, jobID); err != nil {
		return fmt.Errorf("job timeout check failed: %w", err)
	}

	// Simulate enrichment process
	time.Sleep(3 * time.Second)

	log.Infow("YouZan order enrichment completed", "job_id", jobID)
	return nil
}

// Process handles the order processing event.
func (eh *EventHandler) Process(ctx context.Context, jobID string) error {
	log.Infow("Starting YouZan order processing", "job_id", jobID)

	// Update job status to processing
	if err := eh.updateJobStatus(ctx, jobID, known.YouZanOrderProcessing); err != nil {
		return fmt.Errorf("failed to update job status to processing: %w", err)
	}

	// Check for timeout
	if err := eh.checkTimeout(ctx, jobID); err != nil {
		return fmt.Errorf("job timeout check failed: %w", err)
	}

	// Get tasks associated with this job and process them
	tasksResp, err := eh.taskManager.ListTasks(&batch.ListTasksRequest{
		Type: "youzan_order",
	})
	if err != nil {
		return fmt.Errorf("failed to list tasks for job %s: %w", jobID, err)
	}

	// Process each task
	for _, task := range tasksResp.Tasks {
		if err := eh.taskManager.ProcessTask(task.ID); err != nil {
			log.Errorw("Failed to process YouZan order task", "job_id", jobID, "task_id", task.ID, "error", err)
			return fmt.Errorf("failed to process task %s: %w", task.ID, err)
		}
	}

	log.Infow("YouZan order processing completed", "job_id", jobID)
	return nil
}

// updateJobStatus updates the job status in the store.
func (eh *EventHandler) updateJobStatus(ctx context.Context, jobID, status string) error {
	jobObj, err := eh.store.Job().Get(ctx, where.F("id = ?", jobID))
	if err != nil {
		return fmt.Errorf("failed to get job %s: %w", jobID, err)
	}

	jobObj.Status = status
	jobObj.UpdatedAt = time.Now()

	if err := eh.store.Job().Update(ctx, jobObj); err != nil {
		return fmt.Errorf("failed to update job %s status to %s: %w", jobID, status, err)
	}

	return nil
}

// checkTimeout checks if the job has exceeded the timeout limit.
func (eh *EventHandler) checkTimeout(ctx context.Context, jobID string) error {
	jobObj, err := eh.store.Job().Get(ctx, where.F("id = ?", jobID))
	if err != nil {
		return fmt.Errorf("failed to get job %s: %w", jobID, err)
	}

	// Check if job has exceeded timeout
	if time.Since(jobObj.CreatedAt) > time.Duration(known.YouZanOrderTimeout)*time.Second {
		log.Warnw("YouZan order job has exceeded timeout", "job_id", jobID, "created_at", jobObj.CreatedAt, "timeout", known.YouZanOrderTimeout)

		// Update job status to failed
		jobObj.Status = known.YouZanOrderFailed
		jobObj.UpdatedAt = time.Now()
		if err := eh.store.Job().Update(ctx, jobObj); err != nil {
			return fmt.Errorf("failed to update job %s status to failed: %w", jobID, err)
		}

		return fmt.Errorf("job %s has exceeded timeout of %d seconds", jobID, known.YouZanOrderTimeout)
	}

	return nil
}

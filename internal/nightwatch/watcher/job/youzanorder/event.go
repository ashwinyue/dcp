package youzanorder

import (
	"context"
	"fmt"
	"time"

	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/sirupsen/logrus"

	"github.com/ashwinyue/dcp/internal/nightwatch/batch"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
)

// EventHandler handles YouZan order processing events.
type EventHandler struct {
	store       store.IStore
	logger      *logrus.Entry
	taskManager *batch.TaskManager
}

// NewEventHandler creates a new event handler for YouZan order processing.
func NewEventHandler(store store.IStore, logger *logrus.Entry, taskManager *batch.TaskManager) *EventHandler {
	return &EventHandler{
		store:       store,
		logger:      logger,
		taskManager: taskManager,
	}
}

// Fetch handles the order fetching event.
func (eh *EventHandler) Fetch(ctx context.Context, jobID string) error {
	eh.logger.WithField("job_id", jobID).Info("Starting YouZan order fetching")

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

	eh.logger.WithFields(logrus.Fields{
		"job_id":  jobID,
		"task_id": taskResp.TaskID,
	}).Info("YouZan order fetching completed")

	return nil
}

// Validate handles the order validation event.
func (eh *EventHandler) Validate(ctx context.Context, jobID string) error {
	eh.logger.WithField("job_id", jobID).Info("Starting YouZan order validation")

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

	eh.logger.WithField("job_id", jobID).Info("YouZan order validation completed")
	return nil
}

// Enrich handles the order enrichment event.
func (eh *EventHandler) Enrich(ctx context.Context, jobID string) error {
	eh.logger.WithField("job_id", jobID).Info("Starting YouZan order enrichment")

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

	eh.logger.WithField("job_id", jobID).Info("YouZan order enrichment completed")
	return nil
}

// Process handles the order processing event.
func (eh *EventHandler) Process(ctx context.Context, jobID string) error {
	eh.logger.WithField("job_id", jobID).Info("Starting YouZan order processing")

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
			eh.logger.WithFields(logrus.Fields{
				"job_id":  jobID,
				"task_id": task.ID,
				"error":   err,
			}).Error("Failed to process YouZan order task")
			return fmt.Errorf("failed to process task %s: %w", task.ID, err)
		}
	}

	eh.logger.WithField("job_id", jobID).Info("YouZan order processing completed")
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
		eh.logger.WithFields(logrus.Fields{
			"job_id":     jobID,
			"created_at": jobObj.CreatedAt,
			"timeout":    known.YouZanOrderTimeout,
		}).Warn("YouZan order job has exceeded timeout")

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

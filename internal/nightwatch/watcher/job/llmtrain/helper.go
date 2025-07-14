package llmtrain

import (
	"context"
	"fmt"
	"time"

	"github.com/onexstack/onexstack/pkg/store/where"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// isJobTimeout checks if a job has exceeded the specified timeout duration.
func isJobTimeout(job *model.JobM, timeoutSeconds int) bool {
	if job.CreatedAt.IsZero() {
		return false
	}

	timeout := time.Duration(timeoutSeconds) * time.Second
	return time.Since(job.CreatedAt) > timeout
}

// ShouldSkipOnIdempotency determines if a job should be skipped based on idempotency settings.
func ShouldSkipOnIdempotency(ctx context.Context, store store.IStore, job *model.JobM) bool {
	// Check if there are any successful jobs with the same parameters
	_, jobs, err := store.Job().List(ctx, where.NewWhere().F(
		"scope", job.Scope,
		"status", known.JobSucceeded,
	).L(1))
	if err != nil {
		log.Errorw("Failed to check for existing successful jobs", "error", err)
		return false
	}

	// If there are successful jobs with similar parameters, skip this job
	for _, existingJob := range jobs {
		if isSimilarJob(job, existingJob) {
			log.Infow("Skipping job due to idempotency", "jobID", job.JobID, "existingJobID", existingJob.JobID)
			return true
		}
	}

	return false
}

// isSimilarJob checks if two jobs are similar enough to be considered duplicates.
func isSimilarJob(job1, job2 *model.JobM) bool {
	// Compare basic job parameters
	return job1.Scope == job2.Scope &&
		job1.Watcher == job2.Watcher &&
		job1.Name == job2.Name
}

// SetDefaultJobParams sets default parameters for a job if they are not already set.
func SetDefaultJobParams(job *model.JobM) {
	if job.Scope == "" {
		job.Scope = known.LLMJobScope
	}

	if job.Watcher == "" {
		job.Watcher = known.LLMTrainWatcher
	}
}

// buildEmbedderInputs generates inputs for the embedding process based on the job configuration.
func buildEmbedderInputs(job *model.JobM) []string {
	var inputs []string

	// TODO: Implement logic to build embedder inputs based on job parameters
	// This would typically involve:
	// 1. Reading job configuration
	// 2. Determining embedder type and parameters
	// 3. Generating appropriate input data structures

	// For now, return a placeholder input
	inputs = append(inputs, "default_embedding_input")

	return inputs
}

// validateJobParameters validates that the job has all required parameters for LLM training.
func validateJobParameters(job *model.JobM) error {
	if job.JobID == "" {
		return fmt.Errorf("job ID is required")
	}

	if job.Scope != known.LLMJobScope {
		return fmt.Errorf("invalid job scope: expected %s, got %s", known.LLMJobScope, job.Scope)
	}

	if job.Watcher != known.LLMTrainWatcher {
		return fmt.Errorf("invalid job watcher: expected %s, got %s", known.LLMTrainWatcher, job.Watcher)
	}

	return nil
}

// calculateProgress calculates the progress percentage based on the current job status.
func calculateProgress(status string) int {
	switch status {
	case known.LLMTrainPending:
		return 0
	case known.LLMTrainDownloading:
		return 10
	case known.LLMTrainDownloaded:
		return 20
	case known.LLMTrainEmbedding:
		return 40
	case known.LLMTrainEmbedded:
		return 60
	case known.LLMTrainTraining:
		return 80
	case known.LLMTrainTrained:
		return 95
	case known.LLMTrainSucceeded:
		return 100
	case known.LLMTrainFailed:
		return -1 // Indicate failure
	default:
		return 0
	}
}

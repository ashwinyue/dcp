package batch

import (
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
)

// isJobTimeout checks if the job has exceeded its allowed execution time.
func isJobTimeout(job *model.JobM) bool {
	if job.StartedAt.IsZero() {
		return false
	}

	duration := time.Now().Unix() - job.StartedAt.Unix()
	timeout := int64(known.DataLayerProcessTimeout)

	return duration > timeout
}

// ShouldSkipOnIdempotency determines whether a job should skip execution based on idempotency conditions.
func ShouldSkipOnIdempotency(job *model.JobM, status string) bool {
	// Simple idempotency check: if job is already in the target status, skip
	return job.Status == status
}

// SetDefaultJobParams sets default parameters for the batch job if they are not already set.
func SetDefaultJobParams(job *model.JobM) {
	// Set default timeout if not specified
	if job.Params == nil {
		return
	}

	// Add any default parameter setting logic here
}

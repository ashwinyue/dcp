package llmtrain

import (
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	jobconditionsutil "github.com/ashwinyue/dcp/internal/pkg/util/jobconditions"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// isJobTimeout checks if the job has exceeded its allowed execution time.
func isJobTimeout(job *model.JobM) bool {
	duration := time.Now().Unix() - job.StartedAt.Unix()
	timeout := job.Params.Train.JobTimeout
	if timeout == 0 {
		timeout = int64(known.LLMTrainTimeout)
	}

	return duration > timeout
}

// ShouldSkipOnIdempotency determines whether a job should skip execution based on idempotency conditions.
func ShouldSkipOnIdempotency(job *model.JobM, condType string) bool {
	// If idempotent execution is not set, allow execution regardless of conditions.
	if job.Params.Train.IdempotentExecution != known.IdempotentExecution {
		return false
	}

	return jobconditionsutil.IsTrue(job.Conditions, condType)
}

// SetDefaultJobParams sets default parameters for the job if they are not already set.
func SetDefaultJobParams(job *model.JobM) {
	if job.Params.Train.JobTimeout == 0 {
		job.Params.Train.JobTimeout = int64(known.LLMTrainTimeout)
	}
}

// buildEmbedderInputs generates inputs for embedding based on the specified embedder type.
func buildEmbedderInputs(params *v1.TrainParams) []any {
	// TODO: Implement proper embedder input building
	return []any{}
}

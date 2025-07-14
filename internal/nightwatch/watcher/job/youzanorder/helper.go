package youzanorder

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
		"status", known.YouZanOrderSucceeded,
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
	if job1.Scope != job2.Scope || job1.Watcher != job2.Watcher || job1.Name != job2.Name {
		return false
	}

	// Compare metadata if available
	if job1.Metadata != nil && job2.Metadata != nil {
		// Check if they have the same date range
		if startTime1, ok1 := job1.Metadata["start_time"]; ok1 {
			if startTime2, ok2 := job2.Metadata["start_time"]; ok2 {
				return startTime1 == startTime2
			}
		}
	}

	return true
}

// SetDefaultJobParams sets default parameters for a job if they are not already set.
func SetDefaultJobParams(job *model.JobM) {
	if job.Scope == "" {
		job.Scope = known.YouZanOrderJobScope
	}

	if job.Watcher == "" {
		job.Watcher = known.YouZanOrderWatcher
	}

	// Set default metadata if not present
	if job.Metadata == nil {
		job.Metadata = make(map[string]interface{})
	}

	// Set default time range if not specified
	if _, ok := job.Metadata["start_time"]; !ok {
		job.Metadata["start_time"] = time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	}
	if _, ok := job.Metadata["end_time"]; !ok {
		job.Metadata["end_time"] = time.Now().Format(time.RFC3339)
	}
	if _, ok := job.Metadata["page_size"]; !ok {
		job.Metadata["page_size"] = known.YouZanOrderBatchSize
	}
}

// buildOrderRequest generates an order request based on the job configuration.
func buildOrderRequest(job *model.JobM) *OrderRequest {
	req := &OrderRequest{
		PageSize:   known.YouZanOrderBatchSize,
		PageNumber: 1,
	}

	if job.Metadata != nil {
		// Parse start time
		if startTimeStr, ok := job.Metadata["start_time"].(string); ok {
			if startTime, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
				req.StartTime = startTime
			}
		}
		if req.StartTime.IsZero() {
			req.StartTime = time.Now().Add(-24 * time.Hour)
		}

		// Parse end time
		if endTimeStr, ok := job.Metadata["end_time"].(string); ok {
			if endTime, err := time.Parse(time.RFC3339, endTimeStr); err == nil {
				req.EndTime = endTime
			}
		}
		if req.EndTime.IsZero() {
			req.EndTime = time.Now()
		}

		// Parse page size
		if pageSizeFloat, ok := job.Metadata["page_size"].(float64); ok {
			req.PageSize = int(pageSizeFloat)
		}

		// Parse order status filter
		if orderStatus, ok := job.Metadata["order_status"].(string); ok {
			req.OrderStatus = orderStatus
		}
	}

	return req
}

// validateJobParameters validates that the job has all required parameters for YouZan order processing.
func validateJobParameters(job *model.JobM) error {
	if job.JobID == "" {
		return fmt.Errorf("job ID is required")
	}

	if job.Scope != known.YouZanOrderJobScope {
		return fmt.Errorf("invalid job scope: expected %s, got %s", known.YouZanOrderJobScope, job.Scope)
	}

	if job.Watcher != known.YouZanOrderWatcher {
		return fmt.Errorf("invalid job watcher: expected %s, got %s", known.YouZanOrderWatcher, job.Watcher)
	}

	return nil
}

// calculateProgress calculates the progress percentage based on the current job status.
func calculateProgress(status string) int {
	switch status {
	case known.YouZanOrderPending:
		return 0
	case known.YouZanOrderFetching:
		return 10
	case known.YouZanOrderFetched:
		return 20
	case known.YouZanOrderValidating:
		return 30
	case known.YouZanOrderValidated:
		return 40
	case known.YouZanOrderEnriching:
		return 60
	case known.YouZanOrderEnriched:
		return 80
	case known.YouZanOrderProcessing:
		return 90
	case known.YouZanOrderProcessed:
		return 95
	case known.YouZanOrderSucceeded:
		return 100
	case known.YouZanOrderFailed:
		return -1 // Indicate failure
	default:
		return 0
	}
}

// updateJobProgress updates the job's progress information.
func updateJobProgress(ctx context.Context, store store.IStore, job *model.JobM) error {
	if job.Metadata == nil {
		job.Metadata = make(map[string]interface{})
	}

	// Update progress
	progress := calculateProgress(job.Status)
	job.Metadata["progress"] = progress

	// Update last activity time
	job.Metadata["last_activity"] = time.Now().Format(time.RFC3339)

	// Update job in store
	return store.Job().Update(ctx, job)
}

// shouldRetryJob determines if a job should be retried based on its retry count and error type.
func shouldRetryJob(job *model.JobM, err error) bool {
	if job.Metadata == nil {
		job.Metadata = make(map[string]interface{})
	}

	// Get current retry count
	retryCount := 0
	if retryCountFloat, ok := job.Metadata["retry_count"].(float64); ok {
		retryCount = int(retryCountFloat)
	}

	// Check if we've exceeded the retry limit
	if retryCount >= known.YouZanOrderRetryLimit {
		return false
	}

	// Increment retry count
	job.Metadata["retry_count"] = retryCount + 1
	job.Metadata["last_error"] = err.Error()
	job.Metadata["last_retry_time"] = time.Now().Format(time.RFC3339)

	return true
}

// getOrderCounts returns the count of orders in different states from job metadata.
func getOrderCounts(job *model.JobM) map[string]int {
	counts := make(map[string]int)

	if job.Metadata == nil {
		return counts
	}

	// Extract counts from metadata
	if totalOrdersFloat, ok := job.Metadata["total_orders"].(float64); ok {
		counts["total"] = int(totalOrdersFloat)
	}
	if validCountFloat, ok := job.Metadata["valid_count"].(float64); ok {
		counts["valid"] = int(validCountFloat)
	}
	if invalidCountFloat, ok := job.Metadata["invalid_count"].(float64); ok {
		counts["invalid"] = int(invalidCountFloat)
	}
	if enrichedCountFloat, ok := job.Metadata["enriched_count"].(float64); ok {
		counts["enriched"] = int(enrichedCountFloat)
	}
	if processedCountFloat, ok := job.Metadata["processed_count"].(float64); ok {
		counts["processed"] = int(processedCountFloat)
	}

	return counts
}

// logJobSummary logs a summary of the job processing results.
func logJobSummary(job *model.JobM, operation string) {
	counts := getOrderCounts(job)
	progress := calculateProgress(job.Status)

	log.Infow("YouZan order job summary",
		"jobID", job.JobID,
		"operation", operation,
		"status", job.Status,
		"progress", progress,
		"counts", counts,
	)
}

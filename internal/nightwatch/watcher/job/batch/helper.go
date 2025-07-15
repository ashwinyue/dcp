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

// 简化的恢复机制相关函数

// IsJobRecoverable 检查任务是否可以恢复
func IsJobRecoverable(job *model.JobM) bool {
	// 只要任务不是最终状态，就可以恢复
	finalStates := []string{
		known.DataLayerSucceeded,
		known.DataLayerFailed,
	}

	for _, state := range finalStates {
		if job.Status == state {
			return false
		}
	}
	return true
}

// IsJobInterrupted 检查任务是否被中断
func IsJobInterrupted(job *model.JobM) bool {
	// 如果任务已开始但超时，认为被中断
	if !job.StartedAt.IsZero() && job.EndedAt.IsZero() {
		return isJobTimeout(job)
	}
	return false
}

// PrepareJobForRecovery 为任务恢复做准备
func PrepareJobForRecovery(job *model.JobM) {
	// 简单的恢复准备：只重置时间戳
	if IsJobInterrupted(job) {
		job.StartedAt = time.Now()
		job.UpdatedAt = time.Now()
		job.EndedAt = time.Time{} // 清空结束时间
	}
}

// ValidateJobIntegrity 验证任务数据完整性
func ValidateJobIntegrity(job *model.JobM) bool {
	// 简单验证：检查关键字段
	if job.JobID == "" {
		return false
	}

	// 检查状态是否在有效范围内
	validStates := []string{
		known.DataLayerPending,
		known.DataLayerLandingToODS,
		known.DataLayerODSToDWD,
		known.DataLayerDWDToDWS,
		known.DataLayerDWSToDS,
		known.DataLayerCompleted,
		known.DataLayerSucceeded,
		known.DataLayerFailed,
	}

	for _, state := range validStates {
		if job.Status == state {
			return true
		}
	}

	return false
}

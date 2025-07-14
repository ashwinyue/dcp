package batch

import (
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"time"
)

// Re-export types from types package
type (
	TaskType          = types.TaskType
	TaskStatus        = types.TaskStatus
	BatchTask         = types.BatchTask
	BatchConfig       = types.BatchConfig
	ProcessingContext = types.ProcessingContext
	ProcessingMetrics = types.ProcessingMetrics
	TaskProgress      = types.TaskProgress
	TaskError         = types.TaskError
	Logger            = types.Logger
)

// Re-export generic types (need to be aliased with specific instantiation or used directly)
// Note: Generic type aliases require Go 1.18+ and specific syntax
type BatchItem[T any] = types.BatchItem[T]
type BatchResult[T any] = types.BatchResult[T]
type ProcessorFunc[T, R any] = types.ProcessorFunc[T, R]
type ValidatorFunc[T any] = types.ValidatorFunc[T]
type FilterFunc[T any] = types.FilterFunc[T]
type TransformerFunc[T, R any] = types.TransformerFunc[T, R]
type EnricherFunc[T any] = types.EnricherFunc[T]

// Re-export constants
const (
	TaskTypeTrain        = types.TaskTypeTrain
	TaskTypeYouZanOrder  = types.TaskTypeYouZanOrder
	TaskTypeBatchProcess = types.TaskTypeBatchProcess

	TaskStatusPending    = types.TaskStatusPending
	TaskStatusProcessing = types.TaskStatusProcessing
	TaskStatusCompleted  = types.TaskStatusCompleted
	TaskStatusFailed     = types.TaskStatusFailed
	TaskStatusCancelled  = types.TaskStatusCancelled
)

// Request/Response structures for API operations

// CreateTaskRequest represents a request to create a new task
type CreateTaskRequest struct {
	Type     TaskType               `json:"type"`
	Config   *BatchConfig           `json:"config"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// CreateTaskResponse represents the response after creating a task
type CreateTaskResponse struct {
	TaskID    string     `json:"task_id"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
}

// GetTaskRequest represents a request to get a task
type GetTaskRequest struct {
	TaskID string `json:"task_id"`
}

// GetTaskResponse represents the response when getting a task
type GetTaskResponse struct {
	Task *BatchTask `json:"task"`
}

// GetTaskStatusRequest represents a request to get task status
type GetTaskStatusRequest struct {
	TaskID string `json:"task_id"`
}

// GetTaskStatusResponse represents the response when getting task status
type GetTaskStatusResponse struct {
	TaskID    string        `json:"task_id"`
	Status    TaskStatus    `json:"status"`
	Progress  *TaskProgress `json:"progress"`
	Error     *TaskError    `json:"error,omitempty"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// UpdateTaskStatusRequest represents a request to update task status
type UpdateTaskStatusRequest struct {
	TaskID string     `json:"task_id"`
	Status TaskStatus `json:"status"`
	Error  *TaskError `json:"error,omitempty"`
}

// UpdateTaskStatusResponse represents the response after updating task status
type UpdateTaskStatusResponse struct {
	TaskID    string     `json:"task_id"`
	Status    TaskStatus `json:"status"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ListTasksRequest represents a request to list tasks
type ListTasksRequest struct {
	Status string `json:"status,omitempty"`
	Type   string `json:"type,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

// ListTasksResponse represents the response when listing tasks
type ListTasksResponse struct {
	Tasks []*BatchTask `json:"tasks"`
	Total int          `json:"total"`
}

// ProcessTaskRequest represents a request to process a task
type ProcessTaskRequest struct {
	TaskID string `json:"task_id"`
}

// ProcessTaskResponse represents the response after starting task processing
type ProcessTaskResponse struct {
	TaskID    string     `json:"task_id"`
	Status    TaskStatus `json:"status"`
	StartedAt time.Time  `json:"started_at"`
}

// StopTaskRequest represents a request to stop a task
type StopTaskRequest struct {
	TaskID string `json:"task_id"`
}

// StopTaskResponse represents the response after stopping a task
type StopTaskResponse struct {
	TaskID    string     `json:"task_id"`
	Status    TaskStatus `json:"status"`
	StoppedAt time.Time  `json:"stopped_at"`
}

// 保留所有现有的 API 请求/响应结构
// 注意：核心类型定义已移至 types 包，这里只保留 API 相关的结构体

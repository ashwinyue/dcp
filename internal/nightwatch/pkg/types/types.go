package types

import (
	"time"
)

// TaskType represents the type of batch task
type TaskType string

const (
	TaskTypeTrain        TaskType = "train"
	TaskTypeYouZanOrder  TaskType = "youzan_order"
	TaskTypeBatchProcess TaskType = "batch_process"
)

// TaskStatus represents the status of a batch task
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

// BatchTask represents a batch processing task
type BatchTask struct {
	ID          string                 `json:"id"`
	Type        TaskType               `json:"type"`
	Status      TaskStatus             `json:"status"`
	Config      *BatchConfig           `json:"config"`
	Progress    *TaskProgress          `json:"progress"`
	Error       *TaskError             `json:"error,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// TaskProgress represents the progress of a task
type TaskProgress struct {
	Percentage int       `json:"percentage"`
	Message    string    `json:"message,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// TaskError represents an error that occurred during task processing
type TaskError struct {
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Details   string    `json:"details,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Retryable bool      `json:"retryable"`
}

// BatchConfig represents configuration for batch processing
type BatchConfig struct {
	BatchSize   int           `json:"batch_size"`
	MaxRetries  int           `json:"max_retries"`
	Timeout     time.Duration `json:"timeout"`
	Concurrency int           `json:"concurrency"`
}

// ProcessingContext provides context for processing operations
type ProcessingContext struct {
	TaskID     string                 `json:"task_id"`
	BatchID    string                 `json:"batch_id"`
	WorkerID   string                 `json:"worker_id,omitempty"`
	StartTime  time.Time              `json:"start_time"`
	Timeout    time.Duration          `json:"timeout,omitempty"`
	RetryCount int                    `json:"retry_count"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// ProcessingMetrics represents metrics for processing operations
type ProcessingMetrics struct {
	TotalItems     int       `json:"total_items"`
	ProcessedItems int       `json:"processed_items"`
	FailedItems    int       `json:"failed_items"`
	StartTime      time.Time `json:"start_time"`
}

// Logger interface for logging operations
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
}

// BatchItem represents an item in a batch
type BatchItem[T any] struct {
	ID        string                 `json:"id"`
	Data      T                      `json:"data"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// BatchResult represents the result of processing a batch item
type BatchResult[T any] struct {
	ID          string    `json:"id"`
	Data        T         `json:"data"`
	ProcessedAt time.Time `json:"processed_at"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
}

// Function types for processing operations
type ProcessorFunc[T, R any] func(ctx ProcessingContext, item *BatchItem[T]) (*BatchResult[R], error)
type ValidatorFunc[T any] func(ctx ProcessingContext, item *BatchItem[T]) error
type FilterFunc[T any] func(ctx ProcessingContext, item *BatchItem[T]) bool
type TransformerFunc[T, R any] func(ctx ProcessingContext, item *BatchItem[T]) (*BatchItem[R], error)
type EnricherFunc[T any] func(ctx ProcessingContext, item *BatchItem[T]) error

// TaskManager interface to break circular dependency
type TaskManager interface {
	GetPendingTasks(limit int) ([]*BatchTask, error)
	UpdateTaskStatus(taskID string, status TaskStatus, taskError *TaskError) error
	GetTask(taskID string) (*BatchTask, error)
	CreateTask(taskType TaskType, config *BatchConfig) (*BatchTask, error)
	DeleteTask(taskID string) error
}

// Processor interface for processing batch items
type Processor[T, R any] interface {
	Process(ctx ProcessingContext, item *BatchItem[T]) (*BatchResult[R], error)
	Validate(ctx ProcessingContext, item *BatchItem[T]) error
	GetName() string
}

// BatchProcessor interface for processing batches
type BatchProcessor[T, R any] interface {
	ProcessBatch(ctx ProcessingContext, items []*BatchItem[T]) ([]*BatchResult[R], error)
	GetBatchSize() int
	GetConcurrency() int
}

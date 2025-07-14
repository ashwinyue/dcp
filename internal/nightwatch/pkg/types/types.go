package types

import (
	"context"
	"time"
)

// TaskType represents the type of a task
type TaskType string

// Task type constants
const (
	TaskTypeTrain        TaskType = "train"
	TaskTypeYouZanOrder  TaskType = "youzan_order"
	TaskTypeBatchProcess TaskType = "batch_process"
)

// TaskStatus represents the status of a task
type TaskStatus string

// Task status constants
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
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// BatchConfig represents the configuration for a batch processing task
type BatchConfig struct {
	BatchSize     int                    `json:"batch_size"`
	Timeout       time.Duration          `json:"timeout"`
	Retries       int                    `json:"retries"`
	ConcurrentNum int                    `json:"concurrent_num"`
	Options       map[string]interface{} `json:"options,omitempty"`
}

// ProcessingContext represents the context for processing tasks
type ProcessingContext struct {
	TaskID    string                 `json:"task_id"`
	BatchNum  int                    `json:"batch_num"`
	ItemIndex int                    `json:"item_index"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Logger    Logger                 `json:"-"`
}

// ProcessingMetrics represents metrics for processing tasks
type ProcessingMetrics struct {
	TotalItems       int           `json:"total_items"`
	ProcessedItems   int           `json:"processed_items"`
	SuccessfulItems  int           `json:"successful_items"`
	FailedItems      int           `json:"failed_items"`
	StartTime        time.Time     `json:"start_time"`
	EndTime          *time.Time    `json:"end_time,omitempty"`
	ProcessingTime   time.Duration `json:"processing_time"`
	ThroughputPerSec float64       `json:"throughput_per_sec"`
}

// TaskProgress represents the progress of a task
type TaskProgress struct {
	Current       int           `json:"current"`
	Total         int           `json:"total"`
	Percentage    float64       `json:"percentage"`
	Message       string        `json:"message,omitempty"`
	StartTime     time.Time     `json:"start_time"`
	ElapsedTime   time.Duration `json:"elapsed_time"`
	EstimatedTime time.Duration `json:"estimated_time,omitempty"`
}

// TaskError represents an error that occurred during task processing
type TaskError struct {
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Details   string    `json:"details,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Retryable bool      `json:"retryable"`
}

// Logger represents a logger interface
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
	Warn(msg string, keyvals ...interface{})
	Debug(msg string, keyvals ...interface{})
}

// BatchItem represents an item in a batch
type BatchItem[T any] struct {
	ID       string                 `json:"id"`
	Data     T                      `json:"data"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Index    int                    `json:"index"`
}

// BatchResult represents the result of processing a batch item
type BatchResult[T any] struct {
	Item           *BatchItem[T] `json:"item"`
	Result         T             `json:"result"`
	Error          *TaskError    `json:"error,omitempty"`
	Success        bool          `json:"success"`
	ProcessingTime time.Duration `json:"processing_time"`
}

// ProcessorFunc represents a function that processes a batch item
type ProcessorFunc[T, R any] func(ctx context.Context, item *BatchItem[T]) (*BatchResult[R], error)

// ValidatorFunc represents a function that validates a batch item
type ValidatorFunc[T any] func(ctx context.Context, item *BatchItem[T]) error

// FilterFunc represents a function that filters batch items
type FilterFunc[T any] func(ctx context.Context, item *BatchItem[T]) (bool, error)

// TransformerFunc represents a function that transforms a batch item
type TransformerFunc[T, R any] func(ctx context.Context, item *BatchItem[T]) (*BatchItem[R], error)

// EnricherFunc represents a function that enriches a batch item
type EnricherFunc[T any] func(ctx context.Context, item *BatchItem[T]) error

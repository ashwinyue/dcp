package messagebatch

import (
	"context"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// MessageData represents a message to be sent
type MessageData struct {
	ID            string            `json:"id"`
	Recipient     string            `json:"recipient"`
	Content       string            `json:"content"`
	Template      string            `json:"template"`
	Type          string            `json:"type"`
	PartitionKey  string            `json:"partition_key"`
	Priority      int               `json:"priority"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	ScheduledTime time.Time         `json:"scheduled_time"`
	CreatedAt     time.Time         `json:"created_at"`
}

// PartitionTask represents a partition processing task
type PartitionTask struct {
	ID           string      `json:"id"`
	BatchID      string      `json:"batch_id"`
	PartitionKey string      `json:"partition_key"`
	Status       string      `json:"status"`
	MessageCount int64       `json:"message_count"`
	ProcessedAt  *time.Time  `json:"processed_at,omitempty"`
	CompletedAt  *time.Time  `json:"completed_at,omitempty"`
	ErrorMessage string      `json:"error_message,omitempty"`
	RetryCount   int         `json:"retry_count"`
	TaskCode     string      `json:"task_code"`
	Metadata     interface{} `json:"metadata,omitempty"`
}

// PhaseStatistics represents statistics for a processing phase
type PhaseStatistics struct {
	Total      int64      `json:"total"`
	Processed  int64      `json:"processed"`
	Success    int64      `json:"success"`
	Failed     int64      `json:"failed"`
	Percent    float32    `json:"percent"`
	StartTime  time.Time  `json:"start_time"`
	EndTime    *time.Time `json:"end_time,omitempty"`
	Duration   *int64     `json:"duration_seconds,omitempty"`
	RetryCount int        `json:"retry_count"`
	Partitions int        `json:"partitions"`
}

// BatchExecutionContext represents the execution context for batch processing
type BatchExecutionContext struct {
	Context      context.Context
	Job          *model.JobM
	Store        store.IStore
	BatchID      string
	Phase        string
	WorkerPool   chan struct{}
	ErrorHandler ErrorHandler
}

// BatchReader interface for reading data
type BatchReader[T any] interface {
	// Read reads data from the source with pagination
	Read(ctx context.Context, offset int64, limit int) ([]T, error)
	// HasNext checks if there's more data to read
	HasNext(ctx context.Context, offset int64) (bool, error)
	// Close closes the reader
	Close(ctx context.Context) error
}

// BatchProcessor interface for processing data
type BatchProcessor[T, R any] interface {
	// Process processes a batch of items
	Process(ctx context.Context, items []T) ([]R, error)
	// SetConfig sets processor configuration
	SetConfig(config interface{}) error
}

// BatchWriter interface for writing data
type BatchWriter[T any] interface {
	// Write writes processed data to destination
	Write(ctx context.Context, items []T) error
	// Flush ensures all data is written
	Flush(ctx context.Context) error
	// Close closes the writer
	Close(ctx context.Context) error
}

// BatchStep interface represents a complete batch processing step
type BatchStep[T, R any] interface {
	// Execute executes the step with the given context
	Execute(ctx *BatchExecutionContext) error
	// GetReader returns the data reader
	GetReader() BatchReader[T]
	// GetProcessor returns the data processor
	GetProcessor() BatchProcessor[T, R]
	// GetWriter returns the data writer
	GetWriter() BatchWriter[R]
	// GetStatistics returns current statistics
	GetStatistics() *PhaseStatistics
	// CanPause checks if the step can be paused
	CanPause() bool
	// Pause pauses the step execution
	Pause(ctx context.Context) error
	// Resume resumes the step execution
	Resume(ctx context.Context) error
	// Cancel cancels the step execution
	Cancel(ctx context.Context) error
}

// ErrorHandler interface for handling errors during processing
type ErrorHandler interface {
	// HandleError handles an error and returns whether to retry
	HandleError(ctx context.Context, err error, retryCount int) (shouldRetry bool, delay time.Duration)
	// ShouldFail determines if processing should fail permanently
	ShouldFail(err error, retryCount int) bool
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxRetries    int           `json:"max_retries"`
	BaseDelay     time.Duration `json:"base_delay"`
	MaxDelay      time.Duration `json:"max_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
}

// ConcurrentExecutor interface for concurrent execution
type ConcurrentExecutor interface {
	// ExecuteStep executes a batch step with concurrency control
	ExecuteStep(ctx *BatchExecutionContext, step BatchStep[any, any]) error
	// SetWorkerPool sets the worker pool size
	SetWorkerPool(size int)
	// SetRetryPolicy sets the retry policy
	SetRetryPolicy(policy *RetryPolicy)
}

// PartitionManager interface for managing partitions
type PartitionManager interface {
	// CreatePartitionTasks creates partition tasks for distributed processing
	CreatePartitionTasks(batchID string, totalCount int64, batchSize int64) ([]*PartitionTask, error)
	// DistributeToPartitions distributes messages to partitions using consistent hashing
	DistributeToPartitions(messages []MessageData) (map[string][]MessageData, error)
	// GetPartitionStats returns statistics for partition tasks
	GetPartitionStats(tasks []*PartitionTask) map[string]interface{}
	// IsPartitionComplete checks if a partition is complete
	IsPartitionComplete(batchID, partitionKey string) (bool, error)
	// GetPartitionProgress returns progress for a specific partition
	GetPartitionProgress(batchID, partitionKey string) (*PhaseStatistics, error)
}

// StateManager interface for managing FSM state
type StateManager interface {
	// GetCurrentState returns the current state
	GetCurrentState() string
	// CanTransition checks if a state transition is valid
	CanTransition(event string) bool
	// Transition attempts to transition to a new state
	Transition(ctx context.Context, event string) error
	// GetValidEvents returns valid events for the current state
	GetValidEvents() []string
}

// MonitoringCollector interface for collecting metrics
type MonitoringCollector interface {
	// RecordPhaseStart records the start of a phase
	RecordPhaseStart(batchID, phase string)
	// RecordPhaseComplete records the completion of a phase
	RecordPhaseComplete(batchID, phase string, stats *PhaseStatistics)
	// RecordError records an error
	RecordError(batchID, phase string, err error)
	// RecordRetry records a retry attempt
	RecordRetry(batchID, phase string, retryCount int)
}

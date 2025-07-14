package batch

import (
	"crypto/rand"
	"fmt"
	"time"
)

// generateTaskID generates a unique task ID
func generateTaskID() string {
	timestamp := time.Now().Unix()
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	return fmt.Sprintf("task_%d_%x", timestamp, randomBytes)
}

// TaskStatusString returns string representation of task status
func TaskStatusString(status TaskStatus) string {
	switch status {
	case TaskStatusPending:
		return "pending"
	case TaskStatusProcessing:
		return "processing"
	case TaskStatusCompleted:
		return "completed"
	case TaskStatusFailed:
		return "failed"
	case TaskStatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// TaskTypeString returns string representation of task type
func TaskTypeString(taskType TaskType) string {
	switch taskType {
	case TaskTypeTrain:
		return "train"
	case TaskTypeYouZanOrder:
		return "youzan_order"
	case TaskTypeBatchProcess:
		return "batch_process"
	default:
		return "unknown"
	}
}

// ParseTaskStatus parses string to TaskStatus
func ParseTaskStatus(status string) TaskStatus {
	switch status {
	case "pending":
		return TaskStatusPending
	case "processing":
		return TaskStatusProcessing
	case "completed":
		return TaskStatusCompleted
	case "failed":
		return TaskStatusFailed
	case "cancelled":
		return TaskStatusCancelled
	default:
		return TaskStatusPending
	}
}

// ParseTaskType parses string to TaskType
func ParseTaskType(taskType string) TaskType {
	switch taskType {
	case "train":
		return TaskTypeTrain
	case "youzan_order":
		return TaskTypeYouZanOrder
	case "batch_process":
		return TaskTypeBatchProcess
	default:
		return TaskTypeBatchProcess
	}
}

// ValidateTaskConfig validates task configuration
func ValidateTaskConfig(config *BatchConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got: %d", config.BatchSize)
	}

	if config.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative, got: %d", config.MaxRetries)
	}

	if config.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got: %v", config.Timeout)
	}

	if config.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got: %d", config.Concurrency)
	}

	return nil
}

// CalculateProgress calculates progress percentage
func CalculateProgress(processed, total int) int {
	if total <= 0 {
		return 0
	}
	if processed >= total {
		return 100
	}
	return (processed * 100) / total
}

// FormatDuration formats duration in human readable format
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.0fms", float64(d.Nanoseconds())/1e6)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// IsTerminalStatus checks if task status is terminal (completed, failed, or cancelled)
func IsTerminalStatus(status TaskStatus) bool {
	return status == TaskStatusCompleted || status == TaskStatusFailed || status == TaskStatusCancelled
}

// IsActiveStatus checks if task status is active (pending or processing)
func IsActiveStatus(status TaskStatus) bool {
	return status == TaskStatusPending || status == TaskStatusProcessing
}

// DefaultBatchConfig returns a default batch configuration
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		BatchSize:   10,
		MaxRetries:  3,
		Timeout:     30 * time.Second,
		Concurrency: 2,
	}
}

// MergeBatchConfig merges two batch configurations, with override taking precedence
func MergeBatchConfig(base, override *BatchConfig) *BatchConfig {
	if base == nil {
		base = DefaultBatchConfig()
	}
	if override == nil {
		return base
	}

	result := *base // Copy base config

	if override.BatchSize > 0 {
		result.BatchSize = override.BatchSize
	}
	if override.MaxRetries >= 0 {
		result.MaxRetries = override.MaxRetries
	}
	if override.Timeout > 0 {
		result.Timeout = override.Timeout
	}
	if override.Concurrency > 0 {
		result.Concurrency = override.Concurrency
	}

	return &result
}

// TaskSummary provides a summary of task information
type TaskSummary struct {
	ID        string        `json:"id"`
	Type      string        `json:"type"`
	Status    string        `json:"status"`
	Progress  int           `json:"progress"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
	Duration  time.Duration `json:"duration"`
	HasError  bool          `json:"has_error"`
	ErrorCode string        `json:"error_code,omitempty"`
	ErrorMsg  string        `json:"error_message,omitempty"`
}

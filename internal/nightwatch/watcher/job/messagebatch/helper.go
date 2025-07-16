// Package messagebatch provides helper functions and utilities for message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// RetryConfig defines retry configuration
type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	BackoffFactor float64
	MaxDelay      time.Duration
	Timeout       time.Duration
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  time.Second,
		BackoffFactor: 2.0,
		MaxDelay:      time.Minute,
		Timeout:       time.Minute * 5,
	}
}

// RetryableError defines an error that can be retried
type RetryableError struct {
	Err       error
	Retryable bool
	Code      string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable=%t code=%s error=%s", e.Retryable, e.Code, e.Err.Error())
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// NewRetryableError creates a new retryable error
func NewRetryableError(err error, retryable bool, code string) *RetryableError {
	return &RetryableError{
		Err:       err,
		Retryable: retryable,
		Code:      code,
	}
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if retryErr, ok := err.(*RetryableError); ok {
		return retryErr.Retryable
	}

	// Check for common retryable error patterns
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"timeout",
		"connection refused",
		"network is unreachable",
		"temporary failure",
		"service unavailable",
		"too many requests",
		"rate limit",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// ExecuteWithRetry executes a function with retry logic
func ExecuteWithRetry(
	ctx context.Context,
	config *RetryConfig,
	operation func(ctx context.Context) error,
	logger log.Logger,
	operationName string,
) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	// Create context with timeout
	opCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	var lastErr error
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			logger.Infow("Retrying operation",
				"operation", operationName,
				"attempt", attempt,
				"delay", delay,
				"lastError", lastErr,
			)

			// Wait before retry
			select {
			case <-opCtx.Done():
				return fmt.Errorf("operation timeout: %w", opCtx.Err())
			case <-time.After(delay):
			}
		}

		// Execute operation
		if err := operation(opCtx); err != nil {
			lastErr = err

			// Check if error is retryable
			if !IsRetryableError(err) {
				logger.Errorw("Non-retryable error encountered",
					"operation", operationName,
					"attempt", attempt,
					"error", err,
				)
				return err
			}

			// Calculate next delay with exponential backoff
			if attempt < config.MaxRetries {
				delay = time.Duration(float64(delay) * config.BackoffFactor)
				if delay > config.MaxDelay {
					delay = config.MaxDelay
				}
			}

			continue
		}

		// Success
		if attempt > 0 {
			logger.Infow("Operation succeeded after retry",
				"operation", operationName,
				"attempt", attempt,
			)
		}
		return nil
	}

	logger.Errorw("Operation failed after all retries",
		"operation", operationName,
		"maxRetries", config.MaxRetries,
		"lastError", lastErr,
	)

	return fmt.Errorf("operation failed after %d retries: %w", config.MaxRetries, lastErr)
}

// ValidatePhaseTransition validates if a phase transition is allowed
func ValidatePhaseTransition(fromPhase, toPhase string) error {
	validTransitions := map[string][]string{
		"":            {"PREPARATION"},
		"PREPARATION": {"DELIVERY", "FAILED"},
		"DELIVERY":    {"COMPLETED", "FAILED"},
		"FAILED":      {"PREPARATION", "DELIVERY"}, // Allow restart
		"COMPLETED":   {},                          // Terminal state
	}

	allowedStates, exists := validTransitions[fromPhase]
	if !exists {
		return fmt.Errorf("unknown phase: %s", fromPhase)
	}

	for _, allowed := range allowedStates {
		if allowed == toPhase {
			return nil
		}
	}

	return fmt.Errorf("invalid phase transition from %s to %s", fromPhase, toPhase)
}

// ValidateStateTransition validates if a state transition is allowed within a phase
func ValidateStateTransition(fromState, toState string) error {
	validTransitions := map[string][]string{
		"Pending":   {"Ready", "Failed"},
		"Ready":     {"Running", "Failed"},
		"Running":   {"Pausing", "Completed", "Failed"},
		"Pausing":   {"Paused", "Failed"},
		"Paused":    {"Ready", "Failed"},
		"Completed": {},        // Terminal state
		"Failed":    {"Ready"}, // Allow retry
	}

	allowedStates, exists := validTransitions[fromState]
	if !exists {
		return fmt.Errorf("unknown state: %s", fromState)
	}

	for _, allowed := range allowedStates {
		if allowed == toState {
			return nil
		}
	}

	return fmt.Errorf("invalid state transition from %s to %s", fromState, toState)
}

// CalculatePartitionID calculates partition ID for a given key
func CalculatePartitionID(key string, partitionCount int) int {
	hash := crc32.ChecksumIEEE([]byte(key))
	return int(hash % uint32(partitionCount))
}

// GenerateTaskID generates a unique task ID
func GenerateTaskID(prefix string, partitionID int) string {
	timestamp := time.Now().Unix()
	return fmt.Sprintf("%s_%d_%d", prefix, partitionID, timestamp)
}

// FormatDuration formats duration for human-readable display
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	} else {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
}

// CalculateProgress calculates progress percentage
func CalculateProgress(processed, total int64) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(processed) / float64(total) * 100.0
}

// ExtractPhaseFromState extracts phase from a state string
func ExtractPhaseFromState(state string) string {
	if strings.HasPrefix(state, "Preparation") {
		return "PREPARATION"
	} else if strings.HasPrefix(state, "Delivery") {
		return "DELIVERY"
	}
	return "UNKNOWN"
}

// IsTerminalState checks if a state is terminal (no further transitions)
func IsTerminalState(state string) bool {
	terminalStates := map[string]bool{
		"PreparationCompleted": true,
		"PreparationFailed":    false, // Can be retried
		"DeliveryCompleted":    true,
		"DeliveryFailed":       false, // Can be retried
		"Completed":            true,
		"Failed":               false, // Can be retried
	}

	if terminal, exists := terminalStates[state]; exists {
		return terminal
	}

	return false
}

// ConvertMessageToTask converts MessageData to PartitionTask
func ConvertMessageToTask(messages []MessageData, partitionID int) *PartitionTask {
	if len(messages) == 0 {
		return nil
	}

	task := &PartitionTask{
		ID:           GenerateTaskID("task", partitionID),
		BatchID:      messages[0].ID, // Use first message ID as batch reference
		PartitionKey: fmt.Sprintf("partition_%d", partitionID),
		Status:       "pending",
		MessageCount: int64(len(messages)),
		RetryCount:   0,
		TaskCode:     GenerateTaskCode(messages),
		Metadata:     messages,
	}

	return task
}

// GenerateTaskCode generates a unique task code for messages
func GenerateTaskCode(messages []MessageData) string {
	if len(messages) == 0 {
		return ""
	}

	// Create a hash based on message IDs
	var ids []string
	for _, msg := range messages {
		ids = append(ids, msg.ID)
	}

	combined := strings.Join(ids, ",")
	hash := crc32.ChecksumIEEE([]byte(combined))

	return fmt.Sprintf("task_%x", hash)
}

// SanitizeInput sanitizes user input to prevent injection attacks
func SanitizeInput(input string) string {
	// Remove potentially dangerous characters
	sanitized := strings.ReplaceAll(input, "'", "")
	sanitized = strings.ReplaceAll(sanitized, "\"", "")
	sanitized = strings.ReplaceAll(sanitized, ";", "")
	sanitized = strings.ReplaceAll(sanitized, "--", "")
	sanitized = strings.ReplaceAll(sanitized, "/*", "")
	sanitized = strings.ReplaceAll(sanitized, "*/", "")

	return strings.TrimSpace(sanitized)
}

// ValidateMessageData validates MessageData structure
func ValidateMessageData(msg *MessageData) error {
	if msg == nil {
		return fmt.Errorf("message data is nil")
	}

	if strings.TrimSpace(msg.ID) == "" {
		return fmt.Errorf("message ID is required")
	}

	if strings.TrimSpace(msg.Recipient) == "" {
		return fmt.Errorf("message recipient is required")
	}

	if strings.TrimSpace(msg.Content) == "" {
		return fmt.Errorf("message content is required")
	}

	if strings.TrimSpace(msg.Type) == "" {
		return fmt.Errorf("message type is required")
	}

	// Validate message type
	validTypes := map[string]bool{
		"sms":   true,
		"email": true,
		"push":  true,
	}

	if !validTypes[strings.ToLower(msg.Type)] {
		return fmt.Errorf("invalid message type: %s", msg.Type)
	}

	return nil
}

// ValidatePartitionTask validates PartitionTask structure
func ValidatePartitionTask(task *PartitionTask) error {
	if task == nil {
		return fmt.Errorf("partition task is nil")
	}

	if strings.TrimSpace(task.ID) == "" {
		return fmt.Errorf("task ID is required")
	}

	if strings.TrimSpace(task.BatchID) == "" {
		return fmt.Errorf("batch ID is required")
	}

	if strings.TrimSpace(task.PartitionKey) == "" {
		return fmt.Errorf("partition key is required")
	}

	if task.MessageCount <= 0 {
		return fmt.Errorf("message count must be positive")
	}

	return nil
}

// MergeStatistics merges multiple PhaseStatistics into one
func MergeStatistics(stats ...*PhaseStatistics) *PhaseStatistics {
	if len(stats) == 0 {
		return &PhaseStatistics{}
	}

	merged := &PhaseStatistics{
		StartTime: stats[0].StartTime,
	}

	for _, stat := range stats {
		if stat == nil {
			continue
		}

		merged.Total += stat.Total
		merged.Processed += stat.Processed
		merged.Success += stat.Success
		merged.Failed += stat.Failed
		merged.RetryCount += stat.RetryCount
		merged.Partitions += stat.Partitions

		// Use earliest start time
		if stat.StartTime.Before(merged.StartTime) {
			merged.StartTime = stat.StartTime
		}

		// Use latest end time
		if stat.EndTime != nil {
			if merged.EndTime == nil || stat.EndTime.After(*merged.EndTime) {
				merged.EndTime = stat.EndTime
			}
		}
	}

	// Calculate percentage
	if merged.Total > 0 {
		merged.Percent = float32(merged.Processed) / float32(merged.Total) * 100
	}

	// Calculate duration
	if merged.EndTime != nil {
		duration := int64(merged.EndTime.Sub(merged.StartTime) / time.Second)
		merged.Duration = &duration
	}

	return merged
}

// LogTransition logs state or phase transitions
func LogTransition(logger log.Logger, transitionType, from, to, entity string, metadata map[string]interface{}) {
	logFields := []interface{}{
		"transitionType", transitionType,
		"from", from,
		"to", to,
		"entity", entity,
		"timestamp", time.Now(),
	}

	for key, value := range metadata {
		logFields = append(logFields, key, value)
	}

	logger.Infow("State transition", logFields...)
}

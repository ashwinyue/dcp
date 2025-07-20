package messagebatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// StatusTrackingListener tracks status changes and logs them
type StatusTrackingListener struct {
	logger log.Logger
}

// NewStatusTrackingListener creates a new status tracking listener
func NewStatusTrackingListener() *StatusTrackingListener {
	return &StatusTrackingListener{
		logger: log.New(nil),
	}
}

// GetEventTypes returns the event types this listener is interested in
func (l *StatusTrackingListener) GetEventTypes() []EventType {
	return []EventType{
		EventStatusChange,
		EventPhaseStart,
		EventPhaseComplete,
		EventPhaseFailed,
	}
}

// OnEvent handles the event
func (l *StatusTrackingListener) OnEvent(ctx context.Context, event *BatchEvent) error {
	switch event.Type {
	case EventStatusChange:
		oldStatus, _ := event.Data["old_status"].(string)
		newStatus, _ := event.Data["new_status"].(string)
		l.logger.Infow("Status tracking", 
			"batch_id", event.BatchID,
			"old_status", oldStatus,
			"new_status", newStatus,
			"timestamp", event.Timestamp)
		
	case EventPhaseStart:
		phase, _ := event.Data["phase"].(string)
		l.logger.Infow("Phase started tracking",
			"batch_id", event.BatchID,
			"phase", phase,
			"timestamp", event.Timestamp)
		
	case EventPhaseComplete:
		phase, _ := event.Data["phase"].(string)
		successRate, _ := event.Data["success_rate"].(float64)
		l.logger.Infow("Phase completed tracking",
			"batch_id", event.BatchID,
			"phase", phase,
			"success_rate", successRate,
			"timestamp", event.Timestamp)
		
	case EventPhaseFailed:
		phase, _ := event.Data["phase"].(string)
		errorMsg, _ := event.Data["error"].(string)
		l.logger.Errorw("Phase failed tracking",
			"batch_id", event.BatchID,
			"phase", phase,
			"error", errorMsg,
			"timestamp", event.Timestamp)
	}
	
	return nil
}

// MetricsCollectorListener collects metrics for monitoring
type MetricsCollectorListener struct {
	logger log.Logger
	metrics map[string]interface{}
}

// NewMetricsCollectorListener creates a new metrics collector listener
func NewMetricsCollectorListener() *MetricsCollectorListener {
	return &MetricsCollectorListener{
		logger: log.New(nil),
		metrics: make(map[string]interface{}),
	}
}

// GetEventTypes returns the event types this listener is interested in
func (l *MetricsCollectorListener) GetEventTypes() []EventType {
	return []EventType{
		EventPhaseProgress,
		EventPhaseComplete,
		EventPhaseFailed,
		EventBatchComplete,
		EventBatchFailed,
	}
}

// OnEvent handles the event
func (l *MetricsCollectorListener) OnEvent(ctx context.Context, event *BatchEvent) error {
	switch event.Type {
	case EventPhaseProgress:
		if event.Stats != nil {
			l.collectPhaseMetrics(event.BatchID, event.Phase, event.Stats)
		}
		
	case EventPhaseComplete:
		if event.Stats != nil {
			l.collectPhaseCompletionMetrics(event.BatchID, event.Phase, event.Stats, true)
		}
		
	case EventPhaseFailed:
		if event.Stats != nil {
			l.collectPhaseCompletionMetrics(event.BatchID, event.Phase, event.Stats, false)
		}
		
	case EventBatchComplete:
		l.collectBatchCompletionMetrics(event.BatchID, true)
		
	case EventBatchFailed:
		l.collectBatchCompletionMetrics(event.BatchID, false)
	}
	
	return nil
}

// collectPhaseMetrics collects metrics during phase execution
func (l *MetricsCollectorListener) collectPhaseMetrics(batchID, phase string, stats *PhaseStatistics) {
	key := fmt.Sprintf("%s_%s_progress", batchID, phase)
	l.metrics[key] = map[string]interface{}{
		"total":     stats.Total,
		"processed": stats.Processed,
		"success":   stats.Success,
		"failed":    stats.Failed,
		"percent":   stats.Percent,
		"timestamp": time.Now(),
	}
	
	l.logger.Infow("Phase metrics collected",
		"batch_id", batchID,
		"phase", phase,
		"progress", stats.Percent)
}

// collectPhaseCompletionMetrics collects metrics when phase completes
func (l *MetricsCollectorListener) collectPhaseCompletionMetrics(batchID, phase string, stats *PhaseStatistics, success bool) {
	key := fmt.Sprintf("%s_%s_completion", batchID, phase)
	l.metrics[key] = map[string]interface{}{
		"total":     stats.Total,
		"success":   stats.Success,
		"failed":    stats.Failed,
		"success_rate": float64(stats.Success) / float64(stats.Total) * 100,
		"completed": success,
		"timestamp": time.Now(),
	}
	
	l.logger.Infow("Phase completion metrics collected",
		"batch_id", batchID,
		"phase", phase,
		"success", success,
		"success_rate", float64(stats.Success)/float64(stats.Total)*100)
}

// collectBatchCompletionMetrics collects metrics when batch completes
func (l *MetricsCollectorListener) collectBatchCompletionMetrics(batchID string, success bool) {
	key := fmt.Sprintf("%s_batch_completion", batchID)
	l.metrics[key] = map[string]interface{}{
		"completed": success,
		"timestamp": time.Now(),
	}
	
	l.logger.Infow("Batch completion metrics collected",
		"batch_id", batchID,
		"success", success)
}

// GetMetrics returns collected metrics
func (l *MetricsCollectorListener) GetMetrics() map[string]interface{} {
	return l.metrics
}

// NotificationListener sends notifications for important events
type NotificationListener struct {
	logger log.Logger
	notificationService NotificationService
}

// NotificationService interface for sending notifications
type NotificationService interface {
	SendNotification(ctx context.Context, message string, data map[string]interface{}) error
}

// NewNotificationListener creates a new notification listener
func NewNotificationListener(notificationService NotificationService) *NotificationListener {
	return &NotificationListener{
		logger: log.New(nil),
		notificationService: notificationService,
	}
}

// GetEventTypes returns the event types this listener is interested in
func (l *NotificationListener) GetEventTypes() []EventType {
	return []EventType{
		EventBatchComplete,
		EventBatchFailed,
		EventPhaseFailed,
	}
}

// OnEvent handles the event
func (l *NotificationListener) OnEvent(ctx context.Context, event *BatchEvent) error {
	switch event.Type {
	case EventBatchComplete:
		message := fmt.Sprintf("Batch %s completed successfully", event.BatchID)
		return l.notificationService.SendNotification(ctx, message, event.Data)
		
	case EventBatchFailed:
		message := fmt.Sprintf("Batch %s failed: %s", event.BatchID, event.Error)
		return l.notificationService.SendNotification(ctx, message, event.Data)
		
	case EventPhaseFailed:
		message := fmt.Sprintf("Phase %s failed for batch %s: %s", event.Phase, event.BatchID, event.Error)
		return l.notificationService.SendNotification(ctx, message, event.Data)
	}
	
	return nil
}

// AuditLogListener logs events for audit purposes
type AuditLogListener struct {
	logger log.Logger
}

// NewAuditLogListener creates a new audit log listener
func NewAuditLogListener() *AuditLogListener {
	return &AuditLogListener{
		logger: log.New(nil),
	}
}

// GetEventTypes returns the event types this listener is interested in
func (l *AuditLogListener) GetEventTypes() []EventType {
	return []EventType{
		EventStatusChange,
		EventPhaseStart,
		EventPhaseComplete,
		EventPhaseFailed,
		EventBatchComplete,
		EventBatchFailed,
	}
}

// OnEvent handles the event
func (l *AuditLogListener) OnEvent(ctx context.Context, event *BatchEvent) error {
	// Convert event to JSON for audit logging
	eventData, err := json.Marshal(event)
	if err != nil {
		l.logger.Errorw("Failed to marshal event for audit", "error", err)
		return err
	}
	
	l.logger.Infow("Audit log",
		"event_id", event.ID,
		"batch_id", event.BatchID,
		"event_type", event.Type,
		"phase", event.Phase,
		"timestamp", event.Timestamp,
		"event_data", string(eventData))
	
	return nil
}

// RetryListener handles retry logic for failed operations
type RetryListener struct {
	logger log.Logger
	retryService RetryService
}

// RetryService interface for handling retries
type RetryService interface {
	ScheduleRetry(ctx context.Context, batchID, phase string, retryCount int, delay time.Duration) error
}

// NewRetryListener creates a new retry listener
func NewRetryListener(retryService RetryService) *RetryListener {
	return &RetryListener{
		logger: log.New(nil),
		retryService: retryService,
	}
}

// GetEventTypes returns the event types this listener is interested in
func (l *RetryListener) GetEventTypes() []EventType {
	return []EventType{
		EventPhaseFailed,
	}
}

// OnEvent handles the event
func (l *RetryListener) OnEvent(ctx context.Context, event *BatchEvent) error {
	if event.Type == EventPhaseFailed {
		// Extract retry information from event data
		retryCount, _ := event.Data["retry_count"].(int)
		maxRetries, _ := event.Data["max_retries"].(int)
		
		if maxRetries == 0 {
			maxRetries = 3 // Default max retries
		}
		
		if retryCount < maxRetries {
			// Calculate retry delay (exponential backoff)
			retryDelay := time.Duration(1<<uint(retryCount)) * time.Minute
			
			l.logger.Infow("Scheduling retry for failed phase",
				"batch_id", event.BatchID,
				"phase", event.Phase,
				"retry_count", retryCount+1,
				"retry_delay", retryDelay)
			
			return l.retryService.ScheduleRetry(ctx, event.BatchID, event.Phase, retryCount+1, retryDelay)
		} else {
			l.logger.Errorw("Max retries exceeded for phase",
				"batch_id", event.BatchID,
				"phase", event.Phase,
				"retry_count", retryCount)
		}
	}
	
	return nil
}
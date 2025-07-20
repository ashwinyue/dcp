package messagebatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// DefaultEventBus implements the EventBus interface
type DefaultEventBus struct {
	listeners []EventListener
	mu        sync.RWMutex
	logger    log.Logger
}

// NewEventBus creates a new event bus
func NewEventBus() *DefaultEventBus {
	return &DefaultEventBus{
		listeners: make([]EventListener, 0),
		logger:    log.New(nil),
	}
}

// Subscribe adds an event listener
func (eb *DefaultEventBus) Subscribe(listener EventListener) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.listeners = append(eb.listeners, listener)
	eb.logger.Infow("Event listener subscribed", "event_types", listener.GetEventTypes())
	return nil
}

// Unsubscribe removes an event listener
func (eb *DefaultEventBus) Unsubscribe(listener EventListener) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for i, l := range eb.listeners {
		if l == listener {
			eb.listeners = append(eb.listeners[:i], eb.listeners[i+1:]...)
			eb.logger.Infow("Event listener unsubscribed")
			return nil
		}
	}
	return fmt.Errorf("listener not found")
}

// Publish publishes an event to all interested listeners
func (eb *DefaultEventBus) Publish(ctx context.Context, event *BatchEvent) error {
	eb.mu.RLock()
	listeners := make([]EventListener, len(eb.listeners))
	copy(listeners, eb.listeners)
	eb.mu.RUnlock()

	var errors []error
	for _, listener := range listeners {
		// Check if listener is interested in this event type
		interestedTypes := listener.GetEventTypes()
		interested := false
		for _, eventType := range interestedTypes {
			if eventType == event.Type {
				interested = true
				break
			}
		}

		if interested {
			if err := listener.OnEvent(ctx, event); err != nil {
				eb.logger.Errorw("Event listener error", "event_type", event.Type, "batch_id", event.BatchID, "error", err)
				errors = append(errors, err)
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("event publishing failed with %d errors", len(errors))
	}
	return nil
}

// PublishAsync publishes an event asynchronously
func (eb *DefaultEventBus) PublishAsync(ctx context.Context, event *BatchEvent) {
	go func() {
		if err := eb.Publish(ctx, event); err != nil {
			eb.logger.Errorw("Async event publishing failed", "event_type", event.Type, "batch_id", event.BatchID, "error", err)
		}
	}()
}

// DefaultCallbackHandler implements the CallbackHandler interface
type DefaultCallbackHandler struct {
	eventBus         EventBus
	statusCallbacks  []StatusCallback
	progressCallbacks []ProgressCallback
	errorCallbacks   []ErrorCallback
	completionCallbacks []CompletionCallback
	mu               sync.RWMutex
	logger           log.Logger
}

// NewCallbackHandler creates a new callback handler
func NewCallbackHandler(eventBus EventBus) *DefaultCallbackHandler {
	return &DefaultCallbackHandler{
		eventBus:            eventBus,
		statusCallbacks:     make([]StatusCallback, 0),
		progressCallbacks:   make([]ProgressCallback, 0),
		errorCallbacks:      make([]ErrorCallback, 0),
		completionCallbacks: make([]CompletionCallback, 0),
		logger:              log.New(nil),
	}
}

// RegisterStatusCallback registers a status change callback
func (ch *DefaultCallbackHandler) RegisterStatusCallback(callback StatusCallback) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.statusCallbacks = append(ch.statusCallbacks, callback)
}

// RegisterProgressCallback registers a progress callback
func (ch *DefaultCallbackHandler) RegisterProgressCallback(callback ProgressCallback) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.progressCallbacks = append(ch.progressCallbacks, callback)
}

// RegisterErrorCallback registers an error callback
func (ch *DefaultCallbackHandler) RegisterErrorCallback(callback ErrorCallback) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.errorCallbacks = append(ch.errorCallbacks, callback)
}

// RegisterCompletionCallback registers a completion callback
func (ch *DefaultCallbackHandler) RegisterCompletionCallback(callback CompletionCallback) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.completionCallbacks = append(ch.completionCallbacks, callback)
}

// OnPhaseStart called when a phase starts
func (ch *DefaultCallbackHandler) OnPhaseStart(ctx context.Context, batchID, phase string) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventPhaseStart,
		Phase:     phase,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"phase": phase,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)
	ch.logger.Infow("Phase started", "batch_id", batchID, "phase", phase)
	return nil
}

// OnPhaseProgress called during phase execution with progress updates
func (ch *DefaultCallbackHandler) OnPhaseProgress(ctx context.Context, batchID, phase string, stats *PhaseStatistics) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventPhaseProgress,
		Phase:     phase,
		Timestamp: time.Now(),
		Stats:     stats,
		Data: map[string]interface{}{
			"phase":   phase,
			"percent": stats.Percent,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)

	// Execute progress callbacks
	ch.mu.RLock()
	progressCallbacks := make([]ProgressCallback, len(ch.progressCallbacks))
	copy(progressCallbacks, ch.progressCallbacks)
	ch.mu.RUnlock()

	for _, callback := range progressCallbacks {
		if err := callback(ctx, batchID, phase, stats); err != nil {
			ch.logger.Errorw("Progress callback error", "batch_id", batchID, "phase", phase, "error", err)
		}
	}

	return nil
}

// OnPhaseComplete called when a phase completes successfully
func (ch *DefaultCallbackHandler) OnPhaseComplete(ctx context.Context, batchID, phase string, stats *PhaseStatistics) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventPhaseComplete,
		Phase:     phase,
		Timestamp: time.Now(),
		Stats:     stats,
		Data: map[string]interface{}{
			"phase":        phase,
			"success_rate": float64(stats.Success) / float64(stats.Total) * 100,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)
	ch.logger.Infow("Phase completed", "batch_id", batchID, "phase", phase, "success_rate", float64(stats.Success)/float64(stats.Total)*100)
	return nil
}

// OnPhaseFailed called when a phase fails
func (ch *DefaultCallbackHandler) OnPhaseFailed(ctx context.Context, batchID, phase string, err error, stats *PhaseStatistics) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventPhaseFailed,
		Phase:     phase,
		Timestamp: time.Now(),
		Error:     err.Error(),
		Stats:     stats,
		Data: map[string]interface{}{
			"phase": phase,
			"error": err.Error(),
		},
	}

	ch.eventBus.PublishAsync(ctx, event)

	// Execute error callbacks
	ch.mu.RLock()
	errorCallbacks := make([]ErrorCallback, len(ch.errorCallbacks))
	copy(errorCallbacks, ch.errorCallbacks)
	ch.mu.RUnlock()

	for _, callback := range errorCallbacks {
		if shouldRetry, retryDelay := callback(ctx, batchID, phase, err, stats); shouldRetry {
			ch.logger.Infow("Error callback suggests retry", "batch_id", batchID, "phase", phase, "retry_delay", retryDelay)
		}
	}

	ch.logger.Errorw("Phase failed", "batch_id", batchID, "phase", phase, "error", err)
	return nil
}

// OnBatchComplete called when entire batch processing completes
func (ch *DefaultCallbackHandler) OnBatchComplete(ctx context.Context, batchID string, finalStats map[string]*PhaseStatistics) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventBatchComplete,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"final_stats": finalStats,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)

	// Execute completion callbacks
	ch.mu.RLock()
	completionCallbacks := make([]CompletionCallback, len(ch.completionCallbacks))
	copy(completionCallbacks, ch.completionCallbacks)
	ch.mu.RUnlock()

	for _, callback := range completionCallbacks {
		if err := callback(ctx, batchID, true, finalStats); err != nil {
			ch.logger.Errorw("Completion callback error", "batch_id", batchID, "error", err)
		}
	}

	ch.logger.Infow("Batch completed successfully", "batch_id", batchID)
	return nil
}

// OnBatchFailed called when entire batch processing fails
func (ch *DefaultCallbackHandler) OnBatchFailed(ctx context.Context, batchID string, err error, finalStats map[string]*PhaseStatistics) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventBatchFailed,
		Timestamp: time.Now(),
		Error:     err.Error(),
		Data: map[string]interface{}{
			"error":       err.Error(),
			"final_stats": finalStats,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)

	// Execute completion callbacks
	ch.mu.RLock()
	completionCallbacks := make([]CompletionCallback, len(ch.completionCallbacks))
	copy(completionCallbacks, ch.completionCallbacks)
	ch.mu.RUnlock()

	for _, callback := range completionCallbacks {
		if callbackErr := callback(ctx, batchID, false, finalStats); callbackErr != nil {
			ch.logger.Errorw("Completion callback error", "batch_id", batchID, "error", callbackErr)
		}
	}

	ch.logger.Errorw("Batch failed", "batch_id", batchID, "error", err)
	return nil
}

// OnStatusChange called when batch status changes
func (ch *DefaultCallbackHandler) OnStatusChange(ctx context.Context, batchID, oldStatus, newStatus string) error {
	event := &BatchEvent{
		ID:        uuid.New().String(),
		BatchID:   batchID,
		Type:      EventStatusChange,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"old_status": oldStatus,
			"new_status": newStatus,
		},
	}

	ch.eventBus.PublishAsync(ctx, event)

	// Execute status callbacks
	ch.mu.RLock()
	statusCallbacks := make([]StatusCallback, len(ch.statusCallbacks))
	copy(statusCallbacks, ch.statusCallbacks)
	ch.mu.RUnlock()

	for _, callback := range statusCallbacks {
		if err := callback(ctx, batchID, oldStatus, newStatus, nil); err != nil {
			ch.logger.Errorw("Status callback error", "batch_id", batchID, "error", err)
		}
	}

	ch.logger.Infow("Status changed", "batch_id", batchID, "old_status", oldStatus, "new_status", newStatus)
	return nil
}
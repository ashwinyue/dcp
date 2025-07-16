// Package messaging provides advanced message processing capabilities with Kafka integration
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/segmentio/kafka-go"
)

// MessageProcessor handles advanced message processing with Kafka
type MessageProcessor struct {
	kafkaHelper *KafkaHelper
	cache       *cache.CacheManager
	logger      log.Logger

	// Message routing
	routes      map[string]*MessageRoute
	routesMutex sync.RWMutex

	// Statistics
	atomicStats   *syncer.AtomicStatisticsSyncer
	globalCounter *syncer.GlobalCounter

	// Configuration
	config *MessageProcessorConfig

	// Processing state
	isRunning  bool
	stopChan   chan struct{}
	workerPool chan struct{}
	mu         sync.RWMutex
}

// MessageProcessorConfig defines configuration for message processor
type MessageProcessorConfig struct {
	BatchSize         int           `json:"batch_size"`
	MaxRetries        int           `json:"max_retries"`
	RetryInterval     time.Duration `json:"retry_interval"`
	ProcessingTimeout time.Duration `json:"processing_timeout"`
	MaxConcurrency    int           `json:"max_concurrency"`
	EnableDeadLetter  bool          `json:"enable_dead_letter"`
	DeadLetterTopic   string        `json:"dead_letter_topic"`
	EnablePersistence bool          `json:"enable_persistence"`
	PersistencePrefix string        `json:"persistence_prefix"`
}

// DefaultMessageProcessorConfig returns default configuration
func DefaultMessageProcessorConfig() *MessageProcessorConfig {
	return &MessageProcessorConfig{
		BatchSize:         100,
		MaxRetries:        3,
		RetryInterval:     5 * time.Second,
		ProcessingTimeout: 30 * time.Second,
		MaxConcurrency:    10,
		EnableDeadLetter:  true,
		DeadLetterTopic:   "dead-letter-queue",
		EnablePersistence: true,
		PersistencePrefix: "msg_persistence:",
	}
}

// MessageRoute defines routing configuration for a message type
type MessageRoute struct {
	MessageType    string                                         `json:"message_type"`
	Topic          string                                         `json:"topic"`
	Partition      int                                            `json:"partition"`
	Handler        func(context.Context, *ProcessedMessage) error `json:"-"`
	FilterFunc     func(*ProcessedMessage) bool                   `json:"-"`
	Priority       int                                            `json:"priority"`
	MaxRetries     int                                            `json:"max_retries"`
	RetryInterval  time.Duration                                  `json:"retry_interval"`
	EnableBatching bool                                           `json:"enable_batching"`
	BatchSize      int                                            `json:"batch_size"`
	BatchTimeout   time.Duration                                  `json:"batch_timeout"`
}

// ProcessedMessage represents a message being processed
type ProcessedMessage struct {
	ID           string            `json:"id"`
	Key          string            `json:"key"`
	MessageType  string            `json:"message_type"`
	Payload      interface{}       `json:"payload"`
	Headers      map[string]string `json:"headers"`
	Timestamp    time.Time         `json:"timestamp"`
	RetryCount   int               `json:"retry_count"`
	LastError    string            `json:"last_error,omitempty"`
	Status       MessageStatus     `json:"status"`
	BatchID      string            `json:"batch_id,omitempty"`
	PartitionKey string            `json:"partition_key,omitempty"`
	Priority     int               `json:"priority"`

	// Processing metadata
	ProcessingStartTime *time.Time     `json:"processing_start_time,omitempty"`
	ProcessingEndTime   *time.Time     `json:"processing_end_time,omitempty"`
	ProcessingDuration  *time.Duration `json:"processing_duration,omitempty"`
}

// MessageStatus represents the status of a message
type MessageStatus string

const (
	MessageStatusPending    MessageStatus = "pending"
	MessageStatusProcessing MessageStatus = "processing"
	MessageStatusCompleted  MessageStatus = "completed"
	MessageStatusFailed     MessageStatus = "failed"
	MessageStatusRetrying   MessageStatus = "retrying"
	MessageStatusDeadLetter MessageStatus = "dead_letter"
)

// BatchContext represents a batch of messages being processed together
type BatchContext struct {
	BatchID        string              `json:"batch_id"`
	Messages       []*ProcessedMessage `json:"messages"`
	StartTime      time.Time           `json:"start_time"`
	Route          *MessageRoute       `json:"route"`
	TotalCount     int                 `json:"total_count"`
	ProcessedCount int                 `json:"processed_count"`
	FailedCount    int                 `json:"failed_count"`
}

// NewMessageProcessor creates a new message processor
func NewMessageProcessor(
	kafkaHelper *KafkaHelper,
	cache *cache.CacheManager,
	logger log.Logger,
	config *MessageProcessorConfig,
) *MessageProcessor {
	if config == nil {
		config = DefaultMessageProcessorConfig()
	}

	// Initialize atomic statistics syncer
	atomicStats := syncer.NewAtomicStatisticsSyncer(
		cache,
		logger,
		"message_processor:",
	)

	// Initialize global counter
	globalCounter := syncer.NewGlobalCounter(
		cache,
		logger,
		"global_msg:",
	)

	return &MessageProcessor{
		kafkaHelper:   kafkaHelper,
		cache:         cache,
		logger:        logger,
		routes:        make(map[string]*MessageRoute),
		atomicStats:   atomicStats,
		globalCounter: globalCounter,
		config:        config,
		stopChan:      make(chan struct{}),
		workerPool:    make(chan struct{}, config.MaxConcurrency),
	}
}

// RegisterRoute registers a message route
func (mp *MessageProcessor) RegisterRoute(route *MessageRoute) error {
	if route == nil {
		return fmt.Errorf("route cannot be nil")
	}

	if route.MessageType == "" {
		return fmt.Errorf("message type cannot be empty")
	}

	if route.Handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// Set default values
	if route.MaxRetries == 0 {
		route.MaxRetries = mp.config.MaxRetries
	}
	if route.RetryInterval == 0 {
		route.RetryInterval = mp.config.RetryInterval
	}
	if route.BatchSize == 0 {
		route.BatchSize = mp.config.BatchSize
	}
	if route.BatchTimeout == 0 {
		route.BatchTimeout = 10 * time.Second
	}

	mp.routesMutex.Lock()
	mp.routes[route.MessageType] = route
	mp.routesMutex.Unlock()

	mp.logger.Infow("Message route registered",
		"message_type", route.MessageType,
		"topic", route.Topic,
		"priority", route.Priority,
		"enable_batching", route.EnableBatching,
	)

	return nil
}

// UnregisterRoute removes a message route
func (mp *MessageProcessor) UnregisterRoute(messageType string) {
	mp.routesMutex.Lock()
	delete(mp.routes, messageType)
	mp.routesMutex.Unlock()

	mp.logger.Infow("Message route unregistered",
		"message_type", messageType,
	)
}

// GetRoute retrieves a message route
func (mp *MessageProcessor) GetRoute(messageType string) (*MessageRoute, bool) {
	mp.routesMutex.RLock()
	route, exists := mp.routes[messageType]
	mp.routesMutex.RUnlock()
	return route, exists
}

// PublishMessage publishes a message with routing and persistence
func (mp *MessageProcessor) PublishMessage(ctx context.Context, message *ProcessedMessage) error {
	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Generate message ID if not provided
	if message.ID == "" {
		msgID, err := mp.globalCounter.GetCustomCounter(ctx, "message_id")
		if err != nil {
			return fmt.Errorf("failed to generate message ID: %w", err)
		}
		message.ID = fmt.Sprintf("msg_%d_%d", msgID, time.Now().Unix())
	}

	// Set timestamp if not provided
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now()
	}

	// Set initial status
	message.Status = MessageStatusPending

	// Get route configuration
	route, exists := mp.GetRoute(message.MessageType)
	if !exists {
		return fmt.Errorf("no route found for message type: %s", message.MessageType)
	}

	// Apply message filter if configured
	if route.FilterFunc != nil && !route.FilterFunc(message) {
		mp.logger.Debugw("Message filtered out",
			"message_id", message.ID,
			"message_type", message.MessageType,
		)
		return nil
	}

	// Persist message if enabled
	if mp.config.EnablePersistence {
		if err := mp.persistMessage(ctx, message); err != nil {
			mp.logger.Errorw("Failed to persist message",
				"message_id", message.ID,
				"error", err,
			)
			// Continue processing even if persistence fails
		}
	}

	// Create Kafka message
	kafkaMessage := &BatchEvent{
		EventID:   message.ID,
		EventType: message.MessageType,
		BatchID:   message.BatchID,
		Timestamp: message.Timestamp,
		MessageData: map[string]interface{}{
			"payload":  message.Payload,
			"priority": message.Priority,
		},
		Metadata: message.Headers,
	}

	// Determine topic and partition
	topic := route.Topic
	if topic == "" {
		topic = mp.kafkaHelper.options.Topic
	}

	// Publish to Kafka
	key := message.Key
	if key == "" {
		key = message.ID
	}

	if err := mp.kafkaHelper.PublishBatchEvent(ctx, kafkaMessage); err != nil {
		// Update statistics
		mp.updateMessageStatistics(message.BatchID, 0, 1, false)

		mp.logger.Errorw("Failed to publish message to Kafka",
			"message_id", message.ID,
			"message_type", message.MessageType,
			"topic", topic,
			"error", err,
		)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Update statistics
	mp.updateMessageStatistics(message.BatchID, 1, 0, false)

	mp.logger.Debugw("Message published successfully",
		"message_id", message.ID,
		"message_type", message.MessageType,
		"topic", topic,
		"batch_id", message.BatchID,
	)

	return nil
}

// StartProcessing starts the message processing loop
func (mp *MessageProcessor) StartProcessing(ctx context.Context) error {
	mp.mu.Lock()
	if mp.isRunning {
		mp.mu.Unlock()
		return fmt.Errorf("message processor is already running")
	}
	mp.isRunning = true
	mp.mu.Unlock()

	mp.logger.Infow("Starting message processor",
		"max_concurrency", mp.config.MaxConcurrency,
		"batch_size", mp.config.BatchSize,
	)

	// Start message consumption
	go mp.consumeMessages(ctx)

	return nil
}

// StopProcessing stops the message processing
func (mp *MessageProcessor) StopProcessing() error {
	mp.mu.Lock()
	if !mp.isRunning {
		mp.mu.Unlock()
		return fmt.Errorf("message processor is not running")
	}
	mp.isRunning = false
	mp.mu.Unlock()

	close(mp.stopChan)

	mp.logger.Infow("Message processor stopped")
	return nil
}

// consumeMessages consumes messages from Kafka and processes them
func (mp *MessageProcessor) consumeMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			mp.logger.Infow("Message consumption stopped due to context cancellation")
			return
		case <-mp.stopChan:
			mp.logger.Infow("Message consumption stopped")
			return
		default:
			if err := mp.kafkaHelper.ConsumeMessages(ctx, mp.handleKafkaMessage); err != nil {
				mp.logger.Errorw("Error consuming messages",
					"error", err,
				)
				// Wait before retrying
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// handleKafkaMessage handles a single Kafka message
func (mp *MessageProcessor) handleKafkaMessage(kafkaMsg kafka.Message) error {
	// Parse batch event
	var batchEvent BatchEvent
	if err := json.Unmarshal(kafkaMsg.Value, &batchEvent); err != nil {
		mp.logger.Errorw("Failed to unmarshal Kafka message",
			"error", err,
			"message_key", string(kafkaMsg.Key),
		)
		return err
	}

	// Convert to ProcessedMessage
	processedMsg := &ProcessedMessage{
		ID:          batchEvent.EventID,
		Key:         string(kafkaMsg.Key),
		MessageType: batchEvent.EventType,
		Payload:     batchEvent.MessageData["payload"],
		Headers:     batchEvent.Metadata,
		Timestamp:   batchEvent.Timestamp,
		Status:      MessageStatusProcessing,
		BatchID:     batchEvent.BatchID,
		Priority:    0, // Default priority
	}

	// Extract priority from message data if available
	if priority, ok := batchEvent.MessageData["priority"].(int); ok {
		processedMsg.Priority = priority
	}

	// Load persisted message details if available
	if mp.config.EnablePersistence {
		if persistedMsg, err := mp.loadPersistedMessage(context.Background(), processedMsg.ID); err == nil {
			// Merge persisted data
			processedMsg.RetryCount = persistedMsg.RetryCount
			processedMsg.LastError = persistedMsg.LastError
		}
	}

	// Acquire worker slot
	mp.workerPool <- struct{}{}

	// Process message asynchronously
	go func() {
		defer func() { <-mp.workerPool }()

		if err := mp.processMessage(context.Background(), processedMsg); err != nil {
			mp.logger.Errorw("Failed to process message",
				"message_id", processedMsg.ID,
				"message_type", processedMsg.MessageType,
				"error", err,
			)
		}
	}()

	return nil
}

// processMessage processes a single message
func (mp *MessageProcessor) processMessage(ctx context.Context, message *ProcessedMessage) error {
	startTime := time.Now()
	message.ProcessingStartTime = &startTime

	// Get route
	route, exists := mp.GetRoute(message.MessageType)
	if !exists {
		return fmt.Errorf("no route found for message type: %s", message.MessageType)
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, mp.config.ProcessingTimeout)
	defer cancel()

	// Execute handler
	err := route.Handler(timeoutCtx, message)

	endTime := time.Now()
	message.ProcessingEndTime = &endTime
	duration := endTime.Sub(startTime)
	message.ProcessingDuration = &duration

	if err != nil {
		// Handle processing error
		return mp.handleProcessingError(ctx, message, route, err)
	}

	// Mark as completed
	message.Status = MessageStatusCompleted

	// Update statistics
	mp.updateMessageStatistics(message.BatchID, 1, 0, false)

	// Update persistence
	if mp.config.EnablePersistence {
		mp.persistMessage(ctx, message)
	}

	mp.logger.Debugw("Message processed successfully",
		"message_id", message.ID,
		"message_type", message.MessageType,
		"processing_duration", duration,
	)

	return nil
}

// handleProcessingError handles message processing errors with retry logic
func (mp *MessageProcessor) handleProcessingError(ctx context.Context, message *ProcessedMessage, route *MessageRoute, err error) error {
	message.Status = MessageStatusFailed
	message.LastError = err.Error()
	message.RetryCount++

	// Update statistics
	mp.updateMessageStatistics(message.BatchID, 0, 1, message.RetryCount > 1)

	// Check if should retry
	if message.RetryCount <= route.MaxRetries {
		message.Status = MessageStatusRetrying

		mp.logger.Warnw("Message processing failed, scheduling retry",
			"message_id", message.ID,
			"message_type", message.MessageType,
			"retry_count", message.RetryCount,
			"max_retries", route.MaxRetries,
			"error", err,
		)

		// Schedule retry
		go func() {
			time.Sleep(route.RetryInterval)
			if retryErr := mp.processMessage(ctx, message); retryErr != nil {
				mp.logger.Errorw("Message retry failed",
					"message_id", message.ID,
					"retry_count", message.RetryCount,
					"error", retryErr,
				)
			}
		}()

		return nil
	}

	// Max retries exceeded
	if mp.config.EnableDeadLetter {
		return mp.sendToDeadLetter(ctx, message, err)
	}

	mp.logger.Errorw("Message processing failed permanently",
		"message_id", message.ID,
		"message_type", message.MessageType,
		"retry_count", message.RetryCount,
		"error", err,
	)

	return err
}

// sendToDeadLetter sends failed message to dead letter queue
func (mp *MessageProcessor) sendToDeadLetter(ctx context.Context, message *ProcessedMessage, originalError error) error {
	message.Status = MessageStatusDeadLetter

	deadLetterEvent := &BatchEvent{
		EventID:   fmt.Sprintf("dl_%s", message.ID),
		EventType: "dead_letter",
		BatchID:   message.BatchID,
		Timestamp: time.Now(),
		MessageData: map[string]interface{}{
			"original_message": message,
			"error":            originalError.Error(),
			"failure_time":     time.Now(),
		},
		Metadata: map[string]string{
			"original_message_id":   message.ID,
			"original_message_type": message.MessageType,
			"failure_reason":        originalError.Error(),
		},
	}
	if err := mp.kafkaHelper.PublishBatchEvent(ctx, deadLetterEvent); err != nil {
		mp.logger.Errorw("Failed to send message to dead letter queue",
			"message_id", message.ID,
			"error", err,
		)
		return fmt.Errorf("failed to send to dead letter queue: %w", err)
	}

	mp.logger.Warnw("Message sent to dead letter queue",
		"message_id", message.ID,
		"message_type", message.MessageType,
		"original_error", originalError.Error(),
	)

	return nil
}

// persistMessage persists message to cache
func (mp *MessageProcessor) persistMessage(ctx context.Context, message *ProcessedMessage) error {
	key := mp.config.PersistencePrefix + message.ID
	return mp.cache.Set(ctx, key, message, 24*time.Hour)
}

// loadPersistedMessage loads persisted message from cache
func (mp *MessageProcessor) loadPersistedMessage(ctx context.Context, messageID string) (*ProcessedMessage, error) {
	key := mp.config.PersistencePrefix + messageID
	var message ProcessedMessage
	if err := mp.cache.Get(ctx, key, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// updateMessageStatistics updates processing statistics
func (mp *MessageProcessor) updateMessageStatistics(batchID string, successCount, failureCount int64, isRetry bool) {
	if batchID == "" {
		batchID = "default"
	}

	if err := mp.atomicStats.SyncToCounter(context.Background(), batchID, successCount, failureCount, isRetry); err != nil {
		mp.logger.Errorw("Failed to update message statistics",
			"batch_id", batchID,
			"success_count", successCount,
			"failure_count", failureCount,
			"is_retry", isRetry,
			"error", err,
		)
	}
}

// GetProcessingStatistics returns processing statistics for a batch
func (mp *MessageProcessor) GetProcessingStatistics(batchID string) *syncer.StepStatisticsSnapshot {
	return mp.atomicStats.GetLocalStatistics(batchID)
}

// GetAllStatistics returns statistics for all batches
func (mp *MessageProcessor) GetAllStatistics() map[string]*syncer.StepStatisticsSnapshot {
	return mp.atomicStats.GetAllBatchStatistics()
}

// IsRunning returns whether the processor is running
func (mp *MessageProcessor) IsRunning() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.isRunning
}

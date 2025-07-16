// Package messaging provides Kafka messaging integration for nightwatch
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/onexstack/onexstack/pkg/options"
	"github.com/segmentio/kafka-go"
	"k8s.io/klog/v2"
)

// kafkaLogger implements kafka-go logger interface
type kafkaLogger struct {
	v int32
}

func (l *kafkaLogger) Printf(format string, args ...any) {
	klog.V(klog.Level(l.v)).Infof(format, args...)
}

// KafkaHelper provides kafka producer and consumer functionality
type KafkaHelper struct {
	options  *options.KafkaOptions
	logger   log.Logger
	producer *kafka.Writer
	consumer *kafka.Reader
	mu       sync.RWMutex
	stats    *KafkaStats
}

// KafkaStats holds kafka operation statistics
type KafkaStats struct {
	MessagesProduced int64     `json:"messages_produced"`
	MessagesConsumed int64     `json:"messages_consumed"`
	ProduceErrors    int64     `json:"produce_errors"`
	ConsumeErrors    int64     `json:"consume_errors"`
	LastProduced     time.Time `json:"last_produced"`
	LastConsumed     time.Time `json:"last_consumed"`
	mu               sync.RWMutex
}

// BatchEvent represents a batch processing event
type BatchEvent struct {
	EventID     string                 `json:"event_id"`
	EventType   string                 `json:"event_type"`
	BatchID     string                 `json:"batch_id"`
	JobID       string                 `json:"job_id"`
	Phase       string                 `json:"phase"`
	Status      string                 `json:"status"`
	MessageData map[string]interface{} `json:"message_data"`
	Timestamp   time.Time              `json:"timestamp"`
	Metadata    map[string]string      `json:"metadata,omitempty"`
}

// NewKafkaHelper creates a new kafka helper with staging KafkaOptions
func NewKafkaHelper(kafkaOpts *options.KafkaOptions, logger log.Logger) (*KafkaHelper, error) {
	if kafkaOpts == nil {
		return nil, fmt.Errorf("kafka options cannot be nil")
	}

	if err := kafkaOpts.Validate(); len(err) > 0 {
		return nil, fmt.Errorf("kafka options validation failed: %v", err)
	}

	helper := &KafkaHelper{
		options: kafkaOpts,
		logger:  logger,
		stats:   &KafkaStats{},
	}

	// Initialize producer
	if err := helper.initializeProducer(); err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer: %w", err)
	}

	// Initialize consumer
	if err := helper.initializeConsumer(); err != nil {
		return nil, fmt.Errorf("failed to initialize kafka consumer: %w", err)
	}

	logger.Infow("Kafka helper初始化成功",
		"brokers", kafkaOpts.Brokers,
		"topic", kafkaOpts.Topic,
		"client_id", kafkaOpts.ClientID,
	)

	return helper, nil
}

// initializeProducer creates kafka producer using staging KafkaOptions
func (k *KafkaHelper) initializeProducer() error {
	writer, err := k.options.Writer()
	if err != nil {
		return fmt.Errorf("failed to create kafka writer: %w", err)
	}

	k.producer = writer
	return nil
}

// initializeConsumer creates kafka consumer using staging KafkaOptions
func (k *KafkaHelper) initializeConsumer() error {
	dialer, err := k.options.Dialer()
	if err != nil {
		return fmt.Errorf("failed to create kafka dialer: %w", err)
	}

	// Create kafka reader config
	config := kafka.ReaderConfig{
		Brokers:           k.options.Brokers,
		Topic:             k.options.Topic,
		GroupID:           k.options.ReaderOptions.GroupID,
		Partition:         k.options.ReaderOptions.Partition,
		Dialer:            dialer,
		QueueCapacity:     k.options.ReaderOptions.QueueCapacity,
		MinBytes:          k.options.ReaderOptions.MinBytes,
		MaxBytes:          k.options.ReaderOptions.MaxBytes,
		MaxWait:           k.options.ReaderOptions.MaxWait,
		ReadBatchTimeout:  k.options.ReaderOptions.ReadBatchTimeout,
		HeartbeatInterval: k.options.ReaderOptions.HeartbeatInterval,
		CommitInterval:    k.options.ReaderOptions.CommitInterval,
		RebalanceTimeout:  k.options.ReaderOptions.RebalanceTimeout,
		StartOffset:       k.options.ReaderOptions.StartOffset,
		MaxAttempts:       k.options.ReaderOptions.MaxAttempts,
		Logger:            &kafkaLogger{4},
		ErrorLogger:       &kafkaLogger{1},
	}

	reader := kafka.NewReader(config)
	k.consumer = reader
	return nil
}

// PublishMessage publishes a single message to kafka
func (k *KafkaHelper) PublishMessage(ctx context.Context, key string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		k.updateStats(false, true, false, false)
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	kafkaMessage := kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	if err := k.producer.WriteMessages(ctx, kafkaMessage); err != nil {
		k.updateStats(false, true, false, false)
		k.logger.Errorw("Failed to publish message",
			"key", key,
			"error", err,
		)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	k.updateStats(true, false, false, false)
	k.logger.Debugw("Message published successfully",
		"key", key,
		"message_size", len(data),
	)

	return nil
}

// PublishBatchEvent publishes a batch processing event
func (k *KafkaHelper) PublishBatchEvent(ctx context.Context, event *BatchEvent) error {
	if event == nil {
		return fmt.Errorf("batch event cannot be nil")
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Use event ID as kafka message key
	key := event.EventID
	if key == "" {
		key = fmt.Sprintf("%s_%s_%d", event.BatchID, event.EventType, event.Timestamp.Unix())
	}

	return k.PublishMessage(ctx, key, event)
}

// ConsumeMessages consumes messages from kafka with callback processing
func (k *KafkaHelper) ConsumeMessages(ctx context.Context, handler func(kafka.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			k.logger.Infow("Kafka消费被取消", "context", ctx.Err())
			return ctx.Err()
		default:
			message, err := k.consumer.ReadMessage(ctx)
			if err != nil {
				k.updateStats(false, false, false, true)
				k.logger.Errorw("Failed to read message",
					"error", err,
				)
				continue
			}

			// Process message with handler
			if err := handler(message); err != nil {
				k.updateStats(false, false, false, true)
				k.logger.Errorw("Failed to process message",
					"key", string(message.Key),
					"offset", message.Offset,
					"error", err,
				)
				continue
			}

			k.updateStats(false, false, true, false)
			k.logger.Debugw("Message processed successfully",
				"key", string(message.Key),
				"offset", message.Offset,
				"partition", message.Partition,
			)
		}
	}
}

// updateStats updates kafka operation statistics
func (k *KafkaHelper) updateStats(produced, produceError, consumed, consumeError bool) {
	k.stats.mu.Lock()
	defer k.stats.mu.Unlock()

	if produced {
		k.stats.MessagesProduced++
		k.stats.LastProduced = time.Now()
	}
	if produceError {
		k.stats.ProduceErrors++
	}
	if consumed {
		k.stats.MessagesConsumed++
		k.stats.LastConsumed = time.Now()
	}
	if consumeError {
		k.stats.ConsumeErrors++
	}
}

// GetStats returns current kafka statistics
func (k *KafkaHelper) GetStats() KafkaStats {
	k.stats.mu.RLock()
	defer k.stats.mu.RUnlock()

	return KafkaStats{
		MessagesProduced: k.stats.MessagesProduced,
		MessagesConsumed: k.stats.MessagesConsumed,
		ProduceErrors:    k.stats.ProduceErrors,
		ConsumeErrors:    k.stats.ConsumeErrors,
		LastProduced:     k.stats.LastProduced,
		LastConsumed:     k.stats.LastConsumed,
	}
}

// Close gracefully closes kafka connections
func (k *KafkaHelper) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	var errs []error

	if k.producer != nil {
		if err := k.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
		}
		k.producer = nil
	}

	if k.consumer != nil {
		if err := k.consumer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close consumer: %w", err))
		}
		k.consumer = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka connections: %v", errs)
	}

	k.logger.Infow("Kafka connections closed successfully")
	return nil
}

// GetTopic returns the configured topic
func (k *KafkaHelper) GetTopic() string {
	return k.options.Topic
}

// GetBrokers returns the configured brokers
func (k *KafkaHelper) GetBrokers() []string {
	return k.options.Brokers
}

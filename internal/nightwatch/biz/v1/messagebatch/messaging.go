package messagebatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/messaging"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/segmentio/kafka-go"
)

// MessagingService 提供Kafka消息相关的业务服务
type MessagingService struct {
	kafka  *messaging.KafkaHelper
	logger log.Logger
}

// NewMessagingService 创建新的消息服务
func NewMessagingService(kafka *messaging.KafkaHelper, logger log.Logger) *MessagingService {
	return &MessagingService{
		kafka:  kafka,
		logger: logger,
	}
}

// MessageBatchEvent 表示消息批次事件
type MessageBatchEvent struct {
	BatchID     string                 `json:"batch_id"`
	EventType   string                 `json:"event_type"`
	Timestamp   time.Time              `json:"timestamp"`
	PartitionID int                    `json:"partition_id"`
	MessageID   string                 `json:"message_id"`
	Content     map[string]interface{} `json:"content"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// SendBatchStartEvent 发送批次开始事件
func (m *MessagingService) SendBatchStartEvent(ctx context.Context, batchID string, partitionID int) error {
	event := &MessageBatchEvent{
		BatchID:     batchID,
		EventType:   "batch_start",
		Timestamp:   time.Now(),
		PartitionID: partitionID,
		Content: map[string]interface{}{
			"status": "started",
		},
	}

	return m.sendEvent(ctx, "batch_events", event)
}

// SendBatchCompleteEvent 发送批次完成事件
func (m *MessagingService) SendBatchCompleteEvent(ctx context.Context, batchID string, partitionID int, messageCount int64) error {
	event := &MessageBatchEvent{
		BatchID:     batchID,
		EventType:   "batch_complete",
		Timestamp:   time.Now(),
		PartitionID: partitionID,
		Content: map[string]interface{}{
			"status":        "completed",
			"message_count": messageCount,
		},
	}

	return m.sendEvent(ctx, "batch_events", event)
}

// SendBatchErrorEvent 发送批次错误事件
func (m *MessagingService) SendBatchErrorEvent(ctx context.Context, batchID string, partitionID int, errorMsg string) error {
	event := &MessageBatchEvent{
		BatchID:     batchID,
		EventType:   "batch_error",
		Timestamp:   time.Now(),
		PartitionID: partitionID,
		Content: map[string]interface{}{
			"status": "error",
			"error":  errorMsg,
		},
	}

	return m.sendEvent(ctx, "batch_events", event)
}

// SendMessageProcessedEvent 发送消息处理事件
func (m *MessagingService) SendMessageProcessedEvent(ctx context.Context, messageID string, batchID string, partitionID int) error {
	event := &MessageBatchEvent{
		BatchID:     batchID,
		EventType:   "message_processed",
		Timestamp:   time.Now(),
		PartitionID: partitionID,
		MessageID:   messageID,
		Content: map[string]interface{}{
			"status": "processed",
		},
	}

	return m.sendEvent(ctx, "message_events", event)
}

// SendCustomMessage 发送自定义消息
func (m *MessagingService) SendCustomMessage(ctx context.Context, topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		m.logger.Errorw("Failed to marshal custom message", "error", err, "topic", topic)
		return fmt.Errorf("failed to marshal custom message: %w", err)
	}

	err = m.kafka.PublishMessage(ctx, topic, data)
	if err != nil {
		m.logger.Errorw("Failed to send custom message", "error", err, "topic", topic)
		return fmt.Errorf("failed to send custom message: %w", err)
	}

	m.logger.Infow("Custom message sent successfully", "topic", topic)
	return nil
}

// sendEvent 内部方法，发送事件到指定topic
func (m *MessagingService) sendEvent(ctx context.Context, topic string, event *MessageBatchEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		m.logger.Errorw("Failed to marshal event", "error", err, "eventType", event.EventType)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = m.kafka.PublishMessage(ctx, topic, data)
	if err != nil {
		m.logger.Errorw("Failed to send event", "error", err, "eventType", event.EventType, "topic", topic)
		return fmt.Errorf("failed to send event: %w", err)
	}

	m.logger.Infow("Event sent successfully",
		"eventType", event.EventType,
		"batchID", event.BatchID,
		"partitionID", event.PartitionID,
		"topic", topic)
	return nil
}

// ConsumeMessages 消费消息的业务逻辑
func (m *MessagingService) ConsumeMessages(ctx context.Context, handler func([]byte) error) error {
	m.logger.Infow("Starting message consumption")

	// 包装handler以匹配kafka.Message类型
	kafkaHandler := func(message kafka.Message) error {
		return handler(message.Value)
	}

	return m.kafka.ConsumeMessages(ctx, kafkaHandler)
}

// GetMessagingStats 获取消息统计信息
func (m *MessagingService) GetMessagingStats() map[string]interface{} {
	if m.kafka == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}

	stats := m.kafka.GetStats()
	return map[string]interface{}{
		"messages_produced": stats.MessagesProduced,
		"messages_consumed": stats.MessagesConsumed,
		"produce_errors":    stats.ProduceErrors,
		"consume_errors":    stats.ConsumeErrors,
		"last_produced":     stats.LastProduced,
		"last_consumed":     stats.LastConsumed,
	}
}

// Close 关闭消息服务
func (m *MessagingService) Close() error {
	if m.kafka != nil {
		return m.kafka.Close()
	}
	return nil
}

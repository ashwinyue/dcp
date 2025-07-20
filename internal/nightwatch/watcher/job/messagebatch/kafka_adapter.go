package messagebatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ashwinyue/dcp/internal/pkg/log"
	genericoptions "github.com/onexstack/onexstack/pkg/options"
)

// KafkaAdapter Kafka适配器，用于替代Java项目中的Service Bus
type KafkaAdapter struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	options *genericoptions.KafkaOptions
	logger  log.Logger
}

// NewKafkaAdapter 创建Kafka适配器
func NewKafkaAdapter(options *genericoptions.KafkaOptions, logger log.Logger) (*KafkaAdapter, error) {
	if options == nil {
		return nil, fmt.Errorf("kafka options cannot be nil")
	}

	// 创建Kafka writer
	writer, err := options.Writer()
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka writer: %w", err)
	}

	// 创建Kafka reader
	reader, err := options.Reader()
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka reader: %w", err)
	}

	return &KafkaAdapter{
		writer:  writer,
		reader:  reader,
		options: options,
		logger:  logger,
	}, nil
}

// MessageType 消息类型枚举，对应Java项目中的不同消息类型
type MessageType string

const (
	// SMS相关消息类型
	MessageTypeSMSDelivery   MessageType = "SMS_DELIVERY"
	MessageTypeSMSFeedback   MessageType = "SMS_FEEDBACK"
	MessageTypeSMSBatch      MessageType = "SMS_BATCH"
	
	// CDP相关消息类型
	MessageTypeCDPSMS        MessageType = "CDP_SMS"
	MessageTypeCDPMMS        MessageType = "CDP_MMS"
	
	// 批处理相关消息类型
	MessageTypeBatchDelivery MessageType = "BATCH_DELIVERY"
	MessageTypeBatchStatus   MessageType = "BATCH_STATUS"
	MessageTypeBatchProgress MessageType = "BATCH_PROGRESS"
	MessageTypeBatchComplete MessageType = "BATCH_COMPLETE"
	MessageTypeBatchFailed   MessageType = "BATCH_FAILED"
)

// KafkaMessage Kafka消息结构，对应Java项目中的消息格式
type KafkaMessage struct {
	MessageID   string                 `json:"messageId"`
	MessageType MessageType            `json:"messageType"`
	JobID       string                 `json:"jobId"`
	BatchID     string                 `json:"batchId"`
	Timestamp   time.Time              `json:"timestamp"`
	Payload     map[string]interface{} `json:"payload"`
	Headers     map[string]string      `json:"headers,omitempty"`
	RetryCount  int32                  `json:"retryCount,omitempty"`
}

// SMSDeliveryMessage SMS投递消息，对应Java项目中的SMS投递任务
type SMSDeliveryMessage struct {
	JobID       string    `json:"jobId"`
	BatchID     string    `json:"batchId"`
	PhoneNumber string    `json:"phoneNumber"`
	Message     string    `json:"message"`
	Template    string    `json:"template"`
	Priority    int32     `json:"priority"`
	ScheduledAt time.Time `json:"scheduledAt"`
	RetryCount  int32     `json:"retryCount"`
}

// SMSFeedbackMessage SMS反馈消息，对应Java项目中的SMS状态回调
type SMSFeedbackMessage struct {
	JobID        string    `json:"jobId"`
	BatchID      string    `json:"batchId"`
	PhoneNumber  string    `json:"phoneNumber"`
	Status       string    `json:"status"`
	DeliveryTime time.Time `json:"deliveryTime"`
	ErrorCode    string    `json:"errorCode,omitempty"`
	ErrorMessage string    `json:"errorMessage,omitempty"`
	Provider     string    `json:"provider"`
}

// BatchStatusMessage 批处理状态消息
type BatchStatusMessage struct {
	JobID          string    `json:"jobId"`
	BatchID        string    `json:"batchId"`
	Phase          string    `json:"phase"`
	Status         string    `json:"status"`
	Progress       float32   `json:"progress"`
	TotalCount     int64     `json:"totalCount"`
	ProcessedCount int64     `json:"processedCount"`
	SuccessCount   int64     `json:"successCount"`
	FailedCount    int64     `json:"failedCount"`
	Timestamp      time.Time `json:"timestamp"`
	ErrorMessage   string    `json:"errorMessage,omitempty"`
}

// SendSMSDeliveryMessage 发送SMS投递消息，对应Java项目中的sendBatchDeliveryTask
func (adapter *KafkaAdapter) SendSMSDeliveryMessage(ctx context.Context, topic string, message *SMSDeliveryMessage) error {
	kafkaMsg := &KafkaMessage{
		MessageID:   fmt.Sprintf("%s-%s-%d", message.JobID, message.PhoneNumber, time.Now().UnixNano()),
		MessageType: MessageTypeSMSDelivery,
		JobID:       message.JobID,
		BatchID:     message.BatchID,
		Timestamp:   time.Now(),
		Payload: map[string]interface{}{
			"phoneNumber": message.PhoneNumber,
			"message":     message.Message,
			"template":    message.Template,
			"priority":    message.Priority,
			"scheduledAt": message.ScheduledAt,
			"retryCount":  message.RetryCount,
		},
	}

	return adapter.sendMessage(ctx, topic, kafkaMsg)
}

// SendSMSFeedbackMessage 发送SMS反馈消息，对应Java项目中的消息状态回调
func (adapter *KafkaAdapter) SendSMSFeedbackMessage(ctx context.Context, topic string, message *SMSFeedbackMessage) error {
	kafkaMsg := &KafkaMessage{
		MessageID:   fmt.Sprintf("%s-%s-feedback-%d", message.JobID, message.PhoneNumber, time.Now().UnixNano()),
		MessageType: MessageTypeSMSFeedback,
		JobID:       message.JobID,
		BatchID:     message.BatchID,
		Timestamp:   time.Now(),
		Payload: map[string]interface{}{
			"phoneNumber":  message.PhoneNumber,
			"status":       message.Status,
			"deliveryTime": message.DeliveryTime,
			"errorCode":    message.ErrorCode,
			"errorMessage": message.ErrorMessage,
			"provider":     message.Provider,
		},
	}

	return adapter.sendMessage(ctx, topic, kafkaMsg)
}

// SendBatchStatusMessage 发送批处理状态消息
func (adapter *KafkaAdapter) SendBatchStatusMessage(ctx context.Context, topic string, message *BatchStatusMessage) error {
	kafkaMsg := &KafkaMessage{
		MessageID:   fmt.Sprintf("%s-status-%d", message.JobID, time.Now().UnixNano()),
		MessageType: MessageTypeBatchStatus,
		JobID:       message.JobID,
		BatchID:     message.BatchID,
		Timestamp:   time.Now(),
		Payload: map[string]interface{}{
			"phase":          message.Phase,
			"status":         message.Status,
			"progress":       message.Progress,
			"totalCount":     message.TotalCount,
			"processedCount": message.ProcessedCount,
			"successCount":   message.SuccessCount,
			"failedCount":    message.FailedCount,
			"errorMessage":   message.ErrorMessage,
		},
	}

	return adapter.sendMessage(ctx, topic, kafkaMsg)
}

// SendBatchCompleteMessage 发送批处理完成消息
func (adapter *KafkaAdapter) SendBatchCompleteMessage(ctx context.Context, topic string, jobID, batchID string, stats map[string]interface{}) error {
	kafkaMsg := &KafkaMessage{
		MessageID:   fmt.Sprintf("%s-complete-%d", jobID, time.Now().UnixNano()),
		MessageType: MessageTypeBatchComplete,
		JobID:       jobID,
		BatchID:     batchID,
		Timestamp:   time.Now(),
		Payload:     stats,
	}

	return adapter.sendMessage(ctx, topic, kafkaMsg)
}

// SendBatchFailedMessage 发送批处理失败消息
func (adapter *KafkaAdapter) SendBatchFailedMessage(ctx context.Context, topic string, jobID, batchID, errorMsg string, stats map[string]interface{}) error {
	kafkaMsg := &KafkaMessage{
		MessageID:   fmt.Sprintf("%s-failed-%d", jobID, time.Now().UnixNano()),
		MessageType: MessageTypeBatchFailed,
		JobID:       jobID,
		BatchID:     batchID,
		Timestamp:   time.Now(),
		Payload: map[string]interface{}{
			"errorMessage": errorMsg,
			"stats":        stats,
		},
	}

	return adapter.sendMessage(ctx, topic, kafkaMsg)
}

// sendMessage 发送消息到Kafka
func (adapter *KafkaAdapter) sendMessage(ctx context.Context, topic string, message *KafkaMessage) error {
	// 序列化消息
	payload, err := json.Marshal(message)
	if err != nil {
		adapter.logger.Errorw("Failed to marshal message", "error", err, "messageId", message.MessageID)
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 创建Kafka消息
	kafkaMessage := kafka.Message{
		Topic: topic,
		Key:   []byte(message.JobID), // 使用JobID作为分区键
		Value: payload,
		Headers: []kafka.Header{
			{Key: "messageType", Value: []byte(message.MessageType)},
			{Key: "messageId", Value: []byte(message.MessageID)},
			{Key: "timestamp", Value: []byte(message.Timestamp.Format(time.RFC3339))},
		},
		Time: message.Timestamp,
	}

	// 发送消息
	err = adapter.writer.WriteMessages(ctx, kafkaMessage)
	if err != nil {
		adapter.logger.Errorw("Failed to send message to Kafka", "error", err, "topic", topic, "messageId", message.MessageID)
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	adapter.logger.Infow("Successfully sent message to Kafka", "topic", topic, "messageId", message.MessageID, "messageType", message.MessageType)
	return nil
}

// MessageHandler 消息处理器接口，对应Java项目中的MessageHandler
type MessageHandler interface {
	HandleMessage(ctx context.Context, message *KafkaMessage) error
}

// ConsumeMessages 消费Kafka消息，对应Java项目中的AbstractConsumer
func (adapter *KafkaAdapter) ConsumeMessages(ctx context.Context, handler MessageHandler) error {
	adapter.logger.Infow("Starting to consume messages from Kafka", "topic", adapter.options.Topic, "groupId", adapter.options.ReaderOptions.GroupID)

	for {
		select {
		case <-ctx.Done():
			adapter.logger.Infow("Context cancelled, stopping message consumption")
			return ctx.Err()
		default:
			// 读取消息
			kafkaMessage, err := adapter.reader.ReadMessage(ctx)
			if err != nil {
				adapter.logger.Errorw("Failed to read message from Kafka", "error", err)
				continue
			}

			// 解析消息
			var message KafkaMessage
			if err := json.Unmarshal(kafkaMessage.Value, &message); err != nil {
				adapter.logger.Errorw("Failed to unmarshal message", "error", err, "offset", kafkaMessage.Offset)
				continue
			}

			// 处理消息
			if err := handler.HandleMessage(ctx, &message); err != nil {
				adapter.logger.Errorw("Failed to handle message", "error", err, "messageId", message.MessageID)
				// 可以在这里实现重试逻辑
				continue
			}

			adapter.logger.Infow("Successfully processed message", "messageId", message.MessageID, "messageType", message.MessageType)
		}
	}
}

// BatchMessageConsumer 批处理消息消费者，对应Java项目中的SmsBatchDeliveryConsumer
type BatchMessageConsumer struct {
	mongoAdapter *MongoDBAdapter
	logger       log.Logger
}

// NewBatchMessageConsumer 创建批处理消息消费者
func NewBatchMessageConsumer(mongoAdapter *MongoDBAdapter, logger log.Logger) *BatchMessageConsumer {
	return &BatchMessageConsumer{
		mongoAdapter: mongoAdapter,
		logger:       logger,
	}
}

// HandleMessage 处理消息，实现MessageHandler接口
func (consumer *BatchMessageConsumer) HandleMessage(ctx context.Context, message *KafkaMessage) error {
	switch message.MessageType {
	case MessageTypeSMSDelivery:
		return consumer.handleSMSDelivery(ctx, message)
	case MessageTypeSMSFeedback:
		return consumer.handleSMSFeedback(ctx, message)
	case MessageTypeBatchStatus:
		return consumer.handleBatchStatus(ctx, message)
	default:
		consumer.logger.Warnw("Unknown message type", "messageType", message.MessageType, "messageId", message.MessageID)
		return nil
	}
}

// handleSMSDelivery 处理SMS投递消息
func (consumer *BatchMessageConsumer) handleSMSDelivery(ctx context.Context, message *KafkaMessage) error {
	phoneNumber, ok := message.Payload["phoneNumber"].(string)
	if !ok {
		return fmt.Errorf("invalid phoneNumber in message payload")
	}

	// 更新SMS参数状态为处理中
	err := consumer.mongoAdapter.UpdateSmsParamStatus(ctx, message.JobID, phoneNumber, "PROCESSING", "")
	if err != nil {
		return fmt.Errorf("failed to update SMS param status: %w", err)
	}

	// 这里可以添加实际的SMS发送逻辑
	consumer.logger.Infow("Processing SMS delivery", "jobId", message.JobID, "phoneNumber", phoneNumber)

	// 模拟处理结果
	// 在实际实现中，这里会调用SMS服务提供商的API
	status := "SENT"
	errorMsg := ""

	// 更新最终状态
	err = consumer.mongoAdapter.UpdateSmsParamStatus(ctx, message.JobID, phoneNumber, status, errorMsg)
	if err != nil {
		return fmt.Errorf("failed to update final SMS param status: %w", err)
	}

	return nil
}

// handleSMSFeedback 处理SMS反馈消息
func (consumer *BatchMessageConsumer) handleSMSFeedback(ctx context.Context, message *KafkaMessage) error {
	phoneNumber, ok := message.Payload["phoneNumber"].(string)
	if !ok {
		return fmt.Errorf("invalid phoneNumber in message payload")
	}

	status, ok := message.Payload["status"].(string)
	if !ok {
		return fmt.Errorf("invalid status in message payload")
	}

	// 保存反馈报告
	report := &TsReport{
		JobID:       message.JobID,
		BatchID:     message.BatchID,
		PhoneNumber: phoneNumber,
		EventType:   "SMS_FEEDBACK",
		Status:      status,
		Timestamp:   message.Timestamp,
	}

	if errorCode, exists := message.Payload["errorCode"]; exists {
		report.ErrorCode = errorCode.(string)
	}

	if errorMessage, exists := message.Payload["errorMessage"]; exists {
		report.ErrorMsg = errorMessage.(string)
	}

	err := consumer.mongoAdapter.SaveTsReport(ctx, report)
	if err != nil {
		return fmt.Errorf("failed to save TS report: %w", err)
	}

	consumer.logger.Infow("Processed SMS feedback", "jobId", message.JobID, "phoneNumber", phoneNumber, "status", status)
	return nil
}

// handleBatchStatus 处理批处理状态消息
func (consumer *BatchMessageConsumer) handleBatchStatus(ctx context.Context, message *KafkaMessage) error {
	phase, ok := message.Payload["phase"].(string)
	if !ok {
		return fmt.Errorf("invalid phase in message payload")
	}

	status, ok := message.Payload["status"].(string)
	if !ok {
		return fmt.Errorf("invalid status in message payload")
	}

	progress, ok := message.Payload["progress"].(float64)
	if !ok {
		return fmt.Errorf("invalid progress in message payload")
	}

	processedCount, ok := message.Payload["processedCount"].(float64)
	if !ok {
		return fmt.Errorf("invalid processedCount in message payload")
	}

	successCount, ok := message.Payload["successCount"].(float64)
	if !ok {
		return fmt.Errorf("invalid successCount in message payload")
	}

	failedCount, ok := message.Payload["failedCount"].(float64)
	if !ok {
		return fmt.Errorf("invalid failedCount in message payload")
	}

	// 更新批处理作业状态
	err := consumer.mongoAdapter.UpdateBatchJobStatus(ctx, message.JobID, phase, float32(progress), int64(processedCount), int64(successCount), int64(failedCount))
	if err != nil {
		return fmt.Errorf("failed to update batch job status: %w", err)
	}

	consumer.logger.Infow("Updated batch status", "jobId", message.JobID, "phase", phase, "status", status, "progress", progress)
	return nil
}

// Close 关闭Kafka适配器
func (adapter *KafkaAdapter) Close() error {
	var errs []error

	if adapter.writer != nil {
		if err := adapter.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close kafka writer: %w", err))
		}
	}

	if adapter.reader != nil {
		if err := adapter.reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close kafka reader: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka adapter: %v", errs)
	}

	adapter.logger.Infow("Successfully closed Kafka adapter")
	return nil
}
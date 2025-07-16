package messagebatch

import (
	"context"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/messaging"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// MessageBatchService 聚合所有message batch相关的业务服务
type MessageBatchService struct {
	Counter   *CounterService
	Messaging *MessagingService
	Mongo     *MongoService
	logger    log.Logger
}

// NewMessageBatchService 创建新的MessageBatch业务服务
func NewMessageBatchService(
	cacheManager *cache.CacheManager,
	kafkaHelper *messaging.KafkaHelper,
	store store.IStore,
	logger log.Logger,
) *MessageBatchService {
	return &MessageBatchService{
		Counter:   NewCounterService(cacheManager, logger),
		Messaging: NewMessagingService(kafkaHelper, logger),
		Mongo:     NewMongoService(store, logger),
		logger:    logger,
	}
}

// ProcessBatch 处理消息批次的完整业务流程
func (s *MessageBatchService) ProcessBatch(ctx context.Context, batchID string, partitionID int, messages []MessageItem) error {
	s.logger.Infow("Starting batch processing",
		"batchID", batchID,
		"partitionID", partitionID,
		"messageCount", len(messages))

	// 1. 增加批次计数器
	count, err := s.Counter.IncrBatchCounter(ctx, batchID)
	if err != nil {
		return fmt.Errorf("failed to increment batch counter: %w", err)
	}

	// 2. 发送批次开始事件
	if err := s.Messaging.SendBatchStartEvent(ctx, batchID, partitionID); err != nil {
		s.logger.Errorw("Failed to send batch start event", "error", err, "batchID", batchID)
		// 继续处理，不因为消息发送失败而停止
	}

	// 3. 创建批次文档
	batchDoc := &MessageBatchDocument{
		BatchID:      batchID,
		PartitionID:  partitionID,
		Status:       "processing",
		MessageCount: int64(len(messages)),
		StartTime:    time.Now(),
		Messages:     messages,
		Metadata: map[string]interface{}{
			"counter_value": count,
			"created_by":    "message_batch_service",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.Mongo.CreateBatchDocument(ctx, batchDoc); err != nil {
		return fmt.Errorf("failed to create batch document: %w", err)
	}

	// 4. 处理每个消息
	var processedCount int64
	for _, message := range messages {
		if err := s.processMessage(ctx, batchID, partitionID, &message); err != nil {
			s.logger.Errorw("Failed to process message",
				"error", err,
				"batchID", batchID,
				"messageID", message.MessageID)
			continue
		}
		processedCount++
	}

	// 5. 更新批次状态
	finalStatus := "completed"
	if processedCount < int64(len(messages)) {
		finalStatus = "partial_failure"
	}

	if err := s.Mongo.UpdateBatchStatus(ctx, batchID, finalStatus); err != nil {
		s.logger.Errorw("Failed to update batch status", "error", err, "batchID", batchID)
	}

	// 6. 发送批次完成事件
	if err := s.Messaging.SendBatchCompleteEvent(ctx, batchID, partitionID, processedCount); err != nil {
		s.logger.Errorw("Failed to send batch complete event", "error", err, "batchID", batchID)
	}

	s.logger.Infow("Batch processing completed",
		"batchID", batchID,
		"status", finalStatus,
		"processedCount", processedCount,
		"totalCount", len(messages))

	return nil
}

// processMessage 处理单个消息
func (s *MessageBatchService) processMessage(ctx context.Context, batchID string, partitionID int, message *MessageItem) error {
	// 1. 增加消息计数器
	if _, err := s.Counter.IncrMessageCounter(ctx, fmt.Sprintf("partition_%d", partitionID)); err != nil {
		s.logger.Errorw("Failed to increment message counter", "error", err, "messageID", message.MessageID)
	}

	// 2. 更新消息状态
	message.Status = "processed"
	message.Timestamp = time.Now()

	// 3. 发送消息处理事件
	if err := s.Messaging.SendMessageProcessedEvent(ctx, message.MessageID, batchID, partitionID); err != nil {
		s.logger.Errorw("Failed to send message processed event", "error", err, "messageID", message.MessageID)
	}

	return nil
}

// CreateBatch 创建新的消息批次
func (s *MessageBatchService) CreateBatch(ctx context.Context, batchID string, partitionID int) error {
	s.logger.Infow("Creating new batch", "batchID", batchID, "partitionID", partitionID)

	// 1. 重置批次计数器
	if err := s.Counter.ResetBatchCounter(ctx, batchID); err != nil {
		return fmt.Errorf("failed to reset batch counter: %w", err)
	}

	// 2. 设置计数器过期时间 (24小时)
	if err := s.Counter.SetBatchCounterExpiry(ctx, batchID, 24*time.Hour); err != nil {
		s.logger.Errorw("Failed to set batch counter expiry", "error", err, "batchID", batchID)
	}

	// 3. 创建批次文档
	batchDoc := &MessageBatchDocument{
		BatchID:      batchID,
		PartitionID:  partitionID,
		Status:       "created",
		MessageCount: 0,
		StartTime:    time.Now(),
		Messages:     []MessageItem{},
		Metadata: map[string]interface{}{
			"created_by": "message_batch_service",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.Mongo.CreateBatchDocument(ctx, batchDoc); err != nil {
		return fmt.Errorf("failed to create batch document: %w", err)
	}

	s.logger.Infow("Batch created successfully", "batchID", batchID)
	return nil
}

// GetBatchStatus 获取批次状态
func (s *MessageBatchService) GetBatchStatus(ctx context.Context, batchID string) (*MessageBatchDocument, error) {
	return s.Mongo.GetBatchDocument(ctx, batchID)
}

// ListBatches 列出批次
func (s *MessageBatchService) ListBatches(ctx context.Context, partitionID *int, status string, limit int64) ([]*MessageBatchDocument, error) {
	return s.Mongo.ListBatchDocuments(ctx, partitionID, status, limit)
}

// GetBatchMetrics 获取批次指标
func (s *MessageBatchService) GetBatchMetrics(ctx context.Context, batchID string) (map[string]interface{}, error) {
	// 1. 获取批次计数器
	batchCount, err := s.Counter.GetBatchCounter(ctx, batchID)
	if err != nil {
		s.logger.Errorw("Failed to get batch counter", "error", err, "batchID", batchID)
		batchCount = 0
	}

	// 2. 获取批次文档
	batchDoc, err := s.Mongo.GetBatchDocument(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch document: %w", err)
	}

	// 3. 获取消息统计
	messagingStats := s.Messaging.GetMessagingStats()

	// 4. 计算处理时长
	var processingDuration time.Duration
	if batchDoc.EndTime != nil {
		processingDuration = batchDoc.EndTime.Sub(batchDoc.StartTime)
	} else {
		processingDuration = time.Since(batchDoc.StartTime)
	}

	return map[string]interface{}{
		"batch_id":            batchDoc.BatchID,
		"partition_id":        batchDoc.PartitionID,
		"status":              batchDoc.Status,
		"message_count":       batchDoc.MessageCount,
		"batch_counter":       batchCount,
		"start_time":          batchDoc.StartTime,
		"end_time":            batchDoc.EndTime,
		"processing_duration": processingDuration.Seconds(),
		"messaging_stats":     messagingStats,
		"created_at":          batchDoc.CreatedAt,
		"updated_at":          batchDoc.UpdatedAt,
	}, nil
}

// DeleteBatch 删除批次
func (s *MessageBatchService) DeleteBatch(ctx context.Context, batchID string) error {
	s.logger.Infow("Deleting batch", "batchID", batchID)

	// 1. 重置计数器
	if err := s.Counter.ResetBatchCounter(ctx, batchID); err != nil {
		s.logger.Errorw("Failed to reset batch counter", "error", err, "batchID", batchID)
	}

	// 2. 删除MongoDB文档
	if err := s.Mongo.DeleteBatchDocument(ctx, batchID); err != nil {
		return fmt.Errorf("failed to delete batch document: %w", err)
	}

	s.logger.Infow("Batch deleted successfully", "batchID", batchID)
	return nil
}

// Close 关闭服务
func (s *MessageBatchService) Close() error {
	if s.Messaging != nil {
		if err := s.Messaging.Close(); err != nil {
			s.logger.Errorw("Failed to close messaging service", "error", err)
			return err
		}
	}
	return nil
}

package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// DefaultMessageProcessor 默认消息处理器实现
type DefaultMessageProcessor struct {
	syncerManager syncer.SyncerManager
	logger        log.Logger
}

// NewDefaultMessageProcessor 创建默认消息处理器
func NewDefaultMessageProcessor(syncerManager syncer.SyncerManager, logger log.Logger) *DefaultMessageProcessor {
	return &DefaultMessageProcessor{
		syncerManager: syncerManager,
		logger:        logger,
	}
}

// ProcessMessage 处理单个消息
func (dmp *DefaultMessageProcessor) ProcessMessage(ctx context.Context, message *Message) error {
	dmp.logger.Infow("Processing message", "messageID", message.ID, "topic", message.Topic)

	// 根据消息类型进行不同的处理
	switch message.Topic {
	case "status-updates":
		return dmp.processStatusUpdate(ctx, message)
	case "statistics":
		return dmp.processStatistics(ctx, message)
	case "heartbeat":
		return dmp.processHeartbeat(ctx, message)
	default:
		dmp.logger.Warnw("Unknown message topic", "topic", message.Topic)
		return fmt.Errorf("unknown message topic: %s", message.Topic)
	}
}

// processStatusUpdate 处理状态更新消息
func (dmp *DefaultMessageProcessor) processStatusUpdate(ctx context.Context, message *Message) error {
	dmp.logger.Infow("Processing status update", "messageID", message.ID)
	
	// 获取状态同步器
	statusSyncer := dmp.syncerManager.GetStatusSyncer()
	if statusSyncer == nil {
		return fmt.Errorf("status syncer not available")
	}

	// 同步状态
	return statusSyncer.SyncStatus(ctx, string(message.Value))
}

// processStatistics 处理统计消息
func (dmp *DefaultMessageProcessor) processStatistics(ctx context.Context, message *Message) error {
	dmp.logger.Infow("Processing statistics", "messageID", message.ID)
	
	// 获取统计同步器
	statisticsSyncer := dmp.syncerManager.GetStatisticsSyncer()
	if statisticsSyncer == nil {
		return fmt.Errorf("statistics syncer not available")
	}

	// 同步统计数据
	return statisticsSyncer.SyncStatistics(ctx, string(message.Value))
}

// processHeartbeat 处理心跳消息
func (dmp *DefaultMessageProcessor) processHeartbeat(ctx context.Context, message *Message) error {
	dmp.logger.Infow("Processing heartbeat", "messageID", message.ID)
	
	// 获取心跳同步器
	heartbeatSyncer := dmp.syncerManager.GetHeartbeatSyncer()
	if heartbeatSyncer == nil {
		return fmt.Errorf("heartbeat syncer not available")
	}

	// 处理心跳
	return heartbeatSyncer.ProcessHeartbeat(ctx, string(message.Value))
}

// BatchMessageProcessor 批量消息处理器
type BatchMessageProcessor struct {
	messageProcessor MessageProcessor
	batchSize        int
	timeout          time.Duration
	logger           log.Logger
}

// NewBatchMessageProcessor 创建批量消息处理器
func NewBatchMessageProcessor(processor MessageProcessor, batchSize int, timeout time.Duration, logger log.Logger) *BatchMessageProcessor {
	return &BatchMessageProcessor{
		messageProcessor: processor,
		batchSize:        batchSize,
		timeout:          timeout,
		logger:           logger,
	}
}

// ProcessBatch 批量处理消息
func (bmp *BatchMessageProcessor) ProcessBatch(ctx context.Context, messages []*Message) error {
	bmp.logger.Infow("Processing message batch", "count", len(messages))

	// 分批处理消息
	for i := 0; i < len(messages); i += bmp.batchSize {
		end := i + bmp.batchSize
		if end > len(messages) {
			end = len(messages)
		}

		batch := messages[i:end]
		if err := bmp.processBatchChunk(ctx, batch); err != nil {
			return fmt.Errorf("failed to process batch chunk: %w", err)
		}
	}

	return nil
}

// processBatchChunk 处理批量消息块
func (bmp *BatchMessageProcessor) processBatchChunk(ctx context.Context, messages []*Message) error {
	ctx, cancel := context.WithTimeout(ctx, bmp.timeout)
	defer cancel()

	for _, message := range messages {
		if err := bmp.messageProcessor.ProcessMessage(ctx, message); err != nil {
			bmp.logger.Errorw("Failed to process message", "messageID", message.ID, "error", err)
			// 继续处理其他消息，不因单个消息失败而停止整个批次
		}
	}

	return nil
}
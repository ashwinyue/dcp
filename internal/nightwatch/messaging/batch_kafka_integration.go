package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// BatchKafkaIntegration Kafka批量集成
type BatchKafkaIntegration struct {
	kafkaHelper       *KafkaHelper
	messageProcessor  MessageProcessor
	batchProcessor    *BatchMessageProcessor
	syncerManager     syncer.SyncerManager
	logger            log.Logger
	running           bool
	mu                sync.RWMutex
	shutdownCh        chan struct{}
	wg                sync.WaitGroup
}

// BatchKafkaConfig 批量Kafka配置
type BatchKafkaConfig struct {
	BatchSize       int
	BatchTimeout    time.Duration
	ConsumerTimeout time.Duration
	Topics          []string
	GroupID         string
}

// NewBatchKafkaIntegration 创建新的批量Kafka集成
func NewBatchKafkaIntegration(
	kafkaHelper *KafkaHelper,
	syncerManager syncer.SyncerManager,
	config *BatchKafkaConfig,
	logger log.Logger,
) *BatchKafkaIntegration {
	messageProcessor := NewDefaultMessageProcessor(syncerManager, logger)
	batchProcessor := NewBatchMessageProcessor(
		messageProcessor,
		config.BatchSize,
		config.BatchTimeout,
		logger,
	)

	return &BatchKafkaIntegration{
		kafkaHelper:      kafkaHelper,
		messageProcessor: messageProcessor,
		batchProcessor:   batchProcessor,
		syncerManager:    syncerManager,
		logger:           logger,
		shutdownCh:       make(chan struct{}),
	}
}

// Start 启动批量Kafka集成
func (bki *BatchKafkaIntegration) Start(ctx context.Context, config *BatchKafkaConfig) error {
	bki.mu.Lock()
	defer bki.mu.Unlock()

	if bki.running {
		return fmt.Errorf("batch kafka integration is already running")
	}

	bki.running = true
	bki.logger.Infow("Starting batch kafka integration")

	// 为每个主题启动消费者
	for _, topic := range config.Topics {
		bki.wg.Add(1)
		go bki.consumeTopic(ctx, topic, config.GroupID, config.ConsumerTimeout)
	}

	return nil
}

// Stop 停止批量Kafka集成
func (bki *BatchKafkaIntegration) Stop() error {
	bki.mu.Lock()
	defer bki.mu.Unlock()

	if !bki.running {
		return nil
	}

	bki.logger.Infow("Stopping batch kafka integration")
	bki.running = false

	// 发送关闭信号
	close(bki.shutdownCh)

	// 等待所有goroutine完成
	bki.wg.Wait()

	bki.logger.Infow("Batch kafka integration stopped")
	return nil
}

// IsRunning 检查是否正在运行
func (bki *BatchKafkaIntegration) IsRunning() bool {
	bki.mu.RLock()
	defer bki.mu.RUnlock()
	return bki.running
}

// consumeTopic 消费指定主题
func (bki *BatchKafkaIntegration) consumeTopic(ctx context.Context, topic string, groupID string, timeout time.Duration) {
	defer bki.wg.Done()

	bki.logger.Infow("Starting topic consumer", "topic", topic, "groupID", groupID)

	// 创建消息处理函数
	handler := func(key, value []byte) error {
		message := &Message{
			ID:        fmt.Sprintf("%s-%d", topic, time.Now().UnixNano()),
			Topic:     topic,
			Key:       string(key),
			Value:     value,
			Headers:   make(map[string]string),
			Timestamp: time.Now(),
		}

		return bki.messageProcessor.ProcessMessage(ctx, message)
	}

	// 启动消费循环
	for {
		select {
		case <-bki.shutdownCh:
			bki.logger.Infow("Topic consumer shutting down", "topic", topic)
			return
		case <-ctx.Done():
			bki.logger.Infow("Topic consumer context cancelled", "topic", topic)
			return
		default:
			// 消费消息
			if err := bki.kafkaHelper.ConsumeMessages(ctx, topic, groupID, handler); err != nil {
				bki.logger.Errorw("Error consuming messages", "topic", topic, "error", err)
				// 短暂休眠后重试
				time.Sleep(time.Second * 5)
			}
		}
	}
}

// SendBatchMessage 发送批量消息
func (bki *BatchKafkaIntegration) SendBatchMessage(ctx context.Context, topic string, messages []*Message) error {
	bki.logger.Infow("Sending batch messages", "topic", topic, "count", len(messages))

	for _, message := range messages {
		if err := bki.kafkaHelper.SendMessage(ctx, topic, message.Key, message.Value); err != nil {
			return fmt.Errorf("failed to send message %s: %w", message.ID, err)
		}
	}

	return nil
}

// GetProcessedCount 获取已处理数量
func (bki *BatchKafkaIntegration) GetProcessedCount() int64 {
	if statisticsSyncer := bki.syncerManager.GetStatisticsSyncer(); statisticsSyncer != nil {
		if atomicSyncer, ok := statisticsSyncer.(syncer.AtomicStatisticsSyncer); ok {
			return atomicSyncer.GetProcessedCount()
		}
	}
	return 0
}

// GetFailedCount 获取失败数量
func (bki *BatchKafkaIntegration) GetFailedCount() int64 {
	if statisticsSyncer := bki.syncerManager.GetStatisticsSyncer(); statisticsSyncer != nil {
		if atomicSyncer, ok := statisticsSyncer.(syncer.AtomicStatisticsSyncer); ok {
			return atomicSyncer.GetFailedCount()
		}
	}
	return 0
}

// GetStatistics 获取统计信息
func (bki *BatchKafkaIntegration) GetStatistics() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["running"] = bki.IsRunning()
	stats["timestamp"] = time.Now()

	// 从syncer manager获取统计信息
	stats["processed_messages"] = bki.GetProcessedCount()
	stats["error_count"] = bki.GetFailedCount()

	return stats
}

// HealthCheck 健康检查
func (bki *BatchKafkaIntegration) HealthCheck(ctx context.Context) error {
	if !bki.IsRunning() {
		return fmt.Errorf("batch kafka integration is not running")
	}

	// 检查syncer manager状态
	if bki.syncerManager == nil {
		return fmt.Errorf("syncer manager is not available")
	}

	// 检查kafka helper状态
	if bki.kafkaHelper == nil {
		return fmt.Errorf("kafka helper is not available")
	}

	return nil
}
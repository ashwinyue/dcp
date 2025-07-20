package messagebatch

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	genericoptions "github.com/onexstack/onexstack/pkg/options"
)

// IntegrationService 集成服务，负责协调MongoDB和Kafka适配器
type IntegrationService struct {
	mongoAdapter *MongoDBAdapter
	kafkaAdapter *KafkaAdapter
	consumer     *BatchMessageConsumer
	logger       log.Logger
	running      bool
}

// NewIntegrationService 创建集成服务
func NewIntegrationService(
	mongoDB *mongo.Database,
	kafkaOptions *genericoptions.KafkaOptions,
	logger log.Logger,
) (*IntegrationService, error) {
	// 创建MongoDB适配器
	mongoAdapter := NewMongoDBAdapter(mongoDB, logger)
	
	// 创建Kafka适配器
	kafkaAdapter, err := NewKafkaAdapter(kafkaOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka adapter: %w", err)
	}
	
	// 创建消息消费者
	consumer := NewBatchMessageConsumer(mongoAdapter, logger)
	
	return &IntegrationService{
		mongoAdapter: mongoAdapter,
		kafkaAdapter: kafkaAdapter,
		consumer:     consumer,
		logger:       logger,
		running:      false,
	}, nil
}

// Start 启动集成服务
func (service *IntegrationService) Start(ctx context.Context) error {
	if service.running {
		return fmt.Errorf("integration service is already running")
	}
	
	service.logger.Infow("Starting integration service")
	
	// 创建MongoDB索引
	if err := service.mongoAdapter.CreateIndexes(ctx); err != nil {
		service.logger.Errorw("Failed to create MongoDB indexes", "error", err)
		return fmt.Errorf("failed to create MongoDB indexes: %w", err)
	}
	
	// 启动Kafka消息消费
	go func() {
		if err := service.kafkaAdapter.ConsumeMessages(ctx, service.consumer); err != nil {
			service.logger.Errorw("Kafka message consumption failed", "error", err)
		}
	}()
	
	service.running = true
	service.logger.Infow("Integration service started successfully")
	return nil
}

// Stop 停止集成服务
func (service *IntegrationService) Stop() error {
	if !service.running {
		return nil
	}
	
	service.logger.Infow("Stopping integration service")
	
	// 关闭Kafka适配器
	if err := service.kafkaAdapter.Close(); err != nil {
		service.logger.Errorw("Failed to close Kafka adapter", "error", err)
		return fmt.Errorf("failed to close Kafka adapter: %w", err)
	}
	
	service.running = false
	service.logger.Infow("Integration service stopped successfully")
	return nil
}

// IsRunning 检查服务是否运行中
func (service *IntegrationService) IsRunning() bool {
	return service.running
}

// GetMongoAdapter 获取MongoDB适配器
func (service *IntegrationService) GetMongoAdapter() *MongoDBAdapter {
	return service.mongoAdapter
}

// GetKafkaAdapter 获取Kafka适配器
func (service *IntegrationService) GetKafkaAdapter() *KafkaAdapter {
	return service.kafkaAdapter
}

// ProcessMessageBatchJob 处理消息批处理作业，替代Java项目中的主要业务逻辑
func (service *IntegrationService) ProcessMessageBatchJob(ctx context.Context, job *model.MessageBatchJobM) error {
	service.logger.Infow("Starting to process message batch job", "jobId", job.JobID)
	
	// 1. 准备阶段：从数据库读取消息数据并保存到MongoDB
	if err := service.preparePhase(ctx, job); err != nil {
		return fmt.Errorf("preparation phase failed: %w", err)
	}
	
	// 2. 投递阶段：发送消息到Kafka进行异步处理
	if err := service.deliveryPhase(ctx, job); err != nil {
		return fmt.Errorf("delivery phase failed: %w", err)
	}
	
	service.logger.Infow("Message batch job processing completed", "jobId", job.JobID)
	return nil
}

// preparePhase 准备阶段，对应Java项目中的SmsPreparationStep
func (service *IntegrationService) preparePhase(ctx context.Context, job *model.MessageBatchJobM) error {
	service.logger.Infow("Starting preparation phase", "jobId", job.JobID)
	
	// 发送准备开始状态消息
	statusMsg := &BatchStatusMessage{
		JobID:     job.JobID,
		BatchID:   fmt.Sprintf("%s-batch", job.JobID),
		Phase:     "PREPARATION",
		Status:    "RUNNING",
		Progress:  0.0,
		Timestamp: time.Now(),
	}
	
	if err := service.kafkaAdapter.SendBatchStatusMessage(ctx, "batch-status", statusMsg); err != nil {
		service.logger.Errorw("Failed to send preparation start status", "error", err)
	}
	
	// 模拟从原始数据源读取数据并转换为SMS参数
	// 在实际实现中，这里会从数据库或其他数据源读取收件人信息
	smsParams := service.generateSmsParams(job)
	
	// 批量保存SMS参数到MongoDB
	if err := service.mongoAdapter.SaveSmsParams(ctx, smsParams); err != nil {
		return fmt.Errorf("failed to save SMS params: %w", err)
	}
	
	// 保存批处理作业状态
	batchStatus := &BatchJobStatus{
		JobID:        job.JobID,
		BatchID:      fmt.Sprintf("%s-batch", job.JobID),
		Phase:        "PREPARATION",
		Status:       "COMPLETED",
		Progress:     100.0,
		TotalCount:   int64(len(smsParams)),
		ProcessedCount: int64(len(smsParams)),
		SuccessCount: int64(len(smsParams)),
		FailedCount:  0,
		StartTime:    time.Now(),
		EndTime:      time.Now(),
	}
	
	if err := service.mongoAdapter.SaveBatchJobStatus(ctx, batchStatus); err != nil {
		return fmt.Errorf("failed to save batch job status: %w", err)
	}
	
	// 发送准备完成状态消息
	statusMsg.Status = "COMPLETED"
	statusMsg.Progress = 100.0
	statusMsg.TotalCount = int64(len(smsParams))
	statusMsg.ProcessedCount = int64(len(smsParams))
	statusMsg.SuccessCount = int64(len(smsParams))
	statusMsg.Timestamp = time.Now()
	
	if err := service.kafkaAdapter.SendBatchStatusMessage(ctx, "batch-status", statusMsg); err != nil {
		service.logger.Errorw("Failed to send preparation complete status", "error", err)
	}
	
	service.logger.Infow("Preparation phase completed", "jobId", job.JobID, "smsCount", len(smsParams))
	return nil
}

// deliveryPhase 投递阶段，对应Java项目中的SMS投递逻辑
func (service *IntegrationService) deliveryPhase(ctx context.Context, job *model.MessageBatchJobM) error {
	service.logger.Infow("Starting delivery phase", "jobId", job.JobID)
	
	// 发送投递开始状态消息
	statusMsg := &BatchStatusMessage{
		JobID:     job.JobID,
		BatchID:   fmt.Sprintf("%s-batch", job.JobID),
		Phase:     "DELIVERY",
		Status:    "RUNNING",
		Progress:  0.0,
		Timestamp: time.Now(),
	}
	
	if err := service.kafkaAdapter.SendBatchStatusMessage(ctx, "batch-status", statusMsg); err != nil {
		service.logger.Errorw("Failed to send delivery start status", "error", err)
	}
	
	// 从MongoDB获取SMS参数
	batchSize := int64(100) // 批处理大小
	if job.Params != nil && job.Params.BatchSize > 0 {
		batchSize = job.Params.BatchSize
	}
	
	smsParams, err := service.mongoAdapter.GetSmsParamsByJobID(ctx, job.JobID, batchSize)
	if err != nil {
		return fmt.Errorf("failed to get SMS params: %w", err)
	}
	
	// 发送SMS投递消息到Kafka
	successCount := 0
	failedCount := 0
	
	for i, param := range smsParams {
		deliveryMsg := &SMSDeliveryMessage{
			JobID:       job.JobID,
			BatchID:     fmt.Sprintf("%s-batch", job.JobID),
			PhoneNumber: param.PhoneNumber,
			Message:     param.Message,
			Template:    param.Template,
			Priority:    1,
			ScheduledAt: time.Now(),
			RetryCount:  param.RetryCount,
		}
		
		if err := service.kafkaAdapter.SendSMSDeliveryMessage(ctx, "sms-delivery", deliveryMsg); err != nil {
			service.logger.Errorw("Failed to send SMS delivery message", "error", err, "phoneNumber", param.PhoneNumber)
			failedCount++
		} else {
			successCount++
		}
		
		// 更新进度
		if (i+1)%10 == 0 || i == len(smsParams)-1 {
			progress := float32(i+1) / float32(len(smsParams)) * 100
			statusMsg.Progress = progress
			statusMsg.ProcessedCount = int64(i + 1)
			statusMsg.SuccessCount = int64(successCount)
			statusMsg.FailedCount = int64(failedCount)
			statusMsg.Timestamp = time.Now()
			
			if err := service.kafkaAdapter.SendBatchStatusMessage(ctx, "batch-status", statusMsg); err != nil {
				service.logger.Errorw("Failed to send delivery progress status", "error", err)
			}
		}
	}
	
	// 保存投递阶段状态
	batchStatus := &BatchJobStatus{
		JobID:          job.JobID,
		BatchID:        fmt.Sprintf("%s-batch", job.JobID),
		Phase:          "DELIVERY",
		Status:         "COMPLETED",
		Progress:       100.0,
		TotalCount:     int64(len(smsParams)),
		ProcessedCount: int64(len(smsParams)),
		SuccessCount:   int64(successCount),
		FailedCount:    int64(failedCount),
		StartTime:      time.Now(),
		EndTime:        time.Now(),
	}
	
	if err := service.mongoAdapter.SaveBatchJobStatus(ctx, batchStatus); err != nil {
		return fmt.Errorf("failed to save delivery batch job status: %w", err)
	}
	
	// 发送投递完成状态消息
	statusMsg.Status = "COMPLETED"
	statusMsg.Progress = 100.0
	statusMsg.TotalCount = int64(len(smsParams))
	statusMsg.ProcessedCount = int64(len(smsParams))
	statusMsg.SuccessCount = int64(successCount)
	statusMsg.FailedCount = int64(failedCount)
	statusMsg.Timestamp = time.Now()
	
	if err := service.kafkaAdapter.SendBatchStatusMessage(ctx, "batch-status", statusMsg); err != nil {
		service.logger.Errorw("Failed to send delivery complete status", "error", err)
	}
	
	// 发送批处理完成消息
	stats := map[string]interface{}{
		"totalCount":     len(smsParams),
		"successCount":   successCount,
		"failedCount":    failedCount,
		"completionTime": time.Now(),
	}
	
	if err := service.kafkaAdapter.SendBatchCompleteMessage(ctx, "batch-complete", job.JobID, fmt.Sprintf("%s-batch", job.JobID), stats); err != nil {
		service.logger.Errorw("Failed to send batch complete message", "error", err)
	}
	
	service.logger.Infow("Delivery phase completed", "jobId", job.JobID, "successCount", successCount, "failedCount", failedCount)
	return nil
}

// generateSmsParams 生成SMS参数，模拟从数据源读取数据
func (service *IntegrationService) generateSmsParams(job *model.MessageBatchJobM) []*SmsParam {
	// 在实际实现中，这里会从数据库或其他数据源读取收件人信息
	// 这里只是模拟生成一些测试数据
	
	var recipients []string
	var template string
	var message string
	
	if job.Params != nil {
		recipients = job.Params.Recipients
		template = job.Params.Template
		message = "Default message"
	} else {
		// 默认测试数据
		recipients = []string{"13800138000", "13800138001", "13800138002"}
		template = "default_template"
		message = "Test message"
	}
	
	smsParams := make([]*SmsParam, len(recipients))
	for i, phoneNumber := range recipients {
		smsParams[i] = &SmsParam{
			PartitionKey: job.JobID,
			RowKey:       fmt.Sprintf("%s-%s", job.JobID, phoneNumber),
			JobID:        job.JobID,
			BatchID:      fmt.Sprintf("%s-batch", job.JobID),
			PhoneNumber:  phoneNumber,
			Message:      message,
			Template:     template,
			Status:       "PENDING",
			RetryCount:   0,
		}
	}
	
	return smsParams
}

// HealthCheck 健康检查
func (service *IntegrationService) HealthCheck(ctx context.Context) error {
	if !service.running {
		return fmt.Errorf("integration service is not running")
	}
	
	// 检查MongoDB连接
	if err := service.mongoAdapter.db.Client().Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB health check failed: %w", err)
	}
	
	// 这里可以添加Kafka健康检查逻辑
	
	return nil
}

// GetStatistics 获取统计信息
func (service *IntegrationService) GetStatistics(ctx context.Context, jobID string) (map[string]interface{}, error) {
	// 从MongoDB获取批处理作业状态
	statuses, err := service.mongoAdapter.GetBatchJobStatus(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch job status: %w", err)
	}
	
	// 从MongoDB获取报告
	reports, err := service.mongoAdapter.GetTsReportsByJobID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get TS reports: %w", err)
	}
	
	stats := map[string]interface{}{
		"jobId":        jobID,
		"statusCount":  len(statuses),
		"reportCount":  len(reports),
		"lastUpdated": time.Now(),
	}
	
	if len(statuses) > 0 {
		latestStatus := statuses[0]
		stats["latestPhase"] = latestStatus.Phase
		stats["latestStatus"] = latestStatus.Status
		stats["progress"] = latestStatus.Progress
		stats["totalCount"] = latestStatus.TotalCount
		stats["processedCount"] = latestStatus.ProcessedCount
		stats["successCount"] = latestStatus.SuccessCount
		stats["failedCount"] = latestStatus.FailedCount
	}
	
	return stats, nil
}
package messagebatch

import (
	"context"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"go.uber.org/ratelimit"

	"github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/ashwinyue/dcp/internal/pkg/model"
)

// Ensure Watcher implements the registry.Watcher interface.
var _ registry.Watcher = (*Watcher)(nil)

// Limiter holds rate limiters for different operations.
type Limiter struct {
	Processing ratelimit.Limiter
	Messaging  ratelimit.Limiter
}

// Watcher monitors and processes message batch jobs.
type Watcher struct {
	// Business service layer
	MessageBatchService *messagebatch.MessageBatchService

	// Data access
	Store store.IStore

	// Configuration
	MaxWorkers int64
	Limiter    Limiter

	// Logger
	logger log.Logger
}

// Run executes the watcher logic to process message batch jobs.
func (w *Watcher) Run() {
	// Define the phases that the watcher can handle for message batch jobs.
	runablePhase := []string{
		known.MessageBatchPending,
		known.MessageBatchProcessing,
		known.MessageBatchPartialComplete,
		known.MessageBatchRetrying,
	}

	_, jobs, err := w.Store.Job().List(context.Background(), where.F(
		"scope", known.MessageBatchJobScope,
		"watcher", known.MessageBatchWatcher,
		"status", runablePhase,
		"suspend", known.JobNonSuspended,
	))
	if err != nil {
		w.logger.Errorw("Failed to get runnable message batch jobs", "error", err)
		return
	}

	wp := workerpool.New(int(w.MaxWorkers))
	for _, job := range jobs {
		ctx := context.Background()
		w.logger.Infow("Processing message batch job",
			"jobID", job.JobID,
			"status", job.Status,
			"scope", job.Scope)

		wp.Submit(func() {
			if err := w.processMessageBatchJob(ctx, job); err != nil {
				w.logger.Errorw("Failed to process message batch job",
					"error", err,
					"jobID", job.JobID)
			}
		})
	}

	wp.StopWait()
}

// processMessageBatchJob 处理单个消息批次任务
func (w *Watcher) processMessageBatchJob(ctx context.Context, job *model.JobM) error {
	w.Limiter.Processing.Take() // Rate limiting

	// 从job中提取批次信息
	batchID := job.JobID
	partitionID := extractPartitionID(job)

	w.logger.Infow("Starting message batch job processing",
		"batchID", batchID,
		"partitionID", partitionID,
		"status", job.Status)

	switch job.Status {
	case known.MessageBatchPending:
		return w.handlePendingBatch(ctx, batchID, partitionID, job)
	case known.MessageBatchProcessing:
		return w.handleProcessingBatch(ctx, batchID, partitionID, job)
	case known.MessageBatchPartialComplete:
		return w.handlePartialCompleteBatch(ctx, batchID, partitionID, job)
	case known.MessageBatchRetrying:
		return w.handleRetryingBatch(ctx, batchID, partitionID, job)
	default:
		w.logger.Warnw("Unknown message batch job status", "status", job.Status, "jobID", job.JobID)
		return nil
	}
}

// handlePendingBatch 处理待处理的批次
func (w *Watcher) handlePendingBatch(ctx context.Context, batchID string, partitionID int, job *model.JobM) error {
	w.logger.Infow("Handling pending batch", "batchID", batchID, "partitionID", partitionID)

	// 1. 创建批次
	if err := w.MessageBatchService.CreateBatch(ctx, batchID, partitionID); err != nil {
		w.logger.Errorw("Failed to create batch", "error", err, "batchID", batchID)
		return w.updateJobStatus(ctx, job, known.MessageBatchFailed, err.Error())
	}

	// 2. 更新任务状态为处理中
	return w.updateJobStatus(ctx, job, known.MessageBatchProcessing, "Batch created and ready for processing")
}

// handleProcessingBatch 处理正在处理的批次
func (w *Watcher) handleProcessingBatch(ctx context.Context, batchID string, partitionID int, job *model.JobM) error {
	w.logger.Infow("Handling processing batch", "batchID", batchID, "partitionID", partitionID)

	// 模拟消息数据（在实际应用中，这些数据可能来自外部系统或配置）
	messages := w.generateSampleMessages(batchID, 10)

	// 处理批次
	if err := w.MessageBatchService.ProcessBatch(ctx, batchID, partitionID, messages); err != nil {
		w.logger.Errorw("Failed to process batch", "error", err, "batchID", batchID)
		return w.updateJobStatus(ctx, job, known.MessageBatchFailed, err.Error())
	}

	// 更新任务状态为完成
	return w.updateJobStatus(ctx, job, known.MessageBatchCompleted, "Batch processing completed successfully")
}

// handlePartialCompleteBatch 处理部分完成的批次
func (w *Watcher) handlePartialCompleteBatch(ctx context.Context, batchID string, partitionID int, job *model.JobM) error {
	w.logger.Infow("Handling partial complete batch", "batchID", batchID, "partitionID", partitionID)

	// 获取批次状态，检查是否需要重试
	batchDoc, err := w.MessageBatchService.GetBatchStatus(ctx, batchID)
	if err != nil {
		w.logger.Errorw("Failed to get batch status", "error", err, "batchID", batchID)
		return w.updateJobStatus(ctx, job, known.MessageBatchFailed, err.Error())
	}

	// 判断是否需要重试
	if batchDoc.MessageCount > 0 && len(batchDoc.Messages) < int(batchDoc.MessageCount) {
		return w.updateJobStatus(ctx, job, known.MessageBatchRetrying, "Retrying incomplete messages")
	}

	// 标记为完成
	return w.updateJobStatus(ctx, job, known.MessageBatchCompleted, "All messages processed successfully")
}

// handleRetryingBatch 处理重试中的批次
func (w *Watcher) handleRetryingBatch(ctx context.Context, batchID string, partitionID int, job *model.JobM) error {
	w.logger.Infow("Handling retrying batch", "batchID", batchID, "partitionID", partitionID)

	// 重新处理批次（这里可以实现更复杂的重试逻辑）
	return w.handleProcessingBatch(ctx, batchID, partitionID, job)
}

// updateJobStatus 更新任务状态
func (w *Watcher) updateJobStatus(ctx context.Context, job *model.JobM, status, message string) error {
	job.Status = status
	job.Message = message

	return w.Store.Job().Update(ctx, job)
}

// extractPartitionID 从job中提取分区ID
func extractPartitionID(job *model.JobM) int {
	// 这里可以从job的metadata或其他字段中提取分区ID
	// 暂时返回一个固定值
	return 0
}

// generateSampleMessages 生成示例消息
func (w *Watcher) generateSampleMessages(batchID string, count int) []messagebatch.MessageItem {
	messages := make([]messagebatch.MessageItem, count)
	for i := 0; i < count; i++ {
		messages[i] = messagebatch.MessageItem{
			MessageID: fmt.Sprintf("%s_msg_%d", batchID, i),
			Content: map[string]interface{}{
				"index":   i,
				"batchID": batchID,
				"data":    fmt.Sprintf("Sample message %d", i),
			},
			Timestamp: time.Now(),
			Status:    "pending",
		}
	}
	return messages
}

// Spec returns the cron job specification for scheduling.
func (w *Watcher) Spec() string {
	return "@every 3s" // Run every 3 seconds
}

// SetAggregateConfig configures the watcher with the provided aggregate configuration.
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	// Initialize the business service with all required dependencies
	w.MessageBatchService = messagebatch.NewMessageBatchService(
		config.Cache,
		config.Messaging,
		config.Store,
		log.New(nil), // Use default logger
	)

	w.Store = config.Store
	w.logger = log.New(nil)

	// Initialize rate limiters
	w.Limiter = Limiter{
		Processing: ratelimit.New(known.MessageBatchProcessingQPS),
		Messaging:  ratelimit.New(known.MessageBatchMessagingQPS),
	}
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.MaxWorkers = maxWorkers
}

func init() {
	registry.Register(known.MessageBatchWatcher, &Watcher{})
}

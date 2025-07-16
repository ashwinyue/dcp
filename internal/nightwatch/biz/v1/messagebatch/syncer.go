// Package messagebatch provides synchronization services for message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// SyncerService 提供同步器相关的业务服务
type SyncerService struct {
	Delivery *DeliverySyncer
	Prepare  *PrepareSyncer
	logger   log.Logger
}

// NewSyncerService 创建新的同步器服务
func NewSyncerService(cache *cache.CacheManager, logger log.Logger) *SyncerService {
	return &SyncerService{
		Delivery: NewDeliverySyncer(cache, logger),
		Prepare:  NewPrepareSyncer(cache, logger),
		logger:   logger,
	}
}

// DeliverySyncer 发送阶段的同步管理器
type DeliverySyncer struct {
	cache  *cache.CacheManager
	logger log.Logger
	mu     sync.RWMutex
}

// NewDeliverySyncer 创建新的发送同步器
func NewDeliverySyncer(cache *cache.CacheManager, logger log.Logger) *DeliverySyncer {
	return &DeliverySyncer{
		cache:  cache,
		logger: logger,
	}
}

// PrepareSyncer 准备阶段的同步管理器
type PrepareSyncer struct {
	cache  *cache.CacheManager
	logger log.Logger
	mu     sync.RWMutex
}

// NewPrepareSyncer 创建新的准备同步器
func NewPrepareSyncer(cache *cache.CacheManager, logger log.Logger) *PrepareSyncer {
	return &PrepareSyncer{
		cache:  cache,
		logger: logger,
	}
}

// StepStatistics 步骤统计信息
type StepStatistics struct {
	Success int64 `json:"success"`
	Failure int64 `json:"failure"`
	Total   int64 `json:"total"`
}

// ===== DeliverySyncer 方法 =====

// OneSendThreadStart 标记一个发送线程开始
func (ds *DeliverySyncer) OneSendThreadStart(ctx context.Context, batchID string) error {
	key := fmt.Sprintf("delivery:threads:active:%s", batchID)
	_, err := ds.cache.Incr(ctx, key)
	if err != nil {
		ds.logger.Errorw("Failed to increment active send threads", "error", err, "batchID", batchID)
		return err
	}

	ds.logger.Infow("Send thread started", "batchID", batchID)
	return nil
}

// AllSendThreadEnd 检查所有发送线程是否已结束
func (ds *DeliverySyncer) AllSendThreadEnd(ctx context.Context, batchID string) (bool, error) {
	key := fmt.Sprintf("delivery:threads:active:%s", batchID)
	countStr, err := ds.cache.GetString(ctx, key)
	if err != nil {
		ds.logger.Errorw("Failed to get active send threads count", "error", err, "batchID", batchID)
		return false, err
	}

	if countStr == "" {
		return true, nil
	}

	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		ds.logger.Errorw("Failed to parse active threads count", "error", err, "batchID", batchID)
		return false, err
	}

	return count <= 0, nil
}

// OnePkTaskEnd 标记一个分区任务结束
func (ds *DeliverySyncer) OnePkTaskEnd(ctx context.Context, batchID, partitionKey string) error {
	key := fmt.Sprintf("delivery:partition:complete:%s:%s", batchID, partitionKey)
	err := ds.cache.SetString(ctx, key, "true", 24*time.Hour)
	if err != nil {
		ds.logger.Errorw("Failed to mark partition task complete", "error", err, "batchID", batchID, "partitionKey", partitionKey)
		return err
	}

	// 减少活跃线程计数
	threadsKey := fmt.Sprintf("delivery:threads:active:%s", batchID)
	_, err = ds.cache.Decr(ctx, threadsKey)
	if err != nil {
		ds.logger.Warnw("Failed to decrement active send threads", "error", err, "batchID", batchID)
	}

	ds.logger.Infow("Partition task completed", "batchID", batchID, "partitionKey", partitionKey)
	return nil
}

// IsPkTaskComplete 检查分区任务是否完成
func (ds *DeliverySyncer) IsPkTaskComplete(ctx context.Context, batchID, partitionKey string) (bool, error) {
	key := fmt.Sprintf("delivery:partition:complete:%s:%s", batchID, partitionKey)
	result, err := ds.cache.GetString(ctx, key)
	if err != nil {
		ds.logger.Errorw("Failed to check partition task status", "error", err, "batchID", batchID, "partitionKey", partitionKey)
		return false, err
	}

	return result == "true", nil
}

// IsAllPkTaskComplete 检查所有分区任务是否完成
func (ds *DeliverySyncer) IsAllPkTaskComplete(ctx context.Context, batchID string, totalPartitions int) (bool, error) {
	for i := 0; i < totalPartitions; i++ {
		partitionKey := fmt.Sprintf("partition_%d", i)
		complete, err := ds.IsPkTaskComplete(ctx, batchID, partitionKey)
		if err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
	}

	ds.logger.Infow("All partition tasks completed", "batchID", batchID, "totalPartitions", totalPartitions)
	return true, nil
}

// SetHeartBeatTime 设置分区心跳时间
func (ds *DeliverySyncer) SetHeartBeatTime(ctx context.Context, batchID, partitionKey string, timestamp int64) error {
	key := fmt.Sprintf("delivery:heartbeat:%s:%s", batchID, partitionKey)
	err := ds.cache.SetString(ctx, key, strconv.FormatInt(timestamp, 10), time.Hour)
	if err != nil {
		ds.logger.Errorw("Failed to set heartbeat time", "error", err, "batchID", batchID, "partitionKey", partitionKey)
		return err
	}

	return nil
}

// GetHeartBeatTime 获取分区心跳时间
func (ds *DeliverySyncer) GetHeartBeatTime(ctx context.Context, batchID, partitionKey string) (int64, error) {
	key := fmt.Sprintf("delivery:heartbeat:%s:%s", batchID, partitionKey)
	timestampStr, err := ds.cache.GetString(ctx, key)
	if err != nil {
		ds.logger.Errorw("Failed to get heartbeat time", "error", err, "batchID", batchID, "partitionKey", partitionKey)
		return 0, err
	}

	if timestampStr == "" {
		return 0, nil
	}

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		ds.logger.Errorw("Failed to parse heartbeat timestamp", "error", err, "batchID", batchID, "partitionKey", partitionKey)
		return 0, err
	}

	return timestamp, nil
}

// Sync2Counter 同步到计数器
func (ds *DeliverySyncer) Sync2Counter(ctx context.Context, batchID string, successIncr, failureIncr int64, isRetry bool) error {
	if successIncr != 0 {
		successKey := fmt.Sprintf("delivery:counter:success:%s", batchID)
		_, err := ds.cache.IncrBy(ctx, successKey, successIncr)
		if err != nil {
			ds.logger.Errorw("Failed to increment success counter", "error", err, "batchID", batchID)
			return err
		}
	}

	if failureIncr != 0 {
		failureKey := fmt.Sprintf("delivery:counter:failure:%s", batchID)
		_, err := ds.cache.IncrBy(ctx, failureKey, failureIncr)
		if err != nil {
			ds.logger.Errorw("Failed to increment failure counter", "error", err, "batchID", batchID)
			return err
		}
	}

	if isRetry {
		retryKey := fmt.Sprintf("delivery:counter:retry:%s", batchID)
		_, err := ds.cache.Incr(ctx, retryKey)
		if err != nil {
			ds.logger.Warnw("Failed to increment retry counter", "error", err, "batchID", batchID)
		}
	}

	ds.logger.Infow("Synced counters", "batchID", batchID, "successIncr", successIncr, "failureIncr", failureIncr, "isRetry", isRetry)
	return nil
}

// Sync2Statistics 同步到统计信息
func (ds *DeliverySyncer) Sync2Statistics(ctx context.Context, batchID string) (*StepStatistics, error) {
	successKey := fmt.Sprintf("delivery:counter:success:%s", batchID)
	failureKey := fmt.Sprintf("delivery:counter:failure:%s", batchID)

	successStr, err := ds.cache.GetString(ctx, successKey)
	if err != nil {
		ds.logger.Errorw("Failed to get success counter", "error", err, "batchID", batchID)
		return nil, err
	}

	failureStr, err := ds.cache.GetString(ctx, failureKey)
	if err != nil {
		ds.logger.Errorw("Failed to get failure counter", "error", err, "batchID", batchID)
		return nil, err
	}

	success, _ := strconv.ParseInt(successStr, 10, 64)
	failure, _ := strconv.ParseInt(failureStr, 10, 64)

	stats := &StepStatistics{
		Success: success,
		Failure: failure,
		Total:   success + failure,
	}

	ds.logger.Infow("Retrieved delivery statistics", "batchID", batchID, "stats", stats)
	return stats, nil
}

// ===== PrepareSyncer 方法 =====

// SetHeartBeatTime 设置准备阶段心跳时间
func (ps *PrepareSyncer) SetHeartBeatTime(ctx context.Context, batchID string, timestamp int64) error {
	key := fmt.Sprintf("prepare:heartbeat:%s", batchID)
	err := ps.cache.SetString(ctx, key, strconv.FormatInt(timestamp, 10), time.Hour)
	if err != nil {
		ps.logger.Errorw("Failed to set prepare heartbeat time", "error", err, "batchID", batchID)
		return err
	}

	return nil
}

// GetHeartBeatTime 获取准备阶段心跳时间
func (ps *PrepareSyncer) GetHeartBeatTime(ctx context.Context, batchID string) (int64, error) {
	key := fmt.Sprintf("prepare:heartbeat:%s", batchID)
	timestampStr, err := ps.cache.GetString(ctx, key)
	if err != nil {
		ps.logger.Errorw("Failed to get prepare heartbeat time", "error", err, "batchID", batchID)
		return 0, err
	}

	if timestampStr == "" {
		return 0, nil
	}

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		ps.logger.Errorw("Failed to parse prepare heartbeat timestamp", "error", err, "batchID", batchID)
		return 0, err
	}

	return timestamp, nil
}

// ReadThreadComplete 标记读取线程完成
func (ps *PrepareSyncer) ReadThreadComplete(ctx context.Context, batchID string) error {
	key := fmt.Sprintf("prepare:read:complete:%s", batchID)
	err := ps.cache.SetString(ctx, key, "true", 24*time.Hour)
	if err != nil {
		ps.logger.Errorw("Failed to mark read thread complete", "error", err, "batchID", batchID)
		return err
	}

	ps.logger.Infow("Read thread completed", "batchID", batchID)
	return nil
}

// IsReadThreadComplete 检查读取线程是否完成
func (ps *PrepareSyncer) IsReadThreadComplete(ctx context.Context, batchID string) (bool, error) {
	key := fmt.Sprintf("prepare:read:complete:%s", batchID)
	result, err := ps.cache.GetString(ctx, key)
	if err != nil {
		ps.logger.Errorw("Failed to check read thread status", "error", err, "batchID", batchID)
		return false, err
	}

	return result == "true", nil
}

// OneSaveThreadStart 标记一个保存线程开始
func (ps *PrepareSyncer) OneSaveThreadStart(ctx context.Context, batchID string) error {
	key := fmt.Sprintf("prepare:save:active:%s", batchID)
	_, err := ps.cache.Incr(ctx, key)
	if err != nil {
		ps.logger.Errorw("Failed to increment active save threads", "error", err, "batchID", batchID)
		return err
	}

	ps.logger.Infow("Save thread started", "batchID", batchID)
	return nil
}

// OneSaveThreadEnd 标记一个保存线程结束
func (ps *PrepareSyncer) OneSaveThreadEnd(ctx context.Context, batchID string) error {
	key := fmt.Sprintf("prepare:save:active:%s", batchID)
	_, err := ps.cache.Decr(ctx, key)
	if err != nil {
		ps.logger.Errorw("Failed to decrement active save threads", "error", err, "batchID", batchID)
		return err
	}

	ps.logger.Infow("Save thread ended", "batchID", batchID)
	return nil
}

// AllSaveThreadEnd 检查所有保存线程是否已结束
func (ps *PrepareSyncer) AllSaveThreadEnd(ctx context.Context, batchID string) (bool, error) {
	key := fmt.Sprintf("prepare:save:active:%s", batchID)
	countStr, err := ps.cache.GetString(ctx, key)
	if err != nil {
		ps.logger.Errorw("Failed to get active save threads count", "error", err, "batchID", batchID)
		return false, err
	}

	if countStr == "" {
		return true, nil
	}

	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		ps.logger.Errorw("Failed to parse active save threads count", "error", err, "batchID", batchID)
		return false, err
	}

	return count <= 0, nil
}

// GetStatistics 获取准备阶段统计信息
func (ps *PrepareSyncer) GetStatistics(ctx context.Context, batchID string) (*StepStatistics, error) {
	successKey := fmt.Sprintf("prepare:counter:success:%s", batchID)
	failureKey := fmt.Sprintf("prepare:counter:failure:%s", batchID)

	successStr, err := ps.cache.GetString(ctx, successKey)
	if err != nil {
		ps.logger.Errorw("Failed to get prepare success counter", "error", err, "batchID", batchID)
		return nil, err
	}

	failureStr, err := ps.cache.GetString(ctx, failureKey)
	if err != nil {
		ps.logger.Errorw("Failed to get prepare failure counter", "error", err, "batchID", batchID)
		return nil, err
	}

	success, _ := strconv.ParseInt(successStr, 10, 64)
	failure, _ := strconv.ParseInt(failureStr, 10, 64)

	stats := &StepStatistics{
		Success: success,
		Failure: failure,
		Total:   success + failure,
	}

	ps.logger.Infow("Retrieved prepare statistics", "batchID", batchID, "stats", stats)
	return stats, nil
}

// IncrementStatistics 增加统计计数
func (ps *PrepareSyncer) IncrementStatistics(ctx context.Context, batchID string, successIncr, failureIncr int64) error {
	if successIncr > 0 {
		successKey := fmt.Sprintf("prepare:counter:success:%s", batchID)
		_, err := ps.cache.IncrBy(ctx, successKey, successIncr)
		if err != nil {
			ps.logger.Errorw("Failed to increment prepare success counter", "error", err, "batchID", batchID)
			return err
		}
	}

	if failureIncr > 0 {
		failureKey := fmt.Sprintf("prepare:counter:failure:%s", batchID)
		_, err := ps.cache.IncrBy(ctx, failureKey, failureIncr)
		if err != nil {
			ps.logger.Errorw("Failed to increment prepare failure counter", "error", err, "batchID", batchID)
			return err
		}
	}

	ps.logger.Infow("Incremented prepare statistics", "batchID", batchID, "successIncr", successIncr, "failureIncr", failureIncr)
	return nil
}

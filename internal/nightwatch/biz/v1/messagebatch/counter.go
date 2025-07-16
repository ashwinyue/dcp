// Package messagebatch provides business logic for message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// CounterService 提供Redis计数器相关的业务服务
type CounterService struct {
	cache  *cache.CacheManager
	logger log.Logger
}

// NewCounterService 创建新的计数器服务
func NewCounterService(cache *cache.CacheManager, logger log.Logger) *CounterService {
	return &CounterService{
		cache:  cache,
		logger: logger,
	}
}

// IncrBatchCounter 增加批次计数器
func (c *CounterService) IncrBatchCounter(ctx context.Context, batchID string) (int64, error) {
	key := fmt.Sprintf("batch:counter:%s", batchID)
	count, err := c.cache.Incr(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to increment batch counter", "error", err, "batchID", batchID)
		return 0, err
	}

	c.logger.Infow("Batch counter incremented", "batchID", batchID, "count", count)
	return count, nil
}

// GetBatchCounter 获取批次计数器值
func (c *CounterService) GetBatchCounter(ctx context.Context, batchID string) (int64, error) {
	key := fmt.Sprintf("batch:counter:%s", batchID)
	countStr, err := c.cache.GetString(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to get batch counter", "error", err, "batchID", batchID)
		return 0, err
	}

	if countStr == "" {
		return 0, nil
	}

	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		c.logger.Errorw("Failed to parse batch counter", "error", err, "batchID", batchID, "value", countStr)
		return 0, err
	}

	return count, nil
}

// ResetBatchCounter 重置批次计数器
func (c *CounterService) ResetBatchCounter(ctx context.Context, batchID string) error {
	key := fmt.Sprintf("batch:counter:%s", batchID)
	err := c.cache.Delete(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to reset batch counter", "error", err, "batchID", batchID)
		return err
	}

	c.logger.Infow("Batch counter reset", "batchID", batchID)
	return nil
}

// SetBatchCounterExpiry 设置批次计数器过期时间
func (c *CounterService) SetBatchCounterExpiry(ctx context.Context, batchID string, ttl time.Duration) error {
	key := fmt.Sprintf("batch:counter:%s", batchID)
	err := c.cache.SetExpiration(ctx, key, ttl)
	if err != nil {
		c.logger.Errorw("Failed to set batch counter expiry", "error", err, "batchID", batchID, "ttl", ttl)
		return err
	}

	c.logger.Infow("Batch counter expiry set", "batchID", batchID, "ttl", ttl)
	return nil
}

// IncrMessageCounter 增加消息计数器
func (c *CounterService) IncrMessageCounter(ctx context.Context, topicName string) (int64, error) {
	key := fmt.Sprintf("message:counter:%s", topicName)
	count, err := c.cache.Incr(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to increment message counter", "error", err, "topic", topicName)
		return 0, err
	}

	return count, nil
}

// GetMessageCounter 获取消息计数器值
func (c *CounterService) GetMessageCounter(ctx context.Context, topicName string) (int64, error) {
	key := fmt.Sprintf("message:counter:%s", topicName)
	countStr, err := c.cache.GetString(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to get message counter", "error", err, "topic", topicName)
		return 0, err
	}

	if countStr == "" {
		return 0, nil
	}

	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		c.logger.Errorw("Failed to parse message counter", "error", err, "topic", topicName, "value", countStr)
		return 0, err
	}

	return count, nil
}

// IncrPartitionCounter 增加分区计数器
func (c *CounterService) IncrPartitionCounter(ctx context.Context, partitionID int) (int64, error) {
	key := fmt.Sprintf("partition:counter:%d", partitionID)
	count, err := c.cache.Incr(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to increment partition counter", "error", err, "partitionID", partitionID)
		return 0, err
	}

	return count, nil
}

// BatchCounterExists 检查批次计数器是否存在
func (c *CounterService) BatchCounterExists(ctx context.Context, batchID string) (bool, error) {
	key := fmt.Sprintf("batch:counter:%s", batchID)
	count, err := c.cache.Exists(ctx, key)
	if err != nil {
		c.logger.Errorw("Failed to check batch counter existence", "error", err, "batchID", batchID)
		return false, err
	}

	return count > 0, nil
}

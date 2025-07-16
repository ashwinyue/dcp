// Package cache provides Redis cache integration for nightwatch store layer
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/redis/go-redis/v9"
)

// RedisClient defines the interface for Redis operations
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HExists(ctx context.Context, key, field string) *redis.BoolCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	TTL(ctx context.Context, key string) *redis.DurationCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	Decr(ctx context.Context, key string) *redis.IntCmd
	Close() error
}

// CacheManager manages Redis cache operations
type CacheManager struct {
	client RedisClient
	logger log.Logger
	prefix string
}

// CacheOptions defines configuration options for cache manager
type CacheOptions struct {
	Addr         string        `mapstructure:"addr"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool-size"`
	MinIdleConns int           `mapstructure:"min-idle-conns"`
	DialTimeout  time.Duration `mapstructure:"dial-timeout"`
	ReadTimeout  time.Duration `mapstructure:"read-timeout"`
	WriteTimeout time.Duration `mapstructure:"write-timeout"`
	PoolTimeout  time.Duration `mapstructure:"pool-timeout"`
	Prefix       string        `mapstructure:"prefix"`
}

// NewCacheManager creates a new cache manager with Redis client
func NewCacheManager(opts *CacheOptions, logger log.Logger) (*CacheManager, error) {
	if opts == nil {
		return nil, fmt.Errorf("cache options cannot be nil")
	}

	// Set default values
	if opts.PoolSize == 0 {
		opts.PoolSize = 10
	}
	if opts.MinIdleConns == 0 {
		opts.MinIdleConns = 2
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 3 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 3 * time.Second
	}
	if opts.PoolTimeout == 0 {
		opts.PoolTimeout = 4 * time.Second
	}
	if opts.Prefix == "" {
		opts.Prefix = "nightwatch:"
	}

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:         opts.Addr,
		Password:     opts.Password,
		DB:           opts.DB,
		PoolSize:     opts.PoolSize,
		MinIdleConns: opts.MinIdleConns,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		PoolTimeout:  opts.PoolTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Infow("Redis连接成功",
		"addr", opts.Addr,
		"db", opts.DB,
		"prefix", opts.Prefix,
	)

	return &CacheManager{
		client: rdb,
		logger: logger,
		prefix: opts.Prefix,
	}, nil
}

// buildKey builds cache key with prefix
func (c *CacheManager) buildKey(key string) string {
	return c.prefix + key
}

// Set stores a value in cache with expiration
func (c *CacheManager) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	cacheKey := c.buildKey(key)
	if err := c.client.Set(ctx, cacheKey, data, expiration).Err(); err != nil {
		c.logger.Errorw("Failed to set cache",
			"key", cacheKey,
			"error", err,
		)
		return fmt.Errorf("failed to set cache: %w", err)
	}

	c.logger.Debugw("Cache set successfully",
		"key", cacheKey,
		"expiration", expiration,
	)

	return nil
}

// Get retrieves a value from cache
func (c *CacheManager) Get(ctx context.Context, key string, dest interface{}) error {
	cacheKey := c.buildKey(key)
	data, err := c.client.Get(ctx, cacheKey).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("cache miss for key: %s", cacheKey)
		}
		c.logger.Errorw("Failed to get cache",
			"key", cacheKey,
			"error", err,
		)
		return fmt.Errorf("failed to get cache: %w", err)
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to unmarshal cache data: %w", err)
	}

	c.logger.Debugw("Cache retrieved successfully",
		"key", cacheKey,
	)

	return nil
}

// Delete removes keys from cache
func (c *CacheManager) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	cacheKeys := make([]string, len(keys))
	for i, key := range keys {
		cacheKeys[i] = c.buildKey(key)
	}

	deleted, err := c.client.Del(ctx, cacheKeys...).Result()
	if err != nil {
		c.logger.Errorw("Failed to delete cache",
			"keys", cacheKeys,
			"error", err,
		)
		return fmt.Errorf("failed to delete cache: %w", err)
	}

	c.logger.Debugw("Cache deleted successfully",
		"keys", cacheKeys,
		"deleted", deleted,
	)

	return nil
}

// Exists checks if keys exist in cache
func (c *CacheManager) Exists(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	cacheKeys := make([]string, len(keys))
	for i, key := range keys {
		cacheKeys[i] = c.buildKey(key)
	}

	count, err := c.client.Exists(ctx, cacheKeys...).Result()
	if err != nil {
		c.logger.Errorw("Failed to check cache existence",
			"keys", cacheKeys,
			"error", err,
		)
		return 0, fmt.Errorf("failed to check cache existence: %w", err)
	}

	return count, nil
}

// SetHash stores a hash field value
func (c *CacheManager) SetHash(ctx context.Context, key, field string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	cacheKey := c.buildKey(key)
	if err := c.client.HSet(ctx, cacheKey, field, data).Err(); err != nil {
		c.logger.Errorw("Failed to set hash cache",
			"key", cacheKey,
			"field", field,
			"error", err,
		)
		return fmt.Errorf("failed to set hash cache: %w", err)
	}

	c.logger.Debugw("Hash cache set successfully",
		"key", cacheKey,
		"field", field,
	)

	return nil
}

// GetHash retrieves a hash field value
func (c *CacheManager) GetHash(ctx context.Context, key, field string, dest interface{}) error {
	cacheKey := c.buildKey(key)
	data, err := c.client.HGet(ctx, cacheKey, field).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("cache miss for hash key: %s, field: %s", cacheKey, field)
		}
		c.logger.Errorw("Failed to get hash cache",
			"key", cacheKey,
			"field", field,
			"error", err,
		)
		return fmt.Errorf("failed to get hash cache: %w", err)
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to unmarshal hash cache data: %w", err)
	}

	c.logger.Debugw("Hash cache retrieved successfully",
		"key", cacheKey,
		"field", field,
	)

	return nil
}

// DeleteHash removes hash fields
func (c *CacheManager) DeleteHash(ctx context.Context, key string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}

	cacheKey := c.buildKey(key)
	deleted, err := c.client.HDel(ctx, cacheKey, fields...).Result()
	if err != nil {
		c.logger.Errorw("Failed to delete hash cache",
			"key", cacheKey,
			"fields", fields,
			"error", err,
		)
		return fmt.Errorf("failed to delete hash cache: %w", err)
	}

	c.logger.Debugw("Hash cache deleted successfully",
		"key", cacheKey,
		"fields", fields,
		"deleted", deleted,
	)

	return nil
}

// SetExpiration sets expiration for a key
func (c *CacheManager) SetExpiration(ctx context.Context, key string, expiration time.Duration) error {
	cacheKey := c.buildKey(key)
	if err := c.client.Expire(ctx, cacheKey, expiration).Err(); err != nil {
		c.logger.Errorw("Failed to set cache expiration",
			"key", cacheKey,
			"expiration", expiration,
			"error", err,
		)
		return fmt.Errorf("failed to set cache expiration: %w", err)
	}

	c.logger.Debugw("Cache expiration set successfully",
		"key", cacheKey,
		"expiration", expiration,
	)

	return nil
}

// GetTTL gets time to live for a key
func (c *CacheManager) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	cacheKey := c.buildKey(key)
	ttl, err := c.client.TTL(ctx, cacheKey).Result()
	if err != nil {
		c.logger.Errorw("Failed to get cache TTL",
			"key", cacheKey,
			"error", err,
		)
		return 0, fmt.Errorf("failed to get cache TTL: %w", err)
	}

	return ttl, nil
}

// Close closes the Redis connection
func (c *CacheManager) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Incr increments a counter key by 1
func (c *CacheManager) Incr(ctx context.Context, key string) (int64, error) {
	cacheKey := c.buildKey(key)
	val, err := c.client.Incr(ctx, cacheKey).Result()
	if err != nil {
		c.logger.Errorw("Failed to increment counter",
			"key", cacheKey,
			"error", err,
		)
		return 0, fmt.Errorf("failed to increment counter: %w", err)
	}

	c.logger.Debugw("Counter incremented successfully",
		"key", cacheKey,
		"value", val,
	)

	return val, nil
}

// GetString retrieves a string value from cache
func (c *CacheManager) GetString(ctx context.Context, key string) (string, error) {
	cacheKey := c.buildKey(key)
	data, err := c.client.Get(ctx, cacheKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil // Return empty string for cache miss
		}
		c.logger.Errorw("Failed to get string cache",
			"key", cacheKey,
			"error", err,
		)
		return "", fmt.Errorf("failed to get string cache: %w", err)
	}

	c.logger.Debugw("String cache retrieved successfully",
		"key", cacheKey,
	)

	return data, nil
}

// SetString stores a string value in cache with expiration
func (c *CacheManager) SetString(ctx context.Context, key, value string, expiration time.Duration) error {
	cacheKey := c.buildKey(key)
	if err := c.client.Set(ctx, cacheKey, value, expiration).Err(); err != nil {
		c.logger.Errorw("Failed to set string cache",
			"key", cacheKey,
			"error", err,
		)
		return fmt.Errorf("failed to set string cache: %w", err)
	}

	c.logger.Debugw("String cache set successfully",
		"key", cacheKey,
		"expiration", expiration,
	)

	return nil
}

// IncrBy increments a counter key by the specified value
func (c *CacheManager) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	cacheKey := c.buildKey(key)
	val, err := c.client.IncrBy(ctx, cacheKey, value).Result()
	if err != nil {
		c.logger.Errorw("Failed to increment counter by value",
			"key", cacheKey,
			"value", value,
			"error", err,
		)
		return 0, fmt.Errorf("failed to increment counter by value: %w", err)
	}

	c.logger.Debugw("Counter incremented by value successfully",
		"key", cacheKey,
		"value", val,
		"increment", value,
	)

	return val, nil
}

// Decr decrements a counter key by 1
func (c *CacheManager) Decr(ctx context.Context, key string) (int64, error) {
	cacheKey := c.buildKey(key)
	val, err := c.client.Decr(ctx, cacheKey).Result()
	if err != nil {
		c.logger.Errorw("Failed to decrement counter",
			"key", cacheKey,
			"error", err,
		)
		return 0, fmt.Errorf("failed to decrement counter: %w", err)
	}

	c.logger.Debugw("Counter decremented successfully",
		"key", cacheKey,
		"value", val,
	)

	return val, nil
}

// GetStats returns cache statistics
func (c *CacheManager) GetStats() map[string]interface{} {
	// This would be expanded with actual Redis stats
	return map[string]interface{}{
		"prefix": c.prefix,
		"status": "connected",
	}
}

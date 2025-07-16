// Package messagebatch provides MongoDB shard management for message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBShardManager manages MongoDB sharding for message batch processing
type MongoDBShardManager struct {
	client     *mongo.Client
	database   string
	collection string
	logger     log.Logger
	mu         sync.RWMutex

	// Shard configuration
	shardKey     string
	shardCount   int
	replicaCount int

	// Connection pool
	connections  map[string]*mongo.Client
	shardMapping map[int]string // partition ID -> shard key

	// Statistics
	stats *MongoDBShardStats
}

// MongoDBShardStats holds statistics for MongoDB shard operations
type MongoDBShardStats struct {
	TotalShards       int
	ActiveShards      int
	TotalOperations   int64
	FailedOperations  int64
	AverageLatency    time.Duration
	LastOperationTime time.Time
	mu                sync.RWMutex
}

// ShardDocument represents a document structure for MongoDB sharding
type ShardDocument struct {
	ID          string                 `bson:"_id"`
	ShardKey    string                 `bson:"shard_key"`
	PartitionID int                    `bson:"partition_id"`
	MessageID   string                 `bson:"message_id"`
	Content     interface{}            `bson:"content"`
	Metadata    map[string]interface{} `bson:"metadata"`
	CreatedAt   time.Time              `bson:"created_at"`
	UpdatedAt   time.Time              `bson:"updated_at"`
	Status      string                 `bson:"status"`
}

// NewMongoDBShardManager creates a new MongoDB shard manager
func NewMongoDBShardManager(
	client *mongo.Client,
	database, collection string,
	logger log.Logger,
) *MongoDBShardManager {
	manager := &MongoDBShardManager{
		client:       client,
		database:     database,
		collection:   collection,
		logger:       logger,
		shardKey:     "shard_key",
		shardCount:   128, // Match partition count
		replicaCount: 3,   // Default replica count
		connections:  make(map[string]*mongo.Client),
		shardMapping: make(map[int]string),
		stats: &MongoDBShardStats{
			TotalShards: 128,
		},
	}

	// Initialize shard mapping
	manager.initializeShardMapping()

	return manager
}

// initializeShardMapping initializes the mapping between partitions and shards
func (m *MongoDBShardManager) initializeShardMapping() {
	for i := 0; i < m.shardCount; i++ {
		shardKey := m.calculateShardKey(i)
		m.shardMapping[i] = shardKey
	}

	m.logger.Infow("Initialized shard mapping",
		"shardCount", m.shardCount,
		"database", m.database,
		"collection", m.collection,
	)
}

// calculateShardKey calculates shard key for a partition
func (m *MongoDBShardManager) calculateShardKey(partitionID int) string {
	// Use consistent hashing to distribute partitions across shards
	hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("partition_%d", partitionID)))
	shardID := hash % uint32(m.shardCount)
	return fmt.Sprintf("shard_%d", shardID)
}

// InsertPartitionData inserts partition data into the appropriate shard
func (m *MongoDBShardManager) InsertPartitionData(
	ctx context.Context,
	partitionID int,
	messages []MessageData,
) error {
	if len(messages) == 0 {
		return nil
	}

	shardKey, exists := m.shardMapping[partitionID]
	if !exists {
		return fmt.Errorf("no shard mapping found for partition %d", partitionID)
	}

	// Prepare documents for insertion
	documents := make([]interface{}, 0, len(messages))
	for _, msg := range messages {
		doc := ShardDocument{
			ID:          fmt.Sprintf("%s_%s", shardKey, msg.ID),
			ShardKey:    shardKey,
			PartitionID: partitionID,
			MessageID:   msg.ID,
			Content:     msg,
			Metadata: map[string]interface{}{
				"recipient":     msg.Recipient,
				"template":      msg.Template,
				"type":          msg.Type,
				"priority":      msg.Priority,
				"partition_key": msg.PartitionKey,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Status:    "pending",
		}
		documents = append(documents, doc)
	}

	// Insert documents into MongoDB
	startTime := time.Now()
	collection := m.client.Database(m.database).Collection(m.collection)

	opts := options.InsertMany().SetOrdered(false) // Allow partial success
	result, err := collection.InsertMany(ctx, documents, opts)

	duration := time.Since(startTime)
	m.updateStats(int64(len(documents)), err != nil, duration)

	if err != nil {
		m.logger.Errorw("Failed to insert partition data",
			"partitionID", partitionID,
			"shardKey", shardKey,
			"messageCount", len(messages),
			"error", err,
			"duration", duration,
		)
		return fmt.Errorf("failed to insert partition data: %w", err)
	}

	m.logger.Infow("Successfully inserted partition data",
		"partitionID", partitionID,
		"shardKey", shardKey,
		"messageCount", len(messages),
		"insertedCount", len(result.InsertedIDs),
		"duration", duration,
	)

	return nil
}

// UpdatePartitionStatus updates the status of messages in a partition
func (m *MongoDBShardManager) UpdatePartitionStatus(
	ctx context.Context,
	partitionID int,
	status string,
) error {
	shardKey, exists := m.shardMapping[partitionID]
	if !exists {
		return fmt.Errorf("no shard mapping found for partition %d", partitionID)
	}

	startTime := time.Now()
	collection := m.client.Database(m.database).Collection(m.collection)

	filter := bson.M{
		"partition_id": partitionID,
		"shard_key":    shardKey,
	}

	update := bson.M{
		"$set": bson.M{
			"status":     status,
			"updated_at": time.Now(),
		},
	}

	result, err := collection.UpdateMany(ctx, filter, update)
	duration := time.Since(startTime)

	m.updateStats(1, err != nil, duration)

	if err != nil {
		m.logger.Errorw("Failed to update partition status",
			"partitionID", partitionID,
			"shardKey", shardKey,
			"status", status,
			"error", err,
			"duration", duration,
		)
		return fmt.Errorf("failed to update partition status: %w", err)
	}

	m.logger.Infow("Successfully updated partition status",
		"partitionID", partitionID,
		"shardKey", shardKey,
		"status", status,
		"matchedCount", result.MatchedCount,
		"modifiedCount", result.ModifiedCount,
		"duration", duration,
	)

	return nil
}

// GetPartitionData retrieves data for a specific partition
func (m *MongoDBShardManager) GetPartitionData(
	ctx context.Context,
	partitionID int,
	limit int64,
) ([]ShardDocument, error) {
	shardKey, exists := m.shardMapping[partitionID]
	if !exists {
		return nil, fmt.Errorf("no shard mapping found for partition %d", partitionID)
	}

	startTime := time.Now()
	collection := m.client.Database(m.database).Collection(m.collection)

	filter := bson.M{
		"partition_id": partitionID,
		"shard_key":    shardKey,
	}

	opts := options.Find().SetLimit(limit)
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		duration := time.Since(startTime)
		m.updateStats(1, true, duration)
		return nil, fmt.Errorf("failed to find partition data: %w", err)
	}
	defer cursor.Close(ctx)

	var documents []ShardDocument
	if err := cursor.All(ctx, &documents); err != nil {
		duration := time.Since(startTime)
		m.updateStats(1, true, duration)
		return nil, fmt.Errorf("failed to decode partition data: %w", err)
	}

	duration := time.Since(startTime)
	m.updateStats(1, false, duration)

	m.logger.Infow("Retrieved partition data",
		"partitionID", partitionID,
		"shardKey", shardKey,
		"documentCount", len(documents),
		"duration", duration,
	)

	return documents, nil
}

// GetPartitionStats returns statistics for a specific partition
func (m *MongoDBShardManager) GetPartitionStats(
	ctx context.Context,
	partitionID int,
) (*PartitionStats, error) {
	shardKey, exists := m.shardMapping[partitionID]
	if !exists {
		return nil, fmt.Errorf("no shard mapping found for partition %d", partitionID)
	}

	startTime := time.Now()
	collection := m.client.Database(m.database).Collection(m.collection)

	// Aggregate pipeline to get partition statistics
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"partition_id": partitionID,
				"shard_key":    shardKey,
			},
		},
		{
			"$group": bson.M{
				"_id":   "$status",
				"count": bson.M{"$sum": 1},
			},
		},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		duration := time.Since(startTime)
		m.updateStats(1, true, duration)
		return nil, fmt.Errorf("failed to aggregate partition stats: %w", err)
	}
	defer cursor.Close(ctx)

	stats := &PartitionStats{
		PartitionID:  partitionID,
		ShardKey:     shardKey,
		StatusCounts: make(map[string]int64),
	}

	for cursor.Next(ctx) {
		var result struct {
			ID    string `bson:"_id"`
			Count int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			continue
		}
		stats.StatusCounts[result.ID] = result.Count
		stats.TotalCount += result.Count
	}

	duration := time.Since(startTime)
	m.updateStats(1, false, duration)

	return stats, nil
}

// PartitionStats holds statistics for a partition
type PartitionStats struct {
	PartitionID  int              `json:"partition_id"`
	ShardKey     string           `json:"shard_key"`
	TotalCount   int64            `json:"total_count"`
	StatusCounts map[string]int64 `json:"status_counts"`
}

// DeletePartitionData deletes data for a specific partition
func (m *MongoDBShardManager) DeletePartitionData(
	ctx context.Context,
	partitionID int,
) error {
	shardKey, exists := m.shardMapping[partitionID]
	if !exists {
		return fmt.Errorf("no shard mapping found for partition %d", partitionID)
	}

	startTime := time.Now()
	collection := m.client.Database(m.database).Collection(m.collection)

	filter := bson.M{
		"partition_id": partitionID,
		"shard_key":    shardKey,
	}

	result, err := collection.DeleteMany(ctx, filter)
	duration := time.Since(startTime)

	m.updateStats(1, err != nil, duration)

	if err != nil {
		m.logger.Errorw("Failed to delete partition data",
			"partitionID", partitionID,
			"shardKey", shardKey,
			"error", err,
			"duration", duration,
		)
		return fmt.Errorf("failed to delete partition data: %w", err)
	}

	m.logger.Infow("Successfully deleted partition data",
		"partitionID", partitionID,
		"shardKey", shardKey,
		"deletedCount", result.DeletedCount,
		"duration", duration,
	)

	return nil
}

// EnsureShardedCollection ensures the collection is properly sharded
func (m *MongoDBShardManager) EnsureShardedCollection(ctx context.Context) error {
	adminDB := m.client.Database("admin")

	// Enable sharding for database
	enableShardCmd := bson.M{
		"enableSharding": m.database,
	}

	if err := adminDB.RunCommand(ctx, enableShardCmd).Err(); err != nil {
		// Ignore "already enabled" errors
		m.logger.Infow("Database sharding status", "database", m.database, "error", err)
	}

	// Shard the collection
	shardCollectionCmd := bson.M{
		"shardCollection": fmt.Sprintf("%s.%s", m.database, m.collection),
		"key": bson.M{
			m.shardKey: 1,
		},
	}

	if err := adminDB.RunCommand(ctx, shardCollectionCmd).Err(); err != nil {
		// Ignore "already sharded" errors
		m.logger.Infow("Collection sharding status",
			"collection", fmt.Sprintf("%s.%s", m.database, m.collection),
			"error", err,
		)
	}

	m.logger.Infow("Ensured sharded collection",
		"database", m.database,
		"collection", m.collection,
		"shardKey", m.shardKey,
	)

	return nil
}

// updateStats updates MongoDB operation statistics
func (m *MongoDBShardManager) updateStats(operations int64, failed bool, duration time.Duration) {
	m.stats.mu.Lock()
	defer m.stats.mu.Unlock()

	m.stats.TotalOperations += operations
	if failed {
		m.stats.FailedOperations += operations
	}

	// Update average latency
	if m.stats.TotalOperations == 1 {
		m.stats.AverageLatency = duration
	} else {
		// Exponential moving average
		alpha := 0.1
		m.stats.AverageLatency = time.Duration(float64(m.stats.AverageLatency)*(1-alpha) + float64(duration)*alpha)
	}

	m.stats.LastOperationTime = time.Now()
}

// GetStats returns MongoDB shard manager statistics
func (m *MongoDBShardManager) GetStats() *MongoDBShardStats {
	m.stats.mu.RLock()
	defer m.stats.mu.RUnlock()

	return &MongoDBShardStats{
		TotalShards:       m.stats.TotalShards,
		ActiveShards:      m.stats.ActiveShards,
		TotalOperations:   m.stats.TotalOperations,
		FailedOperations:  m.stats.FailedOperations,
		AverageLatency:    m.stats.AverageLatency,
		LastOperationTime: m.stats.LastOperationTime,
	}
}

// Close closes the MongoDB shard manager
func (m *MongoDBShardManager) Close(ctx context.Context) error {
	// Close additional connections
	for name, client := range m.connections {
		if err := client.Disconnect(ctx); err != nil {
			m.logger.Errorw("Failed to close MongoDB connection",
				"connectionName", name,
				"error", err,
			)
		}
	}

	m.logger.Infow("MongoDB shard manager closed")
	return nil
}

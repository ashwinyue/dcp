// Package messagebatch provides helper functions and utilities for message batch processing
package messagebatch

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// MongoHelper provides MongoDB operations for message batch processing
type MongoHelper struct {
	client     *mongo.Client
	database   string
	collection string
	logger     log.Logger
}

// NewMongoHelper creates a new MongoDB helper instance
func NewMongoHelper(client *mongo.Client, database, collection string, logger log.Logger) *MongoHelper {
	return &MongoHelper{
		client:     client,
		database:   database,
		collection: collection,
		logger:     logger,
	}
}

// RetryConfig defines retry configuration
type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	BackoffFactor float64
	MaxDelay      time.Duration
	Timeout       time.Duration
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  time.Second,
		BackoffFactor: 2.0,
		MaxDelay:      time.Minute,
		Timeout:       time.Minute * 5,
	}
}

// RetryableError defines an error that can be retried
type RetryableError struct {
	Err       error
	Retryable bool
	Code      string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable=%t code=%s error=%s", e.Retryable, e.Code, e.Err.Error())
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// NewRetryableError creates a new retryable error
func NewRetryableError(err error, retryable bool, code string) *RetryableError {
	return &RetryableError{
		Err:       err,
		Retryable: retryable,
		Code:      code,
	}
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if retryErr, ok := err.(*RetryableError); ok {
		return retryErr.Retryable
	}

	// Check for common retryable error patterns
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"timeout",
		"connection refused",
		"network is unreachable",
		"temporary failure",
		"service unavailable",
		"too many requests",
		"rate limit",
		"duplicate key error", // MongoDB specific
		"connection reset",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// ExecuteWithRetry executes a function with retry logic and MongoDB support
func ExecuteWithRetry(
	ctx context.Context,
	config *RetryConfig,
	operation func(ctx context.Context) error,
	logger log.Logger,
	operationName string,
) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	// Create context with timeout
	opCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	var lastErr error
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			logger.Infow("Retrying operation",
				"operation", operationName,
				"attempt", attempt,
				"delay", delay,
				"lastError", lastErr,
			)

			// Wait before retry
			select {
			case <-opCtx.Done():
				return fmt.Errorf("operation timeout: %w", opCtx.Err())
			case <-time.After(delay):
			}
		}

		// Execute operation
		if err := operation(opCtx); err != nil {
			lastErr = err

			// Check if error is retryable
			if !IsRetryableError(err) {
				logger.Errorw("Non-retryable error encountered",
					"operation", operationName,
					"attempt", attempt,
					"error", err,
				)
				return err
			}

			// Calculate next delay with exponential backoff
			if attempt < config.MaxRetries {
				delay = time.Duration(float64(delay) * config.BackoffFactor)
				if delay > config.MaxDelay {
					delay = config.MaxDelay
				}
			}

			continue
		}

		// Success
		if attempt > 0 {
			logger.Infow("Operation succeeded after retry",
				"operation", operationName,
				"attempt", attempt,
			)
		}
		return nil
	}

	logger.Errorw("Operation failed after all retries",
		"operation", operationName,
		"maxRetries", config.MaxRetries,
		"lastError", lastErr,
	)

	return fmt.Errorf("operation failed after %d retries: %w", config.MaxRetries, lastErr)
}

// CalculatePartitionID calculates partition ID for a given key
func CalculatePartitionID(key string, partitionCount int) int {
	hash := crc32.ChecksumIEEE([]byte(key))
	return int(hash % uint32(partitionCount))
}

// GenerateTaskID generates a unique task ID with MongoDB document ID support
func GenerateTaskID(prefix string, partitionID int) string {
	timestamp := time.Now().Unix()
	hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s_%d_%d", prefix, partitionID, timestamp)))
	return fmt.Sprintf("%s_%d_%x", prefix, partitionID, hash)
}

// ConvertMessageToTask converts MessageData to PartitionTask with MongoDB support
func (h *MongoHelper) ConvertMessageToTask(ctx context.Context, messages []MessageData, partitionID int) (*PartitionTask, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	task := &PartitionTask{
		ID:           GenerateTaskID("task", partitionID),
		BatchID:      messages[0].ID, // Use first message ID as batch reference
		PartitionKey: fmt.Sprintf("partition_%d", partitionID),
		Status:       "pending",
		MessageCount: int64(len(messages)),
		RetryCount:   0,
		TaskCode:     GenerateTaskCode(messages),
		Metadata:     messages,
	}

	// Store task in MongoDB with retry logic
	err := ExecuteWithRetry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
		return h.StorePartitionTask(ctx, task)
	}, h.logger, fmt.Sprintf("store_partition_task_%s", task.ID))

	if err != nil {
		h.logger.Errorw("Failed to store partition task in MongoDB",
			"taskID", task.ID,
			"partitionID", partitionID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to store partition task: %w", err)
	}

	return task, nil
}

// StorePartitionTask stores a partition task in MongoDB
func (h *MongoHelper) StorePartitionTask(ctx context.Context, task *PartitionTask) error {
	collection := h.client.Database(h.database).Collection(h.collection)

	// Prepare document for MongoDB
	document := bson.M{
		"_id":           task.ID,
		"batch_id":      task.BatchID,
		"partition_key": task.PartitionKey,
		"status":        task.Status,
		"message_count": task.MessageCount,
		"retry_count":   task.RetryCount,
		"task_code":     task.TaskCode,
		"metadata":      task.Metadata,
		"created_at":    time.Now(),
		"updated_at":    time.Now(),
	}

	if task.ProcessedAt != nil {
		document["processed_at"] = *task.ProcessedAt
	}
	if task.CompletedAt != nil {
		document["completed_at"] = *task.CompletedAt
	}
	if task.ErrorMessage != "" {
		document["error_message"] = task.ErrorMessage
	}

	_, err := collection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to insert partition task: %w", err)
	}

	h.logger.Infow("Stored partition task in MongoDB",
		"taskID", task.ID,
		"partitionKey", task.PartitionKey,
		"messageCount", task.MessageCount,
	)

	return nil
}

// UpdatePartitionTaskStatus updates a partition task status in MongoDB
func (h *MongoHelper) UpdatePartitionTaskStatus(ctx context.Context, taskID, status string, errorMessage ...string) error {
	collection := h.client.Database(h.database).Collection(h.collection)

	update := bson.M{
		"$set": bson.M{
			"status":     status,
			"updated_at": time.Now(),
		},
	}

	if len(errorMessage) > 0 && errorMessage[0] != "" {
		update["$set"].(bson.M)["error_message"] = errorMessage[0]
	}

	if status == "completed" {
		update["$set"].(bson.M)["completed_at"] = time.Now()
	}

	filter := bson.M{"_id": taskID}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update partition task status: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("partition task not found: %s", taskID)
	}

	h.logger.Infow("Updated partition task status",
		"taskID", taskID,
		"status", status,
		"modifiedCount", result.ModifiedCount,
	)

	return nil
}

// GetPartitionTasks retrieves partition tasks from MongoDB with optional filtering
func (h *MongoHelper) GetPartitionTasks(ctx context.Context, filter bson.M, limit int64) ([]*PartitionTask, error) {
	collection := h.client.Database(h.database).Collection(h.collection)

	opts := options.Find()
	if limit > 0 {
		opts.SetLimit(limit)
	}
	opts.SetSort(bson.D{{"created_at", -1}})

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find partition tasks: %w", err)
	}
	defer cursor.Close(ctx)

	var tasks []*PartitionTask
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			h.logger.Errorw("Failed to decode partition task", "error", err)
			continue
		}

		task := h.mongoDocToPartitionTask(doc)
		tasks = append(tasks, task)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return tasks, nil
}

// mongoDocToPartitionTask converts MongoDB document to PartitionTask
func (h *MongoHelper) mongoDocToPartitionTask(doc bson.M) *PartitionTask {
	task := &PartitionTask{}

	if id, ok := doc["_id"].(string); ok {
		task.ID = id
	}
	if batchID, ok := doc["batch_id"].(string); ok {
		task.BatchID = batchID
	}
	if partitionKey, ok := doc["partition_key"].(string); ok {
		task.PartitionKey = partitionKey
	}
	if status, ok := doc["status"].(string); ok {
		task.Status = status
	}
	if messageCount, ok := doc["message_count"].(int64); ok {
		task.MessageCount = messageCount
	}
	if retryCount, ok := doc["retry_count"].(int); ok {
		task.RetryCount = retryCount
	}
	if taskCode, ok := doc["task_code"].(string); ok {
		task.TaskCode = taskCode
	}
	if errorMessage, ok := doc["error_message"].(string); ok {
		task.ErrorMessage = errorMessage
	}
	if metadata, ok := doc["metadata"]; ok {
		task.Metadata = metadata
	}

	// Handle time fields
	if processedAt, ok := doc["processed_at"].(time.Time); ok {
		task.ProcessedAt = &processedAt
	}
	if completedAt, ok := doc["completed_at"].(time.Time); ok {
		task.CompletedAt = &completedAt
	}

	return task
}

// GenerateTaskCode generates a unique task code for messages
func GenerateTaskCode(messages []MessageData) string {
	if len(messages) == 0 {
		return ""
	}

	// Create a hash based on message IDs
	var ids []string
	for _, msg := range messages {
		ids = append(ids, msg.ID)
	}

	combined := strings.Join(ids, ",")
	hash := crc32.ChecksumIEEE([]byte(combined))

	return fmt.Sprintf("task_%x", hash)
}

// ValidateMessageData validates MessageData structure
func ValidateMessageData(msg *MessageData) error {
	if msg == nil {
		return fmt.Errorf("message data is nil")
	}

	if strings.TrimSpace(msg.ID) == "" {
		return fmt.Errorf("message ID is required")
	}

	if strings.TrimSpace(msg.Recipient) == "" {
		return fmt.Errorf("message recipient is required")
	}

	if strings.TrimSpace(msg.Content) == "" {
		return fmt.Errorf("message content is required")
	}

	if strings.TrimSpace(msg.Type) == "" {
		return fmt.Errorf("message type is required")
	}

	// Validate message type
	validTypes := map[string]bool{
		"sms":   true,
		"email": true,
		"push":  true,
	}

	if !validTypes[strings.ToLower(msg.Type)] {
		return fmt.Errorf("invalid message type: %s", msg.Type)
	}

	return nil
}

// ValidatePartitionTask validates PartitionTask structure
func ValidatePartitionTask(task *PartitionTask) error {
	if task == nil {
		return fmt.Errorf("partition task is nil")
	}

	if strings.TrimSpace(task.ID) == "" {
		return fmt.Errorf("task ID is required")
	}

	if strings.TrimSpace(task.BatchID) == "" {
		return fmt.Errorf("batch ID is required")
	}

	if strings.TrimSpace(task.PartitionKey) == "" {
		return fmt.Errorf("partition key is required")
	}

	if task.MessageCount <= 0 {
		return fmt.Errorf("message count must be positive")
	}

	return nil
}

// LogTransition logs state or phase transitions with MongoDB storage
func (h *MongoHelper) LogTransition(ctx context.Context, transitionType, from, to, entity string, metadata map[string]interface{}) {
	logFields := []interface{}{
		"transitionType", transitionType,
		"from", from,
		"to", to,
		"entity", entity,
		"timestamp", time.Now(),
	}

	for key, value := range metadata {
		logFields = append(logFields, key, value)
	}

	h.logger.Infow("State transition", logFields...)

	// Store transition log in MongoDB
	if h.client != nil {
		go func() {
			transitionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			h.storeTransitionLog(transitionCtx, transitionType, from, to, entity, metadata)
		}()
	}
}

// storeTransitionLog stores transition log in MongoDB
func (h *MongoHelper) storeTransitionLog(ctx context.Context, transitionType, from, to, entity string, metadata map[string]interface{}) {
	collection := h.client.Database(h.database).Collection("transition_logs")

	document := bson.M{
		"transition_type": transitionType,
		"from_state":      from,
		"to_state":        to,
		"entity":          entity,
		"metadata":        metadata,
		"timestamp":       time.Now(),
	}

	_, err := collection.InsertOne(ctx, document)
	if err != nil {
		h.logger.Errorw("Failed to store transition log",
			"error", err,
			"transitionType", transitionType,
			"entity", entity,
		)
	}
}

// BatchStatistics holds batch processing statistics with MongoDB support
type BatchStatistics struct {
	BatchID         string    `bson:"batch_id" json:"batch_id"`
	TotalMessages   int64     `bson:"total_messages" json:"total_messages"`
	ProcessedCount  int64     `bson:"processed_count" json:"processed_count"`
	SuccessCount    int64     `bson:"success_count" json:"success_count"`
	FailedCount     int64     `bson:"failed_count" json:"failed_count"`
	PartitionCount  int       `bson:"partition_count" json:"partition_count"`
	StartTime       time.Time `bson:"start_time" json:"start_time"`
	EndTime         time.Time `bson:"end_time,omitempty" json:"end_time,omitempty"`
	DurationSeconds int64     `bson:"duration_seconds,omitempty" json:"duration_seconds,omitempty"`
	SuccessRate     float64   `bson:"success_rate" json:"success_rate"`
	ProcessingRate  float64   `bson:"processing_rate" json:"processing_rate"` // messages per second
	LastUpdated     time.Time `bson:"last_updated" json:"last_updated"`
}

// StoreBatchStatistics stores batch statistics in MongoDB
func (h *MongoHelper) StoreBatchStatistics(ctx context.Context, stats *BatchStatistics) error {
	collection := h.client.Database(h.database).Collection("batch_statistics")

	// Calculate derived fields
	if stats.TotalMessages > 0 {
		stats.SuccessRate = float64(stats.SuccessCount) / float64(stats.TotalMessages) * 100
	}

	if !stats.EndTime.IsZero() {
		duration := stats.EndTime.Sub(stats.StartTime)
		stats.DurationSeconds = int64(duration.Seconds())
		if stats.DurationSeconds > 0 {
			stats.ProcessingRate = float64(stats.ProcessedCount) / float64(stats.DurationSeconds)
		}
	}

	stats.LastUpdated = time.Now()

	// Use upsert to update existing document or insert new one
	filter := bson.M{"batch_id": stats.BatchID}
	update := bson.M{"$set": stats}
	opts := options.Update().SetUpsert(true)

	result, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to store batch statistics: %w", err)
	}

	h.logger.Infow("Stored batch statistics",
		"batchID", stats.BatchID,
		"totalMessages", stats.TotalMessages,
		"successRate", stats.SuccessRate,
		"upsertedID", result.UpsertedID,
	)

	return nil
}

// GetBatchStatistics retrieves batch statistics from MongoDB
func (h *MongoHelper) GetBatchStatistics(ctx context.Context, batchID string) (*BatchStatistics, error) {
	collection := h.client.Database(h.database).Collection("batch_statistics")

	filter := bson.M{"batch_id": batchID}
	var stats BatchStatistics

	err := collection.FindOne(ctx, filter).Decode(&stats)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("batch statistics not found for batch ID: %s", batchID)
		}
		return nil, fmt.Errorf("failed to retrieve batch statistics: %w", err)
	}

	return &stats, nil
}

// EnsureIndexes creates necessary MongoDB indexes for optimal performance
func (h *MongoHelper) EnsureIndexes(ctx context.Context) error {
	database := h.client.Database(h.database)

	// Indexes for partition tasks collection
	tasksCollection := database.Collection(h.collection)
	taskIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{"batch_id", 1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"partition_key", 1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"status", 1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"created_at", -1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"batch_id", 1}, {"status", 1}},
			Options: options.Index().SetBackground(true),
		},
	}

	_, err := tasksCollection.Indexes().CreateMany(ctx, taskIndexes)
	if err != nil {
		return fmt.Errorf("failed to create task indexes: %w", err)
	}

	// Indexes for batch statistics collection
	statsCollection := database.Collection("batch_statistics")
	statsIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{"batch_id", 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{"start_time", -1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"success_rate", -1}},
			Options: options.Index().SetBackground(true),
		},
	}

	_, err = statsCollection.Indexes().CreateMany(ctx, statsIndexes)
	if err != nil {
		return fmt.Errorf("failed to create statistics indexes: %w", err)
	}

	// Indexes for transition logs collection
	logsCollection := database.Collection("transition_logs")
	logIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{"entity", 1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"timestamp", -1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"transition_type", 1}},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys:    bson.D{{"timestamp", 1}},
			Options: options.Index().SetBackground(true).SetExpireAfterSeconds(int32(30 * 24 * 3600)), // 30 days TTL
		},
	}

	_, err = logsCollection.Indexes().CreateMany(ctx, logIndexes)
	if err != nil {
		return fmt.Errorf("failed to create log indexes: %w", err)
	}

	h.logger.Infow("Successfully created MongoDB indexes",
		"database", h.database,
		"collections", []string{h.collection, "batch_statistics", "transition_logs"},
	)

	return nil
}

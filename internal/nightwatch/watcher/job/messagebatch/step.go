// Package messagebatch provides implementations for batch processing steps
package messagebatch

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/messaging"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// MessageBatchReader implements BatchReader for reading message data
type MessageBatchReader struct {
	store       store.IStore
	jobID       string
	batchID     string
	logger      log.Logger
	mu          sync.RWMutex
	position    int64
	mongoHelper *MongoHelper
}

// NewMessageBatchReader creates a new message batch reader
func NewMessageBatchReader(store store.IStore, jobID, batchID string, logger log.Logger, mongoHelper *MongoHelper) *MessageBatchReader {
	return &MessageBatchReader{
		store:       store,
		jobID:       jobID,
		batchID:     batchID,
		logger:      logger,
		mongoHelper: mongoHelper,
	}
}

// Read reads message data with pagination from MongoDB
func (r *MessageBatchReader) Read(ctx context.Context, offset int64, limit int) ([]MessageData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// In a real implementation, this would read from MongoDB collection
	// For now, we'll create sample data but with MongoDB-compatible structure
	messages := make([]MessageData, 0, limit)

	// Create realistic message data for batch processing
	for i := 0; i < limit; i++ {
		currentOffset := offset + int64(i)
		recipient := fmt.Sprintf("user_%d@example.com", currentOffset)

		message := MessageData{
			ID:           fmt.Sprintf("msg_%s_%d", r.batchID, currentOffset),
			Recipient:    recipient,
			Content:      fmt.Sprintf("Batch message %d for user %d", currentOffset, currentOffset),
			Template:     "default_template",
			Type:         "sms",
			PartitionKey: fmt.Sprintf("partition_%d", CalculatePartitionID(recipient, known.MessageBatchPartitionCount)),
			Priority:     1,
			Metadata: map[string]string{
				"source":    "batch",
				"job_id":    r.jobID,
				"batch_id":  r.batchID,
				"read_time": time.Now().Format(time.RFC3339),
			},
			ScheduledTime: time.Now().Add(time.Duration(i) * time.Second),
			CreatedAt:     time.Now(),
		}

		// Validate message before including
		if err := ValidateMessageData(&message); err != nil {
			r.logger.Errorw("Generated invalid message data",
				"messageID", message.ID,
				"error", err,
			)
			continue
		}

		messages = append(messages, message)
	}

	r.position = offset + int64(len(messages))

	r.logger.Infow("Read messages from batch",
		"jobID", r.jobID,
		"batchID", r.batchID,
		"offset", offset,
		"requestedLimit", limit,
		"actualCount", len(messages),
		"newPosition", r.position,
	)

	return messages, nil
}

// HasNext checks if there's more data to read
func (r *MessageBatchReader) HasNext(ctx context.Context, offset int64) (bool, error) {
	// Simulate end condition - for demo, let's say we have 10000 messages
	return offset < 10000, nil
}

// Close closes the reader
func (r *MessageBatchReader) Close(ctx context.Context) error {
	r.logger.Infow("Message batch reader closed", "jobID", r.jobID, "batchID", r.batchID)
	return nil
}

// MessageBatchProcessor implements BatchProcessor for processing message data
type MessageBatchProcessor struct {
	config      map[string]interface{}
	logger      log.Logger
	mongoHelper *MongoHelper
}

// NewMessageBatchProcessor creates a new message batch processor
func NewMessageBatchProcessor(logger log.Logger, mongoHelper *MongoHelper) *MessageBatchProcessor {
	return &MessageBatchProcessor{
		config:      make(map[string]interface{}),
		logger:      logger,
		mongoHelper: mongoHelper,
	}
}

// Process processes a batch of messages
func (p *MessageBatchProcessor) Process(ctx context.Context, items []MessageData) ([]PartitionTask, error) {
	tasks := make([]PartitionTask, 0)

	// Validate all messages first
	for i, msg := range items {
		if err := ValidateMessageData(&msg); err != nil {
			p.logger.Errorw("Invalid message data",
				"messageIndex", i,
				"messageID", msg.ID,
				"error", err,
			)
			return nil, fmt.Errorf("invalid message at index %d: %w", i, err)
		}
	}

	// Group messages by partition
	partitionGroups := make(map[string][]MessageData)
	for _, msg := range items {
		partitionKey := p.calculatePartition(msg)
		partitionGroups[partitionKey] = append(partitionGroups[partitionKey], msg)
	}

	// Create partition tasks using MongoHelper
	for partitionKey, messages := range partitionGroups {
		// Extract partition ID from partition key
		partitionID := CalculatePartitionID(messages[0].Recipient, known.MessageBatchPartitionCount)

		// Use MongoHelper to create and store the task
		task, err := p.mongoHelper.ConvertMessageToTask(ctx, messages, partitionID)
		if err != nil {
			p.logger.Errorw("Failed to create partition task",
				"partitionKey", partitionKey,
				"messageCount", len(messages),
				"error", err,
			)
			return nil, fmt.Errorf("failed to create partition task for %s: %w", partitionKey, err)
		}

		if task != nil {
			tasks = append(tasks, *task)
		}
	}

	p.logger.Infow("Processed messages into partition tasks",
		"inputMessages", len(items),
		"outputTasks", len(tasks),
		"partitions", len(partitionGroups),
	)

	return tasks, nil
}

// SetConfig sets processor configuration
func (p *MessageBatchProcessor) SetConfig(config interface{}) error {
	if configMap, ok := config.(map[string]interface{}); ok {
		p.config = configMap
		return nil
	}
	return fmt.Errorf("invalid config type, expected map[string]interface{}")
}

// calculatePartition calculates partition for a message
func (p *MessageBatchProcessor) calculatePartition(msg MessageData) string {
	// Use consistent hashing based on recipient
	hash := crc32.ChecksumIEEE([]byte(msg.Recipient))
	partitionID := hash % uint32(known.MessageBatchPartitionCount)
	return fmt.Sprintf("partition_%d", partitionID)
}

// generateTaskCode generates a unique task code for partition processing
func (p *MessageBatchProcessor) generateTaskCode(partitionKey string, messages []MessageData) string {
	// Generate a hash based on partition key and message count
	data := fmt.Sprintf("%s_%d_%d", partitionKey, len(messages), time.Now().Unix())
	hash := crc32.ChecksumIEEE([]byte(data))
	return fmt.Sprintf("task_%x", hash)
}

// MessageBatchWriter implements BatchWriter for writing partition tasks
type MessageBatchWriter struct {
	store       store.IStore
	jobID       string
	logger      log.Logger
	written     int64
	mu          sync.RWMutex
	mongoHelper *MongoHelper
	kafkaHelper *messaging.KafkaHelper
}

// NewMessageBatchWriter creates a new message batch writer
func NewMessageBatchWriter(store store.IStore, jobID string, logger log.Logger, mongoHelper *MongoHelper, kafkaHelper *messaging.KafkaHelper) *MessageBatchWriter {
	return &MessageBatchWriter{
		store:       store,
		jobID:       jobID,
		logger:      logger,
		mongoHelper: mongoHelper,
		kafkaHelper: kafkaHelper,
	}
}

// Write writes partition tasks to storage using MongoDB
func (w *MessageBatchWriter) Write(ctx context.Context, items []PartitionTask) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, task := range items {
		// Validate task before storing
		if err := ValidatePartitionTask(&task); err != nil {
			w.logger.Errorw("Invalid partition task",
				"taskID", task.ID,
				"error", err,
			)
			return fmt.Errorf("invalid partition task %s: %w", task.ID, err)
		}

		// Store task in MongoDB using helper with retry logic
		err := ExecuteWithRetry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
			return w.mongoHelper.StorePartitionTask(ctx, &task)
		}, w.logger, fmt.Sprintf("write_partition_task_%s", task.ID))

		if err != nil {
			w.logger.Errorw("Failed to write partition task to MongoDB",
				"taskID", task.ID,
				"error", err,
			)
			return fmt.Errorf("failed to write task %s: %w", task.ID, err)
		}

		w.logger.Infow("Successfully wrote partition task",
			"taskID", task.ID,
			"partitionKey", task.PartitionKey,
			"messageCount", task.MessageCount,
		)
	}

	w.written += int64(len(items))

	w.logger.Infow("Batch write completed",
		"jobID", w.jobID,
		"itemsWritten", len(items),
		"totalWritten", w.written,
	)

	return nil
}

// WriteOne writes a single partition task
func (w *MessageBatchWriter) WriteOne(ctx context.Context, item PartitionTask) error {
	return w.Write(ctx, []PartitionTask{item})
}

// Flush flushes any pending writes
func (w *MessageBatchWriter) Flush(ctx context.Context) error {
	w.logger.Infow("Flushing writer", "jobID", w.jobID, "totalWritten", w.written)
	return nil
}

// Close closes the writer
func (w *MessageBatchWriter) Close(ctx context.Context) error {
	if err := w.Flush(ctx); err != nil {
		return err
	}
	w.logger.Infow("Message batch writer closed", "jobID", w.jobID, "totalWritten", w.written)
	return nil
}

// GetWrittenCount returns the number of items written
func (w *MessageBatchWriter) GetWrittenCount() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.written
}

// BatchStepExecutor coordinates the execution of read-process-write steps
type BatchStepExecutor struct {
	reader      BatchReader[MessageData]
	processor   BatchProcessor[MessageData, PartitionTask]
	writer      BatchWriter[PartitionTask]
	logger      log.Logger
	batchSize   int
	mongoHelper *MongoHelper

	// Statistics
	totalRead      int64
	totalProcessed int64
	totalWritten   int64
	mu             sync.RWMutex
}

// NewBatchStepExecutor creates a new batch step executor
func NewBatchStepExecutor(
	reader BatchReader[MessageData],
	processor BatchProcessor[MessageData, PartitionTask],
	writer BatchWriter[PartitionTask],
	logger log.Logger,
	batchSize int,
	mongoHelper *MongoHelper,
) *BatchStepExecutor {
	return &BatchStepExecutor{
		reader:      reader,
		processor:   processor,
		writer:      writer,
		logger:      logger,
		batchSize:   batchSize,
		mongoHelper: mongoHelper,
	}
}

// Execute executes the complete read-process-write pipeline
func (e *BatchStepExecutor) Execute(ctx context.Context) error {
	startTime := time.Now()

	defer func() {
		// Close readers and writers
		if err := e.reader.Close(ctx); err != nil {
			e.logger.Errorw("Failed to close reader", "error", err)
		}
		if err := e.writer.Close(ctx); err != nil {
			e.logger.Errorw("Failed to close writer", "error", err)
		}

		// Store final batch statistics in MongoDB
		e.storeFinalStatistics(ctx, startTime)
	}()

	offset := int64(0)
	successfulTasks := int64(0)
	failedTasks := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if there's more data to read
		hasNext, err := e.reader.HasNext(ctx, offset)
		if err != nil {
			e.logger.Errorw("Failed to check hasNext", "error", err)
			return fmt.Errorf("failed to check hasNext: %w", err)
		}
		if !hasNext {
			break
		}

		// Read batch with retry logic
		var messages []MessageData
		err = ExecuteWithRetry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
			var readErr error
			messages, readErr = e.reader.Read(ctx, offset, e.batchSize)
			return readErr
		}, e.logger, fmt.Sprintf("read_batch_offset_%d", offset))

		if err != nil {
			e.logger.Errorw("Failed to read batch after retries", "offset", offset, "error", err)
			return fmt.Errorf("failed to read batch: %w", err)
		}
		if len(messages) == 0 {
			break
		}

		e.updateStats(int64(len(messages)), 0, 0)
		offset += int64(len(messages))

		// Process batch with retry logic
		var tasks []PartitionTask
		err = ExecuteWithRetry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
			var processErr error
			tasks, processErr = e.processor.Process(ctx, messages)
			return processErr
		}, e.logger, fmt.Sprintf("process_batch_offset_%d", offset))

		if err != nil {
			e.logger.Errorw("Failed to process batch after retries", "offset", offset, "error", err)
			failedTasks += int64(len(messages))
			continue // Continue with next batch instead of failing completely
		}

		e.updateStats(0, int64(len(messages)), 0)

		// Write batch with retry logic
		err = ExecuteWithRetry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
			return e.writer.Write(ctx, tasks)
		}, e.logger, fmt.Sprintf("write_batch_offset_%d", offset))

		if err != nil {
			e.logger.Errorw("Failed to write batch after retries", "offset", offset, "error", err)
			failedTasks += int64(len(tasks))
			continue // Continue with next batch
		}

		successfulTasks += int64(len(tasks))
		e.updateStats(0, 0, int64(len(tasks)))

		e.logger.Infow("Batch step completed",
			"messagesRead", len(messages),
			"tasksCreated", len(tasks),
			"totalRead", e.totalRead,
			"totalProcessed", e.totalProcessed,
			"totalWritten", e.totalWritten,
			"successfulTasks", successfulTasks,
			"failedTasks", failedTasks,
		)
	}

	e.logger.Infow("Batch execution completed",
		"totalRead", e.totalRead,
		"totalProcessed", e.totalProcessed,
		"totalWritten", e.totalWritten,
		"successfulTasks", successfulTasks,
		"failedTasks", failedTasks,
		"duration", time.Since(startTime),
	)

	return nil
}

// updateStats updates execution statistics
func (e *BatchStepExecutor) updateStats(read, processed, written int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.totalRead += read
	e.totalProcessed += processed
	e.totalWritten += written
}

// GetStatistics returns execution statistics
func (e *BatchStepExecutor) GetStatistics() (read, processed, written int64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.totalRead, e.totalProcessed, e.totalWritten
}

// storeFinalStatistics stores final batch execution statistics in MongoDB
func (e *BatchStepExecutor) storeFinalStatistics(ctx context.Context, startTime time.Time) {
	if e.mongoHelper == nil {
		return
	}

	endTime := time.Now()
	e.mu.RLock()
	stats := &BatchStatistics{
		BatchID:        fmt.Sprintf("batch_%d", startTime.Unix()),
		TotalMessages:  e.totalRead,
		ProcessedCount: e.totalProcessed,
		SuccessCount:   e.totalWritten, // Assuming written tasks are successful
		FailedCount:    e.totalProcessed - e.totalWritten,
		PartitionCount: known.MessageBatchPartitionCount,
		StartTime:      startTime,
		EndTime:        endTime,
		LastUpdated:    endTime,
	}
	e.mu.RUnlock()

	// Store statistics with a separate context to avoid cancellation
	statCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := e.mongoHelper.StoreBatchStatistics(statCtx, stats); err != nil {
		e.logger.Errorw("Failed to store final batch statistics",
			"batchID", stats.BatchID,
			"error", err,
		)
	} else {
		e.logger.Infow("Stored final batch statistics",
			"batchID", stats.BatchID,
			"totalMessages", stats.TotalMessages,
			"successRate", stats.SuccessRate,
		)
	}
}

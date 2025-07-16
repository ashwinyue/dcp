// Package messagebatch provides implementations for batch processing steps
package messagebatch

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// MessageBatchReader implements BatchReader for reading message data
type MessageBatchReader struct {
	store    store.IStore
	jobID    string
	batchID  string
	logger   log.Logger
	mu       sync.RWMutex
	position int64
}

// NewMessageBatchReader creates a new message batch reader
func NewMessageBatchReader(store store.IStore, jobID, batchID string, logger log.Logger) *MessageBatchReader {
	return &MessageBatchReader{
		store:   store,
		jobID:   jobID,
		batchID: batchID,
		logger:  logger,
	}
}

// Read reads message data with pagination
func (r *MessageBatchReader) Read(ctx context.Context, offset int64, limit int) ([]MessageData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// For demonstration, we'll simulate reading from a data source
	// In practice, this would read from database, file, or message queue
	messages := make([]MessageData, 0, limit)

	// Simulate batch reading logic
	for i := 0; i < limit; i++ {
		message := MessageData{
			ID:            fmt.Sprintf("msg_%s_%d", r.batchID, offset+int64(i)),
			Recipient:     fmt.Sprintf("user_%d@example.com", offset+int64(i)),
			Content:       fmt.Sprintf("Batch message %d", offset+int64(i)),
			Template:      "default_template",
			Type:          "sms",
			PartitionKey:  fmt.Sprintf("partition_%d", (offset+int64(i))%known.MessageBatchPartitionCount),
			Priority:      1,
			Metadata:      map[string]string{"source": "batch", "job_id": r.jobID},
			ScheduledTime: time.Now(),
			CreatedAt:     time.Now(),
		}
		messages = append(messages, message)
	}

	r.position = offset + int64(len(messages))

	r.logger.Infow("Read messages",
		"jobID", r.jobID,
		"batchID", r.batchID,
		"offset", offset,
		"count", len(messages),
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
	config map[string]interface{}
	logger log.Logger
}

// NewMessageBatchProcessor creates a new message batch processor
func NewMessageBatchProcessor(logger log.Logger) *MessageBatchProcessor {
	return &MessageBatchProcessor{
		config: make(map[string]interface{}),
		logger: logger,
	}
}

// Process processes a batch of messages
func (p *MessageBatchProcessor) Process(ctx context.Context, items []MessageData) ([]PartitionTask, error) {
	tasks := make([]PartitionTask, 0)

	// Group messages by partition
	partitionGroups := make(map[string][]MessageData)
	for _, msg := range items {
		partitionKey := p.calculatePartition(msg)
		partitionGroups[partitionKey] = append(partitionGroups[partitionKey], msg)
	}

	// Create partition tasks
	for partitionKey, messages := range partitionGroups {
		task := PartitionTask{
			ID:           fmt.Sprintf("task_%s_%d", partitionKey, time.Now().Unix()),
			BatchID:      messages[0].ID, // Use first message's ID as batch reference
			PartitionKey: partitionKey,
			Status:       "pending",
			MessageCount: int64(len(messages)),
			RetryCount:   0,
			TaskCode:     p.generateTaskCode(partitionKey, messages),
			Metadata:     messages, // Store messages in metadata for processing
		}
		tasks = append(tasks, task)
	}

	p.logger.Infow("Processed messages into partition tasks",
		"inputMessages", len(items),
		"outputTasks", len(tasks),
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
	store   store.IStore
	jobID   string
	logger  log.Logger
	written int64
	mu      sync.RWMutex
}

// NewMessageBatchWriter creates a new message batch writer
func NewMessageBatchWriter(store store.IStore, jobID string, logger log.Logger) *MessageBatchWriter {
	return &MessageBatchWriter{
		store:  store,
		jobID:  jobID,
		logger: logger,
	}
}

// Write writes partition tasks to storage
func (w *MessageBatchWriter) Write(ctx context.Context, items []PartitionTask) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, task := range items {
		// Convert task to JSON for storage
		taskData, err := json.Marshal(task)
		if err != nil {
			w.logger.Errorw("Failed to marshal partition task",
				"taskID", task.ID,
				"error", err,
			)
			return fmt.Errorf("failed to marshal task %s: %w", task.ID, err)
		}

		// For demonstration, we'll simulate writing to database
		// In practice, this would insert into partition task table
		w.logger.Infow("Writing partition task",
			"taskID", task.ID,
			"partitionKey", task.PartitionKey,
			"messageCount", task.MessageCount,
			"dataSize", len(taskData),
		)

		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
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
	reader    BatchReader[MessageData]
	processor BatchProcessor[MessageData, PartitionTask]
	writer    BatchWriter[PartitionTask]
	logger    log.Logger
	batchSize int

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
) *BatchStepExecutor {
	return &BatchStepExecutor{
		reader:    reader,
		processor: processor,
		writer:    writer,
		logger:    logger,
		batchSize: batchSize,
	}
}

// Execute executes the complete read-process-write pipeline
func (e *BatchStepExecutor) Execute(ctx context.Context) error {
	defer func() {
		if err := e.reader.Close(ctx); err != nil {
			e.logger.Errorw("Failed to close reader", "error", err)
		}
		if err := e.writer.Close(ctx); err != nil {
			e.logger.Errorw("Failed to close writer", "error", err)
		}
	}()

	offset := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if there's more data to read
		hasNext, err := e.reader.HasNext(ctx, offset)
		if err != nil {
			return fmt.Errorf("failed to check hasNext: %w", err)
		}
		if !hasNext {
			break
		}

		// Read batch
		messages, err := e.reader.Read(ctx, offset, e.batchSize)
		if err != nil {
			return fmt.Errorf("failed to read batch: %w", err)
		}
		if len(messages) == 0 {
			break
		}

		e.updateStats(int64(len(messages)), 0, 0)
		offset += int64(len(messages))

		// Process batch
		tasks, err := e.processor.Process(ctx, messages)
		if err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		e.updateStats(0, int64(len(messages)), 0)

		// Write batch
		if err := e.writer.Write(ctx, tasks); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}

		e.updateStats(0, 0, int64(len(tasks)))

		e.logger.Infow("Batch step completed",
			"messagesRead", len(messages),
			"tasksCreated", len(tasks),
			"totalRead", e.totalRead,
			"totalProcessed", e.totalProcessed,
			"totalWritten", e.totalWritten,
		)
	}

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

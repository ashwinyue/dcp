package batch

import (
	"context"
	"fmt"
	"log"
	"time"
)

// SimpleLogger implements the Logger interface for demonstration
type SimpleLogger struct{}

func (l *SimpleLogger) Info(msg string, args ...interface{}) {
	log.Printf("[INFO] "+msg, args...)
}

func (l *SimpleLogger) Error(msg string, args ...interface{}) {
	log.Printf("[ERROR] "+msg, args...)
}

func (l *SimpleLogger) Debug(msg string, args ...interface{}) {
	log.Printf("[DEBUG] "+msg, args...)
}

func (l *SimpleLogger) Warn(msg string, args ...interface{}) {
	log.Printf("[WARN] "+msg, args...)
}

// ExampleUsage demonstrates how to use the onex pump pattern for batch processing
func ExampleUsage() {
	ctx := context.Background()
	logger := &SimpleLogger{}

	// Create task manager
	taskManager := NewTaskManager(ctx, logger)
	defer taskManager.Shutdown()

	// Example 1: Create and process a train task
	fmt.Println("=== Example 1: Train Task ===")
	trainConfig := &BatchConfig{
		BatchSize:     5,
		Retries:       3,
		Timeout:       30 * time.Second,
		ConcurrentNum: 2,
	}

	trainResp, err := CreateTrainTask(taskManager, trainConfig)
	if err != nil {
		log.Fatalf("Failed to create train task: %v", err)
	}
	fmt.Printf("Created train task: %s\n", trainResp.TaskID)

	// Start processing the train task
	if err := taskManager.ProcessTask(trainResp.TaskID); err != nil {
		log.Fatalf("Failed to process train task: %v", err)
	}

	// Monitor task progress
	for {
		status, err := taskManager.GetTaskStatus(trainResp.TaskID)
		if err != nil {
			log.Printf("Error getting task status: %v", err)
			break
		}

		fmt.Printf("Train task status: %s, progress: %d%%\n", status.Status, status.Progress.Percentage)

		if status.Status == TaskStatusCompleted || status.Status == TaskStatusFailed {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Example 2: Create and process a YouZan order task
	fmt.Println("\n=== Example 2: YouZan Order Task ===")
	orderConfig := &BatchConfig{
		BatchSize:     10,
		Retries:       2,
		Timeout:       60 * time.Second,
		ConcurrentNum: 3,
	}

	orderResp, err := CreateYouZanOrderTask(taskManager, orderConfig)
	if err != nil {
		log.Fatalf("Failed to create YouZan order task: %v", err)
	}
	fmt.Printf("Created YouZan order task: %s\n", orderResp.TaskID)

	// Start processing the order task
	if err := taskManager.ProcessTask(orderResp.TaskID); err != nil {
		log.Fatalf("Failed to process YouZan order task: %v", err)
	}

	// Monitor task progress
	for {
		status, err := taskManager.GetTaskStatus(orderResp.TaskID)
		if err != nil {
			log.Printf("Error getting task status: %v", err)
			break
		}

		fmt.Printf("YouZan order task status: %s, progress: %d%%\n", status.Status, status.Progress.Percentage)

		if status.Status == TaskStatusCompleted || status.Status == TaskStatusFailed {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Example 3: Create and process a batch task
	fmt.Println("\n=== Example 3: Batch Process Task ===")
	batchConfig := &BatchConfig{
		BatchSize:     20,
		Retries:       1,
		Timeout:       45 * time.Second,
		ConcurrentNum: 4,
	}

	batchResp, err := CreateBatchProcessTask(taskManager, batchConfig)
	if err != nil {
		log.Fatalf("Failed to create batch process task: %v", err)
	}
	fmt.Printf("Created batch process task: %s\n", batchResp.TaskID)

	// Start processing the batch task
	if err := taskManager.ProcessTask(batchResp.TaskID); err != nil {
		log.Fatalf("Failed to process batch task: %v", err)
	}

	// Monitor task progress
	for {
		status, err := taskManager.GetTaskStatus(batchResp.TaskID)
		if err != nil {
			log.Printf("Error getting task status: %v", err)
			break
		}

		fmt.Printf("Batch task status: %s, progress: %d%%\n", status.Status, status.Progress.Percentage)

		if status.Status == TaskStatusCompleted || status.Status == TaskStatusFailed {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Example 4: List all tasks
	fmt.Println("\n=== Example 4: List All Tasks ===")
	listReq := &ListTasksRequest{}
	listResp, err := taskManager.ListTasks(listReq)
	if err != nil {
		log.Fatalf("Failed to list tasks: %v", err)
	}

	fmt.Printf("Total tasks: %d\n", listResp.Total)
	for _, task := range listResp.Tasks {
		fmt.Printf("Task ID: %s, Type: %s, Status: %s, Created: %s\n",
			task.ID, task.Type, task.Status, task.CreatedAt.Format(time.RFC3339))
	}
}

// ExampleCustomProcessor demonstrates how to create a custom processor using onex pump pattern
func ExampleCustomProcessor() {
	_ = context.Background()
	logger := &SimpleLogger{}

	fmt.Println("\n=== Custom Processor Example ===")

	// Create custom tasks
	_ = []*BatchTask{
		{
			ID:        "custom-1",
			Type:      TaskTypeBatchProcess,
			Status:    TaskStatusPending,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Config: &BatchConfig{
				BatchSize:     5,
				ConcurrentNum: 2,
			},
			Progress: &TaskProgress{},
		},
		{
			ID:        "custom-2",
			Type:      TaskTypeYouZanOrder,
			Status:    TaskStatusPending,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Config: &BatchConfig{
				BatchSize:     3,
				ConcurrentNum: 1,
			},
			Progress: &TaskProgress{},
		},
	}

	// Create custom processor
	// config := &BatchConfig{
	// 	BatchSize:   10,
	// 	Concurrency: 2,
	// 	Timeout:     30 * time.Second,
	// }

	// processor := NewBatchProcessor(ctx, config, logger)

	// TODO: Create source using flow package directly
	// source := flow.NewTaskSource(ctx, tasks)

	// TODO: Create custom validation flow using flow package directly
	// validationFlow := flow.NewValidationFlow(...)

	// TODO: Create custom processing sink using flow package directly
	// processorSink := flow.NewProcessorSink(...)

	// For now, skip the custom processor example due to circular dependency refactoring
	logger.Info("Custom processor example temporarily disabled during refactoring")
	return

	// Original code (commented out during refactoring):
	/*
		processorSink := NewProcessorSink(ctx, func(ctx ProcessingContext, item *BatchItem[*BatchTask]) (*BatchResult[any], error) {
			task := item.Data
			logger.Info("Processing custom task", "taskID", task.ID, "type", task.Type)

			// Simulate processing time
			time.Sleep(2 * time.Second)

			return &BatchResult[any]{
				ID:          task.ID,
				Data:        map[string]interface{}{"processed": true, "timestamp": time.Now()},
				ProcessedAt: time.Now(),
				Success:     true,
			}, nil
		}, logger)

		// Build pipeline: source -> validation -> sink
		processor.
			WithSource(source).
			WithFlow(validationFlow).
			WithSink(processorSink)

		// Start processing
		if err := processor.Run(); err != nil {
			log.Fatalf("Failed to run custom processor: %v", err)
		}

		// Monitor results
		go func() {
			for result := range processorSink.Results() {
				fmt.Printf("Custom task result: ID=%s, Success=%t, Data=%v\n",
					result.ID, result.Success, result.Data)
			}
		}()

		// Wait for processing to complete
		time.Sleep(10 * time.Second)

		// Stop processor
		if err := processor.Stop(); err != nil {
			log.Printf("Error stopping processor: %v", err)
		}

		// Print metrics
		metrics := processorSink.GetMetrics()
		fmt.Printf("Processing metrics: Total=%d, Processed=%d, Failed=%d, Duration=%v\n",
			metrics.TotalItems, metrics.ProcessedItems, metrics.FailedItems, time.Since(metrics.StartTime))
	*/
}

// RunExamples runs all examples
func RunExamples() {
	fmt.Println("Starting onex pump pattern batch processing examples...")

	// Run basic usage example
	ExampleUsage()

	// Wait a bit between examples
	time.Sleep(2 * time.Second)

	// Run custom processor example
	ExampleCustomProcessor()

	fmt.Println("\nAll examples completed!")
}

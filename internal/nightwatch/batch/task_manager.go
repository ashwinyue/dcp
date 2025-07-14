package batch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TaskManager manages batch tasks using onex pump pattern
type TaskManager struct {
	mu         sync.RWMutex
	tasks      map[string]*BatchTask
	processors map[string]*BatchProcessor
	logger     Logger
	ctx        context.Context
	cancelCtx  context.CancelFunc
}

// NewTaskManager creates a new task manager
func NewTaskManager(ctx context.Context, logger Logger) *TaskManager {
	cctx, cancel := context.WithCancel(ctx)
	return &TaskManager{
		tasks:      make(map[string]*BatchTask),
		processors: make(map[string]*BatchProcessor),
		logger:     logger,
		ctx:        cctx,
		cancelCtx:  cancel,
	}
}

// CreateTask creates a new batch task using onex pattern
func (tm *TaskManager) CreateTask(req *CreateTaskRequest) (*CreateTaskResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task := &BatchTask{
		ID:        generateTaskID(),
		Type:      req.Type,
		Status:    TaskStatusPending,
		Config:    req.Config,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Progress:  &TaskProgress{},
		Metadata:  req.Metadata,
	}

	tm.tasks[task.ID] = task
	tm.logger.Info("Task created", "taskID", task.ID, "type", task.Type)

	return &CreateTaskResponse{
		TaskID:    task.ID,
		Status:    task.Status,
		CreatedAt: task.CreatedAt,
	}, nil
}

// ProcessTask processes a task using onex pump pattern
func (tm *TaskManager) ProcessTask(taskID string) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.Status != TaskStatusPending {
		return fmt.Errorf("task %s is not in pending status", taskID)
	}

	// Create processor based on task type
	processor, err := tm.createProcessor(task)
	if err != nil {
		return fmt.Errorf("failed to create processor for task %s: %w", taskID, err)
	}

	tm.mu.Lock()
	tm.processors[taskID] = processor
	tm.mu.Unlock()

	// Start processing in background
	go func() {
		if err := processor.Run(); err != nil {
			tm.logger.Error("Task processing failed", "taskID", taskID, "error", err)
			tm.updateTaskStatus(taskID, TaskStatusFailed, &TaskError{
				Code:      "PROCESSING_ERROR",
				Message:   err.Error(),
				Timestamp: time.Now(),
			})
		}
	}()

	return nil
}

// createProcessor creates a batch processor based on task type
func (tm *TaskManager) createProcessor(task *BatchTask) (*BatchProcessor, error) {
	processor := NewBatchProcessor(tm.ctx, task.Config, tm.logger)

	switch task.Type {
	case TaskTypeTrain:
		return tm.createTrainProcessor(processor, task)
	case TaskTypeYouZanOrder:
		return tm.createYouZanOrderProcessor(processor, task)
	case TaskTypeBatchProcess:
		return tm.createBatchProcessor(processor, task)
	default:
		return nil, fmt.Errorf("unsupported task type: %s", task.Type)
	}
}

// createTrainProcessor creates a processor for train tasks
func (tm *TaskManager) createTrainProcessor(processor *BatchProcessor, task *BatchTask) (*BatchProcessor, error) {
	// TODO: Implement using flow package directly to avoid circular dependency
	// For now, return a basic processor
	return processor, nil
}

// createYouZanOrderProcessor creates a processor for YouZan order tasks
func (tm *TaskManager) createYouZanOrderProcessor(processor *BatchProcessor, task *BatchTask) (*BatchProcessor, error) {
	// TODO: Implement using flow package directly to avoid circular dependency
	// For now, return a basic processor
	return processor, nil
}

// createBatchProcessor creates a processor for general batch tasks
func (tm *TaskManager) createBatchProcessor(processor *BatchProcessor, task *BatchTask) (*BatchProcessor, error) {
	// TODO: Implement using flow package directly to avoid circular dependency
	// For now, return a basic processor
	return processor, nil
}

// Task validation functions
func (tm *TaskManager) validateTrainTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) error {
	task := item.Data
	if task.Config == nil {
		return fmt.Errorf("train task config is required")
	}
	if task.Config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	return nil
}

func (tm *TaskManager) validateYouZanOrderTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) error {
	task := item.Data
	if task.Config == nil {
		return fmt.Errorf("youzan order task config is required")
	}
	return nil
}

func (tm *TaskManager) validateBatchTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) error {
	task := item.Data
	if task.Config == nil {
		return fmt.Errorf("batch task config is required")
	}
	return nil
}

// Task processing functions
func (tm *TaskManager) processTrainTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) (*BatchResult[any], error) {
	task := item.Data
	tm.logger.Info("Processing train task", "taskID", task.ID)

	// Simulate train processing
	time.Sleep(time.Duration(task.Config.BatchSize) * time.Second)

	resultData := map[string]interface{}{"model": "trained", "accuracy": 0.95}
	result := &BatchResult[any]{
		Item: &BatchItem[any]{
			ID:   task.ID,
			Data: task,
		},
		Result:         resultData,
		Success:        true,
		ProcessingTime: time.Since(time.Now()),
	}

	tm.updateTaskProgress(task.ID, 100)
	return result, nil
}

func (tm *TaskManager) processYouZanOrderTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) (*BatchResult[any], error) {
	task := item.Data
	tm.logger.Info("Processing YouZan order task", "taskID", task.ID)

	// Simulate order processing
	time.Sleep(2 * time.Second)

	result := &BatchResult[any]{
		ID:          task.ID,
		Data:        map[string]interface{}{"orders_processed": 100, "status": "completed"},
		ProcessedAt: time.Now(),
		Success:     true,
	}

	tm.updateTaskProgress(task.ID, 100)
	return result, nil
}

func (tm *TaskManager) processBatchTask(ctx ProcessingContext, item *BatchItem[*BatchTask]) (*BatchResult[any], error) {
	task := item.Data
	tm.logger.Info("Processing batch task", "taskID", task.ID)

	// Simulate batch processing
	time.Sleep(3 * time.Second)

	result := &BatchResult[any]{
		ID:          task.ID,
		Data:        map[string]interface{}{"items_processed": task.Config.BatchSize, "status": "completed"},
		ProcessedAt: time.Now(),
		Success:     true,
	}

	tm.updateTaskProgress(task.ID, 100)
	return result, nil
}

// Flow functions for enrichment and filtering
func (tm *TaskManager) enrichYouZanOrder(ctx ProcessingContext, item *BatchItem[*BatchTask]) (*BatchItem[*BatchTask], error) {
	task := item.Data

	// Add enrichment data
	if task.Metadata == nil {
		task.Metadata = make(map[string]interface{})
	}
	task.Metadata["enriched"] = true
	task.Metadata["enriched_at"] = time.Now()
	task.Metadata["region"] = "CN"

	// Return enriched item
	return &BatchItem[*BatchTask]{
		ID:        item.ID,
		Data:      task,
		CreatedAt: item.CreatedAt,
		Metadata:  item.Metadata,
	}, nil
}

func (tm *TaskManager) filterBatchTask(msg any) bool {
	task, ok := msg.(*BatchTask)
	if !ok {
		return false
	}

	// Filter based on task config
	return task.Config != nil && task.Config.BatchSize > 0
}

// Helper functions
func (tm *TaskManager) updateTaskStatus(taskID string, status TaskStatus, taskError *TaskError) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, exists := tm.tasks[taskID]; exists {
		task.Status = status
		task.UpdatedAt = time.Now()
		if taskError != nil {
			task.Error = taskError
		}
		if status == TaskStatusCompleted {
			completedAt := time.Now()
			task.CompletedAt = &completedAt
		}
	}
}

func (tm *TaskManager) updateTaskProgress(taskID string, percentage int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, exists := tm.tasks[taskID]; exists {
		task.Progress.Percentage = percentage
		task.Progress.UpdatedAt = time.Now()
		task.UpdatedAt = time.Now()
	}
}

// GetTask retrieves a task by ID - implements types.TaskManager interface
func (tm *TaskManager) GetTask(taskID string) (*BatchTask, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, nil
}

// GetTaskResponse retrieves a task by ID and returns response format
func (tm *TaskManager) GetTaskResponse(taskID string) (*GetTaskResponse, error) {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return nil, err
	}

	return &GetTaskResponse{
		Task: task,
	}, nil
}

// GetTaskStatus retrieves task status
func (tm *TaskManager) GetTaskStatus(taskID string) (*GetTaskStatusResponse, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return &GetTaskStatusResponse{
		TaskID:    task.ID,
		Status:    task.Status,
		Progress:  task.Progress,
		Error:     task.Error,
		UpdatedAt: task.UpdatedAt,
	}, nil
}

// UpdateTaskStatus updates task status - implements types.TaskManager interface
func (tm *TaskManager) UpdateTaskStatus(taskID string, status TaskStatus, taskError *TaskError) error {
	tm.updateTaskStatus(taskID, status, taskError)
	return nil
}

// UpdateTaskStatusRequest updates task status with request format
func (tm *TaskManager) UpdateTaskStatusRequest(req *UpdateTaskStatusRequest) (*UpdateTaskStatusResponse, error) {
	tm.updateTaskStatus(req.TaskID, req.Status, req.Error)

	return &UpdateTaskStatusResponse{
		TaskID:    req.TaskID,
		Status:    req.Status,
		UpdatedAt: time.Now(),
	}, nil
}

// GetPendingTasks gets pending tasks - implements types.TaskManager interface
func (tm *TaskManager) GetPendingTasks(limit int) ([]*BatchTask, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tasks := make([]*BatchTask, 0, limit)
	count := 0
	for _, task := range tm.tasks {
		if task.Status == TaskStatusPending {
			tasks = append(tasks, task)
			count++
			if count >= limit {
				break
			}
		}
	}

	return tasks, nil
}

// ListTasks lists all tasks
func (tm *TaskManager) ListTasks(req *ListTasksRequest) (*ListTasksResponse, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tasks := make([]*BatchTask, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		// Apply filters if specified
		if req.Status != "" && task.Status != TaskStatus(req.Status) {
			continue
		}
		if req.Type != "" && task.Type != TaskType(req.Type) {
			continue
		}
		tasks = append(tasks, task)
	}

	return &ListTasksResponse{
		Tasks: tasks,
		Total: len(tasks),
	}, nil
}

// StopTask stops a running task
func (tm *TaskManager) StopTask(taskID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	processor, exists := tm.processors[taskID]
	if !exists {
		return fmt.Errorf("processor not found for task: %s", taskID)
	}

	if err := processor.Stop(); err != nil {
		return fmt.Errorf("failed to stop processor for task %s: %w", taskID, err)
	}

	delete(tm.processors, taskID)
	tm.updateTaskStatus(taskID, TaskStatusCancelled, nil)

	return nil
}

// Shutdown gracefully shuts down the task manager
func (tm *TaskManager) Shutdown() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Stop all running processors
	for taskID, processor := range tm.processors {
		if err := processor.Stop(); err != nil {
			tm.logger.Error("Failed to stop processor", "taskID", taskID, "error", err)
		}
	}

	tm.cancelCtx()
	tm.logger.Info("Task manager shutdown completed")
	return nil
}

// Convenience functions for creating specific task types
func CreateTrainTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error) {
	req := &CreateTaskRequest{
		Type:   TaskTypeTrain,
		Config: config,
	}
	return tm.CreateTask(req)
}

func CreateYouZanOrderTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error) {
	req := &CreateTaskRequest{
		Type:   TaskTypeYouZanOrder,
		Config: config,
	}
	return tm.CreateTask(req)
}

func CreateBatchProcessTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error) {
	req := &CreateTaskRequest{
		Type:   TaskTypeBatchProcess,
		Config: config,
	}
	return tm.CreateTask(req)
}

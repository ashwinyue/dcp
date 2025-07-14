package flow

import (
	"context"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/connector/extension"
)

// TaskSource represents a source that generates batch tasks from a task manager.
// It wraps pkg/streams ChanSource and provides task management functionality.
type TaskSource struct {
	*extension.ChanSource
	ctx         context.Context
	cancelCtx   context.CancelFunc
	taskChan    chan any
	taskManager types.TaskManager
	logger      types.Logger
	batchSize   int
	interval    time.Duration
}

// Verify TaskSource satisfies the Source interface.
var _ streams.Source = (*TaskSource)(nil)

// NewTaskSource creates a new task source that fetches tasks from the task manager.
func NewTaskSource(taskManager types.TaskManager, batchSize int) *TaskSource {
	ctx, cancel := context.WithCancel(context.Background())
	taskChan := make(chan any, 100)

	source := &TaskSource{
		ChanSource:  extension.NewChanSource(taskChan),
		ctx:         ctx,
		cancelCtx:   cancel,
		taskChan:    taskChan,
		taskManager: taskManager,
		batchSize:   batchSize,
		interval:    5 * time.Second, // Default interval
	}

	go source.doStream()
	return source
}

// NewTaskSourceWithContext creates a new task source that fetches tasks from a task manager.
//
// ctx is the context for the source.
// taskManager is used to fetch batch tasks.
// logger is used for logging.
// batchSize is the number of tasks to fetch in each batch.
// interval is the time interval between batch fetches.
func NewTaskSourceWithContext(ctx context.Context, taskManager types.TaskManager, logger types.Logger, batchSize int, interval time.Duration) *TaskSource {
	cctx, cancel := context.WithCancel(ctx)
	taskChan := make(chan any, 100) // Buffered channel for better performance

	source := &TaskSource{
		ChanSource:  extension.NewChanSource(taskChan),
		ctx:         cctx,
		cancelCtx:   cancel,
		taskChan:    taskChan,
		taskManager: taskManager,
		logger:      logger,
		batchSize:   batchSize,
		interval:    interval,
	}

	go source.doStream()
	return source
}

// Via, To, and Out methods are inherited from embedded ChanSource

// Start starts the task source
func (ts *TaskSource) Start(ctx context.Context) error {
	return nil
}

// Stop stops the task source
func (ts *TaskSource) Stop() error {
	ts.cancelCtx()
	return nil
}

// doStream generates batch tasks at regular intervals
func (ts *TaskSource) doStream() {
	defer close(ts.taskChan)

	ticker := time.NewTicker(ts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ts.ctx.Done():
			return
		case <-ticker.C:
			// Fetch pending tasks
			tasks, err := ts.taskManager.GetPendingTasks(ts.batchSize)
			if err != nil {
				ts.logger.Error("Failed to fetch pending tasks", "error", err)
				continue
			}

			if len(tasks) == 0 {
				continue
			}

			ts.logger.Info("Fetched batch tasks", "count", len(tasks))

			// Send each task downstream
			for _, task := range tasks {
				select {
				case ts.taskChan <- task:
				case <-ts.ctx.Done():
					return
				}
			}
		}
	}
}

// StaticTaskSource represents a source that generates batch tasks from a static list.
// It wraps pkg/streams ChanSource and provides static task streaming functionality.
type StaticTaskSource struct {
	*extension.ChanSource
	taskChan  chan any
	ctx       context.Context
	cancelCtx context.CancelFunc
	tasks     []*types.BatchTask
}

// Verify StaticTaskSource satisfies the Source interface.
var _ streams.Source = (*StaticTaskSource)(nil)

// NewStaticTaskSource creates a new static task source from a list of tasks.
//
// ctx is the context for the source.
// tasks is the list of tasks to stream.
func NewStaticTaskSource(ctx context.Context, tasks []*types.BatchTask) *StaticTaskSource {
	cctx, cancel := context.WithCancel(ctx)
	taskChan := make(chan any, len(tasks)) // Buffer size based on task count

	source := &StaticTaskSource{
		ChanSource: extension.NewChanSource(taskChan),
		taskChan:   taskChan,
		ctx:        cctx,
		cancelCtx:  cancel,
		tasks:      tasks,
	}

	go source.doStream()
	return source
}

// doStream starts the main loop for task generation
func (sts *StaticTaskSource) doStream() {
	defer close(sts.taskChan)

	// Send all tasks to the channel
	for _, task := range sts.tasks {
		select {
		case sts.taskChan <- task:
		case <-sts.ctx.Done():
			return
		}
	}
}

// Via, To, and Out methods are inherited from embedded ChanSource

// Start starts the task source
func (sts *StaticTaskSource) Start(ctx context.Context) error {
	return nil
}

// Stop stops the task source
func (sts *StaticTaskSource) Stop() error {
	sts.cancelCtx()
	return nil
}

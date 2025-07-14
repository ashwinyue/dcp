package flow

import (
	"context"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/connector/extension"
)

// ProcessorFunction represents a processing function for batch items.
type ProcessorFunction func(types.ProcessingContext, *types.BatchItem[*types.BatchTask]) (*types.BatchResult[any], error)

// ProcessorSink represents a sink that processes batch tasks.
// It wraps pkg/streams ChanSink and provides task processing functionality.
type ProcessorSink struct {
	*extension.ChanSink
	ctx               context.Context
	processorFunction ProcessorFunction
	results           chan *types.BatchResult[any]
	metrics           *types.ProcessingMetrics
	logger            types.Logger
}

// Verify ProcessorSink satisfies the Sink interface.
var _ streams.Sink = (*ProcessorSink)(nil)

// NewProcessorSink creates a new processor sink.
//
// ctx is the context for the processor.
// processorFunction is the processing function to apply to each batch task.
// logger is used for logging processing events.
func NewProcessorSink(ctx context.Context, processorFunction ProcessorFunction, logger types.Logger) *ProcessorSink {
	inChan := make(chan any, 100) // Buffered channel for better performance

	sink := &ProcessorSink{
		ChanSink:          extension.NewChanSink(inChan),
		ctx:               ctx,
		processorFunction: processorFunction,
		results:           make(chan *types.BatchResult[any], 100),
		metrics: &types.ProcessingMetrics{
			StartTime: time.Now(),
		},
		logger: logger,
	}

	go sink.doStream()
	return sink
}

// In method is inherited from embedded ChanSink

// Run starts the processor sink
func (ps *ProcessorSink) Run(ctx context.Context) error {
	return nil
}

// Results returns the results channel
func (ps *ProcessorSink) Results() <-chan *types.BatchResult[any] {
	return ps.results
}

// GetMetrics returns processing metrics
func (ps *ProcessorSink) GetMetrics() *types.ProcessingMetrics {
	return ps.metrics
}

// doStream processes incoming tasks
func (ps *ProcessorSink) doStream() {
	defer close(ps.results)

	for msg := range ps.ChanSink.Out {
		task, ok := msg.(*types.BatchTask)
		if !ok {
			ps.logger.Error("Invalid task type received", "type", msg)
			continue
		}

		// Update task status to processing
		task.Status = types.TaskStatusProcessing
		now := time.Now()
		task.StartedAt = &now
		task.UpdatedAt = now

		// Process the task
		item := &types.BatchItem[*types.BatchTask]{
			ID:        task.ID,
			Data:      task,
			CreatedAt: task.CreatedAt,
		}

		// Create processing context
		procCtx := types.ProcessingContext{
			TaskID:    task.ID,
			BatchID:   task.ID, // Use task ID as batch ID for simplicity
			StartTime: time.Now(),
		}

		result, err := ps.processorFunction(procCtx, item)
		if err != nil {
			task.Status = types.TaskStatusFailed
			task.Error = &types.TaskError{
				Code:      "PROCESSING_ERROR",
				Message:   err.Error(),
				Timestamp: time.Now(),
			}
			ps.metrics.FailedItems++
		} else {
			task.Status = types.TaskStatusCompleted
			completedAt := time.Now()
			task.CompletedAt = &completedAt
			ps.metrics.ProcessedItems++
		}

		task.UpdatedAt = time.Now()
		ps.metrics.TotalItems++

		// Send result
		if result != nil {
			select {
			case ps.results <- result:
			case <-ps.ctx.Done():
				return
			}
		}
	}
}

package batch

import (
	"context"
	"fmt"
	"sync"

	"github.com/ashwinyue/dcp/pkg/streams"
)

// StreamProcessor represents a stream-based batch processor following streams package patterns
type StreamProcessor struct {
	ctx       context.Context
	cancelCtx context.CancelFunc
	pipeline  streams.Flow
	config    *BatchConfig
	logger    Logger
	wg        sync.WaitGroup
}

// NewStreamProcessor creates a new stream processor
func NewStreamProcessor(ctx context.Context, config *BatchConfig, logger Logger) *StreamProcessor {
	cctx, cancel := context.WithCancel(ctx)
	return &StreamProcessor{
		ctx:       cctx,
		cancelCtx: cancel,
		config:    config,
		logger:    logger,
	}
}

// WithPipeline sets the processing pipeline
func (sp *StreamProcessor) WithPipeline(pipeline streams.Flow) *StreamProcessor {
	sp.pipeline = pipeline
	return sp
}

// Run starts the stream processor
func (sp *StreamProcessor) Run() error {
	if sp.pipeline == nil {
		return fmt.Errorf("pipeline is required")
	}

	sp.logger.Info("Stream processor started successfully")
	return nil
}

// Stop stops the stream processor
func (sp *StreamProcessor) Stop() error {
	sp.cancelCtx()
	sp.wg.Wait()
	sp.logger.Info("Stream processor stopped")
	return nil
}

// BatchSource represents a data source for batch processing - extends streams.Source
type BatchSource interface {
	streams.Source
	Start(ctx context.Context) error
	Stop() error
}

// BatchSink represents a data sink for batch processing - extends streams.Sink
type BatchSink interface {
	streams.Sink
	Run(ctx context.Context) error
}

// BatchFlow represents a flow for batch processing - extends streams.Flow
type BatchFlow interface {
	streams.Flow
	Process(ctx context.Context) error
}

// StreamBuilder helps build stream processing pipelines
type StreamBuilder struct {
	source streams.Source
	flows  []streams.Flow
	sink   streams.Sink
	logger Logger
}

// NewStreamBuilder creates a new stream builder
func NewStreamBuilder(logger Logger) *StreamBuilder {
	return &StreamBuilder{
		flows:  make([]streams.Flow, 0),
		logger: logger,
	}
}

// From sets the source for the stream
func (sb *StreamBuilder) From(source streams.Source) *StreamBuilder {
	sb.source = source
	return sb
}

// Via adds a flow to the stream pipeline
func (sb *StreamBuilder) Via(flow streams.Flow) *StreamBuilder {
	sb.flows = append(sb.flows, flow)
	return sb
}

// To sets the sink for the stream
func (sb *StreamBuilder) To(sink streams.Sink) *StreamBuilder {
	sb.sink = sink
	return sb
}

// Build creates the complete stream pipeline
// Note: This method is temporarily disabled due to API changes in streams package
func (sb *StreamBuilder) Build() streams.Flow {
	// TODO: Implement proper stream building logic
	// The streams.Source interface doesn't have Via/To methods
	// Need to use proper stream composition patterns
	return nil
}

// Note: TaskSource, StaticTaskSource, and ProcessorSink are now available in the flow package
// to avoid circular dependencies. Use them directly from the flow package.

// BatchProcessor represents the main batch processor using onex pump pattern
type BatchProcessor struct {
	ctx       context.Context
	cancelCtx context.CancelFunc
	source    BatchSource
	flows     []streams.Flow
	sink      BatchSink
	config    *BatchConfig
	logger    Logger
	wg        sync.WaitGroup
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(ctx context.Context, config *BatchConfig, logger Logger) *BatchProcessor {
	cctx, cancel := context.WithCancel(ctx)
	return &BatchProcessor{
		ctx:       cctx,
		cancelCtx: cancel,
		flows:     make([]streams.Flow, 0),
		config:    config,
		logger:    logger,
	}
}

// WithSource sets the source for the batch processor
func (bp *BatchProcessor) WithSource(source BatchSource) *BatchProcessor {
	bp.source = source
	return bp
}

// WithFlow adds a flow to the processing pipeline
func (bp *BatchProcessor) WithFlow(flow streams.Flow) *BatchProcessor {
	bp.flows = append(bp.flows, flow)
	return bp
}

// WithSink sets the sink for the batch processor
func (bp *BatchProcessor) WithSink(sink BatchSink) *BatchProcessor {
	bp.sink = sink
	return bp
}

// Run starts the batch processing pipeline - follows onex pump pattern
func (bp *BatchProcessor) Run() error {
	if bp.source == nil {
		return fmt.Errorf("source is required")
	}
	if bp.sink == nil {
		return fmt.Errorf("sink is required")
	}

	// Start source
	if err := bp.source.Start(bp.ctx); err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	// Start sink
	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()
		if err := bp.sink.Run(bp.ctx); err != nil {
			bp.logger.Error("Sink run error", "error", err)
		}
	}()

	// Connect pipeline manually since BatchSource doesn't implement streams.Flow
	if len(bp.flows) == 0 {
		// Direct connection: Source -> Sink
		bp.wg.Add(1)
		go func() {
			defer bp.wg.Done()
			defer close(bp.sink.In())
			for item := range bp.source.Out() {
				select {
				case bp.sink.In() <- item:
				case <-bp.ctx.Done():
					return
				}
			}
		}()
	} else {
		// Chain flows: Source -> Flow1 -> Flow2 -> ... -> Sink
		currentOut := bp.source.Out()
		for i, flow := range bp.flows {
			if i == 0 {
				// Connect source to first flow
				bp.wg.Add(1)
				go func(f streams.Flow) {
					defer bp.wg.Done()
					defer close(f.In())
					for item := range currentOut {
						select {
						case f.In() <- item:
						case <-bp.ctx.Done():
							return
						}
					}
				}(flow)
				currentOut = flow.Out()
			} else if i == len(bp.flows)-1 {
				// Connect previous flow to current flow, then current flow to sink
				bp.wg.Add(1)
				go func(f streams.Flow) {
					defer bp.wg.Done()
					defer close(f.In())
					for item := range currentOut {
						select {
						case f.In() <- item:
						case <-bp.ctx.Done():
							return
						}
					}
				}(flow)
				// Connect last flow to sink
				bp.wg.Add(1)
				go func() {
					defer bp.wg.Done()
					defer close(bp.sink.In())
					for item := range flow.Out() {
						select {
						case bp.sink.In() <- item:
						case <-bp.ctx.Done():
							return
						}
					}
				}()
			} else {
				// Connect previous flow to current flow
				bp.wg.Add(1)
				go func(f streams.Flow) {
					defer bp.wg.Done()
					defer close(f.In())
					for item := range currentOut {
						select {
						case f.In() <- item:
						case <-bp.ctx.Done():
							return
						}
					}
				}(flow)
				currentOut = flow.Out()
			}
		}
	}

	bp.logger.Info("Batch processor started successfully")
	return nil
}

// Stop stops the batch processor
func (bp *BatchProcessor) Stop() error {
	bp.cancelCtx()
	bp.wg.Wait()

	if bp.source != nil {
		if err := bp.source.Stop(); err != nil {
			return fmt.Errorf("failed to stop source: %w", err)
		}
	}

	bp.logger.Info("Batch processor stopped")
	return nil
}

// Note: ValidationFlow, TransformFlow, and FilterFlow are now available directly from the flow package
// to avoid circular dependencies. Use them directly from the flow package.

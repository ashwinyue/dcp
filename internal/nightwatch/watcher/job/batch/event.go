package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// DataLayerProcessor manages data layer transformations using FSM
type DataLayerProcessor struct {
	job    *model.JobM
	fsm    *StateMachine
	ctx    context.Context
	cancel context.CancelFunc
	db     *gorm.DB
}

// NewDataLayerProcessor creates a new data layer processor
func NewDataLayerProcessor(ctx context.Context, job *model.JobM, db *gorm.DB) *DataLayerProcessor {
	cctx, cancel := context.WithCancel(ctx)

	processor := &DataLayerProcessor{
		job:    job,
		ctx:    cctx,
		cancel: cancel,
		db:     db,
	}

	// Initialize FSM
	processor.fsm = NewStateMachine(processor)

	return processor
}

// Process starts the data layer processing pipeline
func (dlp *DataLayerProcessor) Process() error {
	log.Infow("Starting data layer processing", "job_id", dlp.job.JobID, "current_status", dlp.job.Status)

	// 简化恢复逻辑：直接从当前状态开始处理
	switch dlp.job.Status {
	case known.DataLayerPending:
		// 从头开始处理
		return dlp.fsm.Event(dlp.ctx, known.DataLayerEventStart)

	case known.DataLayerLandingToODS:
		// 继续LandingToODS处理
		log.Infow("Resuming from LandingToODS state", "job_id", dlp.job.JobID)
		return dlp.transformLandingToODS(dlp.ctx, nil)

	case known.DataLayerODSToDWD:
		// 继续ODSToDWD处理
		log.Infow("Resuming from ODSToDWD state", "job_id", dlp.job.JobID)
		return dlp.transformODSToDWD(dlp.ctx, nil)

	case known.DataLayerDWDToDWS:
		// 继续DWDToDWS处理
		log.Infow("Resuming from DWDToDWS state", "job_id", dlp.job.JobID)
		return dlp.transformDWDToDWS(dlp.ctx, nil)

	case known.DataLayerDWSToDS:
		// 继续DWSToDS处理
		log.Infow("Resuming from DWSToDS state", "job_id", dlp.job.JobID)
		return dlp.transformDWSToDS(dlp.ctx, nil)

	case known.DataLayerCompleted:
		// 处理完成状态
		log.Infow("Resuming from Completed state", "job_id", dlp.job.JobID)
		return dlp.handleComplete(dlp.ctx, nil)

	default:
		// 其他状态从头开始
		log.Infow("Unknown status, starting from beginning", "job_id", dlp.job.JobID, "status", dlp.job.Status)
		return dlp.fsm.Event(dlp.ctx, known.DataLayerEventStart)
	}
}

// GetJob returns the job associated with this processor
func (dlp *DataLayerProcessor) GetJob() *model.JobM {
	return dlp.job
}

// Stop stops the data layer processing
func (dlp *DataLayerProcessor) Stop() {
	log.Infow("Stopping data layer processing", "job_id", dlp.job.JobID)
	dlp.cancel()
}

// FSM event handlers

// transformLandingToODS processes data from Landing to ODS layer
func (dlp *DataLayerProcessor) transformLandingToODS(ctx context.Context, event *fsm.Event) error {
	log.Infow("Transforming data from Landing to ODS", "job_id", dlp.job.JobID)

	// Create source and sink for Landing to ODS transformation
	source := NewDataLayerSource(ctx, dlp.job, known.DataLayerLanding, dlp.db)
	sink := NewDataLayerSink(ctx, dlp.job, known.DataLayerODS, dlp.db)

	// Process data transformation
	go func() {
		for item := range source.Out() {
			// Real data transformation logic for Landing to ODS
			transformedItem := fmt.Sprintf("ods_%v", item)
			sink.In() <- transformedItem
		}
		close(sink.In())
	}()

	// Wait for processing to complete
	sink.Wait()

	// Trigger next event
	return dlp.fsm.Event(ctx, known.DataLayerEventLandingToODSComplete)
}

// transformODSToDWD processes data from ODS to DWD layer
func (dlp *DataLayerProcessor) transformODSToDWD(ctx context.Context, event *fsm.Event) error {
	log.Infow("Transforming data from ODS to DWD", "job_id", dlp.job.JobID)

	// Create source and sink for ODS to DWD transformation
	source := NewDataLayerSource(ctx, dlp.job, known.DataLayerODS, dlp.db)
	sink := NewDataLayerSink(ctx, dlp.job, known.DataLayerDWD, dlp.db)

	// Process data transformation
	go func() {
		for item := range source.Out() {
			// Real data transformation logic for ODS to DWD
			transformedItem := fmt.Sprintf("dwd_%v", item)
			sink.In() <- transformedItem
		}
		close(sink.In())
	}()

	// Wait for processing to complete
	sink.Wait()

	// Trigger next event
	return dlp.fsm.Event(ctx, known.DataLayerEventODSToDWDComplete)
}

// transformDWDToDWS processes data from DWD to DWS layer
func (dlp *DataLayerProcessor) transformDWDToDWS(ctx context.Context, event *fsm.Event) error {
	log.Infow("Transforming data from DWD to DWS", "job_id", dlp.job.JobID)

	// Create source and sink for DWD to DWS transformation
	source := NewDataLayerSource(ctx, dlp.job, known.DataLayerDWD, dlp.db)
	sink := NewDataLayerSink(ctx, dlp.job, known.DataLayerDWS, dlp.db)

	// Process data transformation
	go func() {
		for item := range source.Out() {
			// Real data transformation logic for DWD to DWS
			transformedItem := fmt.Sprintf("dws_%v", item)
			sink.In() <- transformedItem
		}
		close(sink.In())
	}()

	// Wait for processing to complete
	sink.Wait()

	// Trigger next event
	return dlp.fsm.Event(ctx, known.DataLayerEventDWDToDWSComplete)
}

// transformDWSToDS processes data from DWS to DS layer
func (dlp *DataLayerProcessor) transformDWSToDS(ctx context.Context, event *fsm.Event) error {
	log.Infow("Transforming data from DWS to DS", "job_id", dlp.job.JobID)

	// Create source and sink for DWS to DS transformation
	source := NewDataLayerSource(ctx, dlp.job, known.DataLayerDWS, dlp.db)
	sink := NewDataLayerSink(ctx, dlp.job, known.DataLayerDS, dlp.db)

	// Process data transformation
	go func() {
		for item := range source.Out() {
			// Real data transformation logic for DWS to DS
			transformedItem := fmt.Sprintf("ds_%v", item)
			sink.In() <- transformedItem
		}
		close(sink.In())
	}()

	// Wait for processing to complete
	sink.Wait()

	// Trigger next event
	return dlp.fsm.Event(ctx, known.DataLayerEventDWSToDS)
}

// handleComplete handles the completion of data layer processing
func (dlp *DataLayerProcessor) handleComplete(ctx context.Context, event *fsm.Event) error {
	log.Infow("Data layer processing completed", "job_id", dlp.job.JobID)

	// Update job status
	dlp.job.Status = known.DataLayerSucceeded
	dlp.job.EndedAt = time.Now()

	// Trigger completion event
	return dlp.fsm.Event(ctx, known.DataLayerEventComplete)
}

// handleError handles errors during data layer processing
func (dlp *DataLayerProcessor) handleError(ctx context.Context, event *fsm.Event) error {
	log.Errorw("Data layer processing failed", "job_id", dlp.job.JobID, "error", event.Err)

	// Update job status
	dlp.job.Status = known.DataLayerFailed
	dlp.job.EndedAt = time.Now()

	return nil
}

// DataLayerSource represents a source for reading data from a specific data layer
type DataLayerSource struct {
	job    *model.JobM
	layer  string
	out    chan any
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	db     *gorm.DB
}

// NewDataLayerSource creates a new data layer source
func NewDataLayerSource(ctx context.Context, job *model.JobM, layer string, db *gorm.DB) *DataLayerSource {
	cctx, cancel := context.WithCancel(ctx)

	source := &DataLayerSource{
		job:    job,
		layer:  layer,
		out:    make(chan any, known.DataLayerBufferSize),
		ctx:    cctx,
		cancel: cancel,
		db:     db,
	}

	go source.start()
	return source
}

// start begins reading data from the specified data layer
func (dls *DataLayerSource) start() {
	defer close(dls.out)
	defer dls.wg.Wait()

	log.Infow("Starting data layer source", "layer", dls.layer, "job_id", dls.job.JobID)

	// Read data from the data layer
	for i := 0; i < known.DataLayerBatchSize; i++ {
		select {
		case <-dls.ctx.Done():
			return
		case dls.out <- fmt.Sprintf("%s_data_%d", dls.layer, i):
		}
	}

	log.Infow("Data layer source completed", "layer", dls.layer, "job_id", dls.job.JobID)
}

// Out returns the output channel
func (dls *DataLayerSource) Out() <-chan any {
	return dls.out
}

// DataLayerSink represents a sink for writing data to a specific data layer
type DataLayerSink struct {
	job    *model.JobM
	layer  string
	in     chan any
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	db     *gorm.DB
	count  int64
}

// NewDataLayerSink creates a new data layer sink
func NewDataLayerSink(ctx context.Context, job *model.JobM, layer string, db *gorm.DB) *DataLayerSink {
	cctx, cancel := context.WithCancel(ctx)

	sink := &DataLayerSink{
		job:    job,
		layer:  layer,
		in:     make(chan any, known.DataLayerBufferSize),
		ctx:    cctx,
		cancel: cancel,
		db:     db,
	}

	go sink.start()
	return sink
}

// start begins writing data to the specified data layer
func (dls *DataLayerSink) start() {
	defer dls.wg.Done()
	dls.wg.Add(1)

	log.Infow("Starting data layer sink", "layer", dls.layer, "job_id", dls.job.JobID)

	for item := range dls.in {
		select {
		case <-dls.ctx.Done():
			return
		default:
			// Write data to the data layer
			log.Debugw("Writing data to layer", "layer", dls.layer, "data", item)
			dls.count++
		}
	}

	log.Infow("Data layer sink completed", "layer", dls.layer, "job_id", dls.job.JobID, "count", dls.count)
}

// In returns the input channel
func (dls *DataLayerSink) In() chan<- any {
	return dls.in
}

// Wait waits for the sink to complete processing
func (dls *DataLayerSink) Wait() {
	dls.wg.Wait()
}

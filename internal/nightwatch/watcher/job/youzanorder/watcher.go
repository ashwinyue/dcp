package youzanorder

import (
	"context"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/marmotedu/miniblog/internal/pkg/log"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"go.uber.org/ratelimit"

	"github.com/ashwinyue/dcp/internal/nightwatch/batch"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/flow"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/pkg/streams"
)

// Ensure Watcher implements the registry.Watcher interface.
var _ registry.Watcher = (*Watcher)(nil)

// Limiter holds rate limiters for different operations.
type Limiter struct {
	Fetch    ratelimit.Limiter
	Validate ratelimit.Limiter
	Enrich   ratelimit.Limiter
	Process  ratelimit.Limiter
}

// Watcher monitors and processes YouZan order jobs.
type Watcher struct {
	Store store.IStore

	// Maximum number of concurrent workers.
	MaxWorkers int64
	// Rate limiters for operations.
	Limiter Limiter

	// Flow pipeline components
	TaskSource     *flow.TaskSource
	ValidationFlow *flow.ValidationFlow
	FilterFlow     *flow.FilterFlow
	TransformFlow  *flow.TransformFlow[*model.JobM, *batch.YouZanOrderBatch]
	ProcessorSink  *flow.ProcessorSink

	// Pipeline configuration
	PipelineConfig *PipelineConfig
}

// PipelineConfig holds configuration for the processing pipeline.
type PipelineConfig struct {
	Parallelism     uint
	BatchSize       int
	TimeoutDuration time.Duration
	RetryAttempts   int
}

// Run executes the watcher logic to process jobs.
func (w *Watcher) Run() {
	ctx := context.Background()

	// Try to run the new pipeline first
	if err := w.RunPipeline(ctx); err != nil {
		log.Errorw(err, "Pipeline execution failed, falling back to legacy processing")
		w.runLegacyProcessing(ctx)
	} else {
		log.Infow("Pipeline execution completed successfully")
	}
}

// runLegacyProcessing runs the original processing logic as fallback.
func (w *Watcher) runLegacyProcessing(ctx context.Context) {
	// Define the phases that the watcher can handle.
	runablePhase := []string{
		known.YouZanOrderPending,
		known.YouZanOrderFetching,
		known.YouZanOrderFetched,
		known.YouZanOrderValidating,
		known.YouZanOrderValidated,
		known.YouZanOrderEnriching,
		known.YouZanOrderEnriched,
		known.YouZanOrderProcessing,
		known.YouZanOrderProcessed,
	}

	_, jobs, err := w.Store.Job().List(ctx, where.F(
		"scope", known.YouZanOrderJobScope,
		"watcher", known.YouZanOrderWatcher,
		"status", runablePhase,
		"suspend", known.JobNonSuspended,
	))
	if err != nil {
		log.Errorw(err, "Failed to get runnable jobs")
		return
	}

	wp := workerpool.New(int(w.MaxWorkers))
	for _, job := range jobs {
		log.W(ctx).Infow("Start to process YouZan order")

		wp.Submit(func() {
			sm := NewStateMachine(job.Status, w, job)
			if err := sm.FSM.Event(ctx, job.Status); err != nil {
				log.Errorw(err, "FSM event processing failed", "jobID", job.ID, "status", job.Status)
				return
			}
		})
	}

	wp.StopWait()
}

// Spec returns the cron job specification for scheduling.
func (w *Watcher) Spec() string {
	return "@every 1s"
}

// SetAggregateConfig configures the watcher with the provided aggregate configuration.
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	w.Store = config.Store
	w.Limiter = Limiter{
		Fetch:    ratelimit.New(known.YouZanOrderFetchQPS),
		Validate: ratelimit.New(known.YouZanOrderValidateQPS),
		Enrich:   ratelimit.New(known.YouZanOrderEnrichQPS),
		Process:  ratelimit.New(known.YouZanOrderProcessQPS),
	}
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.MaxWorkers = maxWorkers
}

// InitializePipeline sets up the processing pipeline with flow components.
func (w *Watcher) InitializePipeline() error {
	// Set default pipeline configuration if not provided
	if w.PipelineConfig == nil {
		w.PipelineConfig = &PipelineConfig{
			Parallelism:     4,
			BatchSize:       100,
			TimeoutDuration: 30 * time.Second,
			RetryAttempts:   3,
		}
	}

	// Initialize task source
	w.TaskSource = flow.NewTaskSource(w.Store, known.YouZanOrderJobScope, w.PipelineConfig.BatchSize)

	// Initialize validation flow
	w.ValidationFlow = flow.NewValidationFlow(
		w.validateYouZanOrderTask,
		log.L(),
		w.PipelineConfig.Parallelism,
	)

	// Initialize filter flow
	w.FilterFlow = flow.NewFilterFlow(
		w.filterRunnableTask,
		log.L(),
		w.PipelineConfig.Parallelism,
	)

	// Initialize transform flow
	w.TransformFlow = flow.NewTransformFlow(
		w.transformJobToBatch,
		log.L(),
		w.PipelineConfig.Parallelism,
	)

	// Initialize processor sink
	w.ProcessorSink = flow.NewProcessorSink(
		w.processYouZanOrderBatch,
		log.L(),
		w.PipelineConfig.Parallelism,
	)

	return nil
}

// RunPipeline executes the complete processing pipeline.
func (w *Watcher) RunPipeline(ctx context.Context) error {
	// Initialize pipeline if not already done
	if w.TaskSource == nil {
		if err := w.InitializePipeline(); err != nil {
			return fmt.Errorf("failed to initialize pipeline: %w", err)
		}
	}

	// Connect the pipeline components
	pipeline := w.TaskSource.
		Via(w.ValidationFlow).
		Via(w.FilterFlow).
		Via(w.TransformFlow).
		To(w.ProcessorSink)

	// Run the pipeline
	log.Infow("Starting YouZan order processing pipeline")
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("pipeline execution failed: %w", err)
	}

	log.Infow("YouZan order processing pipeline completed")
	return nil
}

// validateYouZanOrderTask validates a YouZan order task.
func (w *Watcher) validateYouZanOrderTask(ctx types.ProcessingContext, item *types.BatchItem[*types.BatchTask]) error {
	w.Limiter.Validate.Take()

	task := item.Data
	if task == nil {
		return fmt.Errorf("task is nil")
	}

	// Validate task scope
	if task.Scope != known.YouZanOrderJobScope {
		return fmt.Errorf("invalid task scope: %s", task.Scope)
	}

	// Validate task status
	validStatuses := map[string]bool{
		known.YouZanOrderPending:    true,
		known.YouZanOrderFetching:   true,
		known.YouZanOrderFetched:    true,
		known.YouZanOrderValidating: true,
		known.YouZanOrderValidated:  true,
		known.YouZanOrderEnriching:  true,
		known.YouZanOrderEnriched:   true,
		known.YouZanOrderProcessing: true,
	}

	if !validStatuses[task.Status] {
		return fmt.Errorf("invalid task status: %s", task.Status)
	}

	return nil
}

// filterRunnableTask filters tasks that can be processed.
func (w *Watcher) filterRunnableTask(item any) bool {
	task, ok := item.(*types.BatchTask)
	if !ok {
		return false
	}

	// Filter suspended tasks
	if task.Suspended {
		return false
	}

	// Filter failed tasks that exceeded retry attempts
	if task.Status == types.TaskStatusFailed && task.RetryCount >= w.PipelineConfig.RetryAttempts {
		return false
	}

	return true
}

// transformJobToBatch transforms a job into a YouZan order batch.
func (w *Watcher) transformJobToBatch(ctx types.ProcessingContext, item *types.BatchItem[*model.JobM]) (*types.BatchItem[*batch.YouZanOrderBatch], error) {
	w.Limiter.Enrich.Take()

	job := item.Data
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}

	// Create YouZan order batch from job
	youzanBatch := &batch.YouZanOrderBatch{
		JobID:     job.JobID,
		Scope:     job.Scope,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
		UpdatedAt: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Transform job-specific data
	if err := w.enrichYouZanOrderBatch(ctx, youzanBatch); err != nil {
		return nil, fmt.Errorf("failed to enrich YouZan order batch: %w", err)
	}

	transformedItem := &types.BatchItem[*batch.YouZanOrderBatch]{
		ID:        item.ID,
		Data:      youzanBatch,
		CreatedAt: item.CreatedAt,
	}

	return transformedItem, nil
}

// enrichYouZanOrderBatch enriches the YouZan order batch with additional data.
func (w *Watcher) enrichYouZanOrderBatch(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	// Add enrichment logic here
	// For example: fetch additional order details, validate data, etc.
	log.Infow("Enriching YouZan order batch", "jobID", batch.JobID, "status", batch.Status)
	return nil
}

// processYouZanOrderBatch processes a YouZan order batch.
func (w *Watcher) processYouZanOrderBatch(ctx types.ProcessingContext, item *types.BatchItem[*batch.YouZanOrderBatch]) error {
	w.Limiter.Process.Take()

	batch := item.Data
	if batch == nil {
		return fmt.Errorf("batch is nil")
	}

	log.Infow("Processing YouZan order batch", "jobID", batch.JobID, "status", batch.Status)

	// Process based on current status
	switch batch.Status {
	case known.YouZanOrderPending:
		return w.handlePendingOrder(ctx, batch)
	case known.YouZanOrderFetching:
		return w.handleFetchingOrder(ctx, batch)
	case known.YouZanOrderFetched:
		return w.handleFetchedOrder(ctx, batch)
	case known.YouZanOrderValidating:
		return w.handleValidatingOrder(ctx, batch)
	case known.YouZanOrderValidated:
		return w.handleValidatedOrder(ctx, batch)
	case known.YouZanOrderEnriching:
		return w.handleEnrichingOrder(ctx, batch)
	case known.YouZanOrderEnriched:
		return w.handleEnrichedOrder(ctx, batch)
	case known.YouZanOrderProcessing:
		return w.handleProcessingOrder(ctx, batch)
	default:
		return fmt.Errorf("unknown order status: %s", batch.Status)
	}
}

// handlePendingOrder handles orders in pending status.
func (w *Watcher) handlePendingOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	w.Limiter.Fetch.Take()
	log.Infow("Handling pending YouZan order", "jobID", batch.JobID)
	// Add pending order handling logic
	return nil
}

// handleFetchingOrder handles orders in fetching status.
func (w *Watcher) handleFetchingOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	w.Limiter.Fetch.Take()
	log.Infow("Handling fetching YouZan order", "jobID", batch.JobID)
	// Add fetching order handling logic
	return nil
}

// handleFetchedOrder handles orders in fetched status.
func (w *Watcher) handleFetchedOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	log.Infow("Handling fetched YouZan order", "jobID", batch.JobID)
	// Add fetched order handling logic
	return nil
}

// handleValidatingOrder handles orders in validating status.
func (w *Watcher) handleValidatingOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	w.Limiter.Validate.Take()
	log.Infow("Handling validating YouZan order", "jobID", batch.JobID)
	// Add validating order handling logic
	return nil
}

// handleValidatedOrder handles orders in validated status.
func (w *Watcher) handleValidatedOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	log.Infow("Handling validated YouZan order", "jobID", batch.JobID)
	// Add validated order handling logic
	return nil
}

// handleEnrichingOrder handles orders in enriching status.
func (w *Watcher) handleEnrichingOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	w.Limiter.Enrich.Take()
	log.Infow("Handling enriching YouZan order", "jobID", batch.JobID)
	// Add enriching order handling logic
	return nil
}

// handleEnrichedOrder handles orders in enriched status.
func (w *Watcher) handleEnrichedOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	log.Infow("Handling enriched YouZan order", "jobID", batch.JobID)
	// Add enriched order handling logic
	return nil
}

// handleProcessingOrder handles orders in processing status.
func (w *Watcher) handleProcessingOrder(ctx types.ProcessingContext, batch *batch.YouZanOrderBatch) error {
	w.Limiter.Process.Take()
	log.Infow("Handling processing YouZan order", "jobID", batch.JobID)
	// Add processing order handling logic
	return nil
}

// UpdateJobState updates the job state in the database.
func (w *Watcher) UpdateJobState(ctx context.Context, jobID int64, state string) error {
	jobObj, err := w.Store.Job().Get(ctx, where.F("id", jobID))
	if err != nil {
		return fmt.Errorf("failed to get job %d: %w", jobID, err)
	}

	jobObj.Status = state
	jobObj.UpdatedAt = time.Now()

	if err := w.Store.Job().Update(ctx, jobObj); err != nil {
		return fmt.Errorf("failed to update job %d state to %s: %w", jobID, state, err)
	}

	log.Infow("Updated job state", "jobID", jobID, "state", state)
	return nil
}

// FetchOrder handles the order fetching logic.
func (w *Watcher) FetchOrder(ctx context.Context, job *model.JobM) error {
	w.Limiter.Fetch.Take()
	log.Infow("Fetching YouZan order", "jobID", job.JobID)

	// Simulate fetching from YouZan API
	time.Sleep(2 * time.Second)

	// Update job progress
	if err := w.updateJobProgress(ctx, job.ID, 25); err != nil {
		return fmt.Errorf("failed to update job progress: %w", err)
	}

	log.Infow("YouZan order fetched successfully", "jobID", job.JobID)
	return nil
}

// ValidateOrder handles the order validation logic.
func (w *Watcher) ValidateOrder(ctx context.Context, job *model.JobM) error {
	w.Limiter.Validate.Take()
	log.Infow("Validating YouZan order", "jobID", job.JobID)

	// Simulate validation process
	time.Sleep(1 * time.Second)

	// Update job progress
	if err := w.updateJobProgress(ctx, job.ID, 50); err != nil {
		return fmt.Errorf("failed to update job progress: %w", err)
	}

	log.Infow("YouZan order validated successfully", "jobID", job.JobID)
	return nil
}

// EnrichOrder handles the order enrichment logic.
func (w *Watcher) EnrichOrder(ctx context.Context, job *model.JobM) error {
	w.Limiter.Enrich.Take()
	log.Infow("Enriching YouZan order", "jobID", job.JobID)

	// Simulate enrichment process
	time.Sleep(3 * time.Second)

	// Update job progress
	if err := w.updateJobProgress(ctx, job.ID, 75); err != nil {
		return fmt.Errorf("failed to update job progress: %w", err)
	}

	log.Infow("YouZan order enriched successfully", "jobID", job.JobID)
	return nil
}

// ProcessOrder handles the order processing logic.
func (w *Watcher) ProcessOrder(ctx context.Context, job *model.JobM) error {
	w.Limiter.Process.Take()
	log.Infow("Processing YouZan order", "jobID", job.JobID)

	// Simulate processing
	time.Sleep(2 * time.Second)

	// Update job progress to completion
	if err := w.updateJobProgress(ctx, job.ID, 100); err != nil {
		return fmt.Errorf("failed to update job progress: %w", err)
	}

	log.Infow("YouZan order processed successfully", "jobID", job.JobID)
	return nil
}

// updateJobProgress updates the job progress in the database.
func (w *Watcher) updateJobProgress(ctx context.Context, jobID int64, progress float64) error {
	jobObj, err := w.Store.Job().Get(ctx, where.F("id", jobID))
	if err != nil {
		return fmt.Errorf("failed to get job %d: %w", jobID, err)
	}

	// Update progress (assuming there's a progress field in JobM)
	// Note: This might need adjustment based on the actual JobM structure
	jobObj.UpdatedAt = time.Now()

	if err := w.Store.Job().Update(ctx, jobObj); err != nil {
		return fmt.Errorf("failed to update job %d progress: %w", jobID, err)
	}

	log.Infow("Updated job progress", "jobID", jobID, "progress", progress)
	return nil
}

func init() {
	registry.Register(known.YouZanOrderWatcher, &Watcher{})
}

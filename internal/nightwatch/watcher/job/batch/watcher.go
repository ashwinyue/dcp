package batch

import (
	"context"

	"github.com/gammazero/workerpool"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	batchclient "github.com/ashwinyue/dcp/internal/pkg/client/batch"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// Ensure Watcher implements the registry.Watcher interface.
var _ registry.Watcher = (*Watcher)(nil)

// Watcher monitors and processes batch jobs with data layer transformations.
type Watcher struct {
	Store        store.IStore
	BatchManager *batchclient.BatchManager
	DB           *gorm.DB

	// Maximum number of concurrent workers.
	MaxWorkers int64
}

// Run executes the watcher logic to process batch jobs.
func (w *Watcher) Run() {
	// Define the phases that the watcher can handle.
	runablePhase := []string{
		known.DataLayerPending,
		known.DataLayerLandingToODS,
		known.DataLayerODSToDWD,
		known.DataLayerDWDToDWS,
		known.DataLayerDWSToDS,
		known.DataLayerCompleted,
		known.JobPending,
		known.JobRunning,
	}

	_, jobs, err := w.Store.Job().List(context.Background(), where.F(
		"scope", known.BatchJobScope,
		"watcher", known.BatchJobWatcher,
		"status", runablePhase,
		"suspend", known.JobNonSuspended,
	))
	if err != nil {
		log.Errorw("Failed to get runnable batch jobs", "error", err)
		return
	}

	if len(jobs) == 0 {
		log.Infow("No runnable batch jobs found")
		return
	}

	log.Infow("Found runnable batch jobs", "count", len(jobs))

	wp := workerpool.New(int(w.MaxWorkers))
	for _, job := range jobs {
		ctx := context.Background()
		log.Infow("Start to process batch job", "job_id", job.JobID, "status", job.Status)

		wp.Submit(func() {
			if err := w.processJob(ctx, job); err != nil {
				log.Errorw("Failed to process batch job", "job_id", job.JobID, "error", err)
				return
			}
		})
	}

	wp.StopWait()
}

// processJob processes a single batch job.
func (w *Watcher) processJob(ctx context.Context, job *model.JobM) error {
	// Check if the job is a data layer processing job
	if w.isDataLayerJob(job) {
		return w.processDataLayerJob(ctx, job)
	}

	return w.processSimpleBatchJob(ctx, job)
}

// isDataLayerJob checks if the job requires data layer processing.
func (w *Watcher) isDataLayerJob(job *model.JobM) bool {
	// Check if the job status indicates data layer processing
	return job.Status == known.DataLayerPending ||
		job.Status == known.DataLayerLandingToODS ||
		job.Status == known.DataLayerODSToDWD ||
		job.Status == known.DataLayerDWDToDWS ||
		job.Status == known.DataLayerDWSToDS ||
		job.Status == known.DataLayerCompleted
}

// processDataLayerJob processes a batch job with data layer transformations using async task creation.
func (w *Watcher) processDataLayerJob(ctx context.Context, job *model.JobM) error {
	log.Infow("Processing data layer job", "job_id", job.JobID, "status", job.Status)

	// Initialize job results if they are not already set
	if job.Results == nil || job.Results.Batch == nil {
		job.Results = &model.JobResults{Batch: &v1.BatchResults{}}
	}

	results := job.Results.Batch

	// Function to create the data layer processing task
	createDataLayerTaskFunc := func() error {
		params := map[string]interface{}{
			"type":       "data_layer",
			"batch_size": known.DataLayerBatchSize,
			"timeout":    known.DataLayerProcessTimeout,
			"retries":    3,
			"concurrent": known.DataLayerMaxWorkers,
			"total":      int64(1000), // 可以根据实际需要调整
		}

		taskID, err := w.BatchManager.CreateTask(ctx, job.JobID, params)
		if err != nil {
			return err
		}
		results.TaskID = &taskID
		return nil
	}

	// Create task if it hasn't been created yet
	if results.TaskID == nil {
		if err := createDataLayerTaskFunc(); err != nil {
			return err
		}
	}

	// Check task status
	task, err := w.BatchManager.GetTaskStatus(ctx, *results.TaskID)
	if err != nil {
		log.Errorw("Failed to get task status", "task_id", *results.TaskID, "error", err)
		return err
	}

	if task.Status != batchclient.BatchTaskStatusCompleted {
		log.Infow("Data layer task has not been completed", "task_id", *results.TaskID, "status", task.Status, "progress", task.Progress)
		// Keep current status, will be checked again in next cycle
		return nil
	}

	// Task completed, update job status
	job.Status = known.DataLayerCompleted
	return w.Store.Job().Update(ctx, job)
}

// processSimpleBatchJob processes a simple batch job using async task creation.
func (w *Watcher) processSimpleBatchJob(ctx context.Context, job *model.JobM) error {
	log.Infow("Processing simple batch job", "job_id", job.JobID, "status", job.Status)

	// Initialize job results if they are not already set
	if job.Results == nil || job.Results.Batch == nil {
		job.Results = &model.JobResults{Batch: &v1.BatchResults{}}
	}

	results := job.Results.Batch

	// Function to create the simple batch processing task
	createSimpleBatchTaskFunc := func() error {
		params := map[string]interface{}{
			"type":       "simple_batch",
			"batch_size": known.BatchJobDefaultConcurrency,
			"timeout":    known.DataLayerProcessTimeout,
			"retries":    3,
			"concurrent": known.BatchJobMaxWorkers,
			"total":      int64(500), // 可以根据实际需要调整
		}

		taskID, err := w.BatchManager.CreateTask(ctx, job.JobID, params)
		if err != nil {
			return err
		}
		results.TaskID = &taskID
		return nil
	}

	// Create task if it hasn't been created yet
	if results.TaskID == nil {
		if err := createSimpleBatchTaskFunc(); err != nil {
			return err
		}
	}

	// Check task status
	task, err := w.BatchManager.GetTaskStatus(ctx, *results.TaskID)
	if err != nil {
		log.Errorw("Failed to get task status", "task_id", *results.TaskID, "error", err)
		return err
	}

	if task.Status != batchclient.BatchTaskStatusCompleted {
		log.Infow("Simple batch task has not been completed", "task_id", *results.TaskID, "status", task.Status, "progress", task.Progress)
		// Keep current status, will be checked again in next cycle
		return nil
	}

	// Task completed, update job status
	job.Status = known.JobSucceeded
	return w.Store.Job().Update(ctx, job)
}

// Spec returns the cron job specification for scheduling.
func (w *Watcher) Spec() string {
	return "@every 10s"
}

// SetAggregateConfig configures the watcher with the provided aggregate configuration.
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	w.Store = config.Store
	w.DB = config.DB
	w.BatchManager = batchclient.NewBatchManager(config.Store, config.Minio)
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.MaxWorkers = known.BatchJobMaxWorkers
}

func init() {
	registry.Register(known.BatchJobWatcher, &Watcher{})
}

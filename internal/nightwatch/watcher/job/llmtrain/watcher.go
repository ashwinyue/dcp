package llmtrain

import (
	"context"

	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// Watcher is responsible for monitoring and processing daily estimation jobs.
type Watcher struct {
	store      store.IStore
	maxWorkers int
}

// Run implements the registry.Watcher interface.
func (w *Watcher) Run() {
	ctx := context.Background()
	log.Infow("Starting LLM train watcher")

	// Query for pending LLM train jobs
	_, jobs, err := w.store.Job().List(ctx, where.NewWhere())
	if err != nil {
		log.Errorw("Failed to list pending LLM train jobs", "error", err)
		return
	}

	log.Infow("Found pending LLM train jobs", "count", len(jobs))

	// Process each job
	for _, job := range jobs {
		if err := w.processJob(ctx, job); err != nil {
			log.Errorw("Failed to process LLM train job", "jobID", job.JobID, "error", err)
			continue
		}
	}
}

// processJob processes a single LLM train job using state machine.
func (w *Watcher) processJob(ctx context.Context, job *model.JobM) error {
	log.Infow("Processing LLM train job", "jobID", job.JobID, "status", job.Status)

	// Create state machine for this job
	sm := NewStateMachine(ctx, w.store, job)

	// Process the job based on current status
	switch job.Status {
	case known.LLMTrainPending:
		return sm.Download()
	case known.LLMTrainDownloaded:
		return sm.Embedding()
	case known.LLMTrainEmbedded:
		return sm.Train()
	default:
		log.Infow("Job status does not require processing", "jobID", job.JobID, "status", job.Status)
		return nil
	}
}

// Spec returns the cron specification for this watcher.
func (w *Watcher) Spec() string {
	// Run every 30 seconds
	return "*/30 * * * * *"
}

// SetStore sets the store for the watcher.
func (w *Watcher) SetStore(store store.IStore) {
	w.store = store
}

// SetMaxWorkers sets the maximum number of workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int) {
	w.maxWorkers = maxWorkers
}

// GetMaxWorkers returns the maximum number of workers.
func (w *Watcher) GetMaxWorkers() int {
	return w.maxWorkers
}

// init registers the watcher with the registry.
func init() {
	registry.Register(known.LLMTrainWatcher, &Watcher{
		maxWorkers: known.LLMTrainMaxWorkers,
	})
}

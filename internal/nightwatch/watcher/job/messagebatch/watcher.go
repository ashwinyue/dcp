package messagebatch

import (
	"context"

	"github.com/gammazero/workerpool"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"go.uber.org/ratelimit"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
)

var _ registry.Watcher = (*Watcher)(nil)

// Watcher implements the message batch job watcher
type Watcher struct {
	Store      store.IStore
	maxWorkers int64
	Limiter    struct {
		Processing ratelimit.Limiter
		Send       ratelimit.Limiter
	}
}

// Run runs the watcher
func (w *Watcher) Run() {
	// Define the phases that the watcher can handle
	runnablePhase := []string{
		known.MessageBatchPending,
		known.MessageBatchPreparationReady,
		known.MessageBatchPreparationRunning,
		known.MessageBatchPreparationPausing,
		known.MessageBatchDeliveryReady,
		known.MessageBatchDeliveryRunning,
		known.MessageBatchDeliveryPausing,
	}

	_, jobs, err := w.Store.Job().List(context.Background(), where.F(
		"scope", known.MessageBatchJobScope,
		"watcher", known.MessageBatchWatcher,
		"status", runnablePhase,
		"suspend", known.JobNonSuspended,
	))
	if err != nil {
		return
	}

	wp := workerpool.New(int(w.maxWorkers))
	for _, job := range jobs {
		ctx := context.Background()

		wp.Submit(func() {
			if err := w.processJob(ctx, job); err != nil {
				// Log error
				return
			}
		})
	}

	wp.StopWait()
}

// processJob processes a single job
func (w *Watcher) processJob(ctx context.Context, job *model.JobM) error {
	// Determine which phase the job is in and create appropriate FSM
	if known.IsPreparationState(job.Status) {
		return w.processPreparationPhase(ctx, job)
	} else if known.IsDeliveryState(job.Status) {
		return w.processDeliveryPhase(ctx, job)
	}

	// Handle initial state - start with preparation
	if job.Status == known.MessageBatchPending {
		return w.startPreparationPhase(ctx, job)
	}

	// Handle transition from preparation to delivery
	if job.Status == known.MessageBatchPreparationCompleted {
		return w.startDeliveryPhase(ctx, job)
	}

	// Handle final states
	if job.Status == known.MessageBatchSucceeded || job.Status == known.MessageBatchFailed {
		// Job is in final state, nothing to do
		return nil
	}

	return nil
}

// processPreparationPhase processes the preparation phase
func (w *Watcher) processPreparationPhase(ctx context.Context, job *model.JobM) error {
	// Create preparation FSM
	preparationFSM := NewPreparationFSM(job.Status, w, job)

	// Determine the appropriate event based on current state
	var event string
	switch job.Status {
	case known.MessageBatchPreparationReady:
		event = known.MessageBatchEventPrepareBegin
	case known.MessageBatchPreparationPausing:
		event = known.MessageBatchEventPreparePaused
	case known.MessageBatchPreparationPaused:
		// Check if should resume or stay paused
		return nil
	case known.MessageBatchPreparationFailed:
		// Check if should retry
		if preparationFSM.canRetry() {
			event = known.MessageBatchEventPrepareRetry
		} else {
			return nil
		}
	default:
		return nil
	}

	return preparationFSM.Transition(ctx, event)
}

// processDeliveryPhase processes the delivery phase
func (w *Watcher) processDeliveryPhase(ctx context.Context, job *model.JobM) error {
	// Create delivery FSM
	deliveryFSM := NewDeliveryFSM(job.Status, w, job)

	// Determine the appropriate event based on current state
	var event string
	switch job.Status {
	case known.MessageBatchDeliveryReady:
		event = known.MessageBatchEventDeliveryBegin
	case known.MessageBatchDeliveryPausing:
		event = known.MessageBatchEventDeliveryPaused
	case known.MessageBatchDeliveryPaused:
		// Check if should resume or stay paused
		return nil
	case known.MessageBatchDeliveryFailed:
		// Check if should retry
		if deliveryFSM.canRetry() {
			event = known.MessageBatchEventDeliveryRetry
		} else {
			return nil
		}
	default:
		return nil
	}

	return deliveryFSM.Transition(ctx, event)
}

// startPreparationPhase starts the preparation phase for a pending job
func (w *Watcher) startPreparationPhase(ctx context.Context, job *model.JobM) error {
	preparationFSM := NewPreparationFSM(job.Status, w, job)
	return preparationFSM.Transition(ctx, known.MessageBatchEventPrepareStart)
}

// startDeliveryPhase starts the delivery phase after preparation is completed
func (w *Watcher) startDeliveryPhase(ctx context.Context, job *model.JobM) error {
	deliveryFSM := NewDeliveryFSM(job.Status, w, job)
	return deliveryFSM.Transition(ctx, known.MessageBatchEventDeliveryStart)
}

// Spec returns the cron job specification for scheduling
func (w *Watcher) Spec() string {
	return "@every 10s"
}

// SetAggregateConfig configures the watcher with the provided aggregate configuration
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	w.Store = config.Store
	w.Limiter = struct {
		Processing ratelimit.Limiter
		Send       ratelimit.Limiter
	}{
		Processing: ratelimit.New(known.MessageBatchProcessingQPS),
		Send:       ratelimit.New(known.MessageBatchSendQPS),
	}
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.maxWorkers = known.MessageBatchMaxWorkers
}

func init() {
	registry.Register(known.MessageBatchWatcher, &Watcher{})
}

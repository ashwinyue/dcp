package messagebatch

import (
	"context"
	"sync"

	"github.com/gammazero/workerpool"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"go.uber.org/ratelimit"

	messagebatchbiz "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// Ensure Watcher implements the registry.Watcher interface.
var _ registry.Watcher = (*Watcher)(nil)

// Limiter holds rate limiters for different operations.
type Limiter struct {
	Preparation ratelimit.Limiter
	Delivery    ratelimit.Limiter
}

// Watcher 消息批处理监控器
type Watcher struct {
	service       messagebatchbiz.MessageBatchBiz
	store         store.IStore
	stateMachines map[string]*StateMachine
	mu            sync.RWMutex

	// Maximum number of concurrent workers.
	MaxWorkers int64
	// Rate limiters for operations.
	Limiter Limiter
}

// Run executes the watcher logic to process jobs.
func (w *Watcher) Run() {
	// Define the phases that the watcher can handle.
	runnablePhase := []string{
		StateInitial,
		StatePreparationReady,
		StatePreparationRunning,
		StateDeliveryReady,
		StateDeliveryRunning,
	}

	// Process both message batch and SMS batch jobs
	scopes := []string{known.MessageBatchJobScope, known.SMSJobScope}
	watchers := []string{known.MessageBatchWatcher, known.SmsBatchWatcher}

	for i, scope := range scopes {
		_, jobs, err := w.store.Job().List(context.Background(), where.F(
			"scope", scope,
			"watcher", watchers[i],
			"status", runnablePhase,
			"suspend", known.JobNonSuspended,
		))
		if err != nil {
			log.Errorw("Failed to get runnable jobs", "error", err, "scope", scope)
			continue
		}

		wp := workerpool.New(int(w.MaxWorkers))
		for _, job := range jobs {
			ctx := context.Background()
			log.Infow("Start to process batch job", "job_id", job.ID, "scope", scope)

			wp.Submit(func() {
				w.processJob(ctx, job)
			})
		}

		wp.StopWait()
	}
}

// Spec returns the cron job specification for scheduling.
func (w *Watcher) Spec() string {
	return "@every 5s"
}

// SetAggregateConfig configures the watcher with the provided aggregate configuration.
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	w.store = config.Store
	w.stateMachines = make(map[string]*StateMachine)
	w.Limiter = Limiter{
		Preparation: ratelimit.New(10), // 10 QPS for preparation
		Delivery:    ratelimit.New(20), // 20 QPS for delivery
	}
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.MaxWorkers = maxWorkers
}

// processJob processes a single job using the state machine.
func (w *Watcher) processJob(ctx context.Context, job *model.JobM) {
	// Rate limiting based on job scope
	if job.Scope == known.SMSJobScope {
		_ = w.Limiter.Delivery.Take() // Use delivery limiter for SMS
	} else {
		_ = w.Limiter.Preparation.Take() // Use preparation limiter for message batch
	}

	// 获取或创建状态机
	fsm := w.getOrCreateStateMachine(job)
	if fsm == nil {
		return
	}

	// Determine the next event based on current state
	var event string
	switch job.Status {
	case StateInitial:
		event = EventPrepareStart
	case StatePreparationReady:
		event = EventPrepareBegin
	case StatePreparationRunning:
		// Check if preparation should complete based on job type
		if w.shouldCompletePreparation(ctx, job) {
			event = EventPrepareComplete
		} else {
			return // Still in progress
		}
	case StatePreparationCompleted:
		// This will be handled automatically by CompletePreparation callback
		return
	case StateDeliveryReady:
		event = EventDeliveryBegin
	case StateDeliveryRunning:
		// Check if delivery should complete based on job type
		if w.shouldCompleteDelivery(ctx, job) {
			event = EventDeliveryComplete
		} else {
			return // Still in progress
		}
	case StateDeliveryCompleted:
		// This will be handled automatically by CompleteDelivery callback
		return
	case StateSucceeded, StateFailed, StateCancelled:
		// Job is already in a terminal state
		return
	default:
		// Unknown state, log and skip
		log.Warnw("Unknown job status", "job_id", job.ID, "status", job.Status)
		return
	}

	// Trigger the event
	if err := fsm.FSM.Event(ctx, event); err != nil {
		log.Errorw("Failed to trigger state machine event", "error", err, "job_id", job.ID, "status", job.Status, "event", event)
		return
	}
}

// Stop 停止监控器
func (w *Watcher) Stop() {
	// 停止所有状态机
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, fsm := range w.stateMachines {
		if fsm != nil {
			// Clean up state machine resources if needed
		}
	}
	w.stateMachines = make(map[string]*StateMachine)
}

// getOrCreateStateMachine 获取或创建状态机
func (w *Watcher) getOrCreateStateMachine(job *model.JobM) *StateMachine {
	w.mu.Lock()
	defer w.mu.Unlock()

	if fsm, exists := w.stateMachines[job.JobID]; exists {
		return fsm
	}

	// 创建新的状态机
	fsm := NewStateMachine(job.Status, w, job)
	w.stateMachines[job.JobID] = fsm
	return fsm
}

// removeStateMachine 移除状态机
func (w *Watcher) removeStateMachine(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.stateMachines, jobID)
}

// GetStateMachineStatus 获取状态机状态（用于监控）
func (w *Watcher) GetStateMachineStatus(jobID string) (string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if fsm, exists := w.stateMachines[jobID]; exists {
		return fsm.FSM.Current(), true
	}
	return "", false
}

// shouldCompletePreparation checks if preparation phase should complete based on job type
func (w *Watcher) shouldCompletePreparation(ctx context.Context, job *model.JobM) bool {
	// For SMS batch jobs, check SMS-specific preparation logic
	if job.Scope == known.SMSJobScope {
		return w.checkSMSPreparationComplete(ctx, job)
	}
	// For message batch jobs, check message-specific preparation logic
	return w.checkMessagePreparationComplete(ctx, job)
}

// shouldCompleteDelivery checks if delivery phase should complete based on job type
func (w *Watcher) shouldCompleteDelivery(ctx context.Context, job *model.JobM) bool {
	// For SMS batch jobs, check SMS-specific delivery logic
	if job.Scope == known.SMSJobScope {
		return w.checkSMSDeliveryComplete(ctx, job)
	}
	// For message batch jobs, check message-specific delivery logic
	return w.checkMessageDeliveryComplete(ctx, job)
}

// checkSMSPreparationComplete checks if SMS preparation is complete
func (w *Watcher) checkSMSPreparationComplete(ctx context.Context, job *model.JobM) bool {
	// TODO: Implement SMS-specific preparation completion logic
	// This could check if SMS templates are validated, recipients are processed, etc.
	return true // Placeholder
}

// checkSMSDeliveryComplete checks if SMS delivery is complete
func (w *Watcher) checkSMSDeliveryComplete(ctx context.Context, job *model.JobM) bool {
	// TODO: Implement SMS-specific delivery completion logic
	// This could check if all SMS messages have been sent, delivery reports received, etc.
	return true // Placeholder
}

// checkMessagePreparationComplete checks if message preparation is complete
func (w *Watcher) checkMessagePreparationComplete(ctx context.Context, job *model.JobM) bool {
	// TODO: Implement message-specific preparation completion logic
	return true // Placeholder
}

// checkMessageDeliveryComplete checks if message delivery is complete
func (w *Watcher) checkMessageDeliveryComplete(ctx context.Context, job *model.JobM) bool {
	// TODO: Implement message-specific delivery completion logic
	return true // Placeholder
}

// GetActiveStateMachines 获取活跃状态机数量
func (w *Watcher) GetActiveStateMachines() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.stateMachines)
}

func init() {
	registry.Register(known.MessageBatchWatcher, &Watcher{})
}

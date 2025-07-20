package messagebatch

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	messagebatchbiz "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/onexstack/onexstack/pkg/store/where"
	"golang.org/x/time/rate"
)

// Watcher 消息批处理监控器
type Watcher struct {
	service       messagebatchbiz.MessageBatchBiz
	store         store.JobStore
	workerCount   int
	rateLimiter   *rate.Limiter
	ctx           context.Context
	cancel        context.CancelFunc
	stateMachines map[string]*StateMachine
	mu            sync.RWMutex
}

// NewWatcher 创建新的监控器
func NewWatcher(
	service messagebatchbiz.MessageBatchBiz,
	store store.JobStore,
	workerCount int,
	rateLimit rate.Limit,
) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		service:       service,
		store:         store,
		workerCount:   workerCount,
		rateLimiter:   rate.NewLimiter(rateLimit, int(rateLimit)),
		ctx:           ctx,
		cancel:        cancel,
		stateMachines: make(map[string]*StateMachine),
	}
}

// Run 启动监控器
func (w *Watcher) Run() error {
	workerPool := make(chan struct{}, w.workerCount)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-ticker.C:
			if err := w.processJobs(workerPool); err != nil {
				fmt.Printf("Error processing jobs: %v\n", err)
			}
		}
	}
}

// Stop 停止监控器
func (w *Watcher) Stop() {
	w.cancel()
	// 停止所有状态机
	w.mu.Lock()
	for _, fsm := range w.stateMachines {
		fsm.Stop()
	}
	w.stateMachines = make(map[string]*StateMachine)
	w.mu.Unlock()
}

// processJobs 处理任务
func (w *Watcher) processJobs(workerPool chan struct{}) error {
	// 查询可运行的任务
	whereClause := where.F()
	_, jobs, err := w.store.List(w.ctx, whereClause)
	if err != nil {
		return fmt.Errorf("failed to find runnable jobs: %w", err)
	}

	// 过滤可运行的任务
	runnableJobs := make([]*model.JobM, 0)
	for _, job := range jobs {
		if job.Status == "PENDING" || job.Status == "PROCESSING" || job.Status == "PARTIAL_COMPLETE" || job.Status == "RETRYING" {
			runnableJobs = append(runnableJobs, job)
		}
	}

	// 并发处理任务
	var wg sync.WaitGroup
	for _, job := range runnableJobs {
		select {
		case workerPool <- struct{}{}:
			wg.Add(1)
			go func(job *model.JobM) {
				defer func() {
					<-workerPool
					wg.Done()
				}()
				w.processJob(job)
			}(job)
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	wg.Wait()
	return nil
}

// processJob 处理单个任务
func (w *Watcher) processJob(job *model.JobM) {
	// 速率限制
	if err := w.rateLimiter.Wait(w.ctx); err != nil {
		return
	}

	// 获取或创建状态机
	fsm := w.getOrCreateStateMachine(job)
	if fsm == nil {
		return
	}

	// 根据当前状态处理任务
	switch job.Status {
	case "PENDING":
		w.handlePendingJob(fsm, job)
	case "PROCESSING":
		w.handleProcessingJob(fsm, job)
	case "PARTIAL_COMPLETE":
		w.handlePartialCompleteJob(fsm, job)
	case "RETRYING":
		w.handleRetryingJob(fsm, job)
	default:
		fmt.Printf("Unknown job status: %v for job %s\n", job.Status, job.ID)
	}
}

// getOrCreateStateMachine 获取或创建状态机
func (w *Watcher) getOrCreateStateMachine(job *model.JobM) *StateMachine {
	w.mu.Lock()
	defer w.mu.Unlock()

	if fsm, exists := w.stateMachines[strconv.FormatInt(job.ID, 10)]; exists {
		return fsm
	}

	// 创建新的状态机
	fsm := NewStateMachine(w, job)
	w.stateMachines[strconv.FormatInt(job.ID, 10)] = fsm
	return fsm
}

// removeStateMachine 移除状态机
func (w *Watcher) removeStateMachine(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.stateMachines, jobID)
}

// handlePendingJob 处理待处理任务
func (w *Watcher) handlePendingJob(fsm *StateMachine, job *model.JobM) {
	// 更新任务状态为处理中
	job.Status = "PROCESSING"
	job.StartedAt = time.Now()
	if err := w.store.Update(w.ctx, job); err != nil {
		fmt.Printf("Failed to update job status: %v\n", err)
		return
	}

	// 触发准备开始事件
	if err := fsm.TriggerEvent("PrepareStart"); err != nil {
		fmt.Printf("Failed to trigger PrepareStart event: %v\n", err)
	}
}

// handleProcessingJob 处理处理中任务
func (w *Watcher) handleProcessingJob(fsm *StateMachine, job *model.JobM) {
	// 检查当前状态并继续处理
	currentState := fsm.GetCurrentState()
	switch currentState {
	case "PreparationReady":
		if err := fsm.TriggerEvent("PrepareBegin"); err != nil {
			fmt.Printf("Failed to trigger PrepareBegin event: %v\n", err)
		}
	case "DeliveryReady":
		if err := fsm.TriggerEvent("DeliveryBegin"); err != nil {
			fmt.Printf("Failed to trigger DeliveryBegin event: %v\n", err)
		}
	case "Succeeded", "Failed", "Cancelled":
		// 任务已完成，移除状态机
		w.removeStateMachine(strconv.FormatInt(job.ID, 10))
	}
}

// handlePartialCompleteJob 处理部分完成任务
func (w *Watcher) handlePartialCompleteJob(fsm *StateMachine, job *model.JobM) {
	// 检查是否可以继续处理
	currentState := fsm.GetCurrentState()
	if currentState == "DeliveryReady" {
		if err := fsm.TriggerEvent("DeliveryBegin"); err != nil {
			fmt.Printf("Failed to trigger DeliveryBegin event: %v\n", err)
		}
	}
}

// handleRetryingJob 处理重试任务
func (w *Watcher) handleRetryingJob(fsm *StateMachine, job *model.JobM) {
	// 检查重试条件
	currentState := fsm.GetCurrentState()
	switch currentState {
	case "PreparationFailed":
		if err := fsm.TriggerEvent("PrepareRetry"); err != nil {
			fmt.Printf("Failed to trigger PrepareRetry event: %v\n", err)
		}
	case "DeliveryFailed":
		if err := fsm.TriggerEvent("DeliveryRetry"); err != nil {
			fmt.Printf("Failed to trigger DeliveryRetry event: %v\n", err)
		}
	}
}

// GetStateMachineStatus 获取状态机状态（用于监控）
func (w *Watcher) GetStateMachineStatus(jobID string) (string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if fsm, exists := w.stateMachines[jobID]; exists {
		return fsm.GetCurrentState(), true
	}
	return "", false
}

// GetActiveStateMachines 获取活跃状态机数量
func (w *Watcher) GetActiveStateMachines() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.stateMachines)
}

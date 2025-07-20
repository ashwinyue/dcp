package messagebatch

import (
	"context"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
	"golang.org/x/time/rate"
)

// ExampleMessageBatchProcessing 展示如何使用状态机进行消息批处理
func ExampleMessageBatchProcessing() {
	// 1. 初始化服务依赖
	ctx := context.Background()

	// 假设这些服务已经初始化
	var (
		messageBatchService *messagebatch.MessageBatchService
		messageBatchStore   model.MessageBatchJobStore
	)

	// 2. 创建监控器
	watcher := NewWatcher(
		messageBatchService,
		messageBatchStore,
		10,              // 工作协程数
		rate.Limit(100), // 每秒处理100个任务
	)

	// 3. 启动监控器（在实际应用中应该在goroutine中运行）
	go func() {
		if err := watcher.Run(); err != nil {
			fmt.Printf("Watcher error: %v\n", err)
		}
	}()

	// 4. 创建一个示例任务
	job := &model.MessageBatchJob{
		ID:          "example-job-001",
		BatchID:     "batch-001",
		Status:      v1.MessageBatchJobStatus_PENDING,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		ScheduledAt: time.Now(),
		// 其他字段...
	}

	// 5. 手动创建状态机进行演示
	fsm := NewStateMachine(ctx, job, messageBatchService, messageBatchStore)

	// 6. 演示状态转换流程
	fmt.Println("=== 统一状态机演示 ===")
	fmt.Printf("初始状态: %s\n", fsm.GetCurrentState())

	// 开始准备阶段
	if err := fsm.TriggerEvent(PrepareStart); err != nil {
		fmt.Printf("触发PrepareStart事件失败: %v\n", err)
		return
	}
	fmt.Printf("准备开始后状态: %s\n", fsm.GetCurrentState())

	// 开始准备执行
	if err := fsm.TriggerEvent(PrepareBegin); err != nil {
		fmt.Printf("触发PrepareBegin事件失败: %v\n", err)
		return
	}
	fmt.Printf("准备执行后状态: %s\n", fsm.GetCurrentState())

	// 等待准备阶段完成（在实际应用中由状态机内部处理）
	time.Sleep(2 * time.Second)

	// 模拟准备完成
	if err := fsm.TriggerEvent(PrepareComplete); err != nil {
		fmt.Printf("触发PrepareComplete事件失败: %v\n", err)
		return
	}
	fmt.Printf("准备完成后状态: %s\n", fsm.GetCurrentState())

	// 开始投递阶段
	if err := fsm.TriggerEvent(DeliveryBegin); err != nil {
		fmt.Printf("触发DeliveryBegin事件失败: %v\n", err)
		return
	}
	fmt.Printf("投递开始后状态: %s\n", fsm.GetCurrentState())

	// 等待投递阶段完成
	time.Sleep(3 * time.Second)

	// 模拟投递完成
	if err := fsm.TriggerEvent(DeliveryComplete); err != nil {
		fmt.Printf("触发DeliveryComplete事件失败: %v\n", err)
		return
	}
	fmt.Printf("最终状态: %s\n", fsm.GetCurrentState())

	// 7. 获取统计信息
	stats := fsm.GetStatistics()
	fmt.Printf("\n=== 统计信息 ===")
	fmt.Printf("准备阶段: 成功=%d, 失败=%d, 总数=%d\n",
		stats.PreparationPhase.SuccessCount,
		stats.PreparationPhase.FailureCount,
		stats.PreparationPhase.TotalCount)
	fmt.Printf("投递阶段: 成功=%d, 失败=%d, 总数=%d\n",
		stats.DeliveryPhase.SuccessCount,
		stats.DeliveryPhase.FailureCount,
		stats.DeliveryPhase.TotalCount)

	// 8. 停止监控器
	watcher.Stop()
	fmt.Println("\n监控器已停止")
}

// ExampleStateMachineTransitions 展示状态机的各种转换场景
func ExampleStateMachineTransitions() {
	ctx := context.Background()

	// 假设这些服务已经初始化
	var (
		messageBatchService *messagebatch.MessageBatchService
		messageBatchStore   model.MessageBatchJobStore
	)

	job := &model.MessageBatchJob{
		ID:      "transition-example",
		BatchID: "batch-002",
		Status:  v1.MessageBatchJobStatus_PENDING,
	}

	fsm := NewStateMachine(ctx, job, messageBatchService, messageBatchStore)

	fmt.Println("=== 状态转换场景演示 ===")

	// 场景1: 正常流程
	fmt.Println("\n场景1: 正常流程")
	demonstrataNormalFlow(fsm)

	// 场景2: 准备阶段失败和重试
	fmt.Println("\n场景2: 准备阶段失败和重试")
	fsm2 := NewStateMachine(ctx, &model.MessageBatchJob{
		ID:      "retry-example",
		BatchID: "batch-003",
		Status:  v1.MessageBatchJobStatus_PENDING,
	}, messageBatchService, messageBatchStore)
	demonstrateRetryFlow(fsm2)

	// 场景3: 暂停和恢复
	fmt.Println("\n场景3: 暂停和恢复")
	fsm3 := NewStateMachine(ctx, &model.MessageBatchJob{
		ID:      "pause-example",
		BatchID: "batch-004",
		Status:  v1.MessageBatchJobStatus_PENDING,
	}, messageBatchService, messageBatchStore)
	demonstratePauseResumeFlow(fsm3)
}

// demonstrataNormalFlow 演示正常流程
func demonstrataNormalFlow(fsm *StateMachine) {
	events := []Event{
		PrepareStart, PrepareBegin, PrepareComplete,
		DeliveryBegin, DeliveryComplete,
	}

	for _, event := range events {
		if err := fsm.TriggerEvent(event); err != nil {
			fmt.Printf("事件 %s 失败: %v\n", event, err)
			return
		}
		fmt.Printf("事件 %s -> 状态 %s\n", event, fsm.GetCurrentState())
		time.Sleep(500 * time.Millisecond)
	}
}

// demonstrateRetryFlow 演示重试流程
func demonstrateRetryFlow(fsm *StateMachine) {
	events := []Event{
		PrepareStart, PrepareBegin, PrepareFail, PrepareRetry,
		PrepareBegin, PrepareComplete, DeliveryBegin, DeliveryComplete,
	}

	for _, event := range events {
		if err := fsm.TriggerEvent(event); err != nil {
			fmt.Printf("事件 %s 失败: %v\n", event, err)
			return
		}
		fmt.Printf("事件 %s -> 状态 %s\n", event, fsm.GetCurrentState())
		time.Sleep(500 * time.Millisecond)
	}
}

// demonstratePauseResumeFlow 演示暂停恢复流程
func demonstratePauseResumeFlow(fsm *StateMachine) {
	events := []Event{
		PrepareStart, PrepareBegin, PreparePause,
		PrepareResume, PrepareComplete, DeliveryBegin,
		DeliveryPause, DeliveryResume, DeliveryComplete,
	}

	for _, event := range events {
		if err := fsm.TriggerEvent(event); err != nil {
			fmt.Printf("事件 %s 失败: %v\n", event, err)
			return
		}
		fmt.Printf("事件 %s -> 状态 %s\n", event, fsm.GetCurrentState())
		time.Sleep(500 * time.Millisecond)
	}
}

// ExampleMonitoring 展示监控功能
func ExampleMonitoring() {
	// 假设这些服务已经初始化
	var (
		messageBatchService *messagebatch.MessageBatchService
		messageBatchStore   model.MessageBatchJobStore
	)

	watcher := NewWatcher(
		messageBatchService,
		messageBatchStore,
		5,
		rate.Limit(50),
	)

	fmt.Println("=== 监控功能演示 ===")

	// 获取活跃状态机数量
	activeCount := watcher.GetActiveStateMachines()
	fmt.Printf("当前活跃状态机数量: %d\n", activeCount)

	// 获取特定任务的状态
	if state, exists := watcher.GetStateMachineStatus("example-job-001"); exists {
		fmt.Printf("任务 example-job-001 当前状态: %s\n", state)
	} else {
		fmt.Println("任务 example-job-001 不存在")
	}
}

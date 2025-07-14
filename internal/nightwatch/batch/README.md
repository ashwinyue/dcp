# Batch Processing System with onex Pump Pattern

这是一个基于 onex pump 模式重构的批处理系统，采用 Source-Flow-Sink 架构实现高效的流式数据处理。

## 架构概述

### onex Pump 模式

本系统采用 onex pump 模式，这是一种现代化的流式处理架构，具有以下特点：

- **Source-Flow-Sink 架构**：数据从 Source 流向 Sink，中间通过 Flow 进行处理
- **响应式流处理**：支持背压控制和异步处理
- **可组合性**：Flow 可以链式组合，构建复杂的处理管道
- **并发控制**：内置并发控制和资源管理

### 核心组件

1. **Source（数据源）**
   - `TaskSource`：生成批处理任务的数据源
   - 实现 `streams.Source` 接口
   - 支持 `Via()` 方法连接 Flow

2. **Flow（处理流）**
   - `ValidationFlow`：任务验证流
   - `TransformFlow`：数据转换流
   - 实现 `streams.Flow` 接口
   - 支持 `Via()` 和 `To()` 方法

3. **Sink（数据汇）**
   - `ProcessorSink`：任务处理汇
   - 实现 `streams.Sink` 接口
   - 负责最终的数据处理和结果输出

4. **BatchProcessor（批处理器）**
   - 管道编排器，连接 Source、Flow 和 Sink
   - 支持链式配置：`WithSource().WithFlow().WithSink()`

## 文件结构

```
batch/
├── models.go          # 数据模型和接口定义
├── stream.go          # onex pump 核心组件实现
├── task_manager.go    # 任务管理器
├── utils.go           # 工具函数
├── example.go         # 使用示例
├── batch_test.go      # 单元测试
└── README.md          # 说明文档
```

## 快速开始

### 1. 基本使用

```go
package main

import (
    "context"
    "time"
    "github.com/ashwinyue/dcp/internal/nightwatch/batch"
)

func main() {
    ctx := context.Background()
    logger := &batch.SimpleLogger{}
    
    // 创建任务管理器
    taskManager := batch.NewTaskManager(ctx, logger)
    defer taskManager.Shutdown()
    
    // 创建批处理配置
    config := &batch.BatchConfig{
        BatchSize:   10,
        MaxRetries:  3,
        Timeout:     30 * time.Second,
        Concurrency: 2,
    }
    
    // 创建训练任务
    resp, err := batch.CreateTrainTask(taskManager, config)
    if err != nil {
        panic(err)
    }
    
    // 开始处理任务
    err = taskManager.ProcessTask(resp.TaskID)
    if err != nil {
        panic(err)
    }
    
    // 监控任务状态
    for {
        status, err := taskManager.GetTaskStatus(resp.TaskID)
        if err != nil {
            break
        }
        
        fmt.Printf("Task status: %s, progress: %d%%\n", 
            status.Status, status.Progress.Percentage)
        
        if batch.IsTerminalStatus(status.Status) {
            break
        }
        
        time.Sleep(1 * time.Second)
    }
}
```

### 2. 自定义处理器

```go
// 创建自定义批处理器
processor := batch.NewBatchProcessor(ctx, config, logger)

// 创建数据源
source := batch.NewTaskSource(ctx, tasks)

// 创建验证流
validationFlow := batch.NewValidationFlow(func(ctx context.Context, item *batch.BatchItem[*batch.BatchTask]) error {
    // 自定义验证逻辑
    return nil
}, logger)

// 创建处理汇
processorSink := batch.NewProcessorSink(ctx, func(ctx context.Context, item *batch.BatchItem[*batch.BatchTask]) (*batch.BatchResult[any], error) {
    // 自定义处理逻辑
    return &batch.BatchResult[any]{
        ID:          item.ID,
        Data:        processedData,
        ProcessedAt: time.Now(),
        Success:     true,
    }, nil
}, logger)

// 构建处理管道：source -> validation -> sink
processor.
    WithSource(source).
    WithFlow(validationFlow).
    WithSink(processorSink)

// 启动处理
err := processor.Run()
```

## 支持的任务类型

### 1. 训练任务 (TaskTypeTrain)
- 机器学习模型训练
- 支持批量数据处理
- 可配置训练参数

### 2. 有赞订单任务 (TaskTypeYouZanOrder)
- 订单数据处理
- 支持数据丰富化
- 包含地区和产品分类提取

### 3. 批处理任务 (TaskTypeBatchProcess)
- 通用批处理
- 支持数据过滤
- 可配置处理逻辑

## 配置选项

```go
type BatchConfig struct {
    BatchSize   int           // 批处理大小
    MaxRetries  int           // 最大重试次数
    Timeout     time.Duration // 超时时间
    Concurrency int           // 并发数
}
```

## API 接口

### 任务管理

- `CreateTask(req *CreateTaskRequest) (*CreateTaskResponse, error)`
- `ProcessTask(taskID string) error`
- `GetTask(taskID string) (*GetTaskResponse, error)`
- `GetTaskStatus(taskID string) (*GetTaskStatusResponse, error)`
- `UpdateTaskStatus(req *UpdateTaskStatusRequest) (*UpdateTaskStatusResponse, error)`
- `ListTasks(req *ListTasksRequest) (*ListTasksResponse, error)`
- `StopTask(taskID string) error`

### 便捷函数

- `CreateTrainTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error)`
- `CreateYouZanOrderTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error)`
- `CreateBatchProcessTask(tm *TaskManager, config *BatchConfig) (*CreateTaskResponse, error)`

## 监控和指标

### 任务状态
- `TaskStatusPending`：等待处理
- `TaskStatusProcessing`：正在处理
- `TaskStatusCompleted`：处理完成
- `TaskStatusFailed`：处理失败
- `TaskStatusCancelled`：已取消

### 处理指标
```go
type ProcessingMetrics struct {
    TotalItems     int
    ProcessedItems int
    FailedItems    int
    StartTime      time.Time
}
```

### 批处理指标
```go
type BatchMetrics struct {
    TotalTasks      int
    PendingTasks    int
    ProcessingTasks int
    CompletedTasks  int
    FailedTasks     int
    CancelledTasks  int
    SuccessRate     float64
    AvgDuration     time.Duration
    TotalDuration   time.Duration
}
```

## 错误处理

系统提供完善的错误处理机制：

```go
type TaskError struct {
    Code      string    // 错误代码
    Message   string    // 错误消息
    Timestamp time.Time // 错误时间
}
```

常见错误代码：
- `VALIDATION_ERROR`：验证失败
- `PROCESSING_ERROR`：处理失败
- `TIMEOUT_ERROR`：超时错误
- `CONFIGURATION_ERROR`：配置错误

## 最佳实践

### 1. 配置优化
- 根据数据量调整 `BatchSize`
- 根据系统资源设置 `Concurrency`
- 设置合理的 `Timeout` 避免长时间阻塞

### 2. 错误处理
- 实现自定义验证逻辑
- 设置适当的重试次数
- 监控任务状态和错误日志

### 3. 性能优化
- 使用流式处理减少内存占用
- 合理设置并发数避免资源竞争
- 监控处理指标进行性能调优

### 4. 资源管理
- 及时关闭不需要的处理器
- 使用 context 进行生命周期管理
- 实现优雅关闭机制

## 测试

运行单元测试：

```bash
go test ./internal/nightwatch/batch/
```

运行示例：

```bash
go run ./internal/nightwatch/batch/example.go
```

## 与传统管道的对比

| 特性 | 传统管道 | onex Stream |
|------|----------|-------------|
| 架构 | 函数调用链 | Source-Flow-Sink |
| 数据流 | 推送式 | 拉取式 + 背压控制 |
| 并发控制 | 管道级别 | Flow 组件内部 |
| 可组合性 | 有限 | 高度可组合 |
| 错误处理 | 手动传播 | 内置错误处理 |
| 资源管理 | 手动管理 | 自动管理 |
| 监控能力 | 基础 | 丰富的指标 |

## 总结

基于 onex pump 模式的批处理系统提供了：

1. **现代化架构**：采用响应式流处理模式
2. **高性能**：支持并发处理和背压控制
3. **易扩展**：组件化设计，易于扩展新功能
4. **易维护**：清晰的接口定义和错误处理
5. **易监控**：丰富的指标和状态管理

这使得系统更适合处理大规模、高并发的批处理任务，同时保持了良好的可维护性和扩展性。
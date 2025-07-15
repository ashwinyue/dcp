# Batch Job System

这个 batch job 系统基于 onex 项目的设计模式实现，专注于数据层转换的有限状态机处理和异步任务管理。

## 文件结构

与 llmtrain 包保持一致的简洁命名：

- `watcher.go` - 数据层转换任务监控器，支持异步任务创建
- `fsm.go` - 数据层转换的有限状态机定义
- `event.go` - FSM 事件处理逻辑和数据层组件
- `helper.go` - 辅助函数（超时检查、幂等性等）
- `README.md` - 系统文档

## 系统架构

### 1. 核心组件

- **Watcher**: 监控和调度数据层转换任务，使用异步 CreateTask 模式
- **StateMachine**: 数据层转换的有限状态机
- **DataLayerProcessor**: 数据层转换处理器
- **DataLayerSource/Sink**: 数据层的读写连接器
- **BatchManager**: 批处理任务管理器（位于 `internal/pkg/client/batch/`）

### 2. 数据层转换流程

系统支持标准的数据仓库分层架构：

```
Landing → ODS → DWD → DWS → DS
```

- **Landing**: 原始数据层
- **ODS**: 操作数据存储层 (Operational Data Store)
- **DWD**: 数据仓库明细层 (Data Warehouse Detail)
- **DWS**: 数据仓库汇总层 (Data Warehouse Summary)
- **DS**: 数据服务层 (Data Service)

### 3. 有限状态机

数据层转换使用有限状态机管理：

```
Pending → LandingToODS → ODSToDWD → DWDToDWS → DWSToDS → Completed → Succeeded
```

## 异步处理机制

### 为什么使用异步处理？

类似于 LLMTrain 的 Train 方法，数据层转换任务是耗时的操作：
- 涉及多个数据层的转换（Landing → ODS → DWD → DWS → DS）
- 每个转换阶段都可能很耗时
- 为避免 worker 被长时间占用，采用异步 CreateTask 模式

### 异步处理流程

1. **任务创建**: 使用 `createDataLayerTaskFunc` 创建异步任务
2. **状态轮询**: 通过 `GetTaskStatus` 检查任务状态
3. **非阻塞处理**: Worker 不会被长时间占用，可以处理其他任务
4. **状态管理**: 任务状态保存在 `JobResults.Batch` 中

```go
// 异步任务创建示例
createDataLayerTaskFunc := func() error {
    taskID, err := w.BatchManager.CreateTask(ctx, job.JobID, params)
    if err != nil {
        return err
    }
    results.TaskID = &taskID
    return nil
}

// 状态检查
task, err := w.BatchManager.GetTaskStatus(ctx, *results.TaskID)
if task.Status != batchclient.BatchTaskStatusCompleted {
    // 任务未完成，等待下次轮询
    return nil
}
```

## 使用方式

### 数据层转换任务

系统会自动识别数据层转换任务并使用异步处理：

```go
// Watcher 会自动处理
// 1. 检查任务状态
// 2. 创建异步任务（如果尚未创建）
// 3. 轮询任务状态
// 4. 更新作业状态
```

## 配置选项

```go
// 数据层处理配置
params := map[string]interface{}{
    "batch_size": known.DataLayerBatchSize,
    "timeout":    known.DataLayerProcessTimeout,
    "retries":    3,
    "concurrent": known.DataLayerMaxWorkers,
    "total":      int64(1000),
}
```

## 监控和统计

系统提供详细的处理统计信息：

- 任务 ID 和状态
- 处理进度（百分比）
- 当前转换阶段
- 结果输出路径
- 错误信息（如果有）

## 与 LLMTrain 的一致性

- **异步处理**: 都使用 CreateTask 避免 worker 被占用
- **状态轮询**: 都使用 GetTaskStatus 检查任务状态
- **文件结构**: 都使用 watcher.go、fsm.go、event.go、helper.go 的简洁命名
- **错误处理**: 都支持超时检查和幂等性处理

## 扩展性

系统设计为可扩展的：

1. **自定义数据源**: 扩展 DataLayerSource
2. **自定义处理器**: 扩展 DataLayerProcessor
3. **自定义转换逻辑**: 修改各层的转换函数
4. **自定义状态机**: 扩展 StateMachine 的状态和事件

## 错误处理

- 支持幂等性检查
- 自动重试机制
- 超时处理
- 状态恢复
- 异步任务失败处理

## 性能优化

- 异步处理避免 worker 阻塞
- 并发处理支持
- 缓冲区优化
- 限流控制
- 资源管理 
# Batch Job System with Pump Mode

这个 batch job 系统基于 onex 项目的 pump 模式实现，支持数据层转换的有限状态机处理。

## 系统架构

### 1. 核心组件

- **BatchJob**: 使用 pump 模式的简单批处理任务
- **DataLayerProcessor**: 数据层转换处理器，包含有限状态机
- **DataLayerSource/Sink**: 数据层的读写连接器
- **Watcher**: 监控和调度批处理任务

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

## 使用方式

### 1. 简单批处理任务

```go
// 创建批处理任务
config := DefaultBatchConfig()
batchJob := NewBatchJob(ctx, job, config)

// 运行任务
err := batchJob.Run()
```

### 2. 数据层转换任务

```go
// 创建数据层处理器
processor := NewDataLayerProcessor(ctx, job, config)

// 执行数据层转换
err := processor.ProcessDataLayers()
```

### 3. 通过 Watcher 自动处理

系统会自动识别任务类型并选择合适的处理方式：

- 简单批处理任务：使用 pump 模式的 source → processor → sink
- 数据层转换任务：使用有限状态机控制的多阶段处理

## 配置选项

```go
type BatchConfig struct {
    BatchSize      int           // 批处理大小
    Concurrency    int           // 并发数
    BufferSize     int           // 缓冲区大小
    Timeout        time.Duration // 超时时间
    RetryAttempts  int           // 重试次数
    RetryDelay     time.Duration // 重试延迟
}
```

## 监控和统计

系统提供详细的处理统计信息：

- 处理项目总数
- 成功处理数量
- 失败处理数量
- 处理时间统计

## 扩展性

系统设计为可扩展的：

1. **自定义数据源**: 实现 `streams.Source` 接口
2. **自定义处理器**: 实现 `streams.Flow` 接口
3. **自定义数据输出**: 实现 `streams.Sink` 接口
4. **自定义转换逻辑**: 修改各层的转换函数

## 错误处理

- 支持幂等性检查
- 自动重试机制
- 超时处理
- 状态恢复

## 性能优化

- 并发处理
- 缓冲区优化
- 限流控制
- 资源管理 
# Batch Job System

这个 batch job 系统基于 onex 项目的设计模式实现，专注于数据层转换的有限状态机处理和异步任务管理。

## 文件结构

与 llmtrain 包保持一致的简洁命名：

- `watcher.go` - 数据层转换任务监控器，支持异步任务创建和恢复机制
- `fsm.go` - 数据层转换的有限状态机定义
- `event.go` - FSM 事件处理逻辑和数据层组件，支持断点续传
- `helper.go` - 辅助函数（超时检查、幂等性、恢复机制等）
- `README.md` - 系统文档

## 系统架构

### 1. 核心组件

- **Watcher**: 监控和调度数据层转换任务，使用异步 CreateTask 模式，支持任务恢复
- **StateMachine**: 数据层转换的有限状态机
- **DataLayerProcessor**: 数据层转换处理器，支持从任意状态恢复
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

## 恢复机制

### 核心原理

你的理解是完全正确的！系统通过**状态标记**实现恢复：

1. **数据库持久化**: 所有任务状态保存在 `nw_job` 表中
2. **状态查询**: 重启后查询所有未完成的任务
3. **断点续传**: 直接从当前状态继续处理

### 工作流程

```
1. 系统重启
2. Watcher启动（每10秒运行）
3. 查询未完成任务（Pending → Completed之间的所有状态）
4. 检查是否超时中断
5. 直接从当前状态继续处理
```

### 恢复逻辑

系统会根据任务的当前状态直接恢复处理：

```go
switch job.Status {
case DataLayerPending:      // 从头开始
case DataLayerLandingToODS: // 继续Landing→ODS
case DataLayerODSToDWD:     // 继续ODS→DWD  
case DataLayerDWDToDWS:     // 继续DWD→DWS
case DataLayerDWSToDS:      // 继续DWS→DS
case DataLayerCompleted:    // 完成处理
}
```

### 中断检测

- **超时检测**: 检查任务是否超过配置的超时时间
- **状态一致性**: 确保任务状态与实际处理进度一致

### 使用示例

```go
// 1. 有一批数据需要处理，状态标记为 Pending
// 2. 系统开始处理，状态变为 LandingToODS
// 3. 系统崩溃/重启
// 4. 重启后，查询到状态为 LandingToODS 的任务
// 5. 直接从 LandingToODS 状态继续处理
// 6. 完成后状态变为 ODSToDWD，以此类推
```

### 优势

- **简单可靠**: 基于数据库状态，无需复杂的检查点机制
- **自动恢复**: 重启后自动继续处理
- **幂等性**: 重复执行不会产生副作用
- **透明化**: 对业务逻辑透明，无需特殊处理 
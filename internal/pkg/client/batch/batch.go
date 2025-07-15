package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/onexstack/onexstack/pkg/store/where"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/client/minio"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
	"github.com/ashwinyue/dcp/pkg/streams"
)

// BatchTaskStatus 表示批处理任务的状态
type BatchTaskStatus string

const (
	BatchTaskStatusCreated    BatchTaskStatus = "created"
	BatchTaskStatusProcessing BatchTaskStatus = "processing"
	BatchTaskStatusCompleted  BatchTaskStatus = "completed"
	BatchTaskStatusFailed     BatchTaskStatus = "failed"
)

// BatchTask 表示批处理任务
type BatchTask struct {
	ID       string                 `json:"id"`
	Status   BatchTaskStatus        `json:"status"`
	Progress float32                `json:"progress"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Error    string                 `json:"error,omitempty"`
	Created  time.Time              `json:"created"`
	Updated  time.Time              `json:"updated"`
}

// BatchManager 管理批处理任务
type BatchManager struct {
	store store.IStore
	minio minio.IMinio
}

// NewBatchManager 创建新的批处理管理器
func NewBatchManager(store store.IStore, minio minio.IMinio) *BatchManager {
	return &BatchManager{
		store: store,
		minio: minio,
	}
}

// CreateTask 创建异步批处理任务
func (bm *BatchManager) CreateTask(ctx context.Context, jobID string, params map[string]interface{}) (string, error) {
	// 创建任务ID
	taskID := fmt.Sprintf("batch_%s_%d", jobID, time.Now().Unix())

	// 启动异步处理
	go func() {
		if err := bm.ProcessBatch(ctx, jobID, params); err != nil {
			log.Errorw("Failed to process batch in background", "taskID", taskID, "error", err)
			// 更新任务状态为失败
			bm.updateJobStatus(ctx, jobID, taskID, BatchTaskStatusFailed, 0, nil, err.Error())
		}
	}()

	return taskID, nil
}

// ProcessBatch 直接使用 pump 模式处理批处理任务
func (bm *BatchManager) ProcessBatch(ctx context.Context, jobID string, params map[string]interface{}) error {
	// 从参数中获取配置
	total := int64(1000)
	batchSize := int64(100)

	if t, ok := params["total"].(int64); ok {
		total = t
	}
	if bs, ok := params["batch_size"].(int64); ok {
		batchSize = bs
	}

	// 创建任务ID
	taskID := fmt.Sprintf("batch_%s_%d", jobID, time.Now().Unix())

	// 更新任务状态为处理中
	if err := bm.updateJobStatus(ctx, jobID, taskID, BatchTaskStatusProcessing, 0, nil, ""); err != nil {
		log.Errorw("Failed to update task status", "taskID", taskID, "error", err)
		return err
	}

	// 创建 pump 模式的组件
	source := NewBatchSource(taskID, total, batchSize)
	processor := NewBatchProcessor(taskID, bm)
	sink := NewBatchSink(taskID, bm, total, jobID)

	// 启动 sink
	sink.Start(ctx)

	// 构建数据流管道：Source -> Processor -> Sink
	// 连接 source 到 processor
	go func() {
		source.generate()
		for item := range source.Out() {
			processor.In() <- item
		}
		close(processor.In())
	}()

	// 连接 processor 到 sink
	processor.To(sink)

	log.Infow("Batch processing pipeline started", "taskID", taskID, "jobID", jobID, "total", total, "batchSize", batchSize)
	return nil
}

// GetTaskStatus 获取任务状态
func (bm *BatchManager) GetTaskStatus(ctx context.Context, taskID string) (*BatchTask, error) {
	whereOpts := where.F("job_id", taskID)
	job, err := bm.store.Job().Get(ctx, whereOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var task BatchTask
	if job.Results != nil && job.Results.Batch != nil {
		progress := float32(0)
		if job.Results.Batch.Percent != nil {
			progress = *job.Results.Batch.Percent
		}
		task = BatchTask{
			ID:       *job.Results.Batch.TaskID,
			Status:   BatchTaskStatus(job.Status),
			Progress: progress,
			Created:  job.CreatedAt,
			Updated:  job.UpdatedAt,
		}
	} else {
		task = BatchTask{
			ID:       taskID,
			Status:   BatchTaskStatus(job.Status),
			Progress: 0,
			Created:  job.CreatedAt,
			Updated:  job.UpdatedAt,
		}
	}

	return &task, nil
}

// BatchItem 表示批处理的数据项
type BatchItem struct {
	ID        int64                  `json:"id"`
	Data      map[string]interface{} `json:"data"`
	Timestamp int64                  `json:"timestamp"`
}

// BatchSource 实现 streams.Source 接口，用于生成批处理数据
type BatchSource struct {
	total     int64
	current   int64
	mu        sync.Mutex
	taskID    string
	batchSize int64
	out       chan interface{}
}

// NewBatchSource 创建新的批处理数据源
func NewBatchSource(taskID string, total, batchSize int64) *BatchSource {
	return &BatchSource{
		total:     total,
		current:   0,
		taskID:    taskID,
		batchSize: batchSize,
		out:       make(chan interface{}, batchSize),
	}
}

// Out 实现 streams.Outlet 接口
func (bs *BatchSource) Out() <-chan interface{} {
	return bs.out
}

// Via 实现 streams.Source 接口
func (bs *BatchSource) Via(flow streams.Flow) streams.Flow {
	go bs.generate()
	return flow
}

// generate 生成批处理数据
func (bs *BatchSource) generate() {
	defer close(bs.out)

	for bs.current < bs.total {
		bs.mu.Lock()
		if bs.current >= bs.total {
			bs.mu.Unlock()
			break
		}

		item := &BatchItem{
			ID: bs.current,
			Data: map[string]interface{}{
				"value":     fmt.Sprintf("item_%d", bs.current),
				"task_id":   bs.taskID,
				"batch_num": bs.current / bs.batchSize,
			},
			Timestamp: time.Now().Unix(),
		}

		bs.current++
		bs.mu.Unlock()

		bs.out <- item
	}
}

// BatchProcessor 实现 streams.Flow 接口，用于处理批处理数据
type BatchProcessor struct {
	taskID string
	bm     *BatchManager
	in     chan interface{}
	out    chan interface{}
}

// NewBatchProcessor 创建新的批处理处理器
func NewBatchProcessor(taskID string, bm *BatchManager) *BatchProcessor {
	return &BatchProcessor{
		taskID: taskID,
		bm:     bm,
		in:     make(chan interface{}, 100),
		out:    make(chan interface{}, 100),
	}
}

// In 实现 streams.Inlet 接口
func (bp *BatchProcessor) In() chan<- interface{} {
	return bp.in
}

// Out 实现 streams.Outlet 接口
func (bp *BatchProcessor) Out() <-chan interface{} {
	return bp.out
}

// Via 实现 streams.Flow 接口
func (bp *BatchProcessor) Via(flow streams.Flow) streams.Flow {
	go bp.process()
	return flow
}

// To 实现 streams.Flow 接口
func (bp *BatchProcessor) To(sink streams.Sink) {
	go bp.process()

	// 将输出传递给 sink
	go func() {
		for item := range bp.out {
			sink.In() <- item
		}
		close(sink.In())
	}()
}

// process 处理数据
func (bp *BatchProcessor) process() {
	defer close(bp.out)

	for item := range bp.in {
		if batchItem, ok := item.(*BatchItem); ok {
			// 模拟数据处理
			time.Sleep(10 * time.Millisecond)

			// 处理数据
			processedItem := &BatchItem{
				ID: batchItem.ID,
				Data: map[string]interface{}{
					"original":     batchItem.Data,
					"processed_at": time.Now().Unix(),
					"status":       "processed",
				},
				Timestamp: time.Now().Unix(),
			}

			bp.out <- processedItem
		}
	}
}

// BatchSink 实现 streams.Sink 接口，用于收集批处理结果
type BatchSink struct {
	in        chan interface{}
	taskID    string
	bm        *BatchManager
	results   []interface{}
	mu        sync.Mutex
	total     int64
	processed int64
	ctx       context.Context
	jobID     string
}

// NewBatchSink 创建新的批处理结果收集器
func NewBatchSink(taskID string, bm *BatchManager, total int64, jobID string) *BatchSink {
	return &BatchSink{
		in:        make(chan interface{}, 100),
		taskID:    taskID,
		bm:        bm,
		results:   make([]interface{}, 0),
		total:     total,
		processed: 0,
		jobID:     jobID,
	}
}

// In 实现 streams.Sink 接口
func (bs *BatchSink) In() chan<- interface{} {
	return bs.in
}

// Start 启动 sink 处理
func (bs *BatchSink) Start(ctx context.Context) {
	bs.ctx = ctx
	go bs.consume()
}

// consume 消费数据
func (bs *BatchSink) consume() {
	for item := range bs.in {
		bs.mu.Lock()
		bs.results = append(bs.results, item)
		bs.processed++

		progress := float32(bs.processed) / float32(bs.total) * 100

		// 更新任务状态
		result := map[string]interface{}{
			"total":     bs.total,
			"processed": bs.processed,
			"progress":  progress,
		}

		bs.bm.updateJobStatus(bs.ctx, bs.jobID, bs.taskID, BatchTaskStatusProcessing, progress, result, "")
		bs.mu.Unlock()

		// 每处理100个项目记录一次日志
		if bs.processed%100 == 0 {
			log.Infow("Batch processing progress", "taskID", bs.taskID, "processed", bs.processed, "total", bs.total, "progress", progress)
		}
	}

	// 处理完成
	bs.mu.Lock()
	result := map[string]interface{}{
		"total":       bs.total,
		"processed":   bs.processed,
		"progress":    100.0,
		"status":      "completed",
		"output_path": fmt.Sprintf("batch-results/%s/output.json", bs.taskID),
	}

	// 将结果写入 MinIO
	resultLines := make([]string, len(bs.results))
	for i, item := range bs.results {
		data, _ := json.Marshal(item)
		resultLines[i] = string(data)
	}

	if err := bs.bm.minio.Write(bs.ctx, result["output_path"].(string), resultLines); err != nil {
		log.Errorw("Failed to write result to MinIO", "taskID", bs.taskID, "error", err)
		bs.bm.updateJobStatus(bs.ctx, bs.jobID, bs.taskID, BatchTaskStatusFailed, 100.0, nil, err.Error())
	} else {
		bs.bm.updateJobStatus(bs.ctx, bs.jobID, bs.taskID, BatchTaskStatusCompleted, 100.0, result, "")
	}
	bs.mu.Unlock()

	log.Infow("Batch processing completed", "taskID", bs.taskID, "total", bs.total, "processed", bs.processed)
}

// updateJobStatus 更新作业状态
func (bm *BatchManager) updateJobStatus(ctx context.Context, jobID, taskID string, status BatchTaskStatus, progress float32, result map[string]interface{}, errorMsg string) error {
	whereOpts := where.F("job_id", jobID)
	job, err := bm.store.Job().Get(ctx, whereOpts)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// 更新任务状态
	job.Status = string(status)
	job.UpdatedAt = time.Now()

	// 更新批处理结果
	if job.Results == nil {
		job.Results = &model.JobResults{}
	}
	if job.Results.Batch == nil {
		job.Results.Batch = &v1.BatchResults{}
	}

	job.Results.Batch.TaskID = &taskID
	job.Results.Batch.Percent = &progress

	if result != nil {
		if total, ok := result["total"].(int64); ok {
			job.Results.Batch.Total = &total
		}
		if processed, ok := result["processed"].(int64); ok {
			job.Results.Batch.Processed = &processed
		}
		if outputPath, ok := result["output_path"].(string); ok {
			job.Results.Batch.ResultPath = &outputPath
		}
	}

	return bm.store.Job().Update(ctx, job)
}

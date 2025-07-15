package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/onexstack/onexstack/pkg/store/where"
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/client/minio"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
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

// DataLayerProcessor 接口定义数据层处理器的行为
type DataLayerProcessor interface {
	Process() error
	GetJob() *model.JobM
	Stop()
}

// DataLayerProcessorFactory 工厂函数类型，用于创建DataLayerProcessor
type DataLayerProcessorFactory func(ctx context.Context, job *model.JobM, db *gorm.DB) DataLayerProcessor

// BatchManager 管理批处理任务
type BatchManager struct {
	store                     store.IStore
	minio                     minio.IMinio
	db                        *gorm.DB
	dataLayerProcessorFactory DataLayerProcessorFactory
}

// NewBatchManager 创建带数据库连接的批处理管理器
func NewBatchManager(store store.IStore, minio minio.IMinio, db *gorm.DB) *BatchManager {
	return &BatchManager{
		store: store,
		minio: minio,
		db:    db,
	}
}

// SetDataLayerProcessorFactory 设置数据层处理器工厂函数
func (bm *BatchManager) SetDataLayerProcessorFactory(factory DataLayerProcessorFactory) {
	bm.dataLayerProcessorFactory = factory
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

// ProcessBatch 处理批处理任务，专注于数据层转换
func (bm *BatchManager) ProcessBatch(ctx context.Context, jobID string, params map[string]interface{}) error {
	// 创建任务ID
	taskID := fmt.Sprintf("batch_%s_%d", jobID, time.Now().Unix())

	// 更新任务状态为处理中
	if err := bm.updateJobStatus(ctx, jobID, taskID, BatchTaskStatusProcessing, 0, nil, ""); err != nil {
		log.Errorw("Failed to update task status", "taskID", taskID, "error", err)
		return err
	}

	// 处理数据层转换任务
	return bm.processDataLayerTask(ctx, jobID, taskID, params)
}

// processDataLayerTask 处理数据层转换任务，使用FSM
func (bm *BatchManager) processDataLayerTask(ctx context.Context, jobID, taskID string, params map[string]interface{}) error {
	log.Infow("Processing data layer task with FSM", "taskID", taskID, "jobID", jobID)

	// 获取作业信息
	whereOpts := where.F("job_id", jobID)
	job, err := bm.store.Job().Get(ctx, whereOpts)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// 必须有数据层处理器工厂才能处理
	if bm.dataLayerProcessorFactory == nil {
		return fmt.Errorf("data layer processor factory not set")
	}

	// 创建数据层处理器
	processor := bm.dataLayerProcessorFactory(ctx, job, bm.db)
	defer processor.Stop()

	log.Infow("Created DataLayerProcessor, starting FSM processing", "taskID", taskID, "job_id", job.JobID)

	// 启动FSM处理
	if err := processor.Process(); err != nil {
		log.Errorw("FSM processing failed", "taskID", taskID, "error", err)
		return bm.updateJobStatus(ctx, job.JobID, taskID, BatchTaskStatusFailed, 0, nil, err.Error())
	}

	// 完成处理
	result := map[string]interface{}{
		"status":      "completed",
		"method":      "FSM",
		"output_path": fmt.Sprintf("data-layer-results/%s/output.json", taskID),
	}

	return bm.updateJobStatus(ctx, job.JobID, taskID, BatchTaskStatusCompleted, 100.0, result, "")
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

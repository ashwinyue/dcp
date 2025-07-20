package messagebatch

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ashwinyue/dcp/internal/pkg/log"
)

// MongoDBAdapter MongoDB适配器，用于替代Java项目中的Table Storage
type MongoDBAdapter struct {
	db     *mongo.Database
	logger log.Logger
}

// NewMongoDBAdapter 创建MongoDB适配器
func NewMongoDBAdapter(db *mongo.Database, logger log.Logger) *MongoDBAdapter {
	return &MongoDBAdapter{
		db:     db,
		logger: logger,
	}
}

// SmsParam SMS参数实体，对应Java项目中的SmsParam
type SmsParam struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	PartitionKey string            `bson:"partitionKey" json:"partitionKey"`
	RowKey      string             `bson:"rowKey" json:"rowKey"`
	JobID       string             `bson:"jobId" json:"jobId"`
	BatchID     string             `bson:"batchId" json:"batchId"`
	PhoneNumber string             `bson:"phoneNumber" json:"phoneNumber"`
	Message     string             `bson:"message" json:"message"`
	Template    string             `bson:"template" json:"template"`
	Status      string             `bson:"status" json:"status"`
	RetryCount  int32              `bson:"retryCount" json:"retryCount"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt" json:"updatedAt"`
	ErrorMsg    string             `bson:"errorMsg,omitempty" json:"errorMsg,omitempty"`
}

// TsReport 报告实体，对应Java项目中的TsReport
type TsReport struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	PartitionKey string             `bson:"partitionKey" json:"partitionKey"`
	RowKey       string             `bson:"rowKey" json:"rowKey"`
	JobID        string             `bson:"jobId" json:"jobId"`
	BatchID      string             `bson:"batchId" json:"batchId"`
	PhoneNumber  string             `bson:"phoneNumber" json:"phoneNumber"`
	EventType    string             `bson:"eventType" json:"eventType"`
	Status       string             `bson:"status" json:"status"`
	Timestamp    time.Time          `bson:"timestamp" json:"timestamp"`
	DeliveryTime time.Time          `bson:"deliveryTime,omitempty" json:"deliveryTime,omitempty"`
	ErrorCode    string             `bson:"errorCode,omitempty" json:"errorCode,omitempty"`
	ErrorMsg     string             `bson:"errorMsg,omitempty" json:"errorMsg,omitempty"`
	CreatedAt    time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// BatchJobStatus 批处理作业状态实体
type BatchJobStatus struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	JobID        string             `bson:"jobId" json:"jobId"`
	BatchID      string             `bson:"batchId" json:"batchId"`
	Phase        string             `bson:"phase" json:"phase"`
	Status       string             `bson:"status" json:"status"`
	Progress     float32            `bson:"progress" json:"progress"`
	TotalCount   int64              `bson:"totalCount" json:"totalCount"`
	ProcessedCount int64            `bson:"processedCount" json:"processedCount"`
	SuccessCount int64              `bson:"successCount" json:"successCount"`
	FailedCount  int64              `bson:"failedCount" json:"failedCount"`
	RetryCount   int32              `bson:"retryCount" json:"retryCount"`
	StartTime    time.Time          `bson:"startTime" json:"startTime"`
	EndTime      time.Time          `bson:"endTime,omitempty" json:"endTime,omitempty"`
	ErrorMsg     string             `bson:"errorMsg,omitempty" json:"errorMsg,omitempty"`
	CreatedAt    time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// SaveSmsParams 批量保存SMS参数，对应Java项目中的TableStorageTemplate.saveBatch
func (adapter *MongoDBAdapter) SaveSmsParams(ctx context.Context, params []*SmsParam) error {
	if len(params) == 0 {
		return nil
	}

	collection := adapter.db.Collection("sms_params")
	
	// 准备批量插入的文档
	documents := make([]interface{}, len(params))
	for i, param := range params {
		param.CreatedAt = time.Now()
		param.UpdatedAt = time.Now()
		documents[i] = param
	}

	// 批量插入
	_, err := collection.InsertMany(ctx, documents)
	if err != nil {
		adapter.logger.Errorw("Failed to save SMS params", "error", err)
		return fmt.Errorf("failed to save SMS params: %w", err)
	}

	adapter.logger.Infow("Successfully saved SMS params", "count", len(params))
	return nil
}

// GetSmsParamsByJobID 根据JobID查询SMS参数，对应Java项目中的TableStorageTemplate.queryByPartitionKey
func (adapter *MongoDBAdapter) GetSmsParamsByJobID(ctx context.Context, jobID string, limit int64) ([]*SmsParam, error) {
	collection := adapter.db.Collection("sms_params")
	
	filter := bson.M{"jobId": jobID}
	opts := options.Find().SetLimit(limit).SetSort(bson.D{{"createdAt", 1}})
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		adapter.logger.Errorw("Failed to query SMS params", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to query SMS params: %w", err)
	}
	defer cursor.Close(ctx)
	
	var params []*SmsParam
	if err = cursor.All(ctx, &params); err != nil {
		adapter.logger.Errorw("Failed to decode SMS params", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to decode SMS params: %w", err)
	}
	
	adapter.logger.Infow("Successfully retrieved SMS params", "jobID", jobID, "count", len(params))
	return params, nil
}

// UpdateSmsParamStatus 更新SMS参数状态
func (adapter *MongoDBAdapter) UpdateSmsParamStatus(ctx context.Context, jobID, phoneNumber, status string, errorMsg string) error {
	collection := adapter.db.Collection("sms_params")
	
	filter := bson.M{
		"jobId":       jobID,
		"phoneNumber": phoneNumber,
	}
	
	update := bson.M{
		"$set": bson.M{
			"status":    status,
			"updatedAt": time.Now(),
		},
	}
	
	if errorMsg != "" {
		update["$set"].(bson.M)["errorMsg"] = errorMsg
		update["$inc"] = bson.M{"retryCount": 1}
	}
	
	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		adapter.logger.Errorw("Failed to update SMS param status", "jobID", jobID, "phoneNumber", phoneNumber, "error", err)
		return fmt.Errorf("failed to update SMS param status: %w", err)
	}
	
	adapter.logger.Infow("Successfully updated SMS param status", "jobID", jobID, "phoneNumber", phoneNumber, "status", status)
	return nil
}

// SaveTsReport 保存报告，对应Java项目中的TableStorageTemplate.save
func (adapter *MongoDBAdapter) SaveTsReport(ctx context.Context, report *TsReport) error {
	collection := adapter.db.Collection("ts_reports")
	
	report.CreatedAt = time.Now()
	report.UpdatedAt = time.Now()
	
	_, err := collection.InsertOne(ctx, report)
	if err != nil {
		adapter.logger.Errorw("Failed to save TS report", "error", err)
		return fmt.Errorf("failed to save TS report: %w", err)
	}
	
	adapter.logger.Infow("Successfully saved TS report", "jobID", report.JobID, "phoneNumber", report.PhoneNumber)
	return nil
}

// GetTsReportsByJobID 根据JobID查询报告
func (adapter *MongoDBAdapter) GetTsReportsByJobID(ctx context.Context, jobID string) ([]*TsReport, error) {
	collection := adapter.db.Collection("ts_reports")
	
	filter := bson.M{"jobId": jobID}
	opts := options.Find().SetSort(bson.D{{"timestamp", -1}})
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		adapter.logger.Errorw("Failed to query TS reports", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to query TS reports: %w", err)
	}
	defer cursor.Close(ctx)
	
	var reports []*TsReport
	if err = cursor.All(ctx, &reports); err != nil {
		adapter.logger.Errorw("Failed to decode TS reports", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to decode TS reports: %w", err)
	}
	
	adapter.logger.Infow("Successfully retrieved TS reports", "jobID", jobID, "count", len(reports))
	return reports, nil
}

// SaveBatchJobStatus 保存批处理作业状态
func (adapter *MongoDBAdapter) SaveBatchJobStatus(ctx context.Context, status *BatchJobStatus) error {
	collection := adapter.db.Collection("batch_job_status")
	
	status.CreatedAt = time.Now()
	status.UpdatedAt = time.Now()
	
	_, err := collection.InsertOne(ctx, status)
	if err != nil {
		adapter.logger.Errorw("Failed to save batch job status", "error", err)
		return fmt.Errorf("failed to save batch job status: %w", err)
	}
	
	adapter.logger.Infow("Successfully saved batch job status", "jobID", status.JobID, "phase", status.Phase)
	return nil
}

// UpdateBatchJobStatus 更新批处理作业状态
func (adapter *MongoDBAdapter) UpdateBatchJobStatus(ctx context.Context, jobID, phase string, progress float32, processedCount, successCount, failedCount int64) error {
	collection := adapter.db.Collection("batch_job_status")
	
	filter := bson.M{
		"jobId": jobID,
		"phase": phase,
	}
	
	update := bson.M{
		"$set": bson.M{
			"progress":       progress,
			"processedCount": processedCount,
			"successCount":   successCount,
			"failedCount":    failedCount,
			"updatedAt":      time.Now(),
		},
	}
	
	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		adapter.logger.Errorw("Failed to update batch job status", "jobID", jobID, "phase", phase, "error", err)
		return fmt.Errorf("failed to update batch job status: %w", err)
	}
	
	adapter.logger.Infow("Successfully updated batch job status", "jobID", jobID, "phase", phase, "progress", progress)
	return nil
}

// GetBatchJobStatus 获取批处理作业状态
func (adapter *MongoDBAdapter) GetBatchJobStatus(ctx context.Context, jobID string) ([]*BatchJobStatus, error) {
	collection := adapter.db.Collection("batch_job_status")
	
	filter := bson.M{"jobId": jobID}
	opts := options.Find().SetSort(bson.D{{"createdAt", -1}})
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		adapter.logger.Errorw("Failed to query batch job status", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to query batch job status: %w", err)
	}
	defer cursor.Close(ctx)
	
	var statuses []*BatchJobStatus
	if err = cursor.All(ctx, &statuses); err != nil {
		adapter.logger.Errorw("Failed to decode batch job status", "jobID", jobID, "error", err)
		return nil, fmt.Errorf("failed to decode batch job status: %w", err)
	}
	
	adapter.logger.Infow("Successfully retrieved batch job status", "jobID", jobID, "count", len(statuses))
	return statuses, nil
}

// CreateIndexes 创建MongoDB索引，优化查询性能
func (adapter *MongoDBAdapter) CreateIndexes(ctx context.Context) error {
	// SMS参数集合索引
	smsParamsCollection := adapter.db.Collection("sms_params")
	smsParamsIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"jobId", 1}},
			Options: options.Index().SetName("idx_job_id"),
		},
		{
			Keys: bson.D{{"jobId", 1}, {"phoneNumber", 1}},
			Options: options.Index().SetName("idx_job_phone").SetUnique(true),
		},
		{
			Keys: bson.D{{"status", 1}},
			Options: options.Index().SetName("idx_status"),
		},
		{
			Keys: bson.D{{"createdAt", 1}},
			Options: options.Index().SetName("idx_created_at"),
		},
	}
	
	_, err := smsParamsCollection.Indexes().CreateMany(ctx, smsParamsIndexes)
	if err != nil {
		return fmt.Errorf("failed to create SMS params indexes: %w", err)
	}
	
	// 报告集合索引
	tsReportsCollection := adapter.db.Collection("ts_reports")
	tsReportsIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"jobId", 1}},
			Options: options.Index().SetName("idx_job_id"),
		},
		{
			Keys: bson.D{{"phoneNumber", 1}},
			Options: options.Index().SetName("idx_phone_number"),
		},
		{
			Keys: bson.D{{"eventType", 1}},
			Options: options.Index().SetName("idx_event_type"),
		},
		{
			Keys: bson.D{{"timestamp", -1}},
			Options: options.Index().SetName("idx_timestamp"),
		},
	}
	
	_, err = tsReportsCollection.Indexes().CreateMany(ctx, tsReportsIndexes)
	if err != nil {
		return fmt.Errorf("failed to create TS reports indexes: %w", err)
	}
	
	// 批处理作业状态集合索引
	batchStatusCollection := adapter.db.Collection("batch_job_status")
	batchStatusIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"jobId", 1}},
			Options: options.Index().SetName("idx_job_id"),
		},
		{
			Keys: bson.D{{"jobId", 1}, {"phase", 1}},
			Options: options.Index().SetName("idx_job_phase"),
		},
		{
			Keys: bson.D{{"status", 1}},
			Options: options.Index().SetName("idx_status"),
		},
		{
			Keys: bson.D{{"createdAt", -1}},
			Options: options.Index().SetName("idx_created_at"),
		},
	}
	
	_, err = batchStatusCollection.Indexes().CreateMany(ctx, batchStatusIndexes)
	if err != nil {
		return fmt.Errorf("failed to create batch status indexes: %w", err)
	}
	
	adapter.logger.Infow("Successfully created MongoDB indexes")
	return nil
}
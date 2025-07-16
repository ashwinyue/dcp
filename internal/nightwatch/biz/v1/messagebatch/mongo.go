package messagebatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
)

// MongoService 提供MongoDB相关的业务服务
type MongoService struct {
	store  store.IStore
	logger log.Logger
}

// NewMongoService 创建新的MongoDB服务
func NewMongoService(store store.IStore, logger log.Logger) *MongoService {
	return &MongoService{
		store:  store,
		logger: logger,
	}
}

// MessageBatchDocument 表示消息批次文档
type MessageBatchDocument struct {
	BatchID      string                 `json:"batch_id" bson:"batch_id"`
	PartitionID  int                    `json:"partition_id" bson:"partition_id"`
	Status       string                 `json:"status" bson:"status"`
	MessageCount int64                  `json:"message_count" bson:"message_count"`
	StartTime    time.Time              `json:"start_time" bson:"start_time"`
	EndTime      *time.Time             `json:"end_time,omitempty" bson:"end_time,omitempty"`
	Messages     []MessageItem          `json:"messages" bson:"messages"`
	Metadata     map[string]interface{} `json:"metadata" bson:"metadata"`
	CreatedAt    time.Time              `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at" bson:"updated_at"`
}

// MessageItem 表示单个消息项
type MessageItem struct {
	MessageID string                 `json:"message_id" bson:"message_id"`
	Content   map[string]interface{} `json:"content" bson:"content"`
	Timestamp time.Time              `json:"timestamp" bson:"timestamp"`
	Status    string                 `json:"status" bson:"status"`
}

// CreateBatchDocument 创建批次文档
func (m *MongoService) CreateBatchDocument(ctx context.Context, batchDoc *MessageBatchDocument) error {
	// 将batchDoc序列化为JSON字符串
	contentBytes, err := json.Marshal(batchDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal batch document: %w", err)
	}

	// 转换为Document模型
	doc := &model.Document{
		DocumentID: batchDoc.BatchID,
		Type:       "message_batch",
		Title:      fmt.Sprintf("Message Batch %s", batchDoc.BatchID),
		Content:    string(contentBytes),
		Status:     batchDoc.Status,
		Metadata: map[string]interface{}{
			"partition_id":   batchDoc.PartitionID,
			"status":         batchDoc.Status,
			"message_count":  batchDoc.MessageCount,
			"batch_category": "processing",
		},
	}

	err = m.store.Document().Create(ctx, doc)
	if err != nil {
		m.logger.Errorw("Failed to create batch document", "error", err, "batchID", batchDoc.BatchID)
		return fmt.Errorf("failed to create batch document: %w", err)
	}

	m.logger.Infow("Batch document created successfully", "batchID", batchDoc.BatchID, "partitionID", batchDoc.PartitionID)
	return nil
}

// GetBatchDocument 获取批次文档
func (m *MongoService) GetBatchDocument(ctx context.Context, batchID string) (*MessageBatchDocument, error) {
	doc, err := m.store.Document().Get(ctx, batchID)
	if err != nil {
		m.logger.Errorw("Failed to get batch document", "error", err, "batchID", batchID)
		return nil, fmt.Errorf("failed to get batch document: %w", err)
	}

	// 转换content为MessageBatchDocument
	var batchDoc MessageBatchDocument
	err = json.Unmarshal([]byte(doc.Content), &batchDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch document: %w", err)
	}

	return &batchDoc, nil
}

// UpdateBatchDocument 更新批次文档
func (m *MongoService) UpdateBatchDocument(ctx context.Context, batchID string, batchDoc *MessageBatchDocument) error {
	// 更新时间
	batchDoc.UpdatedAt = time.Now()

	// 将batchDoc序列化为JSON字符串
	contentBytes, err := json.Marshal(batchDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal batch document: %w", err)
	}

	// 转换为Document模型
	doc := &model.Document{
		Type:    "message_batch",
		Title:   fmt.Sprintf("Message Batch %s", batchDoc.BatchID),
		Content: string(contentBytes),
		Status:  batchDoc.Status,
		Metadata: map[string]interface{}{
			"partition_id":   batchDoc.PartitionID,
			"status":         batchDoc.Status,
			"message_count":  batchDoc.MessageCount,
			"batch_category": "processing",
		},
	}

	err = m.store.Document().Update(ctx, batchID, doc)
	if err != nil {
		m.logger.Errorw("Failed to update batch document", "error", err, "batchID", batchID)
		return fmt.Errorf("failed to update batch document: %w", err)
	}

	m.logger.Infow("Batch document updated successfully", "batchID", batchID, "status", batchDoc.Status)
	return nil
}

// UpdateBatchStatus 更新批次状态
func (m *MongoService) UpdateBatchStatus(ctx context.Context, batchID string, status string) error {
	// 先获取现有文档
	batchDoc, err := m.GetBatchDocument(ctx, batchID)
	if err != nil {
		return err
	}

	// 更新状态
	batchDoc.Status = status
	batchDoc.UpdatedAt = time.Now()

	if status == "completed" || status == "failed" {
		now := time.Now()
		batchDoc.EndTime = &now
	}

	return m.UpdateBatchDocument(ctx, batchID, batchDoc)
}

// AddMessageToBatch 向批次添加消息
func (m *MongoService) AddMessageToBatch(ctx context.Context, batchID string, message *MessageItem) error {
	// 先获取现有文档
	batchDoc, err := m.GetBatchDocument(ctx, batchID)
	if err != nil {
		return err
	}

	// 添加消息
	batchDoc.Messages = append(batchDoc.Messages, *message)
	batchDoc.MessageCount = int64(len(batchDoc.Messages))
	batchDoc.UpdatedAt = time.Now()

	return m.UpdateBatchDocument(ctx, batchID, batchDoc)
}

// ListBatchDocuments 列出批次文档
func (m *MongoService) ListBatchDocuments(ctx context.Context, partitionID *int, status string, limit int64) ([]*MessageBatchDocument, error) {
	// 构建请求
	req := &model.DocumentListRequest{
		Type:     "message_batch",
		PageSize: int(limit),
		Page:     1,
	}

	// 添加过滤条件
	if status != "" {
		req.Status = status
	}

	// 设置默认值
	req.SetDefaults()

	resp, err := m.store.Document().List(ctx, req)
	if err != nil {
		m.logger.Errorw("Failed to list batch documents", "error", err)
		return nil, fmt.Errorf("failed to list batch documents: %w", err)
	}

	// 转换为MessageBatchDocument并根据partitionID过滤
	var batchDocs []*MessageBatchDocument
	for _, doc := range resp.Documents {
		var batchDoc MessageBatchDocument
		err = json.Unmarshal([]byte(doc.Content), &batchDoc)
		if err != nil {
			m.logger.Errorw("Failed to unmarshal batch document", "error", err, "documentID", doc.DocumentID)
			continue
		}

		// 如果指定了partitionID，进行过滤
		if partitionID != nil && batchDoc.PartitionID != *partitionID {
			continue
		}

		batchDocs = append(batchDocs, &batchDoc)
	}

	m.logger.Infow("Batch documents listed successfully", "count", len(batchDocs))
	return batchDocs, nil
}

// DeleteBatchDocument 删除批次文档
func (m *MongoService) DeleteBatchDocument(ctx context.Context, batchID string) error {
	err := m.store.Document().Delete(ctx, batchID)
	if err != nil {
		m.logger.Errorw("Failed to delete batch document", "error", err, "batchID", batchID)
		return fmt.Errorf("failed to delete batch document: %w", err)
	}

	m.logger.Infow("Batch document deleted successfully", "batchID", batchID)
	return nil
}

// CountBatchDocuments 统计批次文档数量
func (m *MongoService) CountBatchDocuments(ctx context.Context, filter map[string]interface{}) (int64, error) {
	// 添加类型过滤
	if filter == nil {
		filter = make(map[string]interface{})
	}
	filter["type"] = "message_batch"

	count, err := m.store.Document().Count(ctx, bson.M(filter))
	if err != nil {
		m.logger.Errorw("Failed to count batch documents", "error", err)
		return 0, fmt.Errorf("failed to count batch documents: %w", err)
	}

	return count, nil
}

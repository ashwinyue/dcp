// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package store

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
)

// DocumentStore 定义了document模块在store层所实现的方法
type DocumentStore interface {
	Create(ctx context.Context, document *model.Document) error
	Get(ctx context.Context, documentID string) (*model.Document, error)
	GetByObjectID(ctx context.Context, id primitive.ObjectID) (*model.Document, error)
	Update(ctx context.Context, documentID string, document *model.Document) error
	Delete(ctx context.Context, documentID string) error
	List(ctx context.Context, req *model.DocumentListRequest) (*model.DocumentListResponse, error)
	Count(ctx context.Context, filter bson.M) (int64, error)
}

// documentStore 是DocumentStore的具体实现
type documentStore struct {
	collection *mongo.Collection
}

// 确保documentStore实现了DocumentStore接口
var _ DocumentStore = (*documentStore)(nil)

// newDocumentStore 创建一个documentStore实例
func newDocumentStore(collection *mongo.Collection) *documentStore {
	return &documentStore{collection: collection}
}

// Create 创建一个新文档
func (d *documentStore) Create(ctx context.Context, document *model.Document) error {
	// 设置创建时间
	now := time.Now()
	document.CreatedAt = now
	document.UpdatedAt = now

	// 如果没有设置DocumentID，则生成一个
	if document.DocumentID == "" {
		document.DocumentID = primitive.NewObjectID().Hex()
	}

	_, err := d.collection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("创建文档失败: %w", err)
	}

	return nil
}

// Get 根据DocumentID获取文档
func (d *documentStore) Get(ctx context.Context, documentID string) (*model.Document, error) {
	filter := bson.M{"document_id": documentID}

	var document model.Document
	err := d.collection.FindOne(ctx, filter).Decode(&document)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("文档不存在: %s", documentID)
		}
		return nil, fmt.Errorf("查询文档失败: %w", err)
	}

	return &document, nil
}

// GetByObjectID 根据MongoDB ObjectID获取文档
func (d *documentStore) GetByObjectID(ctx context.Context, id primitive.ObjectID) (*model.Document, error) {
	filter := bson.M{"_id": id}

	var document model.Document
	err := d.collection.FindOne(ctx, filter).Decode(&document)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("文档不存在: %s", id.Hex())
		}
		return nil, fmt.Errorf("查询文档失败: %w", err)
	}

	return &document, nil
}

// Update 更新文档
func (d *documentStore) Update(ctx context.Context, documentID string, document *model.Document) error {
	filter := bson.M{"document_id": documentID}

	// 更新时间
	document.UpdatedAt = time.Now()

	// 构建更新文档
	update := bson.M{"$set": document}

	result, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("更新文档失败: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("文档不存在: %s", documentID)
	}

	return nil
}

// Delete 删除文档
func (d *documentStore) Delete(ctx context.Context, documentID string) error {
	filter := bson.M{"document_id": documentID}

	result, err := d.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("删除文档失败: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("文档不存在: %s", documentID)
	}

	return nil
}

// List 查询文档列表
func (d *documentStore) List(ctx context.Context, req *model.DocumentListRequest) (*model.DocumentListResponse, error) {
	// 设置默认值
	req.SetDefaults()

	// 构建查询过滤器
	filter := d.buildFilter(req)

	// 计算总数
	total, err := d.Count(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("计算文档总数失败: %w", err)
	}

	// 计算分页参数
	skip := int64((req.Page - 1) * req.PageSize)
	limit := int64(req.PageSize)

	// 设置查询选项
	opts := options.Find().
		SetSkip(skip).
		SetLimit(limit).
		SetSort(bson.D{{"created_at", -1}}) // 按创建时间降序排序

	// 执行查询
	cursor, err := d.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("查询文档列表失败: %w", err)
	}
	defer cursor.Close(ctx)

	// 解析结果
	var documents []*model.Document
	for cursor.Next(ctx) {
		var doc model.Document
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("解析文档失败: %w", err)
		}
		documents = append(documents, &doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("遍历游标失败: %w", err)
	}

	return &model.DocumentListResponse{
		Documents: documents,
		Total:     total,
		Page:      req.Page,
		PageSize:  req.PageSize,
	}, nil
}

// Count 计算匹配过滤器的文档数量
func (d *documentStore) Count(ctx context.Context, filter bson.M) (int64, error) {
	count, err := d.collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("计算文档数量失败: %w", err)
	}
	return count, nil
}

// buildFilter 构建查询过滤器
func (d *documentStore) buildFilter(req *model.DocumentListRequest) bson.M {
	filter := bson.M{}

	// 按类型过滤
	if req.Type != "" {
		filter["type"] = req.Type
	}

	// 按状态过滤
	if req.Status != "" {
		filter["status"] = req.Status
	}

	// 按作者过滤
	if req.Author != "" {
		filter["author"] = req.Author
	}

	// 按标签过滤
	if req.Tag != "" {
		filter["tags"] = bson.M{"$in": []string{req.Tag}}
	}

	// 搜索关键词（标题和内容）
	if req.Search != "" {
		filter["$or"] = []bson.M{
			{"title": bson.M{"$regex": req.Search, "$options": "i"}},
			{"content": bson.M{"$regex": req.Search, "$options": "i"}},
		}
	}

	return filter
}

// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package document

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// DocumentBiz 定义了文档业务层接口.
type DocumentBiz interface {
	Create(ctx context.Context, req *model.DocumentCreateRequest) (*model.Document, error)
	Get(ctx context.Context, documentID string) (*model.Document, error)
	GetByObjectID(ctx context.Context, id primitive.ObjectID) (*model.Document, error)
	Update(ctx context.Context, documentID string, req *model.DocumentUpdateRequest) (*model.Document, error)
	Delete(ctx context.Context, documentID string) error
	List(ctx context.Context, req *model.DocumentListRequest) (*model.DocumentListResponse, error)
}

// documentBiz 实现了DocumentBiz接口.
type documentBiz struct {
	store store.IStore
}

// 确保documentBiz实现了DocumentBiz接口.
var _ DocumentBiz = (*documentBiz)(nil)

// New 创建一个DocumentBiz接口的实例.
func New(store store.IStore) DocumentBiz {
	return &documentBiz{store: store}
}

// Create 创建文档.
func (b *documentBiz) Create(ctx context.Context, req *model.DocumentCreateRequest) (*model.Document, error) {
	// 验证请求参数
	if err := b.validateCreateRequest(req); err != nil {
		return nil, fmt.Errorf("请求参数验证失败: %w", err)
	}

	// 创建文档实例
	document := model.NewDocument(req)

	// 调用store层创建文档
	if err := b.store.Document().Create(ctx, document); err != nil {
		return nil, fmt.Errorf("创建文档失败: %w", err)
	}

	return document, nil
}

// Get 根据文档ID获取文档.
func (b *documentBiz) Get(ctx context.Context, documentID string) (*model.Document, error) {
	if documentID == "" {
		return nil, fmt.Errorf("文档ID不能为空")
	}

	document, err := b.store.Document().Get(ctx, documentID)
	if err != nil {
		return nil, fmt.Errorf("获取文档失败: %w", err)
	}

	return document, nil
}

// GetByObjectID 根据MongoDB ObjectID获取文档.
func (b *documentBiz) GetByObjectID(ctx context.Context, id primitive.ObjectID) (*model.Document, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("ObjectID不能为空")
	}

	document, err := b.store.Document().GetByObjectID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("根据ObjectID获取文档失败: %w", err)
	}

	return document, nil
}

// Update 更新文档.
func (b *documentBiz) Update(ctx context.Context, documentID string, req *model.DocumentUpdateRequest) (*model.Document, error) {
	if documentID == "" {
		return nil, fmt.Errorf("文档ID不能为空")
	}

	// 验证更新请求
	if err := b.validateUpdateRequest(req); err != nil {
		return nil, fmt.Errorf("请求参数验证失败: %w", err)
	}

	// 先获取现有文档
	document, err := b.store.Document().Get(ctx, documentID)
	if err != nil {
		return nil, fmt.Errorf("获取文档失败: %w", err)
	}

	// 应用更新
	document.Update(req)

	// 保存更新
	if err := b.store.Document().Update(ctx, documentID, document); err != nil {
		return nil, fmt.Errorf("更新文档失败: %w", err)
	}

	return document, nil
}

// Delete 删除文档.
func (b *documentBiz) Delete(ctx context.Context, documentID string) error {
	if documentID == "" {
		return fmt.Errorf("文档ID不能为空")
	}

	// 检查文档是否存在
	_, err := b.store.Document().Get(ctx, documentID)
	if err != nil {
		return fmt.Errorf("文档不存在: %w", err)
	}

	// 删除文档
	if err := b.store.Document().Delete(ctx, documentID); err != nil {
		return fmt.Errorf("删除文档失败: %w", err)
	}

	return nil
}

// List 获取文档列表.
func (b *documentBiz) List(ctx context.Context, req *model.DocumentListRequest) (*model.DocumentListResponse, error) {
	// 验证列表请求
	if err := b.validateListRequest(req); err != nil {
		return nil, fmt.Errorf("请求参数验证失败: %w", err)
	}

	// 调用store层获取文档列表
	response, err := b.store.Document().List(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("获取文档列表失败: %w", err)
	}

	return response, nil
}

// validateCreateRequest 验证创建请求.
func (b *documentBiz) validateCreateRequest(req *model.DocumentCreateRequest) error {
	if req == nil {
		return fmt.Errorf("创建请求不能为空")
	}

	if req.Title == "" {
		return fmt.Errorf("文档标题不能为空")
	}

	if req.Content == "" {
		return fmt.Errorf("文档内容不能为空")
	}

	if req.Type == "" {
		return fmt.Errorf("文档类型不能为空")
	}

	if req.Author == "" {
		return fmt.Errorf("作者不能为空")
	}

	// 验证状态值
	if req.Status != "" && !isValidStatus(req.Status) {
		return fmt.Errorf("无效的文档状态: %s", req.Status)
	}

	return nil
}

// validateUpdateRequest 验证更新请求.
func (b *documentBiz) validateUpdateRequest(req *model.DocumentUpdateRequest) error {
	if req == nil {
		return fmt.Errorf("更新请求不能为空")
	}

	// 至少有一个字段需要更新
	if req.Title == nil && req.Content == nil && req.Type == nil &&
		req.Status == nil && req.Tags == nil && req.Metadata == nil {
		return fmt.Errorf("至少需要更新一个字段")
	}

	// 验证状态值
	if req.Status != nil && !isValidStatus(*req.Status) {
		return fmt.Errorf("无效的文档状态: %s", *req.Status)
	}

	return nil
}

// validateListRequest 验证列表请求.
func (b *documentBiz) validateListRequest(req *model.DocumentListRequest) error {
	if req == nil {
		return fmt.Errorf("列表请求不能为空")
	}

	// 设置默认值
	req.SetDefaults()

	return nil
}

// isValidStatus 检查状态是否有效.
func isValidStatus(status string) bool {
	validStatuses := []string{"draft", "published", "archived"}
	for _, validStatus := range validStatuses {
		if status == validStatus {
			return true
		}
	}
	return false
}

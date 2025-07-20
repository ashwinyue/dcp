// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package document

import (
	"context"

	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// 临时类型定义，待proto文件完善后替换
type CreateDocumentRequest struct{}
type CreateDocumentResponse struct{}
type UpdateDocumentRequest struct{}
type UpdateDocumentResponse struct{}
type DeleteDocumentRequest struct{}
type DeleteDocumentResponse struct{}
type GetDocumentRequest struct{}
type GetDocumentResponse struct{}
type ListDocumentRequest struct{}
type ListDocumentResponse struct{}

// DocumentBiz 定义了文档业务层接口.
type DocumentBiz interface {
	// Create 创建文档.
	Create(ctx context.Context, req *CreateDocumentRequest) (*CreateDocumentResponse, error)
	// Update 更新文档.
	Update(ctx context.Context, req *UpdateDocumentRequest) (*UpdateDocumentResponse, error)
	// Delete 删除文档.
	Delete(ctx context.Context, req *DeleteDocumentRequest) (*DeleteDocumentResponse, error)
	// Get 获取文档.
	Get(ctx context.Context, req *GetDocumentRequest) (*GetDocumentResponse, error)
	// List 列出文档.
	List(ctx context.Context, req *ListDocumentRequest) (*ListDocumentResponse, error)
}

// documentBiz 实现了 DocumentBiz 接口.
type documentBiz struct {
	store store.IStore
}

// New 创建一个新的 DocumentBiz 实例.
func New(store store.IStore) DocumentBiz {
	return &documentBiz{
		store: store,
	}
}

// Create 创建文档.
func (b *documentBiz) Create(ctx context.Context, req *CreateDocumentRequest) (*CreateDocumentResponse, error) {
	// TODO: 实现文档创建逻辑
	return &CreateDocumentResponse{}, nil
}

// Update 更新文档.
func (b *documentBiz) Update(ctx context.Context, req *UpdateDocumentRequest) (*UpdateDocumentResponse, error) {
	// TODO: 实现文档更新逻辑
	return &UpdateDocumentResponse{}, nil
}

// Delete 删除文档.
func (b *documentBiz) Delete(ctx context.Context, req *DeleteDocumentRequest) (*DeleteDocumentResponse, error) {
	// TODO: 实现文档删除逻辑
	return &DeleteDocumentResponse{}, nil
}

// Get 获取文档.
func (b *documentBiz) Get(ctx context.Context, req *GetDocumentRequest) (*GetDocumentResponse, error) {
	// TODO: 实现文档获取逻辑
	return &GetDocumentResponse{}, nil
}

// List 列出文档.
func (b *documentBiz) List(ctx context.Context, req *ListDocumentRequest) (*ListDocumentResponse, error) {
	// TODO: 实现文档列表逻辑
	return &ListDocumentResponse{}, nil
}
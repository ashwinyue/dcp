// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package statustracking

import (
	"context"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// 临时类型定义，待proto文件完善后替换
type CreateStatusTrackingRequest struct{}
type CreateStatusTrackingResponse struct{}
type UpdateStatusTrackingRequest struct{}
type UpdateStatusTrackingResponse struct{}
type DeleteStatusTrackingRequest struct{}
type DeleteStatusTrackingResponse struct{}
type GetStatusTrackingRequest struct{}
type GetStatusTrackingResponse struct{}
type ListStatusTrackingRequest struct{}
type ListStatusTrackingResponse struct{}

// StatusTrackingBiz 定义了状态跟踪业务层接口.
type StatusTrackingBiz interface {
	// Create 创建状态跟踪.
	Create(ctx context.Context, req *CreateStatusTrackingRequest) (*CreateStatusTrackingResponse, error)
	// Update 更新状态跟踪.
	Update(ctx context.Context, req *UpdateStatusTrackingRequest) (*UpdateStatusTrackingResponse, error)
	// Delete 删除状态跟踪.
	Delete(ctx context.Context, req *DeleteStatusTrackingRequest) (*DeleteStatusTrackingResponse, error)
	// Get 获取状态跟踪.
	Get(ctx context.Context, req *GetStatusTrackingRequest) (*GetStatusTrackingResponse, error)
	// List 列出状态跟踪.
	List(ctx context.Context, req *ListStatusTrackingRequest) (*ListStatusTrackingResponse, error)
}

// statusTrackingBiz 实现了 StatusTrackingBiz 接口.
type statusTrackingBiz struct {
	store        store.IStore
	cacheManager *cache.CacheManager
}

// New 创建一个新的 StatusTrackingBiz 实例.
func New(store store.IStore, cacheManager *cache.CacheManager) StatusTrackingBiz {
	return &statusTrackingBiz{
		store:        store,
		cacheManager: cacheManager,
	}
}

// Create 创建状态跟踪.
func (b *statusTrackingBiz) Create(ctx context.Context, req *CreateStatusTrackingRequest) (*CreateStatusTrackingResponse, error) {
	// TODO: 实现状态跟踪创建逻辑
	return &CreateStatusTrackingResponse{}, nil
}

// Update 更新状态跟踪.
func (b *statusTrackingBiz) Update(ctx context.Context, req *UpdateStatusTrackingRequest) (*UpdateStatusTrackingResponse, error) {
	// TODO: 实现状态跟踪更新逻辑
	return &UpdateStatusTrackingResponse{}, nil
}

// Delete 删除状态跟踪.
func (b *statusTrackingBiz) Delete(ctx context.Context, req *DeleteStatusTrackingRequest) (*DeleteStatusTrackingResponse, error) {
	// TODO: 实现状态跟踪删除逻辑
	return &DeleteStatusTrackingResponse{}, nil
}

// Get 获取状态跟踪.
func (b *statusTrackingBiz) Get(ctx context.Context, req *GetStatusTrackingRequest) (*GetStatusTrackingResponse, error) {
	// TODO: 实现状态跟踪获取逻辑
	return &GetStatusTrackingResponse{}, nil
}

// List 列出状态跟踪.
func (b *statusTrackingBiz) List(ctx context.Context, req *ListStatusTrackingRequest) (*ListStatusTrackingResponse, error) {
	// TODO: 实现状态跟踪列表逻辑
	return &ListStatusTrackingResponse{}, nil
}
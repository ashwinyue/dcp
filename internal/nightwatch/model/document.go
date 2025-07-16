// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Document 表示MongoDB中的文档模型
type Document struct {
	// MongoDB的ObjectID
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	// 文档唯一标识符
	DocumentID string `bson:"document_id" json:"document_id"`

	// 文档标题
	Title string `bson:"title" json:"title"`

	// 文档内容
	Content string `bson:"content" json:"content"`

	// 文档类型
	Type string `bson:"type" json:"type"`

	// 文档状态 (draft, published, archived)
	Status string `bson:"status" json:"status"`

	// 标签列表
	Tags []string `bson:"tags" json:"tags"`

	// 元数据
	Metadata map[string]interface{} `bson:"metadata,omitempty" json:"metadata,omitempty"`

	// 创建者
	Author string `bson:"author" json:"author"`

	// 版本号
	Version int `bson:"version" json:"version"`

	// 创建时间
	CreatedAt time.Time `bson:"created_at" json:"created_at"`

	// 更新时间
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// DocumentCreateRequest 创建文档的请求
type DocumentCreateRequest struct {
	Title    string                 `json:"title" binding:"required"`
	Content  string                 `json:"content" binding:"required"`
	Type     string                 `json:"type" binding:"required"`
	Status   string                 `json:"status"`
	Tags     []string               `json:"tags"`
	Metadata map[string]interface{} `json:"metadata"`
	Author   string                 `json:"author" binding:"required"`
}

// DocumentUpdateRequest 更新文档的请求
type DocumentUpdateRequest struct {
	Title    *string                `json:"title,omitempty"`
	Content  *string                `json:"content,omitempty"`
	Type     *string                `json:"type,omitempty"`
	Status   *string                `json:"status,omitempty"`
	Tags     []string               `json:"tags,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// DocumentListRequest 查询文档列表的请求
type DocumentListRequest struct {
	// 分页参数
	Page     int `form:"page" binding:"min=1"`
	PageSize int `form:"page_size" binding:"min=1,max=100"`

	// 查询条件
	Type   string `form:"type"`
	Status string `form:"status"`
	Author string `form:"author"`
	Tag    string `form:"tag"`

	// 搜索关键词
	Search string `form:"search"`
}

// DocumentListResponse 文档列表响应
type DocumentListResponse struct {
	Documents []*Document `json:"documents"`
	Total     int64       `json:"total"`
	Page      int         `json:"page"`
	PageSize  int         `json:"page_size"`
}

// NewDocument 创建一个新的文档实例
func NewDocument(req *DocumentCreateRequest) *Document {
	now := time.Now()

	doc := &Document{
		DocumentID: primitive.NewObjectID().Hex(),
		Title:      req.Title,
		Content:    req.Content,
		Type:       req.Type,
		Status:     req.Status,
		Tags:       req.Tags,
		Metadata:   req.Metadata,
		Author:     req.Author,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	// 设置默认状态
	if doc.Status == "" {
		doc.Status = "draft"
	}

	// 初始化标签
	if doc.Tags == nil {
		doc.Tags = []string{}
	}

	// 初始化元数据
	if doc.Metadata == nil {
		doc.Metadata = make(map[string]interface{})
	}

	return doc
}

// Update 更新文档字段
func (d *Document) Update(req *DocumentUpdateRequest) {
	if req.Title != nil {
		d.Title = *req.Title
	}
	if req.Content != nil {
		d.Content = *req.Content
	}
	if req.Type != nil {
		d.Type = *req.Type
	}
	if req.Status != nil {
		d.Status = *req.Status
	}
	if req.Tags != nil {
		d.Tags = req.Tags
	}
	if req.Metadata != nil {
		d.Metadata = req.Metadata
	}

	d.Version++
	d.UpdatedAt = time.Now()
}

// SetDefaults 设置默认分页参数
func (req *DocumentListRequest) SetDefaults() {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}
	if req.PageSize > 100 {
		req.PageSize = 100
	}
}

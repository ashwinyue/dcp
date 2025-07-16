// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"k8s.io/klog/v2"

	"github.com/ashwinyue/dcp/internal/nightwatch/biz"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/pkg/errno"
)

// DocumentHandler 处理文档相关的HTTP请求.
type DocumentHandler struct {
	biz biz.IBiz
}

// NewDocumentHandler 创建一个新的DocumentHandler实例.
func NewDocumentHandler(biz biz.IBiz) *DocumentHandler {
	return &DocumentHandler{biz: biz}
}

// CreateDocument 创建文档
// @Summary 创建文档
// @Description 创建一个新的文档
// @Tags documents
// @Accept json
// @Produce json
// @Param document body model.DocumentCreateRequest true "文档创建请求"
// @Success 201 {object} model.Document "创建成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents [post]
func (h *DocumentHandler) CreateDocument(c *gin.Context) {
	klog.V(4).InfoS("CreateDocument function called.")

	var req model.DocumentCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage(err.Error()))
		return
	}

	// 调用business层创建文档
	document, err := h.biz.Document().Create(c, &req)
	if err != nil {
		klog.ErrorS(err, "Failed to create document")
		c.JSON(http.StatusInternalServerError, errno.ErrInternalServerError.SetMessage(err.Error()))
		return
	}

	c.JSON(http.StatusCreated, document)
}

// GetDocument 获取单个文档
// @Summary 获取文档
// @Description 根据文档ID获取文档详情
// @Tags documents
// @Accept json
// @Produce json
// @Param id path string true "文档ID"
// @Success 200 {object} model.Document "获取成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 404 {object} errno.Errno "文档不存在"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents/{id} [get]
func (h *DocumentHandler) GetDocument(c *gin.Context) {
	klog.V(4).InfoS("GetDocument function called.")

	documentID := c.Param("id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage("文档ID不能为空"))
		return
	}

	// 调用business层获取文档
	document, err := h.biz.Document().Get(c, documentID)
	if err != nil {
		klog.ErrorS(err, "Failed to get document", "documentID", documentID)
		c.JSON(http.StatusNotFound, errno.ErrRecordNotFound.SetMessage(err.Error()))
		return
	}

	c.JSON(http.StatusOK, document)
}

// GetDocumentByObjectID 根据MongoDB ObjectID获取文档
// @Summary 根据ObjectID获取文档
// @Description 根据MongoDB ObjectID获取文档详情
// @Tags documents
// @Accept json
// @Produce json
// @Param objectId path string true "MongoDB ObjectID"
// @Success 200 {object} model.Document "获取成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 404 {object} errno.Errno "文档不存在"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents/by-object-id/{objectId} [get]
func (h *DocumentHandler) GetDocumentByObjectID(c *gin.Context) {
	klog.V(4).InfoS("GetDocumentByObjectID function called.")

	objectIDStr := c.Param("objectId")
	if objectIDStr == "" {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage("ObjectID不能为空"))
		return
	}

	// 转换ObjectID
	objectID, err := primitive.ObjectIDFromHex(objectIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage("无效的ObjectID格式"))
		return
	}

	// 调用business层获取文档
	document, err := h.biz.Document().GetByObjectID(c, objectID)
	if err != nil {
		klog.ErrorS(err, "Failed to get document by ObjectID", "objectID", objectIDStr)
		c.JSON(http.StatusNotFound, errno.ErrRecordNotFound.SetMessage(err.Error()))
		return
	}

	c.JSON(http.StatusOK, document)
}

// UpdateDocument 更新文档
// @Summary 更新文档
// @Description 更新指定ID的文档
// @Tags documents
// @Accept json
// @Produce json
// @Param id path string true "文档ID"
// @Param document body model.DocumentUpdateRequest true "文档更新请求"
// @Success 200 {object} model.Document "更新成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 404 {object} errno.Errno "文档不存在"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents/{id} [put]
func (h *DocumentHandler) UpdateDocument(c *gin.Context) {
	klog.V(4).InfoS("UpdateDocument function called.")

	documentID := c.Param("id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage("文档ID不能为空"))
		return
	}

	var req model.DocumentUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage(err.Error()))
		return
	}

	// 调用business层更新文档
	document, err := h.biz.Document().Update(c, documentID, &req)
	if err != nil {
		klog.ErrorS(err, "Failed to update document", "documentID", documentID)
		c.JSON(http.StatusInternalServerError, errno.ErrInternalServerError.SetMessage(err.Error()))
		return
	}

	c.JSON(http.StatusOK, document)
}

// DeleteDocument 删除文档
// @Summary 删除文档
// @Description 删除指定ID的文档
// @Tags documents
// @Accept json
// @Produce json
// @Param id path string true "文档ID"
// @Success 204 "删除成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 404 {object} errno.Errno "文档不存在"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents/{id} [delete]
func (h *DocumentHandler) DeleteDocument(c *gin.Context) {
	klog.V(4).InfoS("DeleteDocument function called.")

	documentID := c.Param("id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage("文档ID不能为空"))
		return
	}

	// 调用business层删除文档
	err := h.biz.Document().Delete(c, documentID)
	if err != nil {
		klog.ErrorS(err, "Failed to delete document", "documentID", documentID)
		c.JSON(http.StatusInternalServerError, errno.ErrInternalServerError.SetMessage(err.Error()))
		return
	}

	c.Status(http.StatusNoContent)
}

// ListDocuments 获取文档列表
// @Summary 获取文档列表
// @Description 根据查询条件获取文档列表
// @Tags documents
// @Accept json
// @Produce json
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(20)
// @Param type query string false "文档类型"
// @Param status query string false "文档状态"
// @Param author query string false "作者"
// @Param tag query string false "标签"
// @Param search query string false "搜索关键词"
// @Success 200 {object} model.DocumentListResponse "获取成功"
// @Failure 400 {object} errno.Errno "请求参数错误"
// @Failure 500 {object} errno.Errno "服务器内部错误"
// @Router /v1/documents [get]
func (h *DocumentHandler) ListDocuments(c *gin.Context) {
	klog.V(4).InfoS("ListDocuments function called.")

	var req model.DocumentListRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, errno.ErrBind.SetMessage(err.Error()))
		return
	}

	// 调用business层获取文档列表
	response, err := h.biz.Document().List(c, &req)
	if err != nil {
		klog.ErrorS(err, "Failed to list documents")
		c.JSON(http.StatusInternalServerError, errno.ErrInternalServerError.SetMessage(err.Error()))
		return
	}

	c.JSON(http.StatusOK, response)
}

// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package http

import (
	"github.com/gin-gonic/gin"
	"github.com/onexstack/onexstack/pkg/core"
)

// CreateJob 创建任务.
func (h *Handler) CreateJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.JobV1().Create, h.val.ValidateCreateJobRequest)
}

// UpdateJob 更新任务.
func (h *Handler) UpdateJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.JobV1().Update, h.val.ValidateUpdateJobRequest)
}

// DeleteJob 删除任务.
func (h *Handler) DeleteJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.JobV1().Delete, h.val.ValidateDeleteJobRequest)
}

// GetJob 获取任务详情.
func (h *Handler) GetJob(c *gin.Context) {
	core.HandleUriRequest(c, h.biz.JobV1().Get, h.val.ValidateGetJobRequest)
}

// ListJob 获取任务列表.
func (h *Handler) ListJob(c *gin.Context) {
	core.HandleQueryRequest(c, h.biz.JobV1().List, h.val.ValidateListJobRequest)
}

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

// CreateCronJob 创建定时任务.
func (h *Handler) CreateCronJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.CronJobV1().Create, h.val.ValidateCreateCronJobRequest)
}

// UpdateCronJob 更新定时任务.
func (h *Handler) UpdateCronJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.CronJobV1().Update, h.val.ValidateUpdateCronJobRequest)
}

// DeleteCronJob 删除定时任务.
func (h *Handler) DeleteCronJob(c *gin.Context) {
	core.HandleJSONRequest(c, h.biz.CronJobV1().Delete, h.val.ValidateDeleteCronJobRequest)
}

// GetCronJob 获取定时任务详情.
func (h *Handler) GetCronJob(c *gin.Context) {
	core.HandleUriRequest(c, h.biz.CronJobV1().Get, h.val.ValidateGetCronJobRequest)
}

// ListCronJob 获取定时任务列表.
func (h *Handler) ListCronJob(c *gin.Context) {
	core.HandleQueryRequest(c, h.biz.CronJobV1().List, h.val.ValidateListCronJobRequest)
}
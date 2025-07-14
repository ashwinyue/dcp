// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package grpc

import (
	"context"

	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// CreateCronJob 创建定时任务.
func (h *Handler) CreateCronJob(ctx context.Context, rq *apiv1.CreateCronJobRequest) (*apiv1.CreateCronJobResponse, error) {
	return h.biz.CronJobV1().Create(ctx, rq)
}

// UpdateCronJob 更新定时任务.
func (h *Handler) UpdateCronJob(ctx context.Context, rq *apiv1.UpdateCronJobRequest) (*apiv1.UpdateCronJobResponse, error) {
	return h.biz.CronJobV1().Update(ctx, rq)
}

// DeleteCronJob 删除定时任务.
func (h *Handler) DeleteCronJob(ctx context.Context, rq *apiv1.DeleteCronJobRequest) (*apiv1.DeleteCronJobResponse, error) {
	return h.biz.CronJobV1().Delete(ctx, rq)
}

// GetCronJob 获取定时任务详情.
func (h *Handler) GetCronJob(ctx context.Context, rq *apiv1.GetCronJobRequest) (*apiv1.GetCronJobResponse, error) {
	return h.biz.CronJobV1().Get(ctx, rq)
}

// ListCronJob 获取定时任务列表.
func (h *Handler) ListCronJob(ctx context.Context, rq *apiv1.ListCronJobRequest) (*apiv1.ListCronJobResponse, error) {
	return h.biz.CronJobV1().List(ctx, rq)
}

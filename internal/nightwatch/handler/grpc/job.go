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

// CreateJob 创建任务.
func (h *Handler) CreateJob(ctx context.Context, rq *apiv1.CreateJobRequest) (*apiv1.CreateJobResponse, error) {
	return h.biz.JobV1().Create(ctx, rq)
}

// UpdateJob 更新任务.
func (h *Handler) UpdateJob(ctx context.Context, rq *apiv1.UpdateJobRequest) (*apiv1.UpdateJobResponse, error) {
	return h.biz.JobV1().Update(ctx, rq)
}

// DeleteJob 删除任务.
func (h *Handler) DeleteJob(ctx context.Context, rq *apiv1.DeleteJobRequest) (*apiv1.DeleteJobResponse, error) {
	return h.biz.JobV1().Delete(ctx, rq)
}

// GetJob 获取任务详情.
func (h *Handler) GetJob(ctx context.Context, rq *apiv1.GetJobRequest) (*apiv1.GetJobResponse, error) {
	return h.biz.JobV1().Get(ctx, rq)
}

// ListJob 获取任务列表.
func (h *Handler) ListJob(ctx context.Context, rq *apiv1.ListJobRequest) (*apiv1.ListJobResponse, error) {
	return h.biz.JobV1().List(ctx, rq)
}
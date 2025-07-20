// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package conversion

import (
	"github.com/onexstack/onexstack/pkg/core"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// MessageBatchJobMToMessageBatchJobV1 converts a MessageBatchJobM object from the internal model
// to a MessageBatchJob object in the v1 API format.
func MessageBatchJobMToMessageBatchJobV1(jobModel *model.MessageBatchJobM) *apiv1.MessageBatchJob {
	var job apiv1.MessageBatchJob
	_ = core.CopyWithConverters(&job, jobModel)
	return &job
}

// MessageBatchJobV1ToMessageBatchJobM converts a MessageBatchJob object from the v1 API format
// to a MessageBatchJobM object in the internal model.
func MessageBatchJobV1ToMessageBatchJobM(job *apiv1.MessageBatchJob) *model.MessageBatchJobM {
	var jobModel model.MessageBatchJobM
	_ = core.CopyWithConverters(&jobModel, job)
	return &jobModel
}

// MessageBatchJobModelToMessageBatchJobV1 将数据库模型转换为 API v1 版本的 MessageBatchJob (兼容旧接口).
func MessageBatchJobModelToMessageBatchJobV1(jobM *model.MessageBatchJobM) *apiv1.MessageBatchJob {
	return MessageBatchJobMToMessageBatchJobV1(jobM)
}
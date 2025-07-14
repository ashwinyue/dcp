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

// JobMToJobV1 converts a JobM object from the internal model
// to a Job object in the v1 API format.
func JobMToJobV1(jobModel *model.JobM) *apiv1.Job {
	var job apiv1.Job
	_ = core.CopyWithConverters(&job, jobModel)
	return &job
}

// JobV1ToJobM converts a Job object from the v1 API format
// to a JobM object in the internal model.
func JobV1ToJobM(job *apiv1.Job) *model.JobM {
	var jobModel model.JobM
	_ = core.CopyWithConverters(&jobModel, job)
	return &jobModel
}

// JobModelToJobV1 将数据库模型转换为 API v1 版本的 Job (兼容旧接口).
func JobModelToJobV1(jobM *model.JobM) *apiv1.Job {
	return JobMToJobV1(jobM)
}
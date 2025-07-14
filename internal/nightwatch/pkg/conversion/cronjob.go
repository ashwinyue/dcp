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

// CronJobMToCronJobV1 converts a CronJobM object from the internal model
// to a CronJob object in the v1 API format.
func CronJobMToCronJobV1(cronJobModel *model.CronJobM) *apiv1.CronJob {
	var cronJob apiv1.CronJob
	_ = core.CopyWithConverters(&cronJob, cronJobModel)

	var job apiv1.Job
	core.Copy(&job, cronJobModel.JobTemplate)
	cronJob.JobTemplate = &job

	return &cronJob
}

// CronJobV1ToCronJobM converts a CronJob object from the v1 API format
// to a CronJobM object in the internal model.
func CronJobV1ToCronJobM(cronJob *apiv1.CronJob) *model.CronJobM {
	var cronJobModel model.CronJobM
	_ = core.CopyWithConverters(&cronJobModel, cronJob)
	return &cronJobModel
}

// CronJobModelToCronJobV1 将数据库模型转换为 API v1 版本的 CronJob (兼容旧接口).
func CronJobModelToCronJobV1(cronJobM *model.CronJobM) *apiv1.CronJob {
	return CronJobMToCronJobV1(cronJobM)
}

// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package validation

import (
	"github.com/go-playground/validator/v10"

	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// ValidateCreateCronJobRequest 验证创建定时任务请求.
func ValidateCreateCronJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.CreateCronJobRequest)

	if rq.CronJob.GetName() == "" {
		sl.ReportError(rq.CronJob.Name, "name", "Name", "required", "")
	}

	if rq.CronJob.GetSchedule() == "" {
		sl.ReportError(rq.CronJob.Schedule, "schedule", "Schedule", "required", "")
	}
}

// ValidateUpdateCronJobRequest 验证更新定时任务请求.
func ValidateUpdateCronJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.UpdateCronJobRequest)

	if rq.GetCronJobID() == "" {
		sl.ReportError(rq.CronJobID, "cronJobID", "CronJobID", "required", "")
	}
}

// ValidateDeleteCronJobRequest 验证删除定时任务请求.
func ValidateDeleteCronJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.DeleteCronJobRequest)

	if len(rq.GetCronJobIDs()) == 0 {
		sl.ReportError(rq.CronJobIDs, "cronJobIDs", "CronJobIDs", "required", "")
	}
}

// ValidateGetCronJobRequest 验证获取定时任务请求.
func ValidateGetCronJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.GetCronJobRequest)

	if rq.GetCronJobID() == "" {
		sl.ReportError(rq.CronJobID, "cronJobID", "CronJobID", "required", "")
	}
}

// ValidateListCronJobRequest 验证列表定时任务请求.
func ValidateListCronJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.ListCronJobRequest)

	if rq.GetLimit() <= 0 {
		sl.ReportError(rq.Limit, "limit", "Limit", "min", "1")
	}

	if rq.GetLimit() > 1000 {
		sl.ReportError(rq.Limit, "limit", "Limit", "max", "1000")
	}
}

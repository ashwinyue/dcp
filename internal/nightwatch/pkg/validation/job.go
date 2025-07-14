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

// ValidateCreateJobRequest 验证创建任务请求.
func ValidateCreateJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.CreateJobRequest)

	if rq.Job == nil {
		sl.ReportError(rq.Job, "job", "Job", "required", "")
		return
	}

	if rq.Job.GetName() == "" {
		sl.ReportError(rq.Job.Name, "name", "Name", "required", "")
	}
}

// ValidateUpdateJobRequest 验证更新任务请求.
func ValidateUpdateJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.UpdateJobRequest)

	if rq.GetJobID() == "" {
		sl.ReportError(rq.JobID, "jobID", "JobID", "required", "")
	}
}

// ValidateDeleteJobRequest 验证删除任务请求.
func ValidateDeleteJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.DeleteJobRequest)

	if len(rq.GetJobIDs()) == 0 {
		sl.ReportError(rq.JobIDs, "jobIDs", "JobIDs", "required", "")
	}
}

// ValidateGetJobRequest 验证获取任务请求.
func ValidateGetJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.GetJobRequest)

	if rq.GetJobID() == "" {
		sl.ReportError(rq.JobID, "jobID", "JobID", "required", "")
	}
}

// ValidateListJobRequest 验证列表任务请求.
func ValidateListJobRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.ListJobRequest)

	if rq.GetLimit() <= 0 {
		sl.ReportError(rq.Limit, "limit", "Limit", "min", "1")
	}

	if rq.GetLimit() > 1000 {
		sl.ReportError(rq.Limit, "limit", "Limit", "max", "1000")
	}
}
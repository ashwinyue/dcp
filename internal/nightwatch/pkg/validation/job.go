// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package validation

import (
	"context"
	"fmt"

	"github.com/ashwinyue/dcp/internal/pkg/errno"
	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// ValidateCreateJobRequest 验证创建任务请求.
func (v *Validator) ValidateCreateJobRequest(ctx context.Context, req *apiv1.CreateJobRequest) error {
	if req.Job == nil {
		return errno.ErrInvalidArgument.WithMessage("job is required")
	}

	// 验证名称
	if req.Job.GetName() == "" {
		return errno.ErrInvalidArgument.WithMessage("name is required")
	}
	if !lengthRegex.MatchString(req.Job.GetName()) {
		return errno.ErrInvalidArgument.WithMessage("name length must be between 3 and 50 characters")
	}
	if !validRegex.MatchString(req.Job.GetName()) {
		return errno.ErrInvalidArgument.WithMessage("name can only contain letters, numbers, underscores and hyphens")
	}

	// 验证描述长度
	if req.Job.GetDescription() != "" && !descriptionLen.MatchString(req.Job.GetDescription()) {
		return errno.ErrInvalidArgument.WithMessage("description length must not exceed 500 characters")
	}

	return nil
}

// ValidateUpdateJobRequest 验证更新任务请求.
func (v *Validator) ValidateUpdateJobRequest(ctx context.Context, req *apiv1.UpdateJobRequest) error {
	if req.GetJobID() == "" {
		return errno.ErrInvalidArgument.WithMessage("jobID is required")
	}

	// 验证名称（如果提供）
	if req.GetName() != "" {
		if !lengthRegex.MatchString(req.GetName()) {
			return errno.ErrInvalidArgument.WithMessage("name length must be between 3 and 50 characters")
		}
		if !validRegex.MatchString(req.GetName()) {
			return errno.ErrInvalidArgument.WithMessage("name can only contain letters, numbers, underscores and hyphens")
		}
	}

	// 验证描述长度（如果提供）
	if req.GetDescription() != "" && !descriptionLen.MatchString(req.GetDescription()) {
		return errno.ErrInvalidArgument.WithMessage("description length must not exceed 500 characters")
	}

	return nil
}

// ValidateDeleteJobRequest 验证删除任务请求.
func (v *Validator) ValidateDeleteJobRequest(ctx context.Context, req *apiv1.DeleteJobRequest) error {
	if len(req.GetJobIDs()) == 0 {
		return errno.ErrInvalidArgument.WithMessage("jobIDs is required")
	}

	// 验证每个ID不为空
	for i, id := range req.GetJobIDs() {
		if id == "" {
			return errno.ErrInvalidArgument.WithMessage(fmt.Sprintf("jobIDs[%d] cannot be empty", i))
		}
	}

	return nil
}

// ValidateGetJobRequest 验证获取任务请求.
func (v *Validator) ValidateGetJobRequest(ctx context.Context, req *apiv1.GetJobRequest) error {
	if req.GetJobID() == "" {
		return errno.ErrInvalidArgument.WithMessage("jobID is required")
	}

	return nil
}

// ValidateListJobRequest 验证列表任务请求.
func (v *Validator) ValidateListJobRequest(ctx context.Context, req *apiv1.ListJobRequest) error {
	if req.GetLimit() <= 0 {
		return errno.ErrInvalidArgument.WithMessage("limit must be greater than 0")
	}

	if req.GetLimit() > 1000 {
		return errno.ErrInvalidArgument.WithMessage("limit must not exceed 1000")
	}

	if req.GetOffset() < 0 {
		return errno.ErrInvalidArgument.WithMessage("offset must be non-negative")
	}

	return nil
}
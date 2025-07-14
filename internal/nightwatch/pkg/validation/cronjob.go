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

// ValidateCreateCronJobRequest 验证创建定时任务请求.
func (v *Validator) ValidateCreateCronJobRequest(ctx context.Context, req *apiv1.CreateCronJobRequest) error {
	if req.CronJob == nil {
		return errno.ErrInvalidArgument.WithMessage("cronjob is required")
	}

	// 验证名称
	if req.CronJob.GetName() == "" {
		return errno.ErrInvalidArgument.WithMessage("name is required")
	}
	if !lengthRegex.MatchString(req.CronJob.GetName()) {
		return errno.ErrInvalidArgument.WithMessage("name length must be between 3 and 50 characters")
	}
	if !validRegex.MatchString(req.CronJob.GetName()) {
		return errno.ErrInvalidArgument.WithMessage("name can only contain letters, numbers, underscores and hyphens")
	}

	// 验证调度表达式
	if req.CronJob.GetSchedule() == "" {
		return errno.ErrInvalidArgument.WithMessage("schedule is required")
	}
	if !cronRegex.MatchString(req.CronJob.GetSchedule()) {
		return errno.ErrInvalidArgument.WithMessage("invalid cron expression format")
	}

	// 验证描述长度
	if req.CronJob.GetDescription() != "" && !descriptionLen.MatchString(req.CronJob.GetDescription()) {
		return errno.ErrInvalidArgument.WithMessage("description length must not exceed 500 characters")
	}

	// 验证历史记录限制
	if req.CronJob.GetSuccessHistoryLimit() < 0 || req.CronJob.GetSuccessHistoryLimit() > 100 {
		return errno.ErrInvalidArgument.WithMessage("success history limit must be between 0 and 100")
	}
	if req.CronJob.GetFailedHistoryLimit() < 0 || req.CronJob.GetFailedHistoryLimit() > 100 {
		return errno.ErrInvalidArgument.WithMessage("failed history limit must be between 0 and 100")
	}

	return nil
}

// ValidateUpdateCronJobRequest 验证更新定时任务请求.
func (v *Validator) ValidateUpdateCronJobRequest(ctx context.Context, req *apiv1.UpdateCronJobRequest) error {
	if req.GetCronJobID() == "" {
		return errno.ErrInvalidArgument.WithMessage("cronJobID is required")
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

	// 验证调度表达式（如果提供）
	if req.GetSchedule() != "" && !cronRegex.MatchString(req.GetSchedule()) {
		return errno.ErrInvalidArgument.WithMessage("invalid cron expression format")
	}

	// 验证描述长度（如果提供）
	if req.GetDescription() != "" && !descriptionLen.MatchString(req.GetDescription()) {
		return errno.ErrInvalidArgument.WithMessage("description length must not exceed 500 characters")
	}

	// 验证历史记录限制（如果提供）
	if req.SuccessHistoryLimit != nil && (*req.SuccessHistoryLimit < 0 || *req.SuccessHistoryLimit > 100) {
		return errno.ErrInvalidArgument.WithMessage("success history limit must be between 0 and 100")
	}
	if req.FailedHistoryLimit != nil && (*req.FailedHistoryLimit < 0 || *req.FailedHistoryLimit > 100) {
		return errno.ErrInvalidArgument.WithMessage("failed history limit must be between 0 and 100")
	}

	return nil
}

// ValidateDeleteCronJobRequest 验证删除定时任务请求.
func (v *Validator) ValidateDeleteCronJobRequest(ctx context.Context, req *apiv1.DeleteCronJobRequest) error {
	if len(req.GetCronJobIDs()) == 0 {
		return errno.ErrInvalidArgument.WithMessage("cronJobIDs is required")
	}

	// 验证每个ID不为空
	for i, id := range req.GetCronJobIDs() {
		if id == "" {
			return errno.ErrInvalidArgument.WithMessage(fmt.Sprintf("cronJobIDs[%d] cannot be empty", i))
		}
	}

	return nil
}

// ValidateGetCronJobRequest 验证获取定时任务请求.
func (v *Validator) ValidateGetCronJobRequest(ctx context.Context, req *apiv1.GetCronJobRequest) error {
	if req.GetCronJobID() == "" {
		return errno.ErrInvalidArgument.WithMessage("cronJobID is required")
	}

	return nil
}

// ValidateListCronJobRequest 验证列表定时任务请求.
func (v *Validator) ValidateListCronJobRequest(ctx context.Context, req *apiv1.ListCronJobRequest) error {
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

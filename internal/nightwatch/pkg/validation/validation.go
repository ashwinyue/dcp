// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package validation

import (
	"context"
	"regexp"

	"github.com/google/wire"

	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// Validator 是验证逻辑的实现结构体.
type Validator struct {
	// 有些复杂的验证逻辑，可能需要直接查询数据库
	// 这里只是一个举例，如果验证时，有其他依赖的客户端/服务/资源等，
	// 都可以一并注入进来
	store store.IStore
}

// 使用预编译的全局正则表达式，避免重复创建和编译.
var (
	lengthRegex    = regexp.MustCompile(`^.{3,50}$`)                                         // 长度在 3 到 50 个字符之间
	validRegex     = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)                                  // 仅包含字母、数字、下划线和连字符
	cronRegex      = regexp.MustCompile(`^\s*(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$`) // Quartz Cron 表达式格式
	watcherRegex   = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)                                 // Watcher 名称格式
	descriptionLen = regexp.MustCompile(`^.{0,500}$`)                                        // 描述长度限制
)

// ProviderSet 是一个 Wire 的 Provider 集合，用于声明依赖注入的规则.
// 包含 New 构造函数，用于生成 Validator 实例.
var ProviderSet = wire.NewSet(New)

// New 创建一个新的 Validator 实例.
func New(store store.IStore) *Validator {
	return &Validator{store: store}
}

// isValidName 检查名称是否有效.
func isValidName(name string) bool {
	return lengthRegex.MatchString(name) && validRegex.MatchString(name)
}

// isValidCronExpression 检查Cron表达式是否有效.
func isValidCronExpression(cron string) bool {
	return cronRegex.MatchString(cron)
}

// isValidDescription 检查描述是否有效.
func isValidDescription(description string) bool {
	return descriptionLen.MatchString(description)
}

// isValidWatcher 检查Watcher名称是否有效.
func isValidWatcher(watcher string) bool {
	return watcherRegex.MatchString(watcher)
}

// Validate 验证请求参数.
func (v *Validator) Validate(ctx context.Context, req any) error {
	switch rq := req.(type) {
	case *apiv1.CreateCronJobRequest:
		return v.ValidateCreateCronJobRequest(ctx, rq)
	case *apiv1.UpdateCronJobRequest:
		return v.ValidateUpdateCronJobRequest(ctx, rq)
	case *apiv1.DeleteCronJobRequest:
		return v.ValidateDeleteCronJobRequest(ctx, rq)
	case *apiv1.GetCronJobRequest:
		return v.ValidateGetCronJobRequest(ctx, rq)
	case *apiv1.ListCronJobRequest:
		return v.ValidateListCronJobRequest(ctx, rq)
	case *apiv1.CreateJobRequest:
		return v.ValidateCreateJobRequest(ctx, rq)
	case *apiv1.UpdateJobRequest:
		return v.ValidateUpdateJobRequest(ctx, rq)
	case *apiv1.DeleteJobRequest:
		return v.ValidateDeleteJobRequest(ctx, rq)
	case *apiv1.GetJobRequest:
		return v.ValidateGetJobRequest(ctx, rq)
	case *apiv1.ListJobRequest:
		return v.ValidateListJobRequest(ctx, rq)
	default:
		return nil
	}
}

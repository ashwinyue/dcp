// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package validation

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
	"github.com/go-playground/validator/v10"
	genericvalidation "github.com/onexstack/onexstack/pkg/validation"

	"github.com/ashwinyue/dcp/internal/pkg/errno"
	apiv1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// ValidatePostRules 校验字段的有效性.
func (v *Validator) ValidatePostRules() genericvalidation.Rules {
	// 定义各字段的校验逻辑，通过一个 map 实现模块化和简化
	return genericvalidation.Rules{
		"PostID": func(value any) error {
			if value.(string) == "" {
				return errno.ErrInvalidArgument.WithMessage("postID cannot be empty")
			}
			return nil
		},
		"Title": func(value any) error {
			if value.(string) == "" {
				return errno.ErrInvalidArgument.WithMessage("title cannot be empty")
			}
			return nil
		},
		"Content": func(value any) error {
			if value.(string) == "" {
				return errno.ErrInvalidArgument.WithMessage("content cannot be empty")
			}
			return nil
		},
	}
}

// ValidateCreatePostRequest 校验 CreatePostRequest 结构体的有效性.
func (v *Validator) ValidateCreatePostRequest(ctx context.Context, rq *apiv1.CreatePostRequest) error {
	return genericvalidation.ValidateAllFields(rq, v.ValidatePostRules())
}

// ValidateUpdatePostRequest 校验更新用户请求.
func (v *Validator) ValidateUpdatePostRequest(ctx context.Context, rq *apiv1.UpdatePostRequest) error {
	return genericvalidation.ValidateAllFields(rq, v.ValidatePostRules())
}

// ValidateDeletePostRequest 校验 DeletePostRequest 结构体的有效性.
func (v *Validator) ValidateDeletePostRequest(ctx context.Context, rq *apiv1.DeletePostRequest) error {
	return genericvalidation.ValidateAllFields(rq, v.ValidatePostRules())
}

// ValidateGetPostRequest 校验 GetPostRequest 结构体的有效性.
func (v *Validator) ValidateGetPostRequest(ctx context.Context, rq *apiv1.GetPostRequest) error {
	return genericvalidation.ValidateAllFields(rq, v.ValidatePostRules())
}

// ValidateListPostRequest 校验 ListPostRequest 结构体的有效性.
func (v *Validator) ValidateListPostRequest(ctx context.Context, rq *apiv1.ListPostRequest) error {
	if err := validation.Validate(rq.GetTitle(), validation.Length(5, 100), is.URL); err != nil {
		return errno.ErrInvalidArgument.WithMessage(err.Error())
	}
	return genericvalidation.ValidateSelectedFields(rq, v.ValidatePostRules(), "Offset", "Limit")
}

// ValidateCreatePostRequest 验证创建帖子请求（新版本）.
func ValidateCreatePostRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.CreatePostRequest)

	if rq.GetTitle() == "" {
		sl.ReportError(rq.Title, "title", "Title", "required", "")
	}

	if rq.GetContent() == "" {
		sl.ReportError(rq.Content, "content", "Content", "required", "")
	}
}

// ValidateUpdatePostRequest 验证更新帖子请求（新版本）.
func ValidateUpdatePostRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.UpdatePostRequest)

	if rq.GetPostID() == "" {
		sl.ReportError(rq.PostID, "postID", "PostID", "required", "")
	}
}

// ValidateDeletePostRequest 验证删除帖子请求（新版本）.
func ValidateDeletePostRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.DeletePostRequest)

	if len(rq.GetPostIDs()) == 0 {
		sl.ReportError(rq.PostIDs, "postIDs", "PostIDs", "required", "")
	}
}

// ValidateGetPostRequest 验证获取帖子请求（新版本）.
func ValidateGetPostRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.GetPostRequest)

	if rq.GetPostID() == "" {
		sl.ReportError(rq.PostID, "postID", "PostID", "required", "")
	}
}

// ValidateListPostRequest 验证列表帖子请求（新版本）.
func ValidateListPostRequest(sl validator.StructLevel) {
	rq := sl.Current().Interface().(apiv1.ListPostRequest)

	if rq.GetLimit() <= 0 {
		sl.ReportError(rq.Limit, "limit", "Limit", "min", "1")
	}

	if rq.GetLimit() > 1000 {
		sl.ReportError(rq.Limit, "limit", "Limit", "max", "1000")
	}
}

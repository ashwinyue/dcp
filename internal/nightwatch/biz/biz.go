// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package biz

//go:generate mockgen -destination mock_biz.go -package biz github.com/ashwinyue/dcp/internal/apiserver/biz IBiz

import (
	"github.com/google/wire"

	cronjobv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/cronjob"
	jobv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/job"
	postv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/post"

	// Post V2 版本（未实现，仅展示用）
	// postv2 "github.com/ashwinyue/dcp/internal/apiserver/biz/v2/post".
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
)

// ProviderSet 是一个 Wire 的 Provider 集合，用于声明依赖注入的规则.
// 包含 NewBiz 构造函数，用于生成 biz 实例.
// wire.Bind 用于将接口 IBiz 与具体实现 *biz 绑定，
// 这样依赖 IBiz 的地方会自动注入 *biz 实例.
var ProviderSet = wire.NewSet(NewBiz, wire.Bind(new(IBiz), new(*biz)))

// IBiz 定义了业务层需要实现的方法.
type IBiz interface {
	// CronJobV1 获取定时任务业务接口.
	CronJobV1() cronjobv1.CronJobBiz
	// JobV1 获取任务业务接口.
	JobV1() jobv1.JobBiz
	// PostV1 获取帖子业务接口.
	PostV1() postv1.PostBiz
	// PostV2 获取帖子业务接口（V2 版本）.
	// PostV2() post.PostBiz
}

// biz 是 IBiz 的一个具体实现.
type biz struct {
	store store.IStore
}

// 确保 biz 实现了 IBiz 接口.
var _ IBiz = (*biz)(nil)

// NewBiz 创建一个 IBiz 类型的实例.
func NewBiz(store store.IStore) *biz {
	return &biz{store: store}
}

// CronJobV1 返回一个实现了 CronJobBiz 接口的实例.
func (b *biz) CronJobV1() cronjobv1.CronJobBiz {
	return cronjobv1.New(b.store)
}

// JobV1 返回一个实现了 JobBiz 接口的实例.
func (b *biz) JobV1() jobv1.JobBiz {
	return jobv1.New(b.store)
}

// PostV1 返回一个实现了 PostBiz 接口的实例.
func (b *biz) PostV1() postv1.PostBiz {
	return postv1.New(b.store)
}

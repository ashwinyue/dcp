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
	documentv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/document"
	jobv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/job"
	postv1 "github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/post"

	// Post V2 版本（未实现，仅展示用）
	// postv2 "github.com/ashwinyue/dcp/internal/apiserver/biz/v2/post".
	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
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

	// Syncer related methods
	// CacheManager 获取缓存管理器.
	CacheManager() *cache.CacheManager
	// SyncerManager 获取同步器管理器.
	SyncerManager() syncer.SyncerManager
	// StatusSyncer 获取状态同步器.
	StatusSyncer() syncer.StatusSyncer
	// StatisticsSyncer 获取统计同步器.
	StatisticsSyncer() syncer.StatisticsSyncer
	// PartitionSyncer 获取分区同步器.
	PartitionSyncer() syncer.PartitionSyncer
	// HeartbeatSyncer 获取心跳同步器.
	HeartbeatSyncer() syncer.HeartbeatSyncer
}

// biz is the business layer.
type biz struct {
	store         store.IStore
	cacheManager  *cache.CacheManager
	syncerManager syncer.SyncerManager
}

// NewBiz creates a new biz instance.
func NewBiz(store store.IStore, cacheManager *cache.CacheManager, syncerManager syncer.SyncerManager) *biz {
	return &biz{
		store:         store,
		cacheManager:  cacheManager,
		syncerManager: syncerManager,
	}
}

// CacheManager returns the cache manager.
func (b *biz) CacheManager() *cache.CacheManager {
	return b.cacheManager
}

// SyncerManager returns the syncer manager.
func (b *biz) SyncerManager() syncer.SyncerManager {
	return b.syncerManager
}

// StatusSyncer returns the status syncer.
func (b *biz) StatusSyncer() syncer.StatusSyncer {
	return b.syncerManager.GetStatusSyncer()
}

// StatisticsSyncer returns the statistics syncer.
func (b *biz) StatisticsSyncer() syncer.StatisticsSyncer {
	return b.syncerManager.GetStatisticsSyncer()
}

// PartitionSyncer returns the partition syncer.
func (b *biz) PartitionSyncer() syncer.PartitionSyncer {
	return b.syncerManager.GetPartitionSyncer()
}

// HeartbeatSyncer returns the heartbeat syncer.
func (b *biz) HeartbeatSyncer() syncer.HeartbeatSyncer {
	return b.syncerManager.GetHeartbeatSyncer()
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

// Document 返回一个实现了 DocumentBiz 接口的实例.
func (b *biz) Document() documentv1.DocumentBiz {
	return documentv1.New(b.store)
}


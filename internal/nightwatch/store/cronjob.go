// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package store

import (
	"context"

	"github.com/onexstack/onexstack/pkg/store/where"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
)

// CronJobStore 定义了 cronjob 模块在 store 层所实现的方法.
type CronJobStore interface {
	Create(ctx context.Context, cronJob *model.CronJobM) error
	Get(ctx context.Context, where where.Where) (*model.CronJobM, error)
	Update(ctx context.Context, cronJob *model.CronJobM) error
	List(ctx context.Context, where where.Where) (count int64, ret []*model.CronJobM, err error)
	Delete(ctx context.Context, where where.Where) error
}

// cronJobStore 是 CronJobStore 的一个具体实现.
type cronJobStore struct {
	store *datastore
}

// 确保 cronJobStore 实现了 CronJobStore 接口.
var _ CronJobStore = (*cronJobStore)(nil)

// newCronJobStore 创建一个 cronJobStore 实例.
func newCronJobStore(store *datastore) *cronJobStore {
	return &cronJobStore{store: store}
}

// Create 插入一条 cronjob 记录.
func (c *cronJobStore) Create(ctx context.Context, cronJob *model.CronJobM) error {
	return c.store.DB(ctx).Create(&cronJob).Error
}

// Get 根据 where 条件，查询指定的 cronjob 数据库记录.
func (c *cronJobStore) Get(ctx context.Context, where where.Where) (*model.CronJobM, error) {
	var cronJob model.CronJobM
	if err := c.store.DB(ctx, where).First(&cronJob).Error; err != nil {
		return nil, err
	}

	return &cronJob, nil
}

// Update 更新一条 cronjob 数据库记录.
func (c *cronJobStore) Update(ctx context.Context, cronJob *model.CronJobM) error {
	return c.store.DB(ctx).Save(cronJob).Error
}

// List 根据 where 条件，分页查询 cronjob 数据库记录.
func (c *cronJobStore) List(ctx context.Context, where where.Where) (count int64, ret []*model.CronJobM, err error) {
	db := c.store.DB(ctx, where)

	if err := db.Model(&model.CronJobM{}).Count(&count).Error; err != nil {
		return 0, nil, err
	}

	if err := db.Find(&ret).Error; err != nil {
		return 0, nil, err
	}

	return count, ret, nil
}

// Delete 根据 where 条件，删除 cronjob 数据库记录.
func (c *cronJobStore) Delete(ctx context.Context, where where.Where) error {
	return c.store.DB(ctx, where).Delete(&model.CronJobM{}).Error
}

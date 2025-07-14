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

// JobStore 定义了 job 模块在 store 层所实现的方法.
type JobStore interface {
	Create(ctx context.Context, job *model.JobM) error
	Get(ctx context.Context, where where.Where) (*model.JobM, error)
	Update(ctx context.Context, job *model.JobM) error
	List(ctx context.Context, where where.Where) (count int64, ret []*model.JobM, err error)
	Delete(ctx context.Context, where where.Where) error
}

// jobStore 是 JobStore 的一个具体实现.
type jobStore struct {
	store *datastore
}

// 确保 jobStore 实现了 JobStore 接口.
var _ JobStore = (*jobStore)(nil)

// newJobStore 创建一个 jobStore 实例.
func newJobStore(store *datastore) *jobStore {
	return &jobStore{store: store}
}

// Create 插入一条 job 记录.
func (j *jobStore) Create(ctx context.Context, job *model.JobM) error {
	return j.store.DB(ctx).Create(&job).Error
}

// Get 根据 where 条件，查询指定的 job 数据库记录.
func (j *jobStore) Get(ctx context.Context, where where.Where) (*model.JobM, error) {
	var job model.JobM
	if err := j.store.DB(ctx, where).First(&job).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

// Update 更新一条 job 数据库记录.
func (j *jobStore) Update(ctx context.Context, job *model.JobM) error {
	return j.store.DB(ctx).Save(job).Error
}

// List 根据 where 条件，分页查询 job 数据库记录.
func (j *jobStore) List(ctx context.Context, where where.Where) (count int64, ret []*model.JobM, err error) {
	db := j.store.DB(ctx, where)

	if err := db.Model(&model.JobM{}).Count(&count).Error; err != nil {
		return 0, nil, err
	}

	if err := db.Find(&ret).Error; err != nil {
		return 0, nil, err
	}

	return count, ret, nil
}

// Delete 根据 where 条件，删除 job 数据库记录.
func (j *jobStore) Delete(ctx context.Context, where where.Where) error {
	return j.store.DB(ctx, where).Delete(&model.JobM{}).Error
}

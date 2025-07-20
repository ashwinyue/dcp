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
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// MessageBatchJobStore 定义了 messagebatch job 模块在 store 层所实现的方法.
type MessageBatchJobStore interface {
	Create(ctx context.Context, job *model.MessageBatchJobM) error
	Get(ctx context.Context, where where.Where) (*model.MessageBatchJobM, error)
	Update(ctx context.Context, job *model.MessageBatchJobM) error
	UpdateJob(ctx context.Context, job *model.MessageBatchJobM) error
	List(ctx context.Context, where where.Where) (count int64, ret []*model.MessageBatchJobM, err error)
	Delete(ctx context.Context, where where.Where) error
	FindRunnableJobs(ctx context.Context, statuses []v1.MessageBatchJobStatus) ([]*model.MessageBatchJobM, error)
}

// messageBatchJobStore 是 MessageBatchJobStore 的一个具体实现.
type messageBatchJobStore struct {
	store *datastore
}

// 确保 messageBatchJobStore 实现了 MessageBatchJobStore 接口.
var _ MessageBatchJobStore = (*messageBatchJobStore)(nil)

// newMessageBatchJobStore 创建一个 messageBatchJobStore 实例.
func newMessageBatchJobStore(store *datastore) *messageBatchJobStore {
	return &messageBatchJobStore{store: store}
}

// Create 插入一条 messagebatch job 记录.
func (m *messageBatchJobStore) Create(ctx context.Context, job *model.MessageBatchJobM) error {
	return m.store.DB(ctx).Create(&job).Error
}

// Get 根据 where 条件，查询指定的 messagebatch job 数据库记录.
func (m *messageBatchJobStore) Get(ctx context.Context, where where.Where) (*model.MessageBatchJobM, error) {
	var job model.MessageBatchJobM
	if err := m.store.DB(ctx, where).First(&job).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

// Update 更新一条 messagebatch job 数据库记录.
func (m *messageBatchJobStore) Update(ctx context.Context, job *model.MessageBatchJobM) error {
	return m.store.DB(ctx).Save(job).Error
}

// UpdateJob 更新一条 messagebatch job 数据库记录 (别名方法).
func (m *messageBatchJobStore) UpdateJob(ctx context.Context, job *model.MessageBatchJobM) error {
	return m.Update(ctx, job)
}

// List 根据 where 条件，分页查询 messagebatch job 数据库记录.
func (m *messageBatchJobStore) List(ctx context.Context, where where.Where) (count int64, ret []*model.MessageBatchJobM, err error) {
	db := m.store.DB(ctx, where)

	if err := db.Model(&model.MessageBatchJobM{}).Count(&count).Error; err != nil {
		return 0, nil, err
	}

	if err := db.Find(&ret).Error; err != nil {
		return 0, nil, err
	}

	return count, ret, nil
}

// Delete 根据 where 条件，删除 messagebatch job 数据库记录.
func (m *messageBatchJobStore) Delete(ctx context.Context, where where.Where) error {
	return m.store.DB(ctx, where).Delete(&model.MessageBatchJobM{}).Error
}

// FindRunnableJobs 查找可运行的 messagebatch job 记录.
func (m *messageBatchJobStore) FindRunnableJobs(ctx context.Context, statuses []v1.MessageBatchJobStatus) ([]*model.MessageBatchJobM, error) {
	var jobs []*model.MessageBatchJobM
	
	// 将枚举状态转换为字符串
	statusStrings := make([]string, len(statuses))
	for i, status := range statuses {
		statusStrings[i] = status.String()
	}
	
	if err := m.store.DB(ctx).Where("status IN ?", statusStrings).Find(&jobs).Error; err != nil {
		return nil, err
	}

	return jobs, nil
}
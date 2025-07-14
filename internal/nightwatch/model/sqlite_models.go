// Copyright 2022 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/superproj/onex.

package model

import (
	"time"

	"gorm.io/gorm"
)

// CronJobSQLite SQLite兼容的定时任务模型.
type CronJobSQLite struct {
	ID          uint64         `gorm:"column:id;type:integer;primary_key;AUTO_INCREMENT;comment:主键" json:"id"`
	TaskID      string         `gorm:"column:taskID;type:varchar(100);not null;uniqueIndex:idx_task_id;comment:任务唯一标识" json:"taskID"`
	Name        string         `gorm:"column:name;type:varchar(255);not null;comment:任务名称" json:"name"`
	Cron        string         `gorm:"column:cron;type:varchar(100);not null;comment:Cron 表达式" json:"cron"`
	Command     string         `gorm:"column:command;type:text;not null;comment:执行命令" json:"command"`
	Args        string         `gorm:"column:args;type:text;comment:命令参数" json:"args"`
	Timeout     int            `gorm:"column:timeout;type:integer;not null;default:3600;comment:超时时间(秒)" json:"timeout"`
	Retries     int            `gorm:"column:retries;type:integer;not null;default:0;comment:重试次数" json:"retries"`
	Status      CronJobStatus  `gorm:"column:status;type:varchar(20);not null;default:'enabled';comment:任务状态" json:"status"`
	Description string         `gorm:"column:description;type:text;comment:任务描述" json:"description"`
	CreatedAt   time.Time      `gorm:"column:createdAt;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间" json:"createdAt"`
	UpdatedAt   time.Time      `gorm:"column:updatedAt;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间" json:"updatedAt"`
	DeletedAt   gorm.DeletedAt `gorm:"column:deletedAt;type:datetime;index;comment:删除时间" json:"deletedAt"`
}

// TableName 设置表名.
func (CronJobSQLite) TableName() string {
	return "nw_cronjob"
}

// JobSQLite SQLite兼容的任务执行记录模型.
type JobSQLite struct {
	ID        uint64        `gorm:"column:id;type:integer;primary_key;AUTO_INCREMENT;comment:主键" json:"id"`
	TaskID    string        `gorm:"column:taskID;type:varchar(100);not null;index:idx_task_id;comment:任务唯一标识" json:"taskID"`
	JobID     string        `gorm:"column:jobID;type:varchar(100);not null;uniqueIndex:idx_job_id;comment:任务执行唯一标识" json:"jobID"`
	Command   string        `gorm:"column:command;type:text;not null;comment:执行命令" json:"command"`
	Args      string        `gorm:"column:args;type:text;comment:命令参数" json:"args"`
	Timeout   int           `gorm:"column:timeout;type:integer;not null;default:3600;comment:超时时间(秒)" json:"timeout"`
	Params    JobParams     `gorm:"column:params;type:text;comment:任务参数" json:"params"`
	Results   JobResults    `gorm:"column:results;type:text;comment:任务结果" json:"results"`
	Condition JobConditions `gorm:"column:condition;type:text;comment:任务状态" json:"condition"`
	CreatedAt time.Time     `gorm:"column:createdAt;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间" json:"createdAt"`
	UpdatedAt time.Time     `gorm:"column:updatedAt;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间" json:"updatedAt"`
}

// TableName 设置表名.
func (JobSQLite) TableName() string {
	return "nw_job"
}

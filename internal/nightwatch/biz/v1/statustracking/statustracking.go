// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package statustracking

//go:generate mockgen -destination mock_statustracking.go -package statustracking github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/statustracking StatusTrackingBiz

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/onexstack/onexstack/pkg/store/where"

	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// StatusTrackingBiz 定义状态跟踪业务层接口
type StatusTrackingBiz interface {
	// UpdateJobStatus 更新任务状态
	UpdateJobStatus(ctx context.Context, rq *v1.UpdateJobStatusRequest) (*v1.UpdateJobStatusResponse, error)

	// GetJobStatus 获取任务状态
	GetJobStatus(ctx context.Context, rq *v1.GetJobStatusRequest) (*v1.GetJobStatusResponse, error)

	// TrackRunningJobs 跟踪运行中的任务
	TrackRunningJobs(ctx context.Context, rq *v1.TrackRunningJobsRequest) (*v1.TrackRunningJobsResponse, error)

	// GetJobStatistics 获取任务统计信息
	GetJobStatistics(ctx context.Context, rq *v1.GetJobStatisticsRequest) (*v1.GetJobStatisticsResponse, error)

	// GetBatchStatistics 获取批处理统计信息
	GetBatchStatistics(ctx context.Context, rq *v1.GetBatchStatisticsRequest) (*v1.GetBatchStatisticsResponse, error)

	// StatusTrackingExpansion 扩展方法
	StatusTrackingExpansion
}

// StatusTrackingExpansion 定义状态跟踪扩展方法
type StatusTrackingExpansion interface {
	// MonitorJobHealth 监控任务健康状态
	MonitorJobHealth(ctx context.Context, jobID string) (*v1.JobHealthStatus, error)

	// GetSystemMetrics 获取系统级别监控指标
	GetSystemMetrics(ctx context.Context) (*v1.SystemMetrics, error)

	// CleanupExpiredStatus 清理过期状态信息
	CleanupExpiredStatus(ctx context.Context, olderThan time.Duration) error

	// StartStatusWatcher 启动状态监控器
	StartStatusWatcher(ctx context.Context) error

	// StopStatusWatcher 停止状态监控器
	StopStatusWatcher() error
}

// statusTrackingBiz 是 StatusTrackingBiz 的实现
type statusTrackingBiz struct {
	store  store.IStore
	cache  *cache.CacheManager
	mu     sync.RWMutex
	stopCh chan struct{}
}

// BatchStatusInfo 批处理状态信息
type BatchStatusInfo struct {
	BatchID        string            `json:"batch_id"`
	TotalTasks     int64             `json:"total_tasks"`
	CompletedTasks int64             `json:"completed_tasks"`
	FailedTasks    int64             `json:"failed_tasks"`
	Progress       float32           `json:"progress"`
	StartTime      time.Time         `json:"start_time"`
	EstimatedETA   *time.Time        `json:"estimated_eta,omitempty"`
	Partitions     map[string]string `json:"partitions"`
}

// 确保 statusTrackingBiz 实现了 StatusTrackingBiz 接口
var _ StatusTrackingBiz = (*statusTrackingBiz)(nil)

// New 创建 StatusTrackingBiz 实例
func New(store store.IStore, cacheManager *cache.CacheManager) StatusTrackingBiz {
	return &statusTrackingBiz{
		store:  store,
		cache:  cacheManager,
		stopCh: make(chan struct{}),
	}
}

// UpdateJobStatus 实现更新任务状态方法
func (b *statusTrackingBiz) UpdateJobStatus(ctx context.Context, rq *v1.UpdateJobStatusRequest) (*v1.UpdateJobStatusResponse, error) {
	// 获取当前任务
	whr := where.T(ctx).F("jobID", rq.GetJobId())
	job, err := b.store.Job().Get(ctx, whr)
	if err != nil {
		return nil, fmt.Errorf("获取任务失败: %w", err)
	}

	// 记录状态变化
	oldStatus := job.Status
	job.Status = rq.GetStatus()
	job.UpdatedAt = time.Now()

	// 如果状态转换为运行中，记录开始时间
	if rq.GetStatus() == "Running" && oldStatus != "Running" {
		job.StartedAt = time.Now()
	}

	// 如果状态转换为结束状态，记录结束时间
	if rq.GetStatus() == "Succeeded" || rq.GetStatus() == "Failed" {
		job.EndedAt = time.Now()
	}

	// 更新任务条件
	if rq.GetCondition() != nil {
		if job.Conditions == nil {
			job.Conditions = &model.JobConditions{}
		}
		// 这里需要实现条件更新逻辑
	}

	// 更新数据库
	if err := b.store.Job().Update(ctx, job); err != nil {
		return nil, fmt.Errorf("更新任务状态失败: %w", err)
	}

	// 更新缓存
	if b.cache != nil {
		statusKey := fmt.Sprintf("job_status:%s", rq.GetJobId())
		statusInfo := map[string]interface{}{
			"status":     job.Status,
			"updated_at": job.UpdatedAt.Unix(),
			"started_at": job.StartedAt.Unix(),
			"ended_at":   job.EndedAt.Unix(),
		}

		if err := b.cache.Set(ctx, statusKey, statusInfo, 24*time.Hour); err != nil {
			log.Errorw("更新状态缓存失败", "error", err, "job_id", rq.GetJobId())
		}
	}

	log.Infow("任务状态更新成功",
		"job_id", rq.GetJobId(),
		"old_status", oldStatus,
		"new_status", rq.GetStatus(),
	)

	return &v1.UpdateJobStatusResponse{
		Success: true,
		Message: "状态更新成功",
	}, nil
}

// GetJobStatus 实现获取任务状态方法
func (b *statusTrackingBiz) GetJobStatus(ctx context.Context, rq *v1.GetJobStatusRequest) (*v1.GetJobStatusResponse, error) {
	// 先尝试从缓存获取
	if b.cache != nil {
		statusKey := fmt.Sprintf("job_status:%s", rq.GetJobId())
		var cachedStatus map[string]interface{}
		if err := b.cache.Get(ctx, statusKey, &cachedStatus); err == nil {
			return &v1.GetJobStatusResponse{
				JobId:     rq.GetJobId(),
				Status:    cachedStatus["status"].(string),
				UpdatedAt: cachedStatus["updated_at"].(int64),
				FromCache: true,
			}, nil
		}
	}

	// 从数据库获取
	whr := where.T(ctx).F("jobID", rq.GetJobId())
	job, err := b.store.Job().Get(ctx, whr)
	if err != nil {
		return nil, fmt.Errorf("获取任务状态失败: %w", err)
	}

	// 计算运行时间
	var runtimeSeconds int64
	if !job.StartedAt.IsZero() && !job.EndedAt.IsZero() {
		runtimeSeconds = int64(job.EndedAt.Sub(job.StartedAt).Seconds())
	} else if !job.StartedAt.IsZero() {
		runtimeSeconds = int64(time.Since(job.StartedAt).Seconds())
	}

	return &v1.GetJobStatusResponse{
		JobId:          rq.GetJobId(),
		Status:         job.Status,
		UpdatedAt:      job.UpdatedAt.Unix(),
		StartedAt:      job.StartedAt.Unix(),
		EndedAt:        job.EndedAt.Unix(),
		RuntimeSeconds: runtimeSeconds,
		FromCache:      false,
	}, nil
}

// TrackRunningJobs 实现跟踪运行中任务方法
func (b *statusTrackingBiz) TrackRunningJobs(ctx context.Context, rq *v1.TrackRunningJobsRequest) (*v1.TrackRunningJobsResponse, error) {
	// 查询运行中的任务
	runningStatuses := []string{
		"Running",
		"Processing",
		"PreparationRunning",
		"DeliveryRunning",
	}

	whereOpts := where.T(ctx).F("status", runningStatuses)
	if rq.GetScope() != "" {
		whereOpts = whereOpts.F("scope", rq.GetScope())
	}
	if rq.GetWatcher() != "" {
		whereOpts = whereOpts.F("watcher", rq.GetWatcher())
	}

	count, jobs, err := b.store.Job().List(ctx, whereOpts)
	if err != nil {
		return nil, fmt.Errorf("查询运行中任务失败: %w", err)
	}

	// 构造响应
	var runningJobs []*v1.RunningJobInfo
	for _, job := range jobs {
		// 检查任务健康状态
		isHealthy := b.checkJobHealth(ctx, job)

		// 计算运行时间
		runtimeSeconds := int64(0)
		if !job.StartedAt.IsZero() {
			runtimeSeconds = int64(time.Since(job.StartedAt).Seconds())
		}

		runningJobs = append(runningJobs, &v1.RunningJobInfo{
			JobId:          job.JobID,
			Status:         job.Status,
			StartedAt:      job.StartedAt.Unix(),
			RuntimeSeconds: runtimeSeconds,
			IsHealthy:      isHealthy,
			Watcher:        job.Watcher,
			Scope:          job.Scope,
		})
	}

	return &v1.TrackRunningJobsResponse{
		TotalCount:  count,
		RunningJobs: runningJobs,
		TrackTime:   time.Now().Unix(),
	}, nil
}

// GetJobStatistics 实现获取任务统计方法
func (b *statusTrackingBiz) GetJobStatistics(ctx context.Context, rq *v1.GetJobStatisticsRequest) (*v1.GetJobStatisticsResponse, error) {
	// 构建时间范围查询条件
	whereOpts := where.T(ctx)
	if rq.GetStartTime() > 0 {
		whereOpts = whereOpts.F("created_at", ">=", time.Unix(rq.GetStartTime(), 0))
	}
	if rq.GetEndTime() > 0 {
		whereOpts = whereOpts.F("created_at", "<=", time.Unix(rq.GetEndTime(), 0))
	}
	if rq.GetScope() != "" {
		whereOpts = whereOpts.F("scope", rq.GetScope())
	}
	if rq.GetWatcher() != "" {
		whereOpts = whereOpts.F("watcher", rq.GetWatcher())
	}

	// 查询任务列表
	_, jobs, err := b.store.Job().List(ctx, whereOpts)
	if err != nil {
		return nil, fmt.Errorf("查询任务统计失败: %w", err)
	}

	// 统计各状态任务数量
	statusCounts := make(map[string]int64)
	var totalRuntime int64
	var completedJobs int64

	for _, job := range jobs {
		statusCounts[job.Status]++

		if !job.StartedAt.IsZero() && !job.EndedAt.IsZero() {
			runtime := int64(job.EndedAt.Sub(job.StartedAt).Seconds())
			totalRuntime += runtime
			completedJobs++
		}
	}

	// 计算平均运行时间
	var avgRuntimeSeconds int64
	if completedJobs > 0 {
		avgRuntimeSeconds = totalRuntime / completedJobs
	}

	// 计算成功率
	successRate := float64(0)
	totalFinished := statusCounts["Succeeded"] + statusCounts["Failed"]
	if totalFinished > 0 {
		successRate = float64(statusCounts["Succeeded"]) / float64(totalFinished) * 100
	}

	return &v1.GetJobStatisticsResponse{
		TotalJobs:         int64(len(jobs)),
		PendingJobs:       statusCounts["Pending"],
		RunningJobs:       statusCounts["Running"],
		SucceededJobs:     statusCounts["Succeeded"],
		FailedJobs:        statusCounts["Failed"],
		AvgRuntimeSeconds: avgRuntimeSeconds,
		SuccessRate:       successRate,
		GeneratedAt:       time.Now().Unix(),
	}, nil
}

// GetBatchStatistics 实现获取批处理统计方法
func (b *statusTrackingBiz) GetBatchStatistics(ctx context.Context, rq *v1.GetBatchStatisticsRequest) (*v1.GetBatchStatisticsResponse, error) {
	// 查询批处理任务
	whereOpts := where.T(ctx).F("watcher", "messagebatch")
	if rq.GetBatchId() != "" {
		whereOpts = whereOpts.F("jobID", rq.GetBatchId())
	}

	_, jobs, err := b.store.Job().List(ctx, whereOpts)
	if err != nil {
		return nil, fmt.Errorf("查询批处理统计失败: %w", err)
	}

	var batchStats []*v1.BatchStatusInfo
	for _, job := range jobs {
		// 解析批处理结果
		stats := &v1.BatchStatusInfo{
			BatchId:   job.JobID,
			Status:    job.Status,
			StartedAt: job.StartedAt.Unix(),
			UpdatedAt: job.UpdatedAt.Unix(),
		}

		if job.Results != nil && job.Results.MessageBatch != nil {
			mb := job.Results.MessageBatch
			if mb.TotalMessages != nil {
				stats.TotalMessages = *mb.TotalMessages
			}
			if mb.ProcessedMessages != nil {
				stats.ProcessedMessages = *mb.ProcessedMessages
			}
			if mb.FailedMessages != nil {
				stats.FailedMessages = *mb.FailedMessages
			}
			if mb.SuccessMessages != nil {
				stats.SuccessMessages = *mb.SuccessMessages
			}
			if stats.TotalMessages > 0 {
				stats.Progress = float32(stats.ProcessedMessages) / float32(stats.TotalMessages) * 100
			}
		}

		batchStats = append(batchStats, stats)
	}

	return &v1.GetBatchStatisticsResponse{
		BatchStats:   batchStats,
		TotalBatches: int64(len(batchStats)),
		GeneratedAt:  time.Now().Unix(),
	}, nil
}

// MonitorJobHealth 监控任务健康状态
func (b *statusTrackingBiz) MonitorJobHealth(ctx context.Context, jobID string) (*v1.JobHealthStatus, error) {
	whr := where.T(ctx).F("jobID", jobID)
	job, err := b.store.Job().Get(ctx, whr)
	if err != nil {
		return nil, fmt.Errorf("获取任务失败: %w", err)
	}

	healthStatus := &v1.JobHealthStatus{
		JobId:         job.JobID,
		Status:        job.Status,
		LastHeartbeat: job.UpdatedAt.Unix(),
		IsHealthy:     b.checkJobHealth(ctx, job),
		Metrics:       make(map[string]string),
	}

	// 计算处理时间
	if !job.StartedAt.IsZero() {
		if !job.EndedAt.IsZero() {
			healthStatus.ProcessingTimeMs = job.EndedAt.Sub(job.StartedAt).Milliseconds()
		} else {
			healthStatus.ProcessingTimeMs = time.Since(job.StartedAt).Milliseconds()
		}
	}

	// 添加健康指标
	healthStatus.Metrics["last_update"] = job.UpdatedAt.Format(time.RFC3339)
	healthStatus.Metrics["watcher"] = job.Watcher
	healthStatus.Metrics["scope"] = job.Scope

	return healthStatus, nil
}

// GetSystemMetrics 获取系统级别监控指标
func (b *statusTrackingBiz) GetSystemMetrics(ctx context.Context) (*v1.SystemMetrics, error) {
	// 查询各状态任务数量
	_, allJobs, err := b.store.Job().List(ctx, where.T(ctx))
	if err != nil {
		return nil, fmt.Errorf("查询系统指标失败: %w", err)
	}

	metrics := &v1.SystemMetrics{
		Timestamp:     time.Now().Unix(),
		ResourceUsage: make(map[string]string),
	}

	var totalRuntime time.Duration
	var completedCount int64

	for _, job := range allJobs {
		switch job.Status {
		case "Running":
			metrics.ActiveJobs++
		case "Succeeded":
			metrics.CompletedJobs++
			if !job.StartedAt.IsZero() && !job.EndedAt.IsZero() {
				totalRuntime += job.EndedAt.Sub(job.StartedAt)
				completedCount++
			}
		case "Failed":
			metrics.FailedJobs++
		case "Pending":
			metrics.PendingJobs++
		}
	}

	// 计算平均运行时间
	if completedCount > 0 {
		metrics.AverageRuntimeMs = totalRuntime.Milliseconds() / completedCount
	}

	// 计算成功率
	totalFinished := metrics.CompletedJobs + metrics.FailedJobs
	if totalFinished > 0 {
		metrics.SuccessRate = float64(metrics.CompletedJobs) / float64(totalFinished) * 100
	}

	// 计算吞吐量（最近1小时完成的任务数）
	oneHourAgo := time.Now().Add(-time.Hour)
	recentCompleted := int64(0)
	for _, job := range allJobs {
		if !job.EndedAt.IsZero() && job.EndedAt.After(oneHourAgo) {
			recentCompleted++
		}
	}
	metrics.ThroughputPerMin = recentCompleted

	return metrics, nil
}

// CleanupExpiredStatus 清理过期状态信息
func (b *statusTrackingBiz) CleanupExpiredStatus(ctx context.Context, olderThan time.Duration) error {
	cutoffTime := time.Now().Add(-olderThan)

	// 清理数据库中的过期任务
	completedStatuses := []string{"Succeeded", "Failed"}
	whereOpts := where.T(ctx).F("status", completedStatuses).F("ended_at", "<", cutoffTime)

	if err := b.store.Job().Delete(ctx, whereOpts); err != nil {
		return fmt.Errorf("清理过期任务失败: %w", err)
	}

	log.Infow("清理过期状态完成", "cutoff_time", cutoffTime)
	return nil
}

// StartStatusWatcher 启动状态监控器
func (b *statusTrackingBiz) StartStatusWatcher(ctx context.Context) error {
	go b.runStatusWatcher(ctx)
	log.Infow("状态监控器已启动")
	return nil
}

// StopStatusWatcher 停止状态监控器
func (b *statusTrackingBiz) StopStatusWatcher() error {
	close(b.stopCh)
	log.Infow("状态监控器已停止")
	return nil
}

// runStatusWatcher 运行状态监控器
func (b *statusTrackingBiz) runStatusWatcher(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.checkAllJobsHealth(ctx)
		}
	}
}

// checkAllJobsHealth 检查所有任务健康状态
func (b *statusTrackingBiz) checkAllJobsHealth(ctx context.Context) {
	runningStatuses := []string{"Running", "Processing"}
	_, jobs, err := b.store.Job().List(ctx, where.T(ctx).F("status", runningStatuses))
	if err != nil {
		log.Errorw("查询运行中任务失败", "error", err)
		return
	}

	for _, job := range jobs {
		if !b.checkJobHealth(ctx, job) {
			log.Warnw("检测到不健康任务",
				"job_id", job.JobID,
				"status", job.Status,
				"last_update", job.UpdatedAt,
			)

			// 可以在这里实现自动恢复机制
		}
	}
}

// checkJobHealth 检查单个任务健康状态
func (b *statusTrackingBiz) checkJobHealth(ctx context.Context, job *model.JobM) bool {
	// 检查任务是否超时（运行时间超过预期）
	if !job.StartedAt.IsZero() {
		runningTime := time.Since(job.StartedAt)

		// 不同类型的任务有不同的超时阈值
		var timeoutThreshold time.Duration
		switch job.Watcher {
		case "llmtrain":
			timeoutThreshold = 2 * time.Hour
		case "messagebatch":
			timeoutThreshold = 1 * time.Hour
		default:
			timeoutThreshold = 30 * time.Minute
		}

		if runningTime > timeoutThreshold {
			return false
		}
	}

	// 检查最后更新时间（如果太久没有更新可能有问题）
	if time.Since(job.UpdatedAt) > 10*time.Minute {
		return false
	}

	return true
}

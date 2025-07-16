// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

// Package syncer provides data synchronization capabilities for nightwatch
package syncer

import (
	"context"
	"time"
)

// SyncerType represents the type of syncer
type SyncerType string

const (
	// SyncerTypeStatus synchronizes job and task status
	SyncerTypeStatus SyncerType = "status"
	// SyncerTypeStatistics synchronizes statistics data
	SyncerTypeStatistics SyncerType = "statistics"
	// SyncerTypePartition synchronizes partition data
	SyncerTypePartition SyncerType = "partition"
	// SyncerTypeHeartbeat synchronizes heartbeat data
	SyncerTypeHeartbeat SyncerType = "heartbeat"
)

// SyncOperation represents the type of sync operation
type SyncOperation string

const (
	// SyncOperationSet sets a value
	SyncOperationSet SyncOperation = "set"
	// SyncOperationIncrement increments a counter
	SyncOperationIncrement SyncOperation = "increment"
	// SyncOperationDecrement decrements a counter
	SyncOperationDecrement SyncOperation = "decrement"
	// SyncOperationDelete deletes a value
	SyncOperationDelete SyncOperation = "delete"
	// SyncOperationGet gets a value
	SyncOperationGet SyncOperation = "get"
)

// SyncData represents data to be synchronized
type SyncData struct {
	Key       string                 `json:"key"`
	Value     interface{}            `json:"value"`
	Operation SyncOperation          `json:"operation"`
	TTL       time.Duration          `json:"ttl"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// SyncResult represents the result of a sync operation
type SyncResult struct {
	Success   bool                   `json:"success"`
	Value     interface{}            `json:"value,omitempty"`
	Error     error                  `json:"error,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// SyncStatistics represents synchronization statistics
type SyncStatistics struct {
	SuccessCount   int64         `json:"success_count"`
	FailureCount   int64         `json:"failure_count"`
	TotalCount     int64         `json:"total_count"`
	FailureTotal   int64         `json:"failure_total"`
	AverageLatency time.Duration `json:"average_latency"`
	LastSyncTime   time.Time     `json:"last_sync_time"`
	RetryCount     int64         `json:"retry_count"`
}

// PartitionSyncData represents partition-specific sync data
type PartitionSyncData struct {
	PartitionID    string     `json:"partition_id"`
	BatchID        string     `json:"batch_id"`
	Status         string     `json:"status"`
	Progress       float32    `json:"progress"`
	MessageCount   int64      `json:"message_count"`
	ProcessedCount int64      `json:"processed_count"`
	FailedCount    int64      `json:"failed_count"`
	StartTime      time.Time  `json:"start_time"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	LastHeartbeat  time.Time  `json:"last_heartbeat"`
}

// HeartbeatData represents heartbeat information
type HeartbeatData struct {
	ID        string                 `json:"id"`
	Component string                 `json:"component"`
	Status    string                 `json:"status"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Syncer defines the interface for data synchronization
type Syncer interface {
	// Sync synchronizes data
	Sync(ctx context.Context, data *SyncData) (*SyncResult, error)

	// BatchSync synchronizes multiple data items
	BatchSync(ctx context.Context, data []*SyncData) ([]*SyncResult, error)

	// Get retrieves synchronized data
	Get(ctx context.Context, key string) (*SyncResult, error)

	// Delete removes synchronized data
	Delete(ctx context.Context, keys ...string) error

	// GetStatistics returns sync statistics
	GetStatistics() *SyncStatistics

	// Close closes the syncer
	Close() error
}

// StatusSyncer defines interface for status synchronization
type StatusSyncer interface {
	Syncer

	// SetJobStatus sets job status
	SetJobStatus(ctx context.Context, jobID, status string) error

	// GetJobStatus gets job status
	GetJobStatus(ctx context.Context, jobID string) (string, error)

	// SetTaskStatus sets task status
	SetTaskStatus(ctx context.Context, taskID, status string) error

	// GetTaskStatus gets task status
	GetTaskStatus(ctx context.Context, taskID string) (string, error)

	// BatchSetStatus sets multiple statuses
	BatchSetStatus(ctx context.Context, statuses map[string]string) error
}

// StatisticsSyncer defines interface for statistics synchronization
type StatisticsSyncer interface {
	Syncer

	// IncrementCounter increments a counter
	IncrementCounter(ctx context.Context, key string, value int64) (int64, error)

	// DecrementCounter decrements a counter
	DecrementCounter(ctx context.Context, key string, value int64) (int64, error)

	// GetCounter gets counter value
	GetCounter(ctx context.Context, key string) (int64, error)

	// SetStatistics sets statistics data
	SetStatistics(ctx context.Context, key string, stats *SyncStatistics) error

	// GetStatistics gets statistics data
	GetStatisticsData(ctx context.Context, key string) (*SyncStatistics, error)

	// ResetCounter resets a counter to zero
	ResetCounter(ctx context.Context, key string) error
}

// PartitionSyncer defines interface for partition synchronization
type PartitionSyncer interface {
	Syncer

	// SetPartitionStatus sets partition status
	SetPartitionStatus(ctx context.Context, batchID, partitionID, status string) error

	// GetPartitionStatus gets partition status
	GetPartitionStatus(ctx context.Context, batchID, partitionID string) (string, error)

	// SetPartitionData sets partition data
	SetPartitionData(ctx context.Context, data *PartitionSyncData) error

	// GetPartitionData gets partition data
	GetPartitionData(ctx context.Context, batchID, partitionID string) (*PartitionSyncData, error)

	// IsPartitionComplete checks if partition is complete
	IsPartitionComplete(ctx context.Context, batchID, partitionID string) (bool, error)

	// IsAllPartitionsComplete checks if all partitions are complete
	IsAllPartitionsComplete(ctx context.Context, batchID string) (bool, error)

	// GetActivePartitions gets list of active partitions
	GetActivePartitions(ctx context.Context, batchID string) ([]string, error)
}

// HeartbeatSyncer defines interface for heartbeat synchronization
type HeartbeatSyncer interface {
	Syncer

	// SetHeartbeat sets heartbeat data
	SetHeartbeat(ctx context.Context, data *HeartbeatData) error

	// GetHeartbeat gets heartbeat data
	GetHeartbeat(ctx context.Context, id string) (*HeartbeatData, error)

	// IsAlive checks if component is alive based on heartbeat
	IsAlive(ctx context.Context, id string, timeout time.Duration) (bool, error)

	// CleanupExpiredHeartbeats removes expired heartbeat data
	CleanupExpiredHeartbeats(ctx context.Context, maxAge time.Duration) error
}

// SyncerManager manages multiple syncers
type SyncerManager interface {
	// GetStatusSyncer returns status syncer
	GetStatusSyncer() StatusSyncer

	// GetStatisticsSyncer returns statistics syncer
	GetStatisticsSyncer() StatisticsSyncer

	// GetPartitionSyncer returns partition syncer
	GetPartitionSyncer() PartitionSyncer

	// GetHeartbeatSyncer returns heartbeat syncer
	GetHeartbeatSyncer() HeartbeatSyncer

	// Close closes all syncers
	Close() error
}

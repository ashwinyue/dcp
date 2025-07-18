// StatusTracking API 定义，包含状态跟踪的请求和响应消息

// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

// Code generated by protoc-gen-defaults. DO NOT EDIT.

package v1

import (
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	_ *timestamppb.Timestamp
	_ *durationpb.Duration
	_ *wrapperspb.BoolValue
)

func (x *UpdateJobStatusRequest) Default() {
}

func (x *UpdateJobStatusResponse) Default() {
}

func (x *GetJobStatusRequest) Default() {
}

func (x *GetJobStatusResponse) Default() {
}

func (x *TrackRunningJobsRequest) Default() {
}

func (x *RunningJobInfo) Default() {
}

func (x *TrackRunningJobsResponse) Default() {
}

func (x *GetJobStatisticsRequest) Default() {
}

func (x *GetJobStatisticsResponse) Default() {
}

func (x *GetBatchStatisticsRequest) Default() {
}

func (x *BatchStatusInfo) Default() {
}

func (x *GetBatchStatisticsResponse) Default() {
}

func (x *JobHealthCheckRequest) Default() {
}

func (x *JobHealthStatus) Default() {
}

func (x *JobHealthCheckResponse) Default() {
}

func (x *GetSystemMetricsRequest) Default() {
}

func (x *SystemMetrics) Default() {
}

func (x *GetSystemMetricsResponse) Default() {
}

func (x *CleanupExpiredStatusRequest) Default() {
}

func (x *CleanupExpiredStatusResponse) Default() {
}

func (x *StartStatusWatcherRequest) Default() {
}

func (x *StartStatusWatcherResponse) Default() {
}

func (x *StopStatusWatcherRequest) Default() {
}

func (x *StopStatusWatcherResponse) Default() {
}

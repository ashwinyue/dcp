// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package messagebatch

//go:generate mockgen -destination mock_messagebatch.go -package messagebatch github.com/ashwinyue/dcp/internal/nightwatch/biz/v1/messagebatch MessageBatchBiz

import (
	"context"
	"strconv"
	"sync"

	"github.com/onexstack/onexstack/pkg/core"
	"github.com/onexstack/onexstack/pkg/log"
	"github.com/onexstack/onexstack/pkg/store/where"
	"golang.org/x/sync/errgroup"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/conversion"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/pkg/known"
	v1 "github.com/ashwinyue/dcp/pkg/api/nightwatch/v1"
)

// MessageBatchBiz defines the interface that contains methods for handling message batch requests.
type MessageBatchBiz interface {
	// Create creates a new message batch job based on the provided request parameters.
	Create(ctx context.Context, rq *v1.CreateMessageBatchJobRequest) (*v1.CreateMessageBatchJobResponse, error)

	// Update updates an existing message batch job based on the provided request parameters.
	Update(ctx context.Context, rq *v1.UpdateMessageBatchJobRequest) (*v1.UpdateMessageBatchJobResponse, error)

	// Delete removes one or more message batch jobs based on the provided request parameters.
	Delete(ctx context.Context, rq *v1.DeleteMessageBatchJobRequest) (*v1.DeleteMessageBatchJobResponse, error)

	// Get retrieves the details of a specific message batch job based on the provided request parameters.
	Get(ctx context.Context, rq *v1.GetMessageBatchJobRequest) (*v1.GetMessageBatchJobResponse, error)

	// List retrieves a list of message batch jobs and their total count based on the provided request parameters.
	List(ctx context.Context, rq *v1.ListMessageBatchJobRequest) (*v1.ListMessageBatchJobResponse, error)

	// MessageBatchExpansion defines additional methods for extended message batch operations, if needed.
	MessageBatchExpansion
}

// MessageBatchExpansion defines additional methods for message batch operations.
type MessageBatchExpansion interface{}

// messageBatchBiz is the implementation of the MessageBatchBiz.
type messageBatchBiz struct {
	store store.IStore
}

// Ensure that *messageBatchBiz implements the MessageBatchBiz.
var _ MessageBatchBiz = (*messageBatchBiz)(nil)

// New creates and returns a new instance of *messageBatchBiz.
func New(store store.IStore) *messageBatchBiz {
	return &messageBatchBiz{store: store}
}

// Create implements the Create method of the MessageBatchBiz.
func (b *messageBatchBiz) Create(ctx context.Context, rq *v1.CreateMessageBatchJobRequest) (*v1.CreateMessageBatchJobResponse, error) {
	var jobM model.MessageBatchJobM
	_ = core.Copy(&jobM, rq.Job)
	// TODO: Retrieve the UserID from the custom context and assign it as needed.
	// jobM.UserID = contextx.UserID(ctx)

	if err := b.store.MessageBatchJob().Create(ctx, &jobM); err != nil {
		return nil, err
	}

	return &v1.CreateMessageBatchJobResponse{JobID: strconv.FormatInt(jobM.ID, 10)}, nil
}

// Update implements the Update method of the MessageBatchBiz.
func (b *messageBatchBiz) Update(ctx context.Context, rq *v1.UpdateMessageBatchJobRequest) (*v1.UpdateMessageBatchJobResponse, error) {
	whr := where.T(ctx).F("id", rq.GetJobID())
	jobM, err := b.store.MessageBatchJob().Get(ctx, whr)
	if err != nil {
		return nil, err
	}

	if rq.Name != nil {
		jobM.Name = *rq.Name
	}
	if rq.Description != nil {
		jobM.Description = *rq.Description
	}
	if rq.Params != nil {
		jobM.Params = (*model.MessageBatchJobParams)(rq.Params)
	}
	if rq.Results != nil {
		jobM.Results = (*model.MessageBatchJobResults)(rq.Results)
	}
	if rq.Status != nil {
		jobM.Status = rq.Status.String()
	}

	if err := b.store.MessageBatchJob().Update(ctx, jobM); err != nil {
		return nil, err
	}

	return &v1.UpdateMessageBatchJobResponse{}, nil
}

// Delete implements the Delete method of the MessageBatchBiz.
func (b *messageBatchBiz) Delete(ctx context.Context, rq *v1.DeleteMessageBatchJobRequest) (*v1.DeleteMessageBatchJobResponse, error) {
	whr := where.T(ctx).F("id", rq.GetJobIDs())
	if err := b.store.MessageBatchJob().Delete(ctx, whr); err != nil {
		return nil, err
	}

	return &v1.DeleteMessageBatchJobResponse{}, nil
}

// Get implements the Get method of the MessageBatchBiz.
func (b *messageBatchBiz) Get(ctx context.Context, rq *v1.GetMessageBatchJobRequest) (*v1.GetMessageBatchJobResponse, error) {
	whr := where.T(ctx).F("id", rq.GetJobID())
	jobM, err := b.store.MessageBatchJob().Get(ctx, whr)
	if err != nil {
		return nil, err
	}

	return &v1.GetMessageBatchJobResponse{Job: conversion.MessageBatchJobMToMessageBatchJobV1(jobM)}, nil
}

// List implements the List method of the MessageBatchBiz.
func (b *messageBatchBiz) List(ctx context.Context, rq *v1.ListMessageBatchJobRequest) (*v1.ListMessageBatchJobResponse, error) {
	whr := where.T(ctx).P(int(rq.GetOffset()), int(rq.GetLimit()))
	count, jobList, err := b.store.MessageBatchJob().List(ctx, whr)
	if err != nil {
		return nil, err
	}

	var m sync.Map
	eg, ctx := errgroup.WithContext(ctx)

	// Set the maximum concurrency limit using the constant MaxConcurrency
	eg.SetLimit(known.MaxErrGroupConcurrency)

	// Use goroutines to improve API performance
	for _, job := range jobList {
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				converted := conversion.MessageBatchJobMToMessageBatchJobV1(job)
				// TODO: Add additional processing logic and assign values to fields
				// that need updating, for example:
				// xxx := doSomething()
				// converted.XXX = xxx
				m.Store(job.ID, converted)

				return nil
			}
		})
	}

	if err := eg.Wait(); err != nil {
		log.W(ctx).Errorw(err, "Failed to wait all function calls returned")
		return nil, err
	}

	jobs := make([]*v1.MessageBatchJob, 0, len(jobList))
	for _, item := range jobList {
		job, _ := m.Load(item.ID)
		jobs = append(jobs, job.(*v1.MessageBatchJob))
	}

	return &v1.ListMessageBatchJobResponse{Total: count, Jobs: jobs}, nil
}
package server

import (
	"context"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/mock"
)

// queue defines the interface of jobs queue
type queue interface {
	enqueue(ctx context.Context, job *job) error
	size(ctx context.Context) (int, error)
	dequeue(ctx context.Context, functions []string) (*job, error)
	dispose() error
}

// mockQueue is a mock implementation for unittest
type mockQueue struct {
	mock.Mock
}

func (q *mockQueue) enqueue(ctx context.Context, job *job) error {
	return q.Called(ctx, job).Error(0)
}

func (q *mockQueue) size(ctx context.Context) (int, error) {
	returnValues := q.Called(ctx)
	return returnValues.Int(0), returnValues.Error(1)
}

func (q *mockQueue) dequeue(ctx context.Context, functions []string) (*job, error) {
	returnValues := q.Called(functions)
	var j *job
	if returnValues.Get(0) != nil {
		j = returnValues.Get(0).(*job)
	}
	return j, returnValues.Error(1)
}

func (q *mockQueue) appendListenClient(ctx context.Context, handle *gearman.ID, clientID *gearman.ID) error {
	return q.Called(ctx, handle, clientID).Error(0)
}

func (q *mockQueue) dispose() error {
	return q.Called().Error(0)
}

var _ queue = &sqlQueue{}
var _ queue = &mockQueue{}

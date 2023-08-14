// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package testbroker exports a broker implementation that should be used in package testing.
package testbroker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/timeutil"
)

var errRedisDown = errors.New("asynqtest: redis is down")

// TestBroker is a broker implementation which enables
// to simulate Redis failure in tests.
type TestBroker struct {
	mu       sync.Mutex
	sleeping bool

	// real broker
	real base.Broker
}

// Make sure TestBroker implements Broker interface at compile time.
var _ base.Broker = (*TestBroker)(nil)

func NewTestBroker(b base.Broker) *TestBroker {
	return &TestBroker{real: b}
}

func (tb *TestBroker) Inspector() base.Inspector {
	return tb.real.Inspector()
}

func (tb *TestBroker) SetClock(c timeutil.Clock) {
	tb.real.SetClock(c)
}

func (tb *TestBroker) Sleep() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = true
}

func (tb *TestBroker) Wakeup() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = false
}

func (tb *TestBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Enqueue(ctx, msg)
}

func (tb *TestBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration, forceUnique ...bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.EnqueueUnique(ctx, msg, ttl, forceUnique...)
}

func (tb *TestBroker) Dequeue(serverID string, qnames ...string) (*base.TaskMessage, time.Time, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, time.Time{}, errRedisDown
	}
	return tb.real.Dequeue(serverID, qnames...)
}

func (tb *TestBroker) Done(serverID string, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Done(serverID, msg)
}

func (tb *TestBroker) Requeue(serverID string, msg *base.TaskMessage, aborted bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Requeue(serverID, msg, true)
}

func (tb *TestBroker) MarkAsComplete(serverID string, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.MarkAsComplete(serverID, msg)
}

func (tb *TestBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Schedule(ctx, msg, processAt)
}

func (tb *TestBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration, forceUnique ...bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ScheduleUnique(ctx, msg, processAt, ttl, forceUnique...)
}

func (tb *TestBroker) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Retry(msg, processAt, errMsg, isFailure)
}

func (tb *TestBroker) Archive(msg *base.TaskMessage, errMsg string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Archive(msg, errMsg)
}

func (tb *TestBroker) ForwardIfReady(qnames ...string) (int, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return 0, errRedisDown
	}
	return tb.real.ForwardIfReady(qnames...)
}

func (tb *TestBroker) DeleteExpiredCompletedTasks(qname string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.DeleteExpiredCompletedTasks(qname)
}

func (tb *TestBroker) ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListDeadlineExceeded(deadline, qnames...)
}

func (tb *TestBroker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.WriteServerState(info, workers, ttl)
}

func (tb *TestBroker) ClearServerState(host string, pid int, serverID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ClearServerState(host, pid, serverID)
}

func (tb *TestBroker) CancelationPubSub() (base.PubSub, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.CancelationPubSub()
}

func (tb *TestBroker) PublishCancelation(id string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.PublishCancelation(id)
}

func (tb *TestBroker) WriteResult(qname, id string, data []byte) (int, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return 0, errRedisDown
	}
	return tb.real.WriteResult(qname, id, data)
}

func (tb *TestBroker) Ping() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Ping()
}

func (tb *TestBroker) Close() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Close()
}

func (tb *TestBroker) ListServers() ([]*base.ServerInfo, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListServers()
}

func (tb *TestBroker) EnqueueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.EnqueueBatch(ctx, msgs...)
}

func (tb *TestBroker) EnqueueUniqueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.EnqueueUniqueBatch(ctx, msgs...)
}

func (tb *TestBroker) ScheduleBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ScheduleBatch(ctx, msgs...)
}

func (tb *TestBroker) ScheduleUniqueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ScheduleUniqueBatch(ctx, msgs...)
}

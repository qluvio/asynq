// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

const (
	redisType  = "redis"
	rqliteType = "rqlite"
)

// Task represents a unit of work to be performed.
type Task struct {
	// typename indicates the type of task to be performed.
	typename string

	// payload holds data needed to perform the task.
	payload []byte

	// opts holds options for the task.
	opts []Option

	// w is the ResultWriter for the task.
	w *ResultWriter

	// p is the AsyncProcessor for the task.
	p *AsyncProcessor
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }

// ResultWriter returns a pointer to the ResultWriter associated with the task.
//
// Nil pointer is returned if called on a newly created task (i.e. task created by calling NewTask).
// Only the tasks passed to Handler.ProcessTask have a valid ResultWriter pointer.
func (t *Task) ResultWriter() *ResultWriter { return t.w }

// AsyncProcessor returns a pointer to the AsyncProcessor associated with the task.
//
// Nil pointer is returned if called on a newly created task (i.e. task created by calling NewTask).
// Only the tasks passed to Handler.ProcessTask have a valid AsyncProcessor pointer.
func (t *Task) AsyncProcessor() *AsyncProcessor { return t.p }

// NewTask returns a new Task given a type name and payload data.
// Options can be passed to configure task processing behavior.
func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		opts:     opts,
	}
}

// newTask creates a task with the given typename, payload, ResultWriter, and AsyncProcessor.
func newTask(typename string, payload []byte, w *ResultWriter, p *AsyncProcessor) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		w:        w,
		p:        p,
	}
}

// A TaskInfo describes a task and its metadata.
type TaskInfo struct {
	// ID is the identifier of the task.
	ID string

	// Queue is the name of the queue in which the task belongs.
	Queue string

	// Type is the type name of the task.
	Type string

	// Payload is the payload data of the task.
	Payload []byte

	// State indicates the task state.
	State TaskState

	// MaxRetry is the maximum number of times the task can be retried.
	MaxRetry int

	// Retried is the number of times the task has retried so far.
	Retried int

	// LastErr is the error message from the last failure.
	LastErr string

	// LastFailedAt is the time time of the last failure if any.
	// If the task has no failures, LastFailedAt is zero time (i.e. time.Time{}).
	LastFailedAt time.Time

	// Timeout is the duration the task can be processed by Handler before being retried,
	// zero if not specified
	Timeout time.Duration

	// Deadline is the deadline for the task, zero value if not specified.
	Deadline time.Time

	// NextProcessAt is the time the task is scheduled to be processed,
	// zero if not applicable.
	NextProcessAt time.Time

	// Recurrent indicates a recurrent task when true
	Recurrent bool

	// Delay in seconds to re-process a recurrent task after execution.
	ReprocessAfter time.Duration

	// ServerAffinity is used with a recurrent task to specify a timeout after
	// which the task can be handled by a server other than the one that handled
	// it the first time.
	ServerAffinity time.Duration

	// Retention is duration of the retention period after the task is successfully processed.
	Retention time.Duration

	// CompletedAt is the time when the task is processed successfully.
	// Zero value (i.e. time.Time{}) indicates no value.
	CompletedAt time.Time

	// Result holds the result data associated with the task.
	// Use ResultWriter to write result data from the Handler.
	Result []byte
}

// If t is non-zero, returns time converted from t as unix time in seconds.
// If t is zero, returns zero value of time.Time.
func fromUnixTimeOrZero(t int64) time.Time {
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(t, 0)
}

func newTaskInfo(msg *base.TaskMessage, state base.TaskState, nextProcessAt time.Time, result []byte) *TaskInfo {
	info := TaskInfo{
		ID:             msg.ID,
		Queue:          msg.Queue,
		Type:           msg.Type,
		Payload:        msg.Payload, // Do we need to make a copy?
		MaxRetry:       msg.Retry,
		Retried:        msg.Retried,
		LastErr:        msg.ErrorMsg,
		Timeout:        time.Duration(msg.Timeout) * time.Second,
		NextProcessAt:  nextProcessAt,
		LastFailedAt:   fromUnixTimeOrZero(msg.LastFailedAt),
		Recurrent:      msg.Recurrent,
		ReprocessAfter: time.Duration(msg.ReprocessAfter) * time.Second,
		ServerAffinity: time.Duration(msg.ServerAffinity) * time.Second,
		Deadline:       fromUnixTimeOrZero(msg.Deadline),
		Retention:      time.Duration(msg.Retention) * time.Second,
		CompletedAt:    fromUnixTimeOrZero(msg.CompletedAt),
		Result:         result,
	}

	switch state {
	case base.TaskStateActive:
		info.State = TaskStateActive
	case base.TaskStatePending:
		info.State = TaskStatePending
	case base.TaskStateScheduled:
		info.State = TaskStateScheduled
	case base.TaskStateRetry:
		info.State = TaskStateRetry
	case base.TaskStateArchived:
		info.State = TaskStateArchived
	case base.TaskStateCompleted:
		info.State = TaskStateCompleted
	default:
		panic(fmt.Sprintf("internal error: unknown state: %d", state))
	}
	return &info
}

// TaskState denotes the state of a task.
type TaskState int

const (
	// TaskStateActive indicates that the task is currently being processed by Handler.
	TaskStateActive TaskState = iota + 1

	// TaskStatePending indicates that the task is ready to be processed by Handler.
	TaskStatePending

	// TaskStateScheduled indicates that the task is scheduled to be processed some time in the future.
	TaskStateScheduled

	// TaskStateRetry indicates that the task has previously failed and scheduled to be processed some time in the future.
	TaskStateRetry

	// TaskStateArchived indicates that the task is archived and stored for inspection purposes.
	TaskStateArchived

	// Indicates that the task is processed successfully and retained until the retention TTL expires.
	TaskStateCompleted
)

func (s TaskState) String() string {
	switch s {
	case TaskStateActive:
		return "active"
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRetry:
		return "retry"
	case TaskStateArchived:
		return "archived"
	case TaskStateCompleted:
		return "completed"
	}
	panic("asynq: unknown task state")
}

type ClientConnOpt interface {
	// Logger returns a Logger or nil
	Logger() Logger
	// MakeClient returns a new client instance.
	// Return value is intentionally opaque to hide the implementation detail of client.
	MakeClient() interface{}
}

// ResultWriter is a client interface to write result data for a task.
// It writes the data to the redis instance the server is connected to.
type ResultWriter struct {
	id     string // task ID this writer is responsible for
	qname  string // queue name the task belongs to
	broker base.Broker
	ctx    context.Context // context associated with the task
}

// Write writes the given data as a result of the task the ResultWriter is associated with.
func (w *ResultWriter) Write(data []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, fmt.Errorf("failed to result task result: %v", w.ctx.Err())
	default:
	}
	return w.broker.WriteResult(w.qname, w.id, data)
}

// TaskID returns the ID of the task the ResultWriter is associated with.
func (w *ResultWriter) TaskID() string {
	return w.id
}

// AsyncProcessor updates the final state of an asynchronous task.
// TaskCompleted/TaskFailed will block until the task worker goroutine returns; this means that the worker goroutine
// should not make these calls, as would normally be the case for asynchronous tasks.
// Only the first TaskCompleted/TaskFailed call will update the task status; all subsequent calls will have no effect.
type AsyncProcessor struct {
	resCh chan error
	mutex sync.Mutex
	done  bool
}

// TaskCompleted indicates that the task has completed successfully.
func (p *AsyncProcessor) TaskCompleted() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.done {
		p.resCh <- nil
		p.done = true
	}
}

// TaskFailed indicates that the task has failed, with the given error.
func (p *AsyncProcessor) TaskFailed(err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.done {
		p.resCh <- err
		p.done = true
	}
}

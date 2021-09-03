// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
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
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }

// NewTask returns a new Task given a type name and payload data.
func NewTask(typename string, payload []byte) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
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
}

func newTaskInfo(msg *base.TaskMessage, state base.TaskState, nextProcessAt time.Time) *TaskInfo {
	info := TaskInfo{
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		Type:          msg.Type,
		Payload:       msg.Payload, // Do we need to make a copy?
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		LastErr:       msg.ErrorMsg,
		Timeout:       time.Duration(msg.Timeout) * time.Second,
		NextProcessAt: nextProcessAt,
	}
	if msg.LastFailedAt == 0 {
		info.LastFailedAt = time.Time{}
	} else {
		info.LastFailedAt = time.Unix(msg.LastFailedAt, 0)
	}

	if msg.Deadline == 0 {
		info.Deadline = time.Time{}
	} else {
		info.Deadline = time.Unix(msg.Deadline, 0)
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
	}
	panic("asynq: unknown task state")
}

type ClientConnOpt interface {
	// MakeClient returns a new client instance.
	// Return value is intentionally opaque to hide the implementation detail of client.
	MakeClient() interface{}
}

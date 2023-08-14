// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

const (
	RedisType  = "redis"
	RqliteType = "rqlite"
	SqliteType = "sqlite"
)

// Task represents a unit of work to be performed.
type Task struct {
	// typename indicates the type of task to be performed.
	typename string

	// payload holds data needed to perform the task.
	payload []byte

	// opts holds options for the task.
	opts []Option

	// the result of a previous execution when this task was transitioned from another queue
	result []byte

	// w is the ResultWriter for the task.
	w *ResultWriter

	// p is the AsyncProcessor for the task.
	p AsyncProcessor

	// callAfter lets task processing schedule a function call after task execution
	callAfter func(fn func(string, error, bool))
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }
func (t *Task) Result() []byte  { return t.result }

// ResultWriter returns a pointer to the ResultWriter associated with the task.
//
// Nil pointer is returned if called on a newly created task (i.e. task created by calling NewTask).
// Only the tasks passed to Handler.ProcessTask have a valid ResultWriter pointer.
func (t *Task) ResultWriter() *ResultWriter { return t.w }

// AsyncProcessor returns the AsyncProcessor associated with the task.
//
// Only the tasks passed to Handler.ProcessTask have a valid AsyncProcessor.
func (t *Task) AsyncProcessor() AsyncProcessor { return t.p }

// CallAfter enables task processing to schedule execution of a function after the
// task execution. The function is guaranteed to be called once the task state has
// changed in the broker database.
// Function parameter:
// string: task id
// error:  the error returned by task execution.
// bool:   indicates if the error - if not nil - was considered as an actual error
func (t *Task) CallAfter(fn func(string, error, bool)) {
	if t.callAfter != nil {
		t.callAfter(fn)
	}
}

// NewTask returns a new Task given a type name and payload data.
// Options can be passed to configure task processing behavior.
func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		p:        invalidAsyncProcessor,
		opts:     opts,
	}
}

// newTask creates a task with the given typename, payload, ResultWriter, and AsyncProcessor.
func newTask(
	typename string,
	payload []byte,
	result []byte,
	w *ResultWriter,
	p *asyncProcessor,
	ca func(fn func(string, error, bool))) *Task {

	if len(result) == 0 {
		result = nil
	}
	return &Task{
		typename:  typename,
		payload:   payload,
		result:    result,
		w:         w,
		p:         p,
		callAfter: ca,
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

	// LastFailedAt is the time of the last failure if any.
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

	// TaskStateCompleted indicates that the task is processed successfully and retained until the retention TTL expires.
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
// It writes the data to the broker instance the server is connected to.
type ResultWriter struct {
	id     string          // task ID this writer is responsible for
	qname  string          // queue name the task belongs to
	broker base.Broker     // the broker
	ctx    context.Context // context associated with the task
}

// Write writes the given data as a result of the task the ResultWriter is associated with.
func (w *ResultWriter) Write(data []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, fmt.Errorf("task %s: failed to write result: %v", w.id, w.ctx.Err())
	default:
	}
	return w.broker.WriteResult(w.qname, w.id, data)
}

// TaskID returns the ID of the task the ResultWriter is associated with.
func (w *ResultWriter) TaskID() string {
	return w.id
}

// AsyncProcessor is the interface provided to active tasks.
//
// Asynchronous tasks (doing their process asynchronously) return AsynchronousTask as result and
// terminate by calling one of the TaskXX function that update the final state of an asynchronous task.
// - TaskCompleted/TaskFailed/TaskTransition will block until the task worker goroutine returns; this means that the worker
// goroutine should not make these calls, as would normally be the case for asynchronous tasks.
// - Only the first TaskCompleted/TaskFailed/TaskTransition call will update the task status; all subsequent calls will
// have no effect and return a TaskTransitionAlreadyDone error.
//
// TransitionToQueue can be used from the worker goroutine (synchronous tasks).
// It returns a TaskTransitionDone error that should be used as result of execution.
type AsyncProcessor interface {
	// TaskCompleted indicates that the task has completed successfully.
	//
	// The returned error is nil or TaskTransitionAlreadyDone
	TaskCompleted() error

	// TaskFailed indicates that the task has failed, with the given error.
	//
	// The returned error is nil or TaskTransitionAlreadyDone
	TaskFailed(err error) error

	// TaskTransition indicates that the task has completed successfully and
	// moves the task to a new queue. Internally, the asynq processor is notified
	// via a TaskTransitionDone as the result of execution.
	//
	// The returned error is nil or TaskTransitionAlreadyDone
	TaskTransition(newQueue, typename string, opts ...Option) error

	// TransitionToQueue moves the executing 'active' task to a new queue.
	// TransitionToQueue has the same effect as TaskTransition but does not notify
	// the processor of the termination of the task and therefore, after a call to
	// TransitionToQueue, the caller still has to call one of the TaskXX functions.
	// It is recommended to return the error returned by TransitionToQueue as the
	// result of the task execution.
	//
	// After a successful call:
	// - the task is either in pending or scheduled state in newQueue.
	// - the function returns the new state AND error TaskTransitionDone
	// otherwise it returns zero and the error.
	TransitionToQueue(newQueue, typename string, opts ...Option) (TaskState, error)
}

var (
	_                          AsyncProcessor = (*voidAsyncProcessor)(nil)
	invalidAsyncProcessor                     = &voidAsyncProcessor{}
	invalidAsyncProcessorError                = errors.New("invalid async processor")
)

type voidAsyncProcessor struct{}

func (v *voidAsyncProcessor) TaskCompleted() error {
	return invalidAsyncProcessorError
}

func (v *voidAsyncProcessor) TaskFailed(error) error {
	return invalidAsyncProcessorError
}

func (v *voidAsyncProcessor) TaskTransition(string, string, ...Option) error {
	return invalidAsyncProcessorError
}

func (v *voidAsyncProcessor) TransitionToQueue(string, string, ...Option) (TaskState, error) {
	return 0, invalidAsyncProcessorError
}

var _ AsyncProcessor = (*asyncProcessor)(nil)

// asyncProcessor updates the final state of an asynchronous task.
// TaskCompleted/TaskFailed/TaskTransition will block until the task worker goroutine returns; this means that the
// worker goroutine should not make these calls, as would normally be the case for asynchronous tasks.
// Only the first TaskCompleted/TaskFailed/TaskTransition call will update the task status; all subsequent calls will
// have no effect and return an error.
type asyncProcessor struct {
	task      *processorTask
	processor *processor
	results   chan processorResult
	mutex     sync.Mutex
	done      bool
}

// TaskCompleted indicates that the task has completed successfully.
func (p *asyncProcessor) TaskCompleted() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.done {
		p.results <- processorResult{task: p.task, err: nil}
		p.done = true
		return nil
	}
	return TaskTransitionAlreadyDone
}

// TaskFailed indicates that the task has failed, with the given error.
func (p *asyncProcessor) TaskFailed(err error) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.done {
		p.results <- processorResult{task: p.task, err: err}
		p.done = true
		return nil
	}
	return TaskTransitionAlreadyDone
}

// TaskTransition moves the task to a new queue and send a TaskTransitionDone as
// a result of execution.
// An error is returned if the transition failed.
func (p *asyncProcessor) TaskTransition(newQueue, typename string, opts ...Option) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.done {
		_, err := p.TransitionToQueue(newQueue, typename, opts...)
		if err != nil && err != TaskTransitionDone {
			return err
		}
		p.results <- processorResult{task: p.task, err: TaskTransitionDone}
		p.done = true
		return nil
	}
	return TaskTransitionAlreadyDone
}

// TransitionToQueue moves the executing 'active' task to a new queue.
//
// After the call the task is either in pending or scheduled state in newQueue.
// When successful, the function returns the new state AND error TaskTransitionDone
// otherwise it returns zero and the error.
func (p *asyncProcessor) TransitionToQueue(newQueue, typename string, opts ...Option) (TaskState, error) {
	ret, err := p.processor.moveToQueue(p.task.ctx, p.task.msg, newQueue, typename, true, opts...)
	if err != nil {
		return 0, err
	}
	return ret.State, TaskTransitionDone
}

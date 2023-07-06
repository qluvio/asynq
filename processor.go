// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"golang.org/x/time/rate"
)

// SkipRetry is used as a return value from Handler.ProcessTask to indicate that
// the task should not be retried and should be archived instead.
var SkipRetry = errors.New("skip retry for the task")

// TaskCanceled is used as a return value from Handler.ProcessTask to indicate that
// the task has been canceled, should not be retried, and should be archived instead.
var TaskCanceled = errors.New("task canceled")

// AsynchronousTask is used as a return value from Handler.ProcessTask to
// indicate that the task is processing asynchronously separately from the main
// worker goroutine.
var AsynchronousTask = errors.New("task is processing asynchronously")

// TaskTransitionDone is used as a return value from Handler.ProcessTask to
// indicate that the task was transitioned to another queue.
var TaskTransitionDone = errors.New("task transitioned to another queue")
var TaskTransitionAlreadyDone = errors.New("asyncProcessor already done")

type AsynchronousHandler interface {
	// UpdateTask updates the task in the given queue and id with the given data.
	// It returns the corresponding task info and the actual deadline of the task
	// or an error if the operation failed.
	UpdateTask(queueName, taskId string, data []byte) (*TaskInfo, time.Time, error)

	// RequeueCompleted moves the completed task with the given taskId in the given queue to the target queue.
	// The task is enqueued or scheduled with the given 'typeName' task type and the given options.
	// This call fails if the task is not in completed state.
	RequeueCompleted(ctx context.Context, queue, taskId, targetQueue, typeName string, opts ...Option) (*TaskInfo, error)
}

type processor struct {
	logger   *log.Logger
	serverID string
	broker   base.Broker

	handler Handler

	queues Queues

	retryDelay    RetryDelayHandler
	isFailureFunc func(error) bool

	errHandler ErrorHandler

	shutdownTimeout time.Duration

	// channel via which to send sync requests to syncer.
	syncRequestCh chan<- *syncRequest

	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// wait channel used to feed tasks to fini()
	wait       chan *processorTask
	waitClosed bool
	waitMu     sync.Mutex

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort    chan struct{}
	abortNow chan struct{}

	// cancelations is a set of cancel functions for all active tasks.
	cancelations *base.Cancelations

	// afterTasks is a set of function set by active tasks to be called after task execution
	afterTasks *base.AfterTasks

	starting chan<- *workerInfo
	finished chan<- *base.TaskMessage

	emptyQSleep      time.Duration
	asynchronousOnce sync.Once
	asynchronous     *asynchronousHandler
}

type processorParams struct {
	logger          *log.Logger
	serverID        string
	broker          base.Broker
	retryDelayFunc  RetryDelayHandler
	isFailureFunc   func(error) bool
	syncCh          chan<- *syncRequest
	cancelations    *base.Cancelations
	afterTasks      *base.AfterTasks
	concurrency     int // currently limited to 32767; see processor.fini()
	queues          Queues
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *workerInfo
	finished        chan<- *base.TaskMessage
	emptyQSleep     time.Duration
}

type processorTask struct {
	msg     *base.TaskMessage
	ctx     context.Context
	cleanup func()
	resCh   chan error
}

// newProcessor constructs a new processor.
func newProcessor(params processorParams) *processor {

	return &processor{
		logger:          params.logger,
		serverID:        params.serverID,
		broker:          params.broker,
		queues:          params.queues,
		retryDelay:      params.retryDelayFunc,
		isFailureFunc:   params.isFailureFunc,
		syncRequestCh:   params.syncCh,
		cancelations:    params.cancelations,
		afterTasks:      params.afterTasks,
		errLogLimiter:   rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:            make(chan struct{}, params.concurrency),
		done:            make(chan struct{}),
		wait:            make(chan *processorTask, params.concurrency),
		quit:            make(chan struct{}),
		abort:           make(chan struct{}),
		abortNow:        make(chan struct{}),
		errHandler:      params.errHandler,
		handler:         HandlerFunc(func(ctx context.Context, t *Task) error { return fmt.Errorf("handler not set") }),
		shutdownTimeout: params.shutdownTimeout,
		starting:        params.starting,
		finished:        params.finished,
		emptyQSleep:     params.emptyQSleep,
	}
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
func (p *processor) stop() {
	p.once.Do(func() {
		p.logger.Debug("Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.quit)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once shutdown, processor cannot be re-started.
func (p *processor) shutdown() {
	p.shutdownNow(false)
}
func (p *processor) shutdownNow(immediately bool) {
	p.stop()

	if immediately {
		close(p.abortNow)
		close(p.abort)
	} else {
		time.AfterFunc(p.shutdownTimeout, func() { close(p.abort) })

		p.logger.Info("processor: waiting for all workers to finish...")
		// block until all workers have released the token
		for i := 0; i < cap(p.sema); i++ {
			p.sema <- struct{}{}
		}
	}

	p.waitMu.Lock()
	close(p.wait)
	p.waitClosed = true
	p.waitMu.Unlock()
	if !immediately {
		p.logger.Info("processor: all workers have finished")
	} else {
		p.logger.Info("processor: finished")
	}
}

func (p *processor) start(wg *sync.WaitGroup) {
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-p.done:
				p.logger.Debug("Processor done")
				return
			default:
				p.exec()
			}
		}
	}()
	go func() {
		defer wg.Done()
		p.fini()
	}()
}

// exec pulls a task out of the queue and starts a worker goroutine to
// process the task.
func (p *processor) exec() {
	select {
	case <-p.quit:
		return
	case p.sema <- struct{}{}: // acquire token
		qnames := p.queues.Names()
		msg, deadline, err := p.broker.Dequeue(p.serverID, qnames...)
		switch {
		case errors.Is(err, errors.ErrNoProcessableTask):
			p.logger.Debug("All queues are empty")
			// Queues are empty, this is a normal behavior.
			// Sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: We are not using blocking pop operation and polling queues instead.
			// This adds significant load to redis.
			time.Sleep(p.emptyQSleep)
			<-p.sema // release token
			return
		case err != nil:
			if p.errLogLimiter.Allow() {
				p.logger.Errorf("Dequeue error: %v", err)
			}
			// also sleep, otherwise we create a busy loop until errors clear...
			time.Sleep(p.emptyQSleep)
			<-p.sema // release token
			return
		}
		p.logger.Debugf("dequeued %s -> %v", msg.ID, deadline)

		p.starting <- &workerInfo{msg: msg, started: time.Now(), deadline: deadline}
		go func() {
			resCh := make(chan error, 3)
			ctx, cancel := asynqcontext.New(msg, deadline)
			p.cancelations.Add(msg.ID, func() {
				resCh <- TaskCanceled
				// Need to make sure resCh is processed before ctxDoneCh; let cleanup cancel context later
			})
			cleanup := func() {
				cancel()
				p.cancelations.Delete(msg.ID)
				p.finished <- msg
				<-p.sema // release token
			}

			// check context before starting a worker goroutine.
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded).
				p.handleFailedMessage(ctx, msg, "already done", ctx.Err())
				cleanup()
				return
			default:
			}

			rw := &ResultWriter{id: msg.ID, qname: msg.Queue, broker: p.broker, ctx: ctx}
			ap := &asyncProcessor{
				ctx:       ctx,
				msg:       msg,
				processor: p,
				resCh:     resCh,
			}
			// hold mutex until worker goroutine returns; ensures that AsyncProcessor does not send to resCh first
			ap.mutex.Lock()
			go func() {
				defer ap.mutex.Unlock()
				task := newTask(msg.Type, msg.Payload, rw, ap, func(fn func(string, error, bool)) { p.afterTasks.Add(msg.ID, fn) })
				resCh <- p.perform(ctx, task)
			}()
			// finish task processing in p.fini() goroutine to allow this goroutine to end
			t := &processorTask{msg: msg, ctx: ctx, cleanup: cleanup, resCh: resCh}
			p.waitMu.Lock()
			if !p.waitClosed {
				p.wait <- t
			}
			p.waitMu.Unlock()
		}()
	}
}

// fini waits for tasks to complete and finishes processing each task.
func (p *processor) fini() {
	ctxDoneCh := func(t *processorTask) reflect.SelectCase {
		return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.ctx.Done())}
	}
	ctxDoneFn := func(t *processorTask) func(reflect.Value) {
		return func(_ reflect.Value) {
			p.handleFailedMessage(t.ctx, t.msg, "done", t.ctx.Err())
			t.cleanup()
			return
		}
	}

	resCh := func(t *processorTask) reflect.SelectCase {
		return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.resCh)}
	}
	resFn := func(t *processorTask) func(reflect.Value) {
		return func(v reflect.Value) {
			var resErr error
			switch err := v.Interface().(type) {
			case error:
				resErr = err
			case nil:
				resErr = nil
			}

			if resErr == AsynchronousTask {
				// task worker goroutine marked self as asynchronous task
				// check res again in case task already completed
				select {
				case err := <-t.resCh:
					resErr = err
				default:
					p.waitMu.Lock()
					if !p.waitClosed {
						// PENDING(GIL) - TODO
						//p.broker.MarkAsynchronous(p.serverID, t.msg)

						p.afterTasks.Delete(t.msg.ID)
						// wait again for async task to complete; do not cleanup
						p.wait <- t
					}
					p.waitMu.Unlock()
					return
				}
			}

			switch resErr {
			case nil:
				if !t.msg.Recurrent {
					p.handleSucceededMessage(t.ctx, t.msg)
				} else {
					p.requeue(t.ctx, t.msg)
				}
			case TaskTransitionDone:
				// nothing to do: just cleanup
			default:
				p.handleFailedMessage(t.ctx, t.msg, "errored", resErr)
			}
			t.cleanup()
			return
		}
	}

	// these should only be modified by the main fini() goroutine, to prevent races
	var tasks []*processorTask                  // list of waiting tasks
	selectChs := make([]reflect.SelectCase, 2)  // list of select case channel receives
	selectFns := make([]func(reflect.Value), 2) // list of corresponding select case operations

	// select index 0 reserved for p.abort
	abortIdx := 0
	selectChs[abortIdx] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.abort)}
	selectFns[abortIdx] = func(_ reflect.Value) {
		// time is up, push all messages back to queue and quit this goroutine
		abort := func(t *processorTask) {
			select {
			case <-p.abortNow:
				return
			default:
			}
			p.logger.Warnf("Quitting worker. task id=%s", t.msg.ID)
			p.requeueAborting(t.msg)
			t.cleanup()
		}
		for _, t := range tasks {
			abort(t)
		}
		for t := range p.wait { // closed by p.shutdown() after all tasks have completed and been cleaned up
			abort(t)
		}
		return
	}

	// select index 1 reserved for p.wait
	waitIdx := 1
	selectChs[waitIdx] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wait)}
	selectFns[waitIdx] = func(v reflect.Value) {
		select {
		case <-p.abortNow:
			return
		default:
		}
		// add task to waitlist
		t := v.Interface().(*processorTask)
		tasks = append(tasks, t)
		selectChs = append(selectChs, ctxDoneCh(t), resCh(t))
		selectFns = append(selectFns, ctxDoneFn(t), resFn(t))
		return
	}

	var quit bool
	for {
		select {
		case <-p.abortNow:
			return
		default:
			// Note: reflect.Select() is limited to 65536 cases at any one time; see https://pkg.go.dev/reflect#Select
			// As such, this implementation can support a concurrency of up to 32767 (=[65536-2]/2) tasks
			// PENDING: replace reflect.Select() with more efficient solution; consider https://stackoverflow.com/a/66125252
			i, v, ok := reflect.Select(selectChs)
			process := selectFns[i]
			if i == abortIdx {
				// need to abort lists, so process in current goroutine
				process(v)
				return
			} else if i == waitIdx {
				if ok {
					// need to update lists, so process in current goroutine
					process(v)
				} else {
					// p.wait closed; need to quit after finishing remaining tasks
					selectChs[waitIdx].Chan = reflect.Value{} // stop watching p.wait
					quit = true
				}
			} else {
				// remove task from lists
				i = i - (i % 2)                                       // reduce i to first index of pair
				tasks = append(tasks[:i/2-1], tasks[i/2:]...)         // remove (i/2-1)th item
				selectChs = append(selectChs[:i], selectChs[i+2:]...) // remove (i)th pair
				selectFns = append(selectFns[:i], selectFns[i+2:]...) // remove (i)th pair
				// process in separate goroutine to continue fini() immediately
				go process(v)
			}
			if quit && len(tasks) == 0 {
				// shutdown
				return
			}
		}
	}
}

func (p *processor) requeue(ctx context.Context, msg *base.TaskMessage) {

	err := p.broker.Requeue(p.serverID, msg, false)
	if err == nil {
		p.logger.Infof("Pushed recurrent task id=%s back to queue", msg.ID)
		return
	}

	p.logger.Errorf("Could not push recurrent task id=%s back to queue: %v", msg.ID, err)
	errMsg := fmt.Sprintf("Could not push recurrent task id=%s type=%q from %q err: %+v", msg.ID, msg.Type, base.ActiveKey(msg.Queue), err)
	deadline, ok := ctx.Deadline()
	if !ok {
		panic("asynq: internal error: missing deadline in context")
	}
	p.logger.Warnf("%s; Will retry syncing", errMsg)
	p.syncRequestCh <- &syncRequest{
		fn: func() error {
			return p.broker.Requeue(p.serverID, msg, false)
		},
		errMsg:   errMsg,
		deadline: deadline,
	}

}

func (p *processor) requeueAborting(msg *base.TaskMessage) {
	err := p.broker.Requeue(p.serverID, msg, true)
	if err != nil {
		p.logger.Errorf("Could not push task id=%s back to queue: %v", msg.ID, err)
	} else {
		p.logger.Infof("Pushed task id=%s back to queue", msg.ID)
	}
}

func (p *processor) taskFinished(msg *base.TaskMessage, err error, isFailure bool) {
	if fn, ok := p.afterTasks.Get(msg.ID); ok {
		fn(msg.ID, err, isFailure)
		p.afterTasks.Delete(msg.ID)
	}
}

func (p *processor) handleSucceededMessage(ctx context.Context, msg *base.TaskMessage) {
	if msg.Retention > 0 {
		p.markAsComplete(ctx, msg)
	} else {
		p.markAsDone(ctx, msg)
	}
}

func (p *processor) markAsComplete(ctx context.Context, msg *base.TaskMessage) {
	markAsComplete := func() error {
		err := p.broker.MarkAsComplete(p.serverID, msg)
		if err == nil {
			p.taskFinished(msg, nil, false)
		}
		return err
	}
	err := markAsComplete()
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s type=%q from %q to %q:  %+v",
			msg.ID, msg.Type, base.ActiveKey(msg.Queue), base.CompletedKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return markAsComplete()
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) markAsDone(ctx context.Context, msg *base.TaskMessage) {
	markAsDone := func() error {
		err := p.broker.Done(p.serverID, msg)
		if err == nil {
			p.taskFinished(msg, nil, false)
		}
		return err
	}

	err := markAsDone()
	if err != nil {
		errMsg := fmt.Sprintf("Could not remove task id=%s type=%q from %q err: %+v", msg.ID, msg.Type, base.ActiveKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return markAsDone()
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) handleFailedMessage(ctx context.Context, msg *base.TaskMessage, reason string, err error) {
	p.logger.Debugf("Retry or archive (%s) task id=%s error:%v", reason, msg.ID, err)
	if p.errHandler != nil {
		p.errHandler.HandleError(ctx, NewTask(msg.Type, msg.Payload), err)
	}
	if !p.isFailureFunc(err) {
		// retry the task without marking it as failed
		p.logger.Debugf("Retrying task id=%s - not failed - (err: %v)", msg.ID, err)
		p.retry(ctx, msg, err, false /*isFailure*/)
		return
	}
	if msg.Retried >= msg.Retry || errors.Is(err, SkipRetry) || errors.Is(err, TaskCanceled) {
		if msg.Retried >= msg.Retry {
			p.logger.Warnf("Retry exhausted for task id=%s", msg.ID)
		} else {
			p.logger.Debugf("Archiving task id=%s (err: %v)", msg.ID, err)
		}
		p.archive(ctx, msg, err)
	} else {
		p.logger.Debugf("Retrying task id=%s (err: %v)", msg.ID, err)
		p.retry(ctx, msg, err, true /*isFailure*/)
	}
}

func (p *processor) retry(ctx context.Context, msg *base.TaskMessage, e error, isFailure bool) {
	retried := msg.Retried
	if msg.Recurrent {
		retried = 0
	}
	d := p.retryDelay.RetryDelay(retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)

	retry := func() error {
		err := p.broker.Retry(msg, retryAt, e.Error(), isFailure)
		if err == nil {
			p.taskFinished(msg, e, isFailure)
		}
		return err
	}

	err := retry()
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q: %+v", msg.ID, base.ActiveKey(msg.Queue), base.RetryKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return retry()
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) archive(ctx context.Context, msg *base.TaskMessage, e error) {
	archive := func() error {
		err := p.broker.Archive(msg, e.Error())
		if err == nil {
			p.taskFinished(msg, e, true)
		}
		return err
	}

	err := archive()
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q: %+v", msg.ID, base.ActiveKey(msg.Queue), base.ArchivedKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return archive()
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

// perform calls the handler with the given task.
// If the call returns without panic, it simply returns the value,
// otherwise, it recovers from panic and returns an error.
func (p *processor) perform(ctx context.Context, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			p.logger.Errorf("recovering from panic. See the stack trace below for details:\n%s", string(debug.Stack()))
			_, file, line, ok := runtime.Caller(1) // skip the first frame (panic itself)
			if ok && strings.Contains(file, "runtime/") {
				// The panic came from the runtime, most likely due to incorrect
				// map/slice usage. The parent frame should have the real trigger.
				_, file, line, ok = runtime.Caller(2)
			}

			// Include the file and line number info in the error, if runtime.Caller returned ok.
			if ok {
				err = fmt.Errorf("panic [%s:%d]: %v", file, line, x)
			} else {
				err = fmt.Errorf("panic: %v", x)
			}
		}
	}()
	return p.handler.ProcessTask(ctx, task)
}

func (p *processor) moveToQueue(ctx context.Context, msg *base.TaskMessage, newQueue, typename string, active bool, opts ...Option) (*TaskInfo, error) {
	newQueue = strings.TrimSpace(newQueue)
	if newQueue == "" {
		return nil, errors.New("queue name cannot be empty")
	}
	if newQueue == msg.Queue {
		return nil, errors.New("new queue name equals existing queue")
	}
	pmsg := *msg

	newMsg := &pmsg
	if typename != "" {
		newMsg.Type = typename
	}
	newMsg.Retried = 0
	newMsg.ErrorMsg = ""
	newMsg.Queue = newQueue

	processAt := time.Time{}
	now := p.broker.Now()

	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			newMsg.Retry = int(opt)
		case queueOption:
			qname := strings.TrimSpace(string(opt))
			if qname != newQueue {
				return nil, errors.New("queue name specified twice")
			}
		case taskIDOption:
			return nil, errors.New("task ID cannot be changed")
		case timeoutOption:
			newMsg.Timeout = int64(time.Duration(opt).Round(time.Second).Seconds())
		case deadlineOption:
			newMsg.Deadline = time.Time(opt).Unix()
		case uniqueOption:
			ttl := time.Duration(opt)
			if ttl < 1*time.Second {
				return nil, errors.New("Unique TTL cannot be less than 1s")
			}
			newMsg.UniqueKeyTTL = int64(ttl.Seconds())
		case processAtOption:
			processAt = time.Time(opt)
		case processInOption:
			processAt = now.Add(time.Duration(opt))
		case forceUniqueOption:
			// ignore
		case recurrentOption:
			newMsg.Recurrent = bool(opt)
		case reprocessAfterOption:
			newMsg.ReprocessAfter = int64(time.Duration(opt).Seconds())
		case serverAffinityOption:
			if _, ok := p.broker.(*rdb.RDB); ok {
				return nil, errors.New("server affinity not supported with redis")
			}
			newMsg.ServerAffinity = int64(time.Duration(opt).Seconds())
		case retentionOption:
			newMsg.Retention = int64(time.Duration(opt).Seconds())
		default:
			// ignore unexpected option
		}
	}

	state, err := p.broker.MoveToQueue(ctx, msg.Queue, newMsg, processAt, active)
	if err != nil {
		return nil, err
	}

	return newTaskInfo(newMsg, state, processAt, nil), nil
}

func (p *processor) asynchronousHandler() AsynchronousHandler {
	p.asynchronousOnce.Do(func() {
		if p.asynchronous == nil {
			p.asynchronous = &asynchronousHandler{p: p}
		}
	})
	return p.asynchronous
}

type asynchronousHandler struct {
	p *processor
}

// UpdateTask updates the result of the given tasks with the given data.
func (a *asynchronousHandler) UpdateTask(queueName, taskId string, data []byte) (*TaskInfo, time.Time, error) {
	info, dl, err := a.updateTask(queueName, taskId, data)
	if err != nil {
		return nil, dl, err
	}
	return newTaskInfo(info.Message, info.State, info.NextProcessAt, info.Result), dl, nil
}

func (a *asynchronousHandler) updateTask(queueName, taskId string, data []byte) (*base.TaskInfo, time.Time, error) {
	deadline := time.Time{}
	ti, deadline, err := a.p.broker.UpdateTask(queueName, taskId, data)

	switch {
	case errors.IsQueueNotFound(err):
		return nil, deadline, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case errors.IsTaskNotFound(err):
		return nil, deadline, fmt.Errorf("asynq: %w", ErrTaskNotFound)
	case err != nil:
		return nil, deadline, fmt.Errorf("asynq: %v", err)
	}

	return ti, deadline, nil
}

// RequeueCompleted moves the completed task with the given taskId in the given queue to the target queue.
// The task is enqueued or scheduled with the given task type and options.
// This call fails if the task is not in completed state.
func (a *asynchronousHandler) RequeueCompleted(ctx context.Context, fromQueue, taskId, toQueue, typeName string, opts ...Option) (*TaskInfo, error) {
	ti, err := a.p.broker.Inspector().GetTaskInfo(fromQueue, taskId)
	if err != nil {
		return nil, err
	}

	ret, err := a.p.moveToQueue(ctx, ti.Message, toQueue, typeName, false, opts...)
	if err != nil {
		return nil, err
	}
	ret.Result = ti.Result
	return ret, nil
}

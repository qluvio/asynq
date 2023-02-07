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
	"golang.org/x/time/rate"
)

// SkipRetry is used as a return value from Handler.ProcessTask to indicate that
// the task should not be retried and should be archived instead.
var SkipRetry = errors.New("skip retry for the task")

// AsynchronousTask is used as a return value from Handler.ProcessTask to
// indicate that the task is processing asynchronously separately from the main
// worker goroutine.
var AsynchronousTask = errors.New("task is processing asynchronously")

type processor struct {
	logger   *log.Logger
	serverID string
	broker   base.Broker

	handler Handler

	queues Queues

	retryDelayFunc RetryDelayFunc
	isFailureFunc  func(error) bool

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
	wait chan *processorTask

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort chan struct{}

	// cancelations is a set of cancel functions for all active tasks.
	cancelations *base.Cancelations

	starting chan<- *workerInfo
	finished chan<- *base.TaskMessage

	emptyQSleep time.Duration
}

type processorParams struct {
	logger          *log.Logger
	serverID        string
	broker          base.Broker
	retryDelayFunc  RetryDelayFunc
	isFailureFunc   func(error) bool
	syncCh          chan<- *syncRequest
	cancelations    *base.Cancelations
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
		retryDelayFunc:  params.retryDelayFunc,
		isFailureFunc:   params.isFailureFunc,
		syncRequestCh:   params.syncCh,
		cancelations:    params.cancelations,
		errLogLimiter:   rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:            make(chan struct{}, params.concurrency),
		done:            make(chan struct{}),
		wait:            make(chan *processorTask, params.concurrency),
		quit:            make(chan struct{}),
		abort:           make(chan struct{}),
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
	p.stop()

	time.AfterFunc(p.shutdownTimeout, func() { close(p.abort) })

	p.logger.Info("Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	close(p.wait)
	p.logger.Info("All workers have finished")
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
		p.logger.Debug("dequeued %s -> %v", msg.ID, deadline)

		p.starting <- &workerInfo{msg: msg, started: time.Now(), deadline: deadline}
		go func() {
			ctx, cancel := asynqcontext.New(msg, deadline)
			p.cancelations.Add(msg.ID, cancel)
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
				return
			default:
			}

			resCh := make(chan error, 2)
			rw := &ResultWriter{id: msg.ID, qname: msg.Queue, broker: p.broker, ctx: ctx}
			ap := &AsyncProcessor{resCh: resCh}
			// hold mutex until worker goroutine returns; ensures that AsyncProcessor does not send to resCh first
			ap.mutex.Lock()
			go func() {
				defer ap.mutex.Unlock()
				task := newTask(msg.Type, msg.Payload, rw, ap)
				resCh <- p.perform(ctx, task)
			}()
			// finish task processing in p.fini() goroutine to allow this goroutine to end
			t := &processorTask{msg: msg, ctx: ctx, cleanup: cleanup, resCh: resCh}
			p.wait <- t
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
					// wait again for async task to complete; do not cleanup
					p.wait <- t
					return
				}
			}
			if resErr != nil {
				p.handleFailedMessage(t.ctx, t.msg, "errored", resErr)
				t.cleanup()
				return
			}
			if !t.msg.Recurrent {
				p.handleSucceededMessage(t.ctx, t.msg)
			} else {
				p.requeue(t.ctx, t.msg)
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
		// add task to waitlist
		t := v.Interface().(*processorTask)
		tasks = append(tasks, t)
		selectChs = append(selectChs, ctxDoneCh(t), resCh(t))
		selectFns = append(selectFns, ctxDoneFn(t), resFn(t))
		return
	}

	var quit bool
	for {
		// Note: reflect.Select() is limited to 65536 cases at any one time; see https://pkg.go.dev/reflect#Select
		// As such, this implementation can support a concurrency of up to 32767 (=65536/2-1) tasks
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

func (p *processor) handleSucceededMessage(ctx context.Context, msg *base.TaskMessage) {
	if msg.Retention > 0 {
		p.markAsComplete(ctx, msg)
	} else {
		p.markAsDone(ctx, msg)
	}
}

func (p *processor) markAsComplete(ctx context.Context, msg *base.TaskMessage) {
	err := p.broker.MarkAsComplete(p.serverID, msg)
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
				return p.broker.MarkAsComplete(p.serverID, msg)
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) markAsDone(ctx context.Context, msg *base.TaskMessage) {
	err := p.broker.Done(p.serverID, msg)
	if err != nil {
		errMsg := fmt.Sprintf("Could not remove task id=%s type=%q from %q err: %+v", msg.ID, msg.Type, base.ActiveKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Done(p.serverID, msg)
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) handleFailedMessage(ctx context.Context, msg *base.TaskMessage, reason string, err error) {
	p.logger.Debugf("Retry or archive (%s) task id=%s error:%s", reason, msg.ID, err)
	if p.errHandler != nil {
		p.errHandler.HandleError(ctx, NewTask(msg.Type, msg.Payload), err)
	}
	if !p.isFailureFunc(err) {
		// retry the task without marking it as failed
		p.retry(ctx, msg, err, false /*isFailure*/)
		return
	}
	if msg.Retried >= msg.Retry || errors.Is(err, SkipRetry) {
		p.logger.Warnf("Retry exhausted for task id=%s", msg.ID)
		p.archive(ctx, msg, err)
	} else {
		p.retry(ctx, msg, err, true /*isFailure*/)
	}
}

func (p *processor) retry(ctx context.Context, msg *base.TaskMessage, e error, isFailure bool) {
	retried := msg.Retried
	if msg.Recurrent {
		retried = 0
	}
	d := p.retryDelayFunc(retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)
	err := p.broker.Retry(msg, retryAt, e.Error(), isFailure)
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.RetryKey(msg.Queue))
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Retry(msg, retryAt, e.Error(), isFailure)
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) archive(ctx context.Context, msg *base.TaskMessage, e error) {
	err := p.broker.Archive(msg, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.ArchivedKey(msg.Queue))
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Archive(msg, e.Error())
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

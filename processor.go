// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"go.uber.org/atomic"
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
	sema   chan struct{}
	qsemas map[string]chan struct{} // queue string -> chan

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort    chan struct{}
	abortNow chan struct{}

	// tasks is a set of active tasks.
	tasks      map[string]*processorTask // task id -> task info
	tasksMutex sync.Mutex

	// results channel streams task results to fini() to finish processing tasks.
	results chan processorResult

	// deadlines is a set of deadlines for all active tasks.
	deadlines *base.Deadlines

	// cancelations is a set of cancel functions for all active tasks.
	cancelations *base.Cancelations

	// afterTasks is a set of function set by active tasks to be called after task execution
	afterTasks *base.AfterTasks

	starting chan<- *workerInfo
	finished chan<- *base.TaskMessage

	emptyQSleep time.Duration
	firstEmptyQ time.Time
	lastEmptyQ  time.Time
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
	concurrency     int
	queues          Queues
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *workerInfo
	finished        chan<- *base.TaskMessage
	emptyQSleep     time.Duration
}

type processorTask struct {
	ctx     context.Context
	msg     *base.TaskMessage
	done    *atomic.Bool
	cleanup func()
}

type processorResult struct {
	task *processorTask
	err  error
}

// newProcessor constructs a new processor.
func newProcessor(params processorParams) *processor {
	qsemas := make(map[string]chan struct{})
	for q, c := range params.queues.Concurrencies() {
		if c <= 0 {
			c = params.concurrency
		}
		qsemas[q] = make(chan struct{}, c)
	}
	abortNow := make(chan struct{})
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
		qsemas:          qsemas,
		done:            make(chan struct{}),
		quit:            make(chan struct{}),
		abort:           make(chan struct{}),
		abortNow:        abortNow,
		tasks:           make(map[string]*processorTask),
		results:         make(chan processorResult, params.concurrency),
		deadlines:       base.NewDeadlines(abortNow, params.concurrency),
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

		// should be no work left to wait for; abort processes
		close(p.abortNow)
	}

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
		// Only attempt to dequeue a task from queues that are under their respective concurrency limits
		qnames := []string{}
		qsemas := make(map[string]chan struct{})
		for _, qn := range p.queues.Names() {
			qs := p.qsemas[qn]
			select {
			case qs <- struct{}{}: // acquire queue token
				qnames = append(qnames, qn)
				qsemas[qn] = qs
			default:
				continue
			}
		}
		t := time.Now()
		msg, deadline, err := p.broker.Dequeue(p.serverID, qnames...)
		p.logger.Debugf("Dequeue [%s], qnames=%v, msg=%v, err=%s", time.Since(t).String(), qnames, msg, err.Error())
		switch {
		case errors.Is(err, errors.ErrNoProcessableTask):
			if p.lastEmptyQ.IsZero() || time.Since(p.lastEmptyQ) >= time.Minute {
				p.lastEmptyQ = time.Now()
				if p.firstEmptyQ.IsZero() {
					p.firstEmptyQ = p.lastEmptyQ
				}
				p.logger.Debugf("All queues are empty and/or concurrency limits reached [%s]", time.Since(p.firstEmptyQ).String())
			}
			// Queues are empty and/or concurrency limits reached, this is a normal behavior.
			// Sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: We are not using blocking pop operation and polling queues instead.
			// This adds significant load to redis.
			time.Sleep(p.emptyQSleep)
			releaseQSemas(qsemas, "") // release queue tokens
			<-p.sema                  // release token
			return
		case err != nil:
			p.firstEmptyQ = time.Time{}
			p.lastEmptyQ = time.Time{}
			if p.errLogLimiter.Allow() {
				p.logger.Errorf("Dequeue error: %v", err)
			}
			// also sleep, otherwise we create a busy loop until errors clear...
			time.Sleep(p.emptyQSleep)
			releaseQSemas(qsemas, "") // release queue tokens
			<-p.sema                  // release token
			return
		}
		p.firstEmptyQ = time.Time{}
		p.lastEmptyQ = time.Time{}
		p.logger.Debugf("dequeued %s -> %v", msg.ID, deadline)
		qsema := releaseQSemas(qsemas, msg.Queue) // release unused queue tokens

		go func() {
			p.starting <- &workerInfo{msg: msg, started: time.Now(), deadline: deadline}

			ctx, cancel := asynqcontext.New(msg, deadline)
			cleanup := func() {
				cancel()
				p.cancelations.Delete(msg.ID)
				p.deadlines.Delete(msg.ID)
				p.finished <- msg
				<-qsema  // release queue token
				<-p.sema // release token
			}

			ptask := &processorTask{ctx: ctx, msg: msg, done: atomic.NewBool(false), cleanup: cleanup}
			p.tasksMutex.Lock()
			p.tasks[msg.ID] = ptask
			p.tasksMutex.Unlock()
			p.deadlines.Add(msg.ID, deadline, func() {
				p.results <- processorResult{task: ptask, err: context.DeadlineExceeded}
				// ctx will be canceled via context deadline
			})
			p.cancelations.Add(msg.ID, func() {
				p.results <- processorResult{task: ptask, err: TaskCanceled}
				cancel()
			})

			// check context before starting a worker goroutine.
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded); do nothing
				return
			default:
			}

			rw := &ResultWriter{id: msg.ID, qname: msg.Queue, broker: p.broker, ctx: ctx}
			ap := &AsyncProcessor{results: p.results, task: ptask}
			go func() {
				task := newTask(msg.Type, msg.Payload, rw, ap, func(fn func(string, error, bool)) { p.afterTasks.Add(msg.ID, fn) })
				p.results <- processorResult{task: ptask, err: p.perform(ctx, task)}
			}()
		}()
	}
}

// fini waits for tasks to complete and finishes processing each task.
func (p *processor) fini() {
	for {
		select {
		case <-p.abortNow:
			return
		case <-p.abort:
			// time is up, push all messages back to queue and quit this goroutine
			// assumes that exec() is no longer running
			p.tasksMutex.Lock()
			defer p.tasksMutex.Unlock()
			for _, t := range p.tasks {
				select {
				case <-p.abortNow:
					return
				default:
				}
				p.logger.Warnf("Quitting worker. task id=%s", t.msg.ID)
				p.requeueAborting(t.msg)
				t.cleanup()
			}
			return
		case r := <-p.results:
			if errors.Is(r.err, AsynchronousTask) {
				// task worker goroutine marked self as asynchronous task; do nothing
				break
			}
			done := r.task.done.Swap(true)
			if !done {
				// process in separate goroutine to continue fini() immediately
				go func() {
					defer r.task.cleanup()
					p.tasksMutex.Lock()
					delete(p.tasks, r.task.msg.ID)
					p.tasksMutex.Unlock()
					if r.err != nil {
						reason := "errored"
						if errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded) {
							reason = "done"
						}
						p.handleFailedMessage(r.task.ctx, r.task.msg, reason, r.err)
					} else if r.task.msg.Recurrent {
						p.requeue(r.task.ctx, r.task.msg)
					} else {
						p.handleSucceededMessage(r.task.ctx, r.task.msg)
					}
					return
				}()
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

// releaseQSemas releases the given queue tokens, except for the queue token associated with skipQueues, if specified.
// Returns the qsema for the skipped queue
func releaseQSemas(qsemas map[string]chan struct{}, skipQueue string) chan struct{} {
	var ret chan struct{}
	for qname, qsema := range qsemas {
		if qname == skipQueue {
			ret = qsema
			continue
		}
		<-qsema
	}
	return ret
}

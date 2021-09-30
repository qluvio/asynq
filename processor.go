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
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"golang.org/x/time/rate"
)

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
	concurrency     int
	queues          Queues
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *workerInfo
	finished        chan<- *base.TaskMessage
	emptyQSleep     time.Duration
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
	p.logger.Info("All workers have finished")
}

func (p *processor) start(wg *sync.WaitGroup) {
	wg.Add(1)
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
			<-p.sema // release token
			return
		}

		p.starting <- &workerInfo{msg: msg, started: time.Now(), deadline: deadline}
		go func() {
			defer func() {
				p.finished <- msg
				<-p.sema // release token
			}()

			ctx, cancel := createContext(msg, deadline)
			p.cancelations.Add(msg.ID.String(), cancel)
			defer func() {
				cancel()
				p.cancelations.Delete(msg.ID.String())
			}()

			// check context before starting a worker goroutine.
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded).
				p.retryOrArchive(ctx, msg, ctx.Err())
				return
			default:
			}

			resCh := make(chan error, 1)
			go func() {
				resCh <- p.perform(ctx, NewTask(msg.Type, msg.Payload))
			}()

			select {
			case <-p.abort:
				// time is up, push the message back to queue and quit this worker goroutine.
				p.logger.Warnf("Quitting worker. task id=%s", msg.ID)
				p.requeueAborting(msg)
				return
			case <-ctx.Done():
				p.retryOrArchive(ctx, msg, ctx.Err())
				return
			case resErr := <-resCh:
				// Note: One of three things should happen.
				// 1) Done     -> Removes the message from Active
				// 2) Retry    -> Removes the message from Active & Adds the message to Retry
				// 3) Archive  -> Removes the message from Active & Adds the message to archive
				if resErr != nil {
					p.retryOrArchive(ctx, msg, resErr)
					return
				}
				if !msg.Recurrent {
					p.markAsDone(ctx, msg)
				} else {
					p.requeue(ctx, msg)
				}
			}
		}()
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

// SkipRetry is used as a return value from Handler.ProcessTask to indicate that
// the task should not be retried and should be archived instead.
var SkipRetry = errors.New("skip retry for the task")

func (p *processor) retryOrArchive(ctx context.Context, msg *base.TaskMessage, err error) {
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
	d := p.retryDelayFunc(msg.Retried, e, NewTask(msg.Type, msg.Payload))
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

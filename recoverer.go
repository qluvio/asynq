// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type recoverer struct {
	logger        *log.Logger
	broker        base.Broker
	retryDelay    RetryDelayHandler
	isFailureFunc func(error) bool

	// channel to communicate back to the long running "recoverer" goroutine.
	done chan struct{}

	// list of queues to check for deadline.
	queues queues

	// poll interval.
	interval time.Duration

	// expired task
	expiration time.Duration

	// health provider (the healthchecker)
	healthStarted chan struct{}
	healthy       chan struct{}
}

type recovererParams struct {
	logger         *log.Logger
	broker         base.Broker
	queues         queues
	interval       time.Duration
	expiration     time.Duration
	retryDelayFunc RetryDelayHandler
	isFailureFunc  func(error) bool
	healthCheck    healthChecker
}

func newRecoverer(params recovererParams) *recoverer {
	var health chan struct{}
	var healthy chan struct{}
	if params.healthCheck != nil {
		healthy = make(chan struct{})
		params.healthCheck.subscribeHealth(healthy)
		health = params.healthCheck.startedChan()
	}
	return &recoverer{
		logger:        params.logger,
		broker:        params.broker,
		done:          make(chan struct{}),
		queues:        params.queues,
		interval:      params.interval,
		expiration:    params.expiration,
		retryDelay:    params.retryDelayFunc,
		isFailureFunc: params.isFailureFunc,
		healthStarted: health,
		healthy:       healthy,
	}
}

func (r *recoverer) shutdown() {
	r.logger.Debug("Recoverer shutting down...")
	// Signal the recoverer goroutine to stop polling.
	r.done <- struct{}{}
}

func (r *recoverer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// perform an initial run
		if !onceHealthy(
			r.healthStarted,
			r.healthy,
			r.done,
			"Recoverer",
			r.logger,
			r.recover) {
			return
		}

		timer := time.NewTimer(r.interval)
		for {
			select {
			case <-r.done:
				r.logger.Debug("Recoverer done")
				timer.Stop()
				return
			case <-timer.C:
				r.recover()
				timer.Reset(r.interval)
			}
		}
	}()
}

func (r *recoverer) recover() {
	// Get all tasks which have expired 30 seconds ago or earlier.
	deadline := time.Now().Add(-r.expiration)
	msgs, err := r.broker.ListDeadlineExceeded(deadline, r.queues.Names()...)
	if err != nil {
		r.logger.Warnf("recoverer: could not list deadline-exceeded tasks: %+v", err)
		return
	}
	r.logger.Debugf("recoverer: deadline-exceeded tasks count: %d", len(msgs))
	for _, msg := range msgs {
		if msg.Retried >= msg.Retry {
			r.archive(msg, context.DeadlineExceeded)
		} else {
			r.retry(msg, context.DeadlineExceeded)
		}
	}
}

func (r *recoverer) retry(msg *base.TaskMessage, err error) {
	delay := r.retryDelay.RetryDelay(msg.Retried, err, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(delay)
	if err := r.broker.Retry(msg, retryAt, err.Error(), r.isFailureFunc(err)); err != nil {
		r.logger.Warnf("recoverer: could not retry deadline exceeded task: %v", err)
	}
}

func (r *recoverer) archive(msg *base.TaskMessage, err error) {
	if err := r.broker.Archive(msg, err.Error()); err != nil {
		r.logger.Warnf("recoverer: could not move task to archive: %v", err)
	}
}

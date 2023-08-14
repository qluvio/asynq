// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// A forwarder is responsible for moving scheduled and retry tasks to pending state
// so that the tasks get processed by the workers.
type forwarder struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "forwarder" goroutine.
	done chan struct{}

	// list of queue names to check and enqueue.
	queues Queues

	// poll interval on average
	avgInterval time.Duration

	// health provider (the healthchecker)
	healthStarted chan struct{}
	healthy       chan struct{}

	// channel to receive requests to immediately forward tasks
	wakeCh    <-chan bool
	wakePrcCh chan<- bool
}

type forwarderParams struct {
	logger      *log.Logger
	broker      base.Broker
	queues      Queues
	interval    time.Duration
	healthCheck healthChecker
	wakeCh      <-chan bool
	wakePrcCh   chan<- bool
}

func newForwarder(params forwarderParams) *forwarder {
	var health chan struct{}
	var healthy chan struct{}
	if params.healthCheck != nil {
		healthy = make(chan struct{})
		params.healthCheck.subscribeHealth(healthy)
		health = params.healthCheck.startedChan()
	}
	return &forwarder{
		logger:        params.logger,
		broker:        params.broker,
		done:          make(chan struct{}),
		queues:        params.queues,
		avgInterval:   params.interval,
		healthStarted: health,
		healthy:       healthy,
		wakeCh:        params.wakeCh,
		wakePrcCh:     params.wakePrcCh,
	}
}

func (f *forwarder) shutdown() {
	f.logger.Debug("Forwarder shutting down...")
	// Signal the forwarder goroutine to stop polling.
	f.done <- struct{}{}
}

// start starts the "forwarder" goroutine.
func (f *forwarder) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// perform an initial run
		if !onceHealthy(
			f.healthStarted,
			f.healthy,
			f.done,
			"Forwarder",
			f.logger,
			f.exec) {
			return
		}

		for {
			select {
			case <-f.done:
				f.logger.Debug("Forwarder done")
				return
			case <-f.wakeCh:
				f.exec()
			case <-time.After(f.avgInterval):
				f.exec()
			}
		}
	}()
}

func (f *forwarder) exec() {
	if n, err := f.broker.ForwardIfReady(f.queues.Names()...); err != nil {
		f.logger.Errorf("Could not enqueue scheduled tasks: %v", err)
	} else if n > 0 {
		select {
		case f.wakePrcCh <- true:
		default:
		}
	}
}

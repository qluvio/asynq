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

// healthChecker is the interface (of healthchecker) used by components wishing
// to be notified once a healthy state is reached. This enables these components
// to perform an initial action immediately instead of relying on polling periods.
// see func onceHealthy.
type healthChecker interface {
	// startedChan returns a channel that will be closed once the healthChecker
	// started and ran one health check.
	startedChan() chan struct{}
	// subscribeHealth is used to register the given chan for being notified
	// of health. The channel is closed once a healthy state is reached for the
	// first time.
	subscribeHealth(chan struct{})
}

// healthchecker is responsible for pinging broker periodically
// and call user provided HeathCheckFunc with the ping result.
type healthchecker struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "healthchecker" goroutine.
	done chan struct{}

	// interval between healthchecks.
	interval time.Duration

	// channel that is closed once the healthchecker made his first check
	started chan struct{}

	// function to call periodically.
	healthcheckFunc func(error)

	subscribers []chan struct{}
}

type healthcheckerParams struct {
	logger          *log.Logger
	broker          base.Broker
	interval        time.Duration
	healthcheckFunc func(error)
}

func newHealthChecker(params healthcheckerParams) *healthchecker {
	return &healthchecker{
		logger:          params.logger,
		broker:          params.broker,
		done:            make(chan struct{}),
		interval:        params.interval,
		started:         make(chan struct{}),
		healthcheckFunc: params.healthcheckFunc,
	}
}

func (hc *healthchecker) shutdown() {
	hc.logger.Debug("Healthchecker shutting down...")
	// Signal the healthchecker goroutine to stop.
	hc.done <- struct{}{}
}

func (hc *healthchecker) startedChan() chan struct{} {
	return hc.started
}

func (hc *healthchecker) subscribeHealth(h chan struct{}) {
	hc.subscribers = append(hc.subscribers, h)
}

func (hc *healthchecker) healthcheck(err error, started bool) {
	if hc.healthcheckFunc != nil {
		hc.healthcheckFunc(err)
	}
	if !started || err != nil || len(hc.subscribers) == 0 {
		return
	}
	for _, h := range hc.subscribers {
		close(h)
	}
	hc.subscribers = nil
}

func (hc *healthchecker) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// do an initial health check
		select {
		case <-hc.done:
			hc.logger.Debug("Healthchecker done!")
			return
		default:
			err := hc.broker.Ping()
			hc.healthcheck(err, false)
		}
		close(hc.started)

		timer := time.NewTimer(hc.interval)
		for {
			select {
			case <-hc.done:
				hc.logger.Debug("Healthchecker done")
				timer.Stop()
				return
			case <-timer.C:
				err := hc.broker.Ping()
				hc.healthcheck(err, true)
				timer.Reset(hc.interval)
			}
		}
	}()
}

// onceHealthy runs the exec function immediately if healthStarted is nil.
// Otherwise, it first waits until healthStarted is closed then until healthy is
// closed - unless (in both cases) the done channel was closed.
// It returns false when returning because done was closed, true otherwise.
func onceHealthy(
	healthStarted chan struct{},
	healthy chan struct{},
	done chan struct{},
	component string,
	logger *log.Logger,
	exec func()) bool {

	if healthStarted != nil {
		select {
		case <-done:
			logger.Debug(component + " done!!")
			return false
		case <-healthStarted:
		}

		select {
		case <-done:
			logger.Debug(component + " done!")
			return false
		case <-healthy:
		}
	}
	exec()
	return true
}

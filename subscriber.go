// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	once sync.Once
	done chan struct{}

	// cancelations hold cancel functions for all active tasks.
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

type subscriberParams struct {
	logger       *log.Logger
	broker       base.Broker
	cancelations *base.Cancelations
	retryTimeout time.Duration
}

func newSubscriber(params subscriberParams) *subscriber {
	return &subscriber{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}, 1),
		cancelations: params.cancelations,
		retryTimeout: params.retryTimeout,
	}
}

func (s *subscriber) shutdown() {
	s.once.Do(func() {
		s.logger.Debug("Subscriber shutting down...")

		// Signal the subscriber goroutine to stop.
		s.done <- struct{}{}
	})
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var (
			pubsub base.PubSub
			err    error
		)
		// Try until successfully connect to Redis.
		for {
			pubsub, err = s.broker.CancelationPubSub()
			if err != nil {
				s.logger.Errorf("cannot subscribe to cancelation channel: %v", err)
				select {
				case <-time.After(s.retryTimeout):
					continue
				case <-s.done:
					s.logger.Debug("Subscriber done")
					return
				}
			}
			break
		}
		cancelCh := pubsub.Channel()
		for {
			select {
			case <-s.done:
				_ = pubsub.Close()
				s.logger.Debug("Subscriber done")
				return
			case msg, alive := <-cancelCh:
				if !alive {
					s.logger.Debug("Subscriber channel closed: shutting down")
					s.shutdown()
					continue
				}
				id, ok := msg.(string)
				if !ok {
					if msg != nil {
						// avoid clobbering logs with nil warnings
						s.logger.Warn(fmt.Sprintf("Subscriber: invalid value %v", msg))
					}
					continue
				}
				cancel, ok := s.cancelations.Get(id)
				if ok {
					cancel()
				}
			}
		}
	}()
}

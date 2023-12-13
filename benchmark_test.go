// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	h "github.com/hibiken/asynq/internal/asynqtest"
)

// Creates a new task of type "task<n>" with payload {"data": n}.
func makeTask(n int) *Task {
	b, err := json.Marshal(map[string]int{"data": n})
	if err != nil {
		panic(err)
	}
	return NewTask(fmt.Sprintf("task%d", n), b)
}

// Simple E2E Benchmark testing with no scheduled tasks and retries.
func BenchmarkEndToEndSimple(b *testing.B) {
	ctx := setupTestContext(b)
	defer func() { _ = ctx.Close() }()

	const count = 1000 //100000
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		ctx.FlushDB()

		client := NewClient(getClientConnOpt(b))

		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
				return time.Second
			}),
			LogLevel: testLogLevel,
		})
		// Create a bunch of tasks
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(makeTask(i)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(count)
		handler := func(ctx context.Context, t *Task) error {
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		_ = srv.Start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		srv.Stop()
		_ = client.Close()
		b.StartTimer() // end teardown
	}
}

// E2E benchmark with scheduled tasks and retries.
func BenchmarkEndToEnd(b *testing.B) {
	ctx := setupTestContext(b)
	defer func() { _ = ctx.Close() }()

	const count = 1000 //100000
	for n := 0; n < b.N; n++ {
		ctx.FlushDB()

		client := NewClient(getClientConnOpt(b))
		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
				return time.Second
			}),
			LogLevel: testLogLevel,
		})
		// Create a bunch of tasks
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(makeTask(i)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(makeTask(i), ProcessIn(1*time.Second)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(count * 2)
		handler := func(ctx context.Context, t *Task) error {
			var p map[string]int
			if err := json.Unmarshal(t.Payload(), &p); err != nil {
				b.Logf("internal error: %v", err)
			}
			n, ok := p["data"]
			if !ok {
				n = 1
				b.Logf("internal error: could not get data from payload")
			}
			retried, ok := GetRetryCount(ctx)
			if !ok {
				b.Logf("internal error: could not get retry count from context")
			}
			// Fail 1% of tasks for the first attempt.
			if retried == 0 && n%100 == 0 {
				return fmt.Errorf(":(")
			}
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		_ = srv.Start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		srv.Stop()
		_ = client.Close()
		b.StartTimer() // end teardown
	}
}

// Simple E2E Benchmark testing with no scheduled tasks and retries with multiple queues.
func BenchmarkEndToEndMultipleQueues(b *testing.B) {
	ctx := setupTestContext(b)
	defer func() { _ = ctx.Close() }()

	// number of tasks to create for each queue
	const (
		highCount    = 1000 //20000
		defaultCount = 1000 //20000
		lowCount     = 1000 //20000
	)
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup

		ctx.FlushDB()

		client := NewClient(getClientConnOpt(b))
		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			Queues: NewQueuesConfig(map[string]int{
				"high":    6,
				"default": 3,
				"low":     1,
			}),
			LogLevel: testLogLevel,
		})
		// Create a bunch of tasks
		for i := 0; i < highCount; i++ {
			if _, err := client.Enqueue(makeTask(i), Queue("high")); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}
		for i := 0; i < defaultCount; i++ {
			if _, err := client.Enqueue(makeTask(i)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}
		for i := 0; i < lowCount; i++ {
			if _, err := client.Enqueue(makeTask(i), Queue("low")); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(highCount + defaultCount + lowCount)
		handler := func(ctx context.Context, t *Task) error {
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		_ = srv.Start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		srv.Stop()
		_ = client.Close()
		b.StartTimer() // end teardown
	}
}

// E2E benchmark to check client enqueue operation performs correctly,
// while server is busy processing tasks.
func BenchmarkClientWhileServerRunning(b *testing.B) {
	ctx := setupTestContext(b)
	defer func() { _ = ctx.Close() }()

	const count = 10000
	const enqueueCount = 100000
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		ctx.FlushDB()

		client := NewClient(getClientConnOpt(b))
		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
				return time.Second
			}),
			LogLevel: testLogLevel,
		})
		// Enqueue 10,000 tasks.
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(makeTask(i)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}
		// Schedule 10,000 tasks.
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(makeTask(i), ProcessIn(1*time.Second)); err != nil {
				b.Fatalf("could not enqueue a task: %v", err)
			}
		}

		handler := func(ctx context.Context, t *Task) error {
			return nil
		}
		_ = srv.Start(HandlerFunc(handler))

		b.StartTimer() // end setup

		b.Log("Starting enqueueing")
		enqueued := 0
		for enqueued < enqueueCount {
			t := NewTask(fmt.Sprintf("enqueued%d", enqueued), h.JSON(map[string]interface{}{"data": enqueued}))
			if _, err := client.Enqueue(t); err != nil {
				b.Logf("could not enqueue task %d: %v", enqueued, err)
				continue
			}
			enqueued++
		}
		b.Logf("Finished enqueueing %d tasks", enqueued)

		b.StopTimer() // begin teardown
		srv.Stop()
		_ = client.Close()
		b.StartTimer() // end teardown
	}
}

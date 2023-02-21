package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestEndToEnd is similar to BenchmarkEndToEnd
func TestEndToEnd(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	const count = 1000 //100000
	ctx.FlushDB()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 10,
		RetryDelayFunc: func(n int, err error, t *Task) time.Duration {
			return time.Second
		},
		LogLevel: testLogLevel,
	})
	defer srv.Shutdown()

	// Create a bunch of tasks
	for i := 0; i < count; i++ {
		if _, err := client.Enqueue(makeTask(i)); err != nil {
			t.Fatalf("could not enqueue a task: %v", err)
		}
	}
	for i := 0; i < count; i++ {
		if _, err := client.Enqueue(makeTask(i), ProcessIn(1*time.Second)); err != nil {
			t.Fatalf("could not enqueue a task: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(count * 2)
	handler := func(ctx context.Context, task *Task) error {
		var p map[string]int
		if err := json.Unmarshal(task.Payload(), &p); err != nil {
			t.Logf("internal error: %v", err)
		}
		n, ok := p["data"]
		if !ok {
			n = 1
			t.Logf("internal error: could not get data from payload")
		}
		retried, ok := GetRetryCount(ctx)
		if !ok {
			t.Logf("internal error: could not get retry count from context")
		}
		// Fail 1% of tasks for the first attempt.
		if retried == 0 && n%100 == 0 {
			return fmt.Errorf(":(")
		}
		wg.Done()
		return nil
	}

	_ = srv.Start(HandlerFunc(handler))
	wg.Wait()

	srv.Stop()
	_ = client.Close()

}

func TestEndToEndAsync(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	const count = 1000 //100000
	ctx.FlushDB()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 10,
		RetryDelayFunc: func(n int, err error, t *Task) time.Duration {
			return time.Second
		},
		LogLevel: testLogLevel,
	})
	defer srv.Shutdown()

	// Create a bunch of tasks
	for i := 0; i < count; i++ {
		if _, err := client.Enqueue(makeTask(i)); err != nil {
			t.Fatalf("could not enqueue a task: %v", err)
		}
	}
	for i := 0; i < count; i++ {
		if _, err := client.Enqueue(makeTask(i), ProcessIn(1*time.Second)); err != nil {
			t.Fatalf("could not enqueue a task: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(count * 2)
	handler := func(ctx context.Context, task *Task) error {
		var p map[string]int
		if err := json.Unmarshal(task.Payload(), &p); err != nil {
			t.Logf("internal error: %v", err)
		}
		n, ok := p["data"]
		if !ok {
			n = 1
			t.Logf("internal error: could not get data from payload")
		}
		retried, ok := GetRetryCount(ctx)
		if !ok {
			t.Logf("internal error: could not get retry count from context")
		}
		if rand.Intn(100)%2 == 0 {
			// Proceed asynchronously
			go func() {
				// Fail 1% of tasks for the first attempt.
				if retried == 0 && n%100 == 0 {
					task.AsyncProcessor().TaskFailed(fmt.Errorf(":("))
					return
				}
				task.AsyncProcessor().TaskCompleted()
				wg.Done()
			}()
			return AsynchronousTask
		} else {
			// Fail 1% of tasks for the first attempt.
			if retried == 0 && n%100 == 0 {
				return fmt.Errorf(":(")
			}
			wg.Done()
		}
		return nil
	}

	_ = srv.Start(HandlerFunc(handler))
	wg.Wait()

	srv.Stop()
	_ = client.Close()

}

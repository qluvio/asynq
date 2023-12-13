package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/require"
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
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
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

// TestEndToEndAsync is similar to TestEndToEnd but uses AsynchronousTask for
// some tasks while ensuring that all tasks eventually complete.
func TestEndToEndAsync(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	const count = 1000 //100000
	ctx.FlushDB()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 10,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
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

// TestEndToEndRestartWithActiveTasks simulates a 'hard' stop where tasks would be left in active state
func TestEndToEndRestartWithActiveTasks(t *testing.T) {
	doTestEndToEndRestartWithActiveTasks(t, false)
}

// TestEndToEndShutdownWithActiveTasks stops with tasks in active state: during
// shutdown they are moved back to the pending state.
func TestEndToEndShutdownWithActiveTasks(t *testing.T) {
	doTestEndToEndRestartWithActiveTasks(t, true)
}

func doTestEndToEndRestartWithActiveTasks(t *testing.T, regularShutdown bool) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	const count = 10
	ctx.FlushDB()

	taskTimeout := time.Second * 2
	{
		client := NewClient(getClientConnOpt(t))
		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
				return time.Second * 10
			}),
			LogLevel:             DebugLevel,
			ProcessorEmptyQSleep: time.Millisecond * 200,
			ShutdownTimeout:      time.Millisecond * 10,
		})

		// create some tasks
		mkTask := func(i int) *Task {
			ret := makeTask(i)
			ret.opts = append(ret.opts, Timeout(taskTimeout))
			return ret
		}
		for i := 0; i < count; i++ {
			if _, err := client.Enqueue(mkTask(i)); err != nil {
				t.Fatalf("could not enqueue a task: %v", err)
			}
		}

		wg := &sync.WaitGroup{}
		wg.Add(count)
		// a simple handler that returns AsynchronousTask such that the task is left
		// in active state. Use of AsynchronousTask is only a facility: instead we
		// could have stopped the server while tasks were running.
		handler := func(ctx context.Context, task *Task) error {
			wg.Done()
			return AsynchronousTask
		}
		_ = srv.Start(HandlerFunc(handler))
		wg.Wait()
		tis, err := srv.broker.Inspector().ListActive(base.DefaultQueueName, base.Pagination{Size: 100})
		require.NoError(t, err)
		require.Equal(t, count, len(tis))

		srv.Stop()
		if regularShutdown {
			srv.Shutdown()
		} else {
			srv.shutdown(true)
			_ = srv.broker.Close()
			inspector, err := NewInspectorClient(NewClient(getClientConnOpt(t)))
			require.NoError(t, err)
			tis, err := inspector.ListActiveTasks(base.DefaultQueueName, base.Pagination{Size: 100})
			_ = inspector.Close()
			require.NoError(t, err)
			require.Equal(t, count, len(tis))
		}
		_ = client.Close()
		time.Sleep(time.Second)
	}

	fmt.Println("\nrestart server")
	time.Sleep(taskTimeout)

	//
	// restart the server and make sure all tasks are handled
	//
	{
		client := NewClient(getClientConnOpt(t))
		srv := newServer(client.rdb, Config{
			Concurrency: 10,
			RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
				return time.Second
			}),
			LogLevel:             DebugLevel,
			ProcessorEmptyQSleep: time.Millisecond * 200,
			ShutdownTimeout:      time.Millisecond * 10,
			HealthCheckInterval:  time.Millisecond * 2, // health
			RecovererExpiration:  time.Millisecond,     // recover: active + deadline-exceeded -> retry
			RecovererInterval:    time.Second,          // recover
			ForwarderInterval:    time.Second,          // forward: retry -> pending
		})
		wg := &sync.WaitGroup{}
		wg.Add(count)

		counter := int64(0)
		handler := func(ctx context.Context, task *Task) error {
			atomic.AddInt64(&counter, 1)
			wg.Done()
			return nil
		}
		t0 := time.Now()
		_ = srv.Start(HandlerFunc(handler))
		wg.Wait()
		require.Equal(t, int64(count), counter)
		d := time.Now().Sub(t0)
		fmt.Println("all tasks done after", d.String())

		// wait a bit before listing again (ci build slow?)
		time.Sleep(taskTimeout)
		tis, err := srv.broker.Inspector().ListActive(base.DefaultQueueName, base.Pagination{Size: 100})

		srv.Stop()
		srv.Shutdown()
		require.NoError(t, err)
		require.Equal(t, 0, len(tis))
		_ = client.Close()

		// usually done in ~2s if regularShutdown is false and ~1s otherwise
		require.Less(t, d, time.Second*20)
	}
}

// TestCancelProcessing tests canceling a single task
func TestCancelProcessing(t *testing.T) {
	t.Skip("manual only")
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	ctx.FlushDB()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 10,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		HeartBeaterInterval: time.Millisecond * 10,
		LogLevel:            testLogLevel,
	})
	defer srv.Shutdown()
	inspect := newInspector(client.rdb)

	// Create a single task
	task, err := client.Enqueue(makeTask(1000))
	require.NoError(t, err)

	var doneErr error
	doneErrMu := sync.Mutex{}

	setDoneErr := func(err error) {
		doneErrMu.Lock()
		defer doneErrMu.Unlock()
		doneErr = err
	}
	getDoneErr := func() error {
		doneErrMu.Lock()
		defer doneErrMu.Unlock()
		return doneErr
	}

	closeCh := make(chan struct{})
	doneCh := make(chan struct{})
	handler := func(ctx context.Context, task *Task) error {
		close(closeCh)
		var err error
	out:
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break out
			}
		}
		close(doneCh)
		setDoneErr(err)
		return err
	}

	_ = srv.Start(HandlerFunc(handler))
	select {
	case <-closeCh:
	}

	_, ok := srv.processor.cancelations.Get(task.ID)
	require.True(t, ok)

	err = inspect.CancelProcessing(task.ID)
	require.NoError(t, err)

	select {
	case <-doneCh:
	}

	require.Equal(t, context.Canceled, getDoneErr())
	time.Sleep(time.Millisecond * 200) // wait a couple of heartbeat interval
	_, ok = srv.processor.cancelations.Get(task.ID)
	require.False(t, ok)

	ti, err := inspect.GetTaskInfo(base.DefaultQueueName, task.ID)
	require.NoError(t, err)
	require.Equal(t, TaskStateRetry, ti.State)

	srv.Stop()
	_ = client.Close()
}

// TestAfterTask
func TestAfterTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	ctx.FlushDB()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 10,
		LogLevel:    testLogLevel,
	})
	defer srv.Shutdown()

	// Create a bunch of tasks
	for i := 0; i < 10; i++ {
		if _, err := client.Enqueue(makeTask(i)); err != nil {
			t.Fatalf("could not enqueue a task: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(10)

	after := []int(nil)
	mu := sync.Mutex{}

	received := func(i int) {
		mu.Lock()
		defer mu.Unlock()
		after = append(after, i)
	}

	handler := func(ctx context.Context, task *Task) error {
		defer wg.Done()
		m := map[string]int{}
		err := json.Unmarshal(task.payload, &m)
		if err != nil {
			t.Fatalf("unmarshal task: %v", err)
		}
		indx, ok := m["data"]
		if !ok {
			t.Fatalf("invalid payload: %v", string(task.payload))
		}

		if indx%2 == 0 {
			task.CallAfter(func(string, error, bool) {
				received(indx)
			})
		}
		return nil
	}

	_ = srv.Start(HandlerFunc(handler))
	wg.Wait()
	srv.Stop()

	// do the comparison after stopping the server to finish active tasks
	mu.Lock()
	sort.Ints(after)
	mu.Unlock()
	require.Equal(t, []int{0, 2, 4, 6, 8}, after)

	_ = client.Close()
}

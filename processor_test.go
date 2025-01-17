// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/stretchr/testify/require"
)

var taskCmpOpts = []cmp.Option{
	sortTaskOpt,                 // sort the tasks
	cmp.AllowUnexported(Task{}), // allow typename, payload fields to be compared
	cmpopts.IgnoreFields(Task{}, "opts", "w", "p", "callAfter"), // ignore opts, w, p fields
}

// fakeHeartbeater receives from starting and finished channels and do nothing.
func fakeHeartbeater(starting <-chan *workerInfo, finished <-chan *base.TaskMessage, done <-chan struct{}) {
	for {
		select {
		case <-starting:
		case <-finished:
		case <-done:
			return
		}
	}
}

// fakeSyncer receives from sync channel and do nothing.
func fakeSyncer(syncCh <-chan *syncRequest, done <-chan struct{}) {
	for {
		select {
		case <-syncCh:
		case <-done:
			return
		}
	}
}

// Returns a processor instance configured for testing purpose.
func newProcessorForTest(t *testing.T, r base.Broker, h Handler, q ...Queues) *processor {
	starting := make(chan *workerInfo)
	finished := make(chan *base.TaskMessage)
	syncCh := make(chan *syncRequest)
	queues := Queues(defaultQueuesConfig)
	if len(q) > 0 && q[0] != nil {
		queues = q[0]
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go fakeHeartbeater(starting, finished, done)
	go fakeSyncer(syncCh, done)
	p := newProcessor(processorParams{
		logger:          testLogger,
		broker:          r,
		retryDelayFunc:  DefaultRetryDelay,
		isFailureFunc:   defaultIsFailureFunc,
		syncCh:          syncCh,
		cancelations:    base.NewCancelations(),
		concurrency:     10,
		queues:          queues,
		errHandler:      nil,
		shutdownTimeout: defaultShutdownTimeout,
		starting:        starting,
		finished:        finished,
	})
	p.handler = h
	return p
}

func TestProcessorSuccessWithSingleQueue(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessage("task4", nil)

	t1 := NewTask(m1.Type, m1.Payload)
	t2 := NewTask(m2.Type, m2.Payload)
	t3 := NewTask(m3.Type, m3.Payload)
	t4 := NewTask(m4.Type, m4.Payload)

	tests := []struct {
		pending       []*base.TaskMessage // initial default queue state
		incoming      []*base.TaskMessage // tasks to be enqueued during run
		wantProcessed []*Task             // tasks to be processed at the end
	}{
		{
			pending:       []*base.TaskMessage{m1},
			incoming:      []*base.TaskMessage{m2, m3, m4},
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
		{
			pending:       []*base.TaskMessage{},
			incoming:      []*base.TaskMessage{m1},
			wantProcessed: []*Task{t1},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ctx.FlushDB()                                           // clean up db before each test case.
			ctx.SeedPendingQueue(tc.pending, base.DefaultQueueName) // initialize default queue.

			// instantiate a new processor
			var mu sync.Mutex
			var processed []*Task
			handler := func(ctx context.Context, task *Task) error {
				mu.Lock()
				defer mu.Unlock()
				processed = append(processed, task)
				return nil
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler))

			p.start(&sync.WaitGroup{})
			for _, msg := range tc.incoming {
				err := client.rdb.Enqueue(context.Background(), msg)
				if err != nil {
					p.shutdown()
					t.Fatal(err)
				}
			}
			time.Sleep(2 * time.Second) // wait for two second to allow all pending tasks to be processed.
			active := ctx.GetActiveMessages(base.DefaultQueueName)
			if len(active) != 0 {
				t.Errorf("%q has %d tasks, want 0", base.ActiveKey(base.DefaultQueueName), len(active))
			}
			p.shutdown()

			mu.Lock()
			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}
			mu.Unlock()
		})
	}
}

func TestProcessorSuccessWithMultipleQueues(t *testing.T) {
	var (
		ctx    = setupTestContext(t)
		client = NewClient(getClientConnOpt(t))

		m1 = h.NewTaskMessage("task1", nil)
		m2 = h.NewTaskMessage("task2", nil)
		m3 = h.NewTaskMessageWithQueue("task3", nil, "high")
		m4 = h.NewTaskMessageWithQueue("task4", nil, "low")

		t1 = NewTask(m1.Type, m1.Payload)
		t2 = NewTask(m2.Type, m2.Payload)
		t3 = NewTask(m3.Type, m3.Payload)
		t4 = NewTask(m4.Type, m4.Payload)
	)
	defer func() { _ = ctx.Close() }()
	defer func() { _ = client.Close() }()

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		queues        []string // list of queues to consume the tasks from
		wantProcessed []*Task  // tasks to be processed at the end
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"high":    {m3},
				"low":     {m4},
			},
			queues:        []string{"default", "high", "low"},
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			// Set up test case.
			ctx.FlushDB()
			ctx.SeedAllPendingQueues(tc.pending)

			// Instantiate a new processor.
			var mu sync.Mutex
			var processed []*Task
			handler := func(ctx context.Context, task *Task) error {
				mu.Lock()
				defer mu.Unlock()
				processed = append(processed, task)
				return nil
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler), (&QueuesConfig{
				Priority: Lenient,
				Queues: map[string]interface{}{
					"default": 2,
					"high":    3,
					"low":     1,
				},
			}))
			p.start(&sync.WaitGroup{})
			// Wait for two second to allow all pending tasks to be processed.
			time.Sleep(2 * time.Second)
			// Make sure no messages are stuck in active list.
			for _, qname := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != 0 {
					t.Errorf("%q has %d tasks, want 0", base.ActiveKey(qname), len(active))
				}
			}
			p.shutdown()

			mu.Lock()
			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}
			mu.Unlock()
		})
	}
}

func TestProcessorSuccessWithAsyncTasks(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var (
		ctx    = setupTestContext(t)
		client = NewClient(getClientConnOpt(t))

		m1 = h.NewTaskMessage("task1", nil)
		m2 = h.NewTaskMessage("task2", nil)
		m3 = h.NewTaskMessageWithQueue("task3", nil, "high")
		m4 = h.NewTaskMessageWithQueue("task4", nil, "low")
		m5 = h.NewTaskMessage("task5", nil)
		m6 = h.NewTaskMessage("task6", nil)
		m7 = h.NewTaskMessageWithQueue("task7", nil, "high")
		m8 = h.NewTaskMessageWithQueue("task8", nil, "low")

		t1 = NewTask(m1.Type, m1.Payload)
		t2 = NewTask(m2.Type, m2.Payload)
		t3 = NewTask(m3.Type, m3.Payload)
		t4 = NewTask(m4.Type, m4.Payload)
		t5 = NewTask(m5.Type, m5.Payload)
		t6 = NewTask(m6.Type, m6.Payload)
		t7 = NewTask(m7.Type, m7.Payload)
		t8 = NewTask(m8.Type, m8.Payload)
	)
	defer func() { _ = ctx.Close() }()
	defer func() { _ = client.Close() }()

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		queues        []string // list of queues to consume the tasks from
		wantProcessed []*Task  // tasks to be processed at the end
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m5, m6},
				"high":    {m3, m7},
				"low":     {m4, m8},
			},
			queues:        []string{"default", "high", "low"},
			wantProcessed: []*Task{t1, t2, t3, t4, t5, t6, t7, t8},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			// Set up test case.
			ctx.FlushDB()
			ctx.SeedAllPendingQueues(tc.pending)

			// Instantiate a new processor.
			var mu sync.Mutex
			var processed []*Task
			handler := func(ctx context.Context, task *Task) error {
				go func() {
					mu.Lock()
					defer mu.Unlock()
					processed = append(processed, task)
					if rand.Intn(100)%2 == 0 {
						task.AsyncProcessor().TaskCompleted()
					} else {
						task.AsyncProcessor().TaskFailed(errors.New("failed"))
					}
				}()
				return AsynchronousTask
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler), (&QueuesConfig{
				Priority: Lenient,
				Queues: map[string]interface{}{
					"default": 2,
					"high":    3,
					"low":     1,
				},
			}))
			p.start(&sync.WaitGroup{})
			// Wait for two second to allow all pending tasks to be processed.
			time.Sleep(2 * time.Second)
			// Make sure no messages are stuck in active list.
			for _, qname := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != 0 {
					t.Errorf("%q has %d tasks, want 0", base.ActiveKey(qname), len(active))
				}
			}
			p.shutdown()

			mu.Lock()
			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}
			mu.Unlock()
		})
	}
}

func TestProcessorContextDoneWithAsyncTasks(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var (
		ctx    = setupTestContext(t)
		client = NewClient(getClientConnOpt(t))

		lfa = time.Now().Truncate(time.Second).Unix()

		m1 = h.NewTaskMessage("task1", nil)
		m2 = h.NewTaskMessage("task2", nil)
		m3 = h.NewTaskMessageWithQueue("task3", nil, "high")
		m4 = h.NewTaskMessageWithQueue("task4", nil, "low")
		m5 = h.NewTaskMessage("task5", nil)
		m6 = h.NewTaskMessage("task6", nil)
		m7 = h.NewTaskMessageWithQueue("task7", nil, "high")
		m8 = h.NewTaskMessageWithQueue("task8", nil, "low")
	)
	defer func() { _ = ctx.Close() }()
	defer func() { _ = client.Close() }()

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		queues       []string            // list of queues to consume the tasks from
		wantArchived []*base.TaskMessage // tasks to be processed at the end
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m5, m6},
				"high":    {m3, m7},
				"low":     {m4, m8},
			},
			queues:       []string{"default", "high", "low"},
			wantArchived: []*base.TaskMessage{m1, m2, m3, m4, m5, m6, m7, m8},
		},
	}

	for i, tc := range tests {
		for n, m := range tc.wantArchived {
			am := &base.TaskMessage{}
			*am = *m
			am.ErrorMsg = TaskCanceled.Error()
			am.LastFailedAt = lfa
			tc.wantArchived[n] = am
		}

		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			// Set up test case.
			ctx.FlushDB()
			ctx.SeedAllPendingQueues(tc.pending)

			// Instantiate a new processor.
			var wg sync.WaitGroup
			for _, msgs := range tc.pending {
				for _ = range msgs {
					wg.Add(1)
				}
			}
			handler := func(ctx context.Context, task *Task) error {
				go func() {
					select {
					case <-ctx.Done():
					}
					time.Sleep(time.Second)
					task.AsyncProcessor().TaskFailed(errors.New("failed"))
					wg.Done()
				}()
				return AsynchronousTask
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler), (&QueuesConfig{
				Priority: Lenient,
				Queues: map[string]interface{}{
					"default": 2,
					"high":    3,
					"low":     1,
				},
			}))
			p.start(&sync.WaitGroup{})

			// Wait for a second to allow all pending tasks to be processed.
			time.Sleep(time.Second)

			// Cancel all (active) tasks.
			for _, msgs := range tc.pending {
				for _, msg := range msgs {
					cancel, ok := p.cancelations.Get(msg.ID)
					if !ok {
						t.Errorf("%s task has no cancelation", msg.Type)
					}
					cancel()
				}
			}

			// Wait to allow all (active) tasks to be canceled.
			wg.Wait()

			// Wait for a second to allow all (active) tasks to be processed.
			time.Sleep(time.Second)

			// Make sure no messages are stuck in active list.
			for _, qname := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != 0 {
					t.Errorf("%q has %d tasks, want 0", base.ActiveKey(qname), len(active))
				}
			}

			// Make sure all messages are in archived list.
			var archived []*base.TaskMessage
			for _, qname := range tc.queues {
				msgs := ctx.GetArchivedMessages(qname)
				archived = append(archived, msgs...)
			}
			for _, msg := range archived {
				if msg.LastFailedAt != 0 {
					// Replace LastFailedAt for the sake of diff below
					msg.LastFailedAt = lfa
				}
			}
			sort.Slice(archived, func(i int, j int) bool {
				return archived[i].Type < archived[j].Type
			})
			if diff := cmp.Diff(tc.wantArchived, archived, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in archived tasks; (-want, +got)\n%s", diff)
			}

			p.shutdown()
		})
	}
}

// https://github.com/hibiken/asynq/issues/166
func TestProcessTasksWithLargeNumberInPayload(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	m1 := h.NewTaskMessage("large_number", h.JSON(map[string]interface{}{"data": 111111111111111111}))
	t1 := NewTask(m1.Type, m1.Payload)

	tests := []struct {
		pending       []*base.TaskMessage // initial default queue state
		wantProcessed []*Task             // tasks to be processed at the end
	}{
		{
			pending:       []*base.TaskMessage{m1},
			wantProcessed: []*Task{t1},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ctx.FlushDB()                                           // clean up db before each test case.
			ctx.SeedPendingQueue(tc.pending, base.DefaultQueueName) // initialize default queue.

			var mu sync.Mutex
			var processed []*Task
			handler := func(ctx context.Context, task *Task) error {
				mu.Lock()
				defer mu.Unlock()
				var payload map[string]int
				if err := json.Unmarshal(task.Payload(), &payload); err != nil {
					t.Errorf("coult not decode payload: %v", err)
				}
				if data, ok := payload["data"]; ok {
					t.Logf("data == %d", data)
				} else {
					t.Errorf("could not get data from payload")
				}
				processed = append(processed, task)
				return nil
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler))

			p.start(&sync.WaitGroup{})
			time.Sleep(2 * time.Second) // wait for two second to allow all pending tasks to be processed.
			active := ctx.GetActiveMessages(base.DefaultQueueName)
			if len(active) != 0 {
				t.Errorf("%q has %d tasks, want 0", base.ActiveKey(base.DefaultQueueName), len(active))
			}
			p.shutdown()

			mu.Lock()
			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}
			mu.Unlock()
		})
	}
}

func TestProcessorRetry(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	m1 := h.NewTaskMessage("send_email", nil)
	m1.Retried = m1.Retry // m1 has reached its max retry count
	m2 := h.NewTaskMessage("gen_thumbnail", nil)
	m3 := h.NewTaskMessage("reindex", nil)
	m4 := h.NewTaskMessage("sync", nil)

	errMsg := "something went wrong"
	wrappedSkipRetry := fmt.Errorf("%s:%w", errMsg, SkipRetry)

	tests := []struct {
		desc         string              // test description
		pending      []*base.TaskMessage // initial default queue state
		delay        time.Duration       // retry delay duration
		handler      Handler             // task handler
		wait         time.Duration       // wait duration between starting and stopping processor for this test case
		wantErrMsg   string              // error message the task should record
		wantRetry    []*base.TaskMessage // tasks in retry queue at the end
		wantArchived []*base.TaskMessage // tasks in archived queue at the end
		wantErrCount int                 // number of times error handler should be called
	}{
		{
			desc:    "Should automatically retry errored tasks",
			pending: []*base.TaskMessage{m1, m2, m3, m4},
			delay:   time.Minute,
			handler: HandlerFunc(func(ctx context.Context, task *Task) error {
				return fmt.Errorf(errMsg)
			}),
			wait:         2 * time.Second,
			wantErrMsg:   errMsg,
			wantRetry:    []*base.TaskMessage{m2, m3, m4},
			wantArchived: []*base.TaskMessage{m1},
			wantErrCount: 4,
		},
		{
			desc:    "Should skip retry errored tasks",
			pending: []*base.TaskMessage{m1, m2},
			delay:   time.Minute,
			handler: HandlerFunc(func(ctx context.Context, task *Task) error {
				return SkipRetry // return SkipRetry without wrapping
			}),
			wait:         2 * time.Second,
			wantErrMsg:   SkipRetry.Error(),
			wantRetry:    []*base.TaskMessage{},
			wantArchived: []*base.TaskMessage{m1, m2},
			wantErrCount: 2, // ErrorHandler should still be called with SkipRetry error
		},
		{
			desc:    "Should skip retry errored tasks (with error wrapping)",
			pending: []*base.TaskMessage{m1, m2},
			delay:   time.Minute,
			handler: HandlerFunc(func(ctx context.Context, task *Task) error {
				return wrappedSkipRetry
			}),
			wait:         2 * time.Second,
			wantErrMsg:   wrappedSkipRetry.Error(),
			wantRetry:    []*base.TaskMessage{},
			wantArchived: []*base.TaskMessage{m1, m2},
			wantErrCount: 2, // ErrorHandler should still be called with SkipRetry error
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ctx.FlushDB()                                           // clean up db before each test case.
			ctx.SeedPendingQueue(tc.pending, base.DefaultQueueName) // initialize default queue.

			// instantiate a new processor
			delayFunc := RetryDelayFunc(func(n int, e error, t *Task) time.Duration {
				return tc.delay
			})
			var (
				mu sync.Mutex // guards n
				n  int        // number of times error handler is called
			)
			errHandler := func(ctx context.Context, t *Task, err error) {
				mu.Lock()
				defer mu.Unlock()
				n++
			}
			p := newProcessorForTest(t, client.rdb, tc.handler)
			p.errHandler = ErrorHandlerFunc(errHandler)
			p.retryDelay = delayFunc

			p.start(&sync.WaitGroup{})
			runTime := time.Now() // time when processor is running
			time.Sleep(tc.wait)   // FIXME: This makes test flaky.
			p.shutdown()

			cmpOpt := h.EquateInt64Approx(int64(tc.wait.Seconds())) // allow up to a wait-second difference in zset score
			gotRetry := ctx.GetRetryEntries(base.DefaultQueueName)
			var wantRetry []base.Z // Note: construct wantRetry here since `LastFailedAt` and ZSCORE is relative to each test run.
			for _, msg := range tc.wantRetry {
				wantRetry = append(wantRetry,
					base.Z{
						Message: h.TaskMessageAfterRetry(*msg, tc.wantErrMsg, runTime),
						Score:   runTime.Add(tc.delay).Unix(),
					})
			}
			if diff := cmp.Diff(wantRetry, gotRetry, h.SortZSetEntryOpt, cmpOpt); diff != "" {
				t.Errorf("%s: mismatch found in %q after running processor; (-want, +got)\n%s", tc.desc, base.RetryKey(base.DefaultQueueName), diff)
			}

			gotArchived := ctx.GetArchivedEntries(base.DefaultQueueName)
			var wantArchived []base.Z // Note: construct wantArchived here since `LastFailedAt` and ZSCORE is relative to each test run.
			for _, msg := range tc.wantArchived {
				wantArchived = append(wantArchived,
					base.Z{
						Message: h.TaskMessageWithError(*msg, tc.wantErrMsg, runTime),
						Score:   runTime.Unix(),
					})
			}
			if diff := cmp.Diff(wantArchived, gotArchived, h.SortZSetEntryOpt, cmpOpt); diff != "" {
				t.Errorf("%s: mismatch found in %q after running processor; (-want, +got)\n%s", tc.desc, base.ArchivedKey(base.DefaultQueueName), diff)
			}

			active := ctx.GetActiveMessages(base.DefaultQueueName)
			if len(active) != 0 {
				t.Errorf("%s: %q has %d tasks, want 0", base.ActiveKey(base.DefaultQueueName), tc.desc, len(active))
			}

			if n != tc.wantErrCount {
				t.Errorf("error handler was called %d times, want %d", n, tc.wantErrCount)
			}
		})
	}
}

func TestProcessorNilRetry(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	task := NewTask("", nil)

	var retryDelayFunc func(n int, e error, t *Task) time.Duration
	s := newServer(client.rdb, Config{
		RetryDelayFunc: RetryDelayFunc(retryDelayFunc),
	})
	d := s.processor.retryDelay.RetryDelay(1, nil, task)

	require.True(t, d > 0)

	close(s.processor.abortNow)
}

func TestProcessorMarkAsComplete(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	msg1 := h.NewTaskMessage("one", nil)
	msg2 := h.NewTaskMessage("two", nil)
	msg3 := h.NewTaskMessageWithQueue("three", nil, "custom")
	msg1.Retention = 3600
	msg3.Retention = 7200

	handler := func(ctx context.Context, task *Task) error { return nil }

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		completed     map[string][]base.Z
		queueCfg      *QueuesConfig
		wantPending   map[string][]*base.TaskMessage
		wantCompleted func(completedAt time.Time) map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {msg1, msg2},
				"custom":  {msg3},
			},
			completed: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			queueCfg: &QueuesConfig{
				Queues: map[string]interface{}{
					"default": 1,
					"custom":  1,
				},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			wantCompleted: func(completedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {{Message: h.TaskMessageWithCompletedAt(*msg1, completedAt), Score: completedAt.Unix() + msg1.Retention}},
					"custom":  {{Message: h.TaskMessageWithCompletedAt(*msg3, completedAt), Score: completedAt.Unix() + msg3.Retention}},
				}
			},
		},
	}

	now := time.Now()
	client.rdb.SetClock(timeutil.NewSimulatedClock(now))

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllCompletedQueues(tc.completed)

		p := newProcessorForTest(t, client.rdb, HandlerFunc(handler), tc.queueCfg)

		p.start(&sync.WaitGroup{})
		runTime := now // time when processor is running
		time.Sleep(2 * time.Second)
		p.shutdown()

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("diff found in %q pending set; want=%v, got=%v\n%s", qname, want, gotPending, diff)
			}
		}

		for qname, want := range tc.wantCompleted(runTime) {
			gotCompleted := ctx.GetCompletedEntries(qname)
			if diff := cmp.Diff(want, gotCompleted, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("diff found in %q completed set; want=%v, got=%v\n%s", qname, want, gotCompleted, diff)
			}
		}
	}
}

func TestProcessorQueues(t *testing.T) {
	sortOpt := cmp.Transformer("SortStrings", func(in []string) []string {
		out := append([]string(nil), in...) // Copy input to avoid mutating it
		sort.Strings(out)
		return out
	})

	tests := []struct {
		queueCfg *QueuesConfig
		want     []string
	}{
		{
			queueCfg: &QueuesConfig{
				Queues: map[string]interface{}{
					"high":    6,
					"default": 3,
					"low":     1,
				},
			},
			want: []string{"high", "default", "low"},
		},
		{
			queueCfg: &QueuesConfig{
				Queues: map[string]interface{}{
					"default": 1,
				},
			},
			want: []string{"default"},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			p := newProcessorForTest(t, nil, nil, tc.queueCfg)
			got := p.queues.Names()
			if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
				t.Errorf("with queue config: %v\n(*processor).queues() = %v, want %v\n(-want,+got):\n%s",
					tc.queueCfg, got, tc.want, diff)
			}
			close(p.abortNow)
		})
	}
}

func TestProcessorWithStrictPriority(t *testing.T) {
	var (
		ctx    = setupTestContext(t)
		client = NewClient(getClientConnOpt(t))

		m1 = h.NewTaskMessageWithQueue("task1", nil, "critical")
		m2 = h.NewTaskMessageWithQueue("task2", nil, "critical")
		m3 = h.NewTaskMessageWithQueue("task3", nil, "critical")
		m4 = h.NewTaskMessageWithQueue("task4", nil, base.DefaultQueueName)
		m5 = h.NewTaskMessageWithQueue("task5", nil, base.DefaultQueueName)
		m6 = h.NewTaskMessageWithQueue("task6", nil, "low")
		m7 = h.NewTaskMessageWithQueue("task7", nil, "low")

		t1 = NewTask(m1.Type, m1.Payload)
		t2 = NewTask(m2.Type, m2.Payload)
		t3 = NewTask(m3.Type, m3.Payload)
		t4 = NewTask(m4.Type, m4.Payload)
		t5 = NewTask(m5.Type, m5.Payload)
		t6 = NewTask(m6.Type, m6.Payload)
		t7 = NewTask(m7.Type, m7.Payload)
	)
	defer func() { _ = ctx.Close() }()
	defer func() { _ = client.Close() }()

	tests := []struct {
		pending       map[string][]*base.TaskMessage // initial queues state
		queues        []string                       // list of queues to consume tasks from
		wait          time.Duration                  // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task                        // tasks to be processed at the end
	}{
		{
			pending: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m4, m5},
				"critical":            {m1, m2, m3},
				"low":                 {m6, m7},
			},
			queues:        []string{base.DefaultQueueName, "critical", "low"},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4, t5, t6, t7},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ctx.FlushDB() // clean up db before each test case.
			for qname, msgs := range tc.pending {
				ctx.SeedPendingQueue(msgs, qname)
			}

			// instantiate a new processor
			var mu sync.Mutex
			var processed []*Task
			handler := func(ctx context.Context, task *Task) error {
				mu.Lock()
				defer mu.Unlock()
				processed = append(processed, task)
				return nil
			}
			queueCfg := &QueuesConfig{
				Priority: Strict,
				Queues: map[string]interface{}{
					base.DefaultQueueName: 2,
					"critical":            3,
					"low":                 1,
				},
			}
			starting := make(chan *workerInfo)
			finished := make(chan *base.TaskMessage)
			syncCh := make(chan *syncRequest)
			done := make(chan struct{})
			defer func() { close(done) }()
			go fakeHeartbeater(starting, finished, done)
			go fakeSyncer(syncCh, done)
			p := newProcessor(processorParams{
				logger:          testLogger,
				broker:          client.rdb,
				retryDelayFunc:  DefaultRetryDelay,
				isFailureFunc:   defaultIsFailureFunc,
				syncCh:          syncCh,
				cancelations:    base.NewCancelations(),
				concurrency:     1, // Set concurrency to 1 to make sure tasks are processed one at a time.
				queues:          queueCfg,
				errHandler:      nil,
				shutdownTimeout: defaultShutdownTimeout,
				starting:        starting,
				finished:        finished,
			})
			p.handler = HandlerFunc(handler)

			p.start(&sync.WaitGroup{})
			time.Sleep(tc.wait)
			// Make sure no tasks are stuck in active list.
			for _, qname := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != 0 {
					t.Errorf("%q has %d tasks, want 0", base.ActiveKey(qname), len(active))
				}
			}
			p.shutdown()

			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestProcessorWithQueueConcurrency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var (
		ctx    = setupTestContext(t)
		client = NewClient(getClientConnOpt(t))

		m1  = h.NewTaskMessage("task1", nil)
		m2  = h.NewTaskMessage("task2", nil)
		m3  = h.NewTaskMessageWithQueue("task3", nil, "high")
		m4  = h.NewTaskMessageWithQueue("task4", nil, "low")
		m5  = h.NewTaskMessage("task5", nil)
		m6  = h.NewTaskMessage("task6", nil)
		m7  = h.NewTaskMessageWithQueue("task7", nil, "high")
		m8  = h.NewTaskMessageWithQueue("task8", nil, "low")
		m9  = h.NewTaskMessage("task9", nil)
		m10 = h.NewTaskMessage("task10", nil)
		m11 = h.NewTaskMessageWithQueue("task11", nil, "high")
		m12 = h.NewTaskMessageWithQueue("task12", nil, "low")

		t1  = NewTask(m1.Type, m1.Payload)
		t2  = NewTask(m2.Type, m2.Payload)
		t3  = NewTask(m3.Type, m3.Payload)
		t4  = NewTask(m4.Type, m4.Payload)
		t5  = NewTask(m5.Type, m5.Payload)
		t6  = NewTask(m6.Type, m6.Payload)
		t7  = NewTask(m7.Type, m7.Payload)
		t8  = NewTask(m8.Type, m8.Payload)
		t9  = NewTask(m9.Type, m9.Payload)
		t10 = NewTask(m10.Type, m10.Payload)
		t11 = NewTask(m11.Type, m11.Payload)
		t12 = NewTask(m12.Type, m12.Payload)
	)
	defer func() { _ = ctx.Close() }()
	defer func() { _ = client.Close() }()

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		queues        map[string]int // list of queues to consume the tasks from with concurrency values
		wantProcessed []*Task        // tasks to be processed at the end
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m5, m6, m9, m10},
				"high":    {m3, m7, m11},
				"low":     {m4, m8, m12},
			},
			queues:        map[string]int{"default": 4, "high": 3, "low": 3},
			wantProcessed: []*Task{t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12},
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			// Set up test case.
			ctx.FlushDB()
			ctx.SeedAllPendingQueues(tc.pending)

			// Instantiate a new processor.
			var mu sync.Mutex
			var processed []*Task
			done := make(chan bool, 1)
			handler := func(ctx context.Context, task *Task) error {
				select {
				case <-done:
					done <- true
				}
				mu.Lock()
				defer mu.Unlock()
				processed = append(processed, task)
				return nil
			}
			p := newProcessorForTest(t, client.rdb, HandlerFunc(handler), (&QueuesConfig{
				Priority: Lenient,
				Queues: map[string]interface{}{
					"default": QueueConfig{
						Priority:    2,
						Concurrency: tc.queues["default"],
					},
					"high": QueueConfig{
						Priority:    3,
						Concurrency: tc.queues["high"],
					},
					"low": QueueConfig{
						Priority:    1,
						Concurrency: tc.queues["low"],
					},
				},
			}))
			p.start(&sync.WaitGroup{})

			// Wait for a second to allow pending tasks to be processed.
			time.Sleep(time.Second)

			// Check that concurrency limits are not exceeded.
			activeTotal := 0
			for qname, conc := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != conc {
					t.Errorf("%q has %d tasks, want %d", base.ActiveKey(qname), len(active), conc)
				}
				activeTotal += len(active)
			}
			if activeTotal != 10 {
				t.Errorf("processor has %d tasks, want 10", activeTotal)
			}

			// Allow tasks to proceed.
			done <- true

			// Wait for a second to allow all tasks to be processed.
			time.Sleep(time.Second)

			// Make sure no messages are stuck in active list.
			for qname := range tc.queues {
				active := ctx.GetActiveMessages(qname)
				if len(active) != 0 {
					t.Errorf("%q has %d tasks, want 0", base.ActiveKey(qname), len(active))
				}
			}

			// Make sure all messages are in processed list.
			if diff := cmp.Diff(tc.wantProcessed, processed, taskCmpOpts...); diff != "" {
				t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
			}

			p.shutdown()
		})
	}
}

func TestProcessorPerform(t *testing.T) {
	tests := []struct {
		desc    string
		handler HandlerFunc
		task    *Task
		wantErr bool
	}{
		{
			desc: "handler returns nil",
			handler: func(ctx context.Context, t *Task) error {
				return nil
			},
			task:    NewTask("gen_thumbnail", h.JSON(map[string]interface{}{"src": "some/img/path"})),
			wantErr: false,
		},
		{
			desc: "handler returns error",
			handler: func(ctx context.Context, t *Task) error {
				return fmt.Errorf("something went wrong")
			},
			task:    NewTask("gen_thumbnail", h.JSON(map[string]interface{}{"src": "some/img/path"})),
			wantErr: true,
		},
		{
			desc: "handler panics",
			handler: func(ctx context.Context, t *Task) error {
				panic("something went terribly wrong")
			},
			task:    NewTask("gen_thumbnail", h.JSON(map[string]interface{}{"src": "some/img/path"})),
			wantErr: true,
		},
	}
	// Note: We don't need to fully initialized the processor since we are only testing
	// perform method.
	p := newProcessorForTest(t, nil, nil)

	for _, tc := range tests {
		p.handler = tc.handler
		got := p.perform(context.Background(), tc.task)
		if !tc.wantErr && got != nil {
			t.Errorf("%s: perform() = %v, want nil", tc.desc, got)
			continue
		}
		if tc.wantErr && got == nil {
			t.Errorf("%s: perform() = nil, want non-nil error", tc.desc)
			continue
		}
	}

	close(p.abortNow)
}

func TestGCD(t *testing.T) {
	tests := []struct {
		input []int
		want  int
	}{
		{[]int{6, 2, 12}, 2},
		{[]int{3, 3, 3}, 3},
		{[]int{6, 3, 1}, 1},
		{[]int{1}, 1},
		{[]int{1, 0, 2}, 1},
		{[]int{8, 0, 4}, 4},
		{[]int{9, 12, 18, 30}, 3},
	}

	for _, tc := range tests {
		got := gcd(tc.input...)
		if got != tc.want {
			t.Errorf("gcd(%v) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestNormalizeQueues(t *testing.T) {
	tests := []struct {
		input map[string]QueueConfig
		want  map[string]QueueConfig
	}{
		{
			input: map[string]QueueConfig{
				"high": {
					Priority:    100,
					Concurrency: 10,
				},
				"default": {
					Priority:    20,
					Concurrency: 5,
				},
				"low": {
					Priority:    5,
					Concurrency: 1,
				},
			},
			want: map[string]QueueConfig{
				"high": {
					Priority:    20,
					Concurrency: 10,
				},
				"default": {
					Priority:    4,
					Concurrency: 5,
				},
				"low": {
					Priority:    1,
					Concurrency: 1,
				},
			},
		},
		{
			input: map[string]QueueConfig{
				"default": {
					Priority: 10,
				},
			},
			want: map[string]QueueConfig{
				"default": {
					Priority: 1,
				},
			},
		},
		{
			input: map[string]QueueConfig{
				"critical": {
					Priority: 5,
				},
				"default": {
					Priority: 1,
				},
			},
			want: map[string]QueueConfig{
				"critical": {
					Priority: 5,
				},
				"default": {
					Priority: 1,
				},
			},
		},
		{
			input: map[string]QueueConfig{
				"critical": {
					Priority: 6,
				},
				"default": {
					Priority: 3,
				},
				"low": {
					Priority: 0,
				},
			},
			want: map[string]QueueConfig{
				"critical": {
					Priority: 2,
				},
				"default": {
					Priority: 1,
				},
				"low": {
					Priority: 0,
				},
			},
		},
	}

	for _, tc := range tests {
		got := normalizeQueues(tc.input)
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("normalizeQueues(%v) = %v, want %v; (-want, +got):\n%s",
				tc.input, got, tc.want, diff)
		}
	}
}

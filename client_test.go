// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/stretchr/testify/require"
)

func TestClientEnqueueWithProcessAtOption(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))

	var (
		now          = time.Now()
		oneHourLater = now.Add(time.Hour)
	)

	tests := []struct {
		desc          string
		task          *Task
		processAt     time.Time // value for ProcessAt option
		opts          []Option  // other options
		wantInfo      *TaskInfo
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]base.Z
	}{
		{
			desc:      "Process task immediately",
			task:      task,
			processAt: now,
			opts:      []Option{},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
		{
			desc:      "Schedule task to be processed in the future",
			task:      task,
			processAt: oneHourLater,
			opts:      []Option{},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStateScheduled,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: oneHourLater,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]base.Z{
				"default": {
					{
						Message: &base.TaskMessage{
							Type:     task.Type(),
							Payload:  task.Payload(),
							Retry:    defaultMaxRetry,
							Queue:    "default",
							Timeout:  int64(defaultTimeout.Seconds()),
							Deadline: noDeadline.Unix(),
						},
						Score: oneHourLater.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		opts := append(tc.opts, ProcessAt(tc.processAt))
		gotInfo, err := client.Enqueue(tc.task, opts...)
		if err != nil {
			t.Error(err)
			continue
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(TaskInfo{}, "ID"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task, ProcessAt(%v)) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.processAt, gotInfo, tc.wantInfo, diff)
		}

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueue(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	now := time.Now()

	client := NewClient(getClientConnOpt(t), time.UTC)
	client.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = client.Close() }()

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))
	tests := []struct {
		desc        string
		task        *Task
		opts        []Option
		wantInfo    *TaskInfo
		wantPending map[string][]*base.TaskMessage
	}{
		{
			desc: "Process task immediately with a custom retry count",
			task: task,
			opts: []Option{
				MaxRetry(3),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      3,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    3,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Negative retry count",
			task: task,
			opts: []Option{
				MaxRetry(-2),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      0, // Retry count should be set to zero
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    0, // Retry count should be set to zero
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Conflicting options",
			task: task,
			opts: []Option{
				MaxRetry(2),
				MaxRetry(10),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      10, // Last option takes precedence
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    10, // Last option takes precedence
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With queue option",
			task: task,
			opts: []Option{
				Queue("custom"),
			},
			wantInfo: &TaskInfo{
				Queue:         "custom",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"custom": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "custom",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Queue option should be case sensitive",
			task: task,
			opts: []Option{
				Queue("MyQueue"),
			},
			wantInfo: &TaskInfo{
				Queue:         "MyQueue",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"MyQueue": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "MyQueue",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With timeout option",
			task: task,
			opts: []Option{
				Timeout(20 * time.Second),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       20 * time.Second,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  20,
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With deadline option",
			task: task,
			opts: []Option{
				Deadline(time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC)),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       noTimeout,
				Deadline:      time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC),
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(noTimeout.Seconds()),
						Deadline: time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC).Unix(),
					},
				},
			},
		},
		{
			desc: "With both deadline and timeout options",
			task: task,
			opts: []Option{
				Timeout(20 * time.Second),
				Deadline(time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC)),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       20 * time.Second,
				Deadline:      time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC),
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  20,
						Deadline: time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC).Unix(),
					},
				},
			},
		},
		{
			desc: "With Retention option",
			task: task,
			opts: []Option{
				Retention(24 * time.Hour),
			},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
				Retention:     24 * time.Hour,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:      task.Type(),
						Payload:   task.Payload(),
						Retry:     defaultMaxRetry,
						Queue:     "default",
						Timeout:   int64(defaultTimeout.Seconds()),
						Deadline:  noDeadline.Unix(),
						Retention: int64((24 * time.Hour).Seconds()),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		gotInfo, err := client.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}

		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(TaskInfo{}, "ID"),
			cmpopts.EquateApproxTime(time.Second),
		}
		if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, gotInfo, tc.wantInfo, diff)
		}

		for qname, want := range tc.wantPending {
			got := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, got, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueWithTaskIDOption(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	task := NewTask("send_email", nil)
	now := time.Now()

	tests := []struct {
		desc        string
		task        *Task
		opts        []Option
		wantInfo    *TaskInfo
		wantPending map[string][]*base.TaskMessage
	}{
		{
			desc: "With a valid TaskID option",
			task: task,
			opts: []Option{
				TaskID("custom_id"),
			},
			wantInfo: &TaskInfo{
				ID:            "custom_id",
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						ID:       "custom_id",
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		gotInfo, err := client.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Errorf("got non-nil error %v, want nil", err)
			continue
		}

		cmpOptions := []cmp.Option{
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, gotInfo, tc.wantInfo, diff)
		}

		for qname, want := range tc.wantPending {
			got := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueWithConflictingTaskID(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	const taskID = "custom_id"
	task := NewTask("foo", nil)

	if _, err := client.Enqueue(task, TaskID(taskID)); err != nil {
		t.Fatalf("First task: Enqueue failed: %v", err)
	}
	_, err := client.Enqueue(task, TaskID(taskID))
	if !errors.Is(err, ErrTaskIDConflict) {
		t.Errorf("Second task: Enqueue returned %v, want %v", err, ErrTaskIDConflict)
	}
}

func TestClientEnqueueConflictingWithCompleted(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	const (
		initialTask = "initialTask"
		customId    = "customId"
		queueName   = "queueName"
	)

	client := NewClient(getClientConnOpt(t))
	inspector := newInspector(client.rdb)
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		LogLevel: testLogLevel, //DebugLevel,
		Queues: &QueuesConfig{Queues: map[string]interface{}{
			queueName: 1,
		}},
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, task *Task) error {
		if task.Type() == initialTask {
			task.CallAfter(func(string, error, bool) {
				defer wg.Done()
			})
		}
		return nil
	}
	_ = srv.Start(HandlerFunc(handler))

	wg.Add(1)
	opts := []Option{Retention(time.Hour), TaskID(customId), Queue(queueName)}
	task1 := NewTask(initialTask, nil)
	_, err := client.Enqueue(task1, opts...)
	require.NoError(t, err)
	wg.Wait()

	// use a different task type just to not use the wait group
	task2 := NewTask("nextTask", nil)
	_, err = client.Enqueue(task2, opts...)
	require.True(t, errors.Is(err, ErrTaskIDConflict))

	ti, err := inspector.GetTaskInfo(queueName, customId)
	require.NoError(t, err)
	require.Equal(t, TaskStateCompleted, ti.State)
}

func TestClientEnqueueTaskIDConflictingWithCompleted(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	inspector := newInspector(client.rdb)
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		LogLevel: testLogLevel, //DebugLevel,
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, task *Task) error {
		//fmt.Println("handled", "task", task.Type())
		task.CallAfter(func(string, error, bool) {
			defer wg.Done()
		})
		return nil
	}
	_ = srv.Start(HandlerFunc(handler))

	const taskID = "custom_id"
	type testCase struct {
		retention time.Duration
		taskId    string
		unique    time.Duration
		schedule  bool
	}

	for i, tc := range []*testCase{
		{retention: 0, taskId: taskID},
		{retention: time.Hour, taskId: taskID},
		{unique: 0},
		{unique: time.Hour},
		{retention: time.Hour, unique: time.Hour},

		{retention: 0, taskId: taskID, schedule: true},
		{retention: time.Hour, taskId: taskID, schedule: true},
		{unique: 0, schedule: true},
		{unique: time.Hour, schedule: true},
		{retention: time.Hour, unique: time.Hour, schedule: true},
	} {
		ctx.FlushDB()
		srv.logger.Debug("test-case", " index ", i)

		task := NewTask("foo", nil)

		wg.Add(1)
		opts := []Option{Retention(tc.retention)}
		if tc.taskId != "" {
			opts = append(opts, TaskID(tc.taskId))
		}
		if tc.unique > 0 {
			opts = append(opts, Unique(tc.unique))
		}
		eti, err := client.Enqueue(task, opts...)
		if err != nil {
			t.Fatalf("First task: Enqueue failed: %v", err)
		}
		wg.Wait()

		ti, err := inspector.GetTaskInfo("default", eti.ID)
		if tc.retention > 0 {
			if err != nil {
				t.Fatalf("First task: GetTaskInfo failed: %v", err)
			}
			if ti.State != TaskStateCompleted {
				t.Fatalf("First task: GetTaskInfo state: expected 'completed', got %v", ti.State.String())
			}
		} else if err == nil || !strings.Contains(err.Error(), "task not found") {
			t.Fatalf("First task: GetTaskInfo expected not found, got %v", err)
		}

		opts = []Option{}
		if tc.taskId != "" {
			opts = append(opts, TaskID(tc.taskId))
		}
		if tc.unique > 0 {
			opts = append(opts, Unique(tc.unique))
		}
		if tc.schedule {
			opts = append(opts, ProcessIn(time.Hour))
		}
		if !tc.schedule {
			wg.Add(1)
		}
		ti, err = client.Enqueue(task, opts...)
		if err != nil && !tc.schedule {
			wg.Done()
		}
		// there's a conflict with 'completed' task only with task ID
		if tc.retention != 0 && tc.taskId != "" && !errors.Is(err, ErrTaskIDConflict) {
			t.Errorf("Second task: Retention: %v, Enqueue returned %v, want %v", tc.retention, err, ErrTaskIDConflict)
		}
		wg.Wait()
	}
}

func TestClientEnqueueWithProcessInOption(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	now := time.Now()

	client := NewClient(getClientConnOpt(t))
	client.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = client.Close() }()

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))
	tests := []struct {
		desc          string
		task          *Task
		delay         time.Duration // value for ProcessIn option
		opts          []Option      // other options
		wantInfo      *TaskInfo
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]base.Z
	}{
		{
			desc:  "schedule a task to be processed in one hour",
			task:  task,
			delay: 1 * time.Hour,
			opts:  []Option{},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStateScheduled,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: time.Now().Add(1 * time.Hour),
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]base.Z{
				"default": {
					{
						Message: &base.TaskMessage{
							Type:     task.Type(),
							Payload:  task.Payload(),
							Retry:    defaultMaxRetry,
							Queue:    "default",
							Timeout:  int64(defaultTimeout.Seconds()),
							Deadline: noDeadline.Unix(),
						},
						Score: time.Now().Add(time.Hour).Unix(),
					},
				},
			},
		},
		{
			desc:  "Zero delay",
			task:  task,
			delay: 0,
			opts:  []Option{},
			wantInfo: &TaskInfo{
				Queue:         "default",
				Type:          task.Type(),
				Payload:       task.Payload(),
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		opts := append(tc.opts, ProcessIn(tc.delay))
		gotInfo, err := client.Enqueue(tc.task, opts...)
		if err != nil {
			t.Error(err)
			continue
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(TaskInfo{}, "ID"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task, ProcessIn(%v)) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.delay, gotInfo, tc.wantInfo, diff)
		}

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))

	tests := []struct {
		desc string
		task *Task
		opts []Option
	}{
		{
			desc: "With empty queue name",
			task: task,
			opts: []Option{
				Queue(""),
			},
		},
		{
			desc: "With empty task typename",
			task: NewTask("", h.JSON(map[string]interface{}{})),
			opts: []Option{},
		},
		{
			desc: "With blank task typename",
			task: NewTask("    ", h.JSON(map[string]interface{}{})),
			opts: []Option{},
		},
		{
			desc: "With empty task ID",
			task: NewTask("foo", nil),
			opts: []Option{TaskID("")},
		},
		{
			desc: "With blank task ID",
			task: NewTask("foo", nil),
			opts: []Option{TaskID("  ")},
		},
		{
			desc: "With unique option less than 1s",
			task: NewTask("foo", nil),
			opts: []Option{Unique(300 * time.Millisecond)},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()

		_, err := client.Enqueue(tc.task, tc.opts...)
		if err == nil {
			t.Errorf("%s; client.Enqueue(task, opts...) did not return non-nil error", tc.desc)
		}
	}
}

func TestClientWithDefaultOptions(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	now := time.Now()

	tests := []struct {
		desc        string
		defaultOpts []Option // options set at task initialization time
		opts        []Option // options used at enqueue time.
		tasktype    string
		payload     []byte
		wantInfo    *TaskInfo
		queue       string // queue that the message should go into.
		want        *base.TaskMessage
	}{
		{
			desc:        "With queue routing option",
			defaultOpts: []Option{Queue("feed")},
			opts:        []Option{},
			tasktype:    "feed:import",
			payload:     nil,
			wantInfo: &TaskInfo{
				Queue:         "feed",
				Type:          "feed:import",
				Payload:       nil,
				State:         TaskStatePending,
				MaxRetry:      defaultMaxRetry,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			queue: "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    defaultMaxRetry,
				Queue:    "feed",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
		{
			desc:        "With multiple options",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{},
			tasktype:    "feed:import",
			payload:     nil,
			wantInfo: &TaskInfo{
				Queue:         "feed",
				Type:          "feed:import",
				Payload:       nil,
				State:         TaskStatePending,
				MaxRetry:      5,
				Retried:       0,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			queue: "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "feed",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
		{
			desc:        "With overriding options at enqueue time",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{Queue("critical")},
			tasktype:    "feed:import",
			payload:     nil,
			wantInfo: &TaskInfo{
				Queue:         "critical",
				Type:          "feed:import",
				Payload:       nil,
				State:         TaskStatePending,
				MaxRetry:      5,
				LastErr:       "",
				LastFailedAt:  time.Time{},
				Timeout:       defaultTimeout,
				Deadline:      time.Time{},
				NextProcessAt: now,
			},
			queue: "critical",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "critical",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
	}

	for _, tc := range tests {

		t.Run(tc.desc, func(t *testing.T) {
			ctx.FlushDB()
			client := NewClient(getClientConnOpt(t))
			client.rdb.SetClock(timeutil.NewSimulatedClock(now))
			defer func() { _ = client.Close() }()

			task := NewTask(tc.tasktype, tc.payload, tc.defaultOpts...)
			gotInfo, err := client.Enqueue(task, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			cmpOptions := []cmp.Option{
				cmpopts.IgnoreFields(TaskInfo{}, "ID"),
				cmpopts.EquateApproxTime(500 * time.Millisecond),
			}
			if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpOptions...); diff != "" {
				t.Errorf("%s;\nEnqueue(task, opts...) returned %v, want %v; (-want,+got)\n%s",
					tc.desc, gotInfo, tc.wantInfo, diff)
			}
			pending := ctx.GetPendingMessages(tc.queue)
			if len(pending) != 1 {
				t.Errorf("%s;\nexpected queue %q to have one message; got %d messages in the queue.",
					tc.desc, tc.queue, len(pending))
				return
			}
			got := pending[0]
			if diff := cmp.Diff(tc.want, got, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in pending task message; (-want,+got)\n%s",
					tc.desc, diff)
			}
		})
	}
}

func TestClientEnqueueUnique(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	tests := []struct {
		task *Task
		ttl  time.Duration
	}{
		{
			task: NewTask("email", h.JSON(map[string]interface{}{"user_id": 123})),
			ttl:  time.Hour,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := client.Enqueue(tc.task, Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := ctx.GetUniqueKeyTTL(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, tc.ttl)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = client.Enqueue(tc.task, Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}

func TestClientEnqueueUniqueWithProcessInOption(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	tests := []struct {
		task *Task
		d    time.Duration
		ttl  time.Duration
	}{
		{
			NewTask("reindex", nil),
			time.Hour,
			10 * time.Minute,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := client.Enqueue(tc.task, ProcessIn(tc.d), Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := ctx.GetUniqueKeyTTL(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())
		wantTTL := time.Duration(tc.ttl.Seconds()+tc.d.Seconds()) * time.Second
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = client.Enqueue(tc.task, ProcessIn(tc.d), Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}

func TestClientEnqueueUniqueWithProcessAtOption(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	tests := []struct {
		task *Task
		at   time.Time
		ttl  time.Duration
	}{
		{
			task: NewTask("reindex", nil),
			at:   time.Now().Add(time.Hour),
			ttl:  10 * time.Minute,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := client.Enqueue(tc.task, ProcessAt(tc.at), Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := ctx.GetUniqueKeyTTL(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())
		wantTTL := tc.at.Add(tc.ttl).Sub(time.Now())
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = client.Enqueue(tc.task, ProcessAt(tc.at), Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}

func TestClientEnqueueBatch(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	t0 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant0@example.com"}))
	t1 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant1@example.com"}))
	t2 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant2@example.com"}))

	tests := []struct {
		desc  string
		tasks []*Task
		opts  []Option
	}{
		{
			desc:  "Basic 3 tasks",
			tasks: []*Task{t0, t1, t2},
			opts: []Option{
				Queue("purchase"),
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()

		ti, err := client.EnqueueBatch(tc.tasks, tc.opts...)
		require.NoError(t, err, "%s; client.EnqueueBatch(task, opts...) return non-nil error", tc.desc)
		require.NotNil(t, ti)
		require.Equal(t, len(tc.tasks), len(ti))

		for i, task := range tc.tasks {
			taskInfo := ti[i]
			require.NotNil(t, taskInfo)
			require.Equal(t, TaskStatePending, taskInfo.State)
			require.Equal(t, task.typename, taskInfo.Type)
			require.Equal(t, task.payload, taskInfo.Payload)
		}
	}
}

func TestClientEnqueueBatchError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	t0 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant0@example.com"}))
	t1 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant1@example.com"}))
	t2 := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant2@example.com"}))

	tests := []struct {
		desc       string
		tasks      []*Task
		opts       []Option
		expFailure int
	}{
		{
			desc:  "With duplicated task only",
			tasks: []*Task{t1, t1},
			opts: []Option{
				Queue("purchase"),
				Unique(time.Second),
			},
			expFailure: 1,
		},
		{
			desc:  "With duplicated task in the middle",
			tasks: []*Task{t0, t1, t1, t2},
			opts: []Option{
				Queue("purchase"),
				Unique(time.Second),
			},
			expFailure: 2,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()

		ti, err := client.EnqueueBatch(tc.tasks, tc.opts...)
		require.Error(t, err, "%s; client.EnqueueBatch(task, opts...) did not return non-nil error", tc.desc)
		require.NotNil(t, ti)
		require.Equal(t, len(tc.tasks), len(ti))

		berr, ok := err.(BatchError)
		require.True(t, ok, tc.desc)
		require.NotNil(t, berr.MapErrors(), tc.desc)
		gotFailure := berr.MapErrors()[tc.expFailure]
		require.NotNil(t, gotFailure, tc.desc)
		require.True(t, errors.Is(gotFailure, ErrDuplicateTask), tc.desc)

		for i, task := range tc.tasks {
			if i == tc.expFailure {
				require.Nil(t, ti[i])
				continue
			}
			taskInfo := ti[i]
			require.NotNil(t, taskInfo)
			require.Equal(t, TaskStatePending, taskInfo.State)
			require.Equal(t, task.typename, taskInfo.Type)
			require.Equal(t, task.payload, taskInfo.Payload)
		}
	}
}

func TestUpdateTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	inspector := newInspector(client.rdb)
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		LogLevel: testLogLevel, //DebugLevel,
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	wgActive := sync.WaitGroup{}
	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, task *Task) error {
		wgActive.Done()
		task.CallAfter(func(string, error, bool) {
			defer wg.Done()
		})
		time.Sleep(time.Millisecond * 300)
		return nil
	}
	_ = srv.Start(HandlerFunc(handler))

	ctx.FlushDB()
	{
		//
		// retention and update while task is active
		//
		const taskID = "custom_id"
		task := NewTask("foo", nil)
		now := time.Now().UTC()
		deadline := now.Add(time.Hour).Truncate(time.Second)

		wgActive.Add(1)
		wg.Add(1)
		opts := []Option{
			Retention(time.Hour * 2),
			Deadline(deadline),
			TaskID(taskID),
		}
		eti, err := client.Enqueue(task, opts...)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		wgActive.Wait()
		ah := srv.AsynchronousHandler()
		uti, dl, err := ah.UpdateTask(eti.Queue, eti.ID, []byte("azerty"))
		if err != nil {
			t.Fatalf("UpdateTask failed: %v", err)
		}
		if !dl.Equal(deadline) {
			t.Fatalf("deadline failed, expected: %v, actual:%v", deadline, dl)
		}
		if string(uti.Result) != "azerty" {
			t.Fatalf("update result failed, expected: %v, actual:%v", "azerty", string(uti.Result))
		}
		wg.Wait()

		ti, err := inspector.GetTaskInfo(eti.Queue, eti.ID)
		if err != nil {
			t.Fatalf("GetTaskInfo failed: %v", err)
		}
		if ti.State != TaskStateCompleted {
			t.Fatalf("GetTaskInfo state: expected 'completed', got %v", ti.State.String())
		}
		if string(ti.Result) != "azerty" {
			t.Fatalf("update result failed, expected: %v, actual:%v", "azerty", string(uti.Result))
		}
	}
	{
		//
		// retention and update after task has completed
		//
		const taskID = "custom_id2"
		task := NewTask("foo", nil)
		now := time.Now().UTC()
		deadline := now.Add(time.Hour).Truncate(time.Second)

		wgActive.Add(1)
		wg.Add(1)
		opts := []Option{
			Retention(time.Hour * 2),
			Deadline(deadline),
			TaskID(taskID),
		}
		eti, err := client.Enqueue(task, opts...)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		wgActive.Wait()
		wg.Wait()

		ah := srv.AsynchronousHandler()
		uti, dl, err := ah.UpdateTask(eti.Queue, eti.ID, []byte("pompom"))
		if err != nil {
			t.Fatalf("UpdateTask failed: %v", err)
		}
		if uti.State != TaskStateCompleted {
			t.Fatalf("UpdateTask state: expected 'completed', got %v", uti.State.String())
		}
		if dl.Unix() != 0 {
			t.Fatalf("deadline failed, expected zero, actual:%v", dl)
		}
		if string(uti.Result) != "pompom" {
			t.Fatalf("update result failed, expected: %v, actual:%v", "azerty", string(uti.Result))
		}

		ti, err := inspector.GetTaskInfo(eti.Queue, eti.ID)
		if err != nil {
			t.Fatalf("GetTaskInfo failed: %v", err)
		}
		if ti.State != TaskStateCompleted {
			t.Fatalf("GetTaskInfo state: expected 'completed', got %v", ti.State.String())
		}
		if string(ti.Result) != "pompom" {
			t.Fatalf("update result failed, expected: %v, actual:%v", "azerty", string(uti.Result))
		}
	}
	{
		//
		// no retention: update should fail
		//
		const taskID = "custom_id3"
		task := NewTask("foo", nil)
		now := time.Now().UTC()
		deadline := now.Add(time.Hour).Truncate(time.Second)

		wgActive.Add(1)
		wg.Add(1)
		opts := []Option{
			Deadline(deadline),
			TaskID(taskID),
		}
		eti, err := client.Enqueue(task, opts...)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		wgActive.Wait()
		wg.Wait()

		ah := srv.AsynchronousHandler()
		_, _, err = ah.UpdateTask(eti.Queue, eti.ID, []byte("qwerty"))
		if err == nil {
			t.Fatalf("UpdateTask did not fail")
		}

		_, err = inspector.GetTaskInfo(eti.Queue, eti.ID)
		if err == nil {
			t.Fatalf("GetTaskInfo did not fail")
		}
	}

}

func TestUpdateFailedTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		LogLevel: testLogLevel,
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, task *Task) error {
		task.CallAfter(func(string, error, bool) {
			defer wg.Done()
		})
		// handler always fails
		return errors.New("test")
	}
	_ = srv.Start(HandlerFunc(handler))

	ctx.FlushDB()
	{
		//
		// update task after execution 'failed'
		//
		const taskID = "custom_id"
		task := NewTask("foo", nil)
		now := time.Now().UTC()
		deadline := now.Add(time.Hour).Truncate(time.Second)

		wg.Add(1)
		opts := []Option{
			Retention(time.Hour * 2),
			Deadline(deadline),
			TaskID(taskID),
			MaxRetry(0), // no retry: task will be archived
		}
		eti, err := client.Enqueue(task, opts...)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		wg.Wait()
		ah := srv.AsynchronousHandler()
		ti, _, err := ah.UpdateTask(eti.Queue, eti.ID, []byte("azerty"))
		if err != nil {
			t.Fatalf("UpdateTask failed: %v", err)
		}

		assertTask := func(ti *TaskInfo, result string) {
			if ti.State != TaskStateArchived {
				t.Fatalf("GetTaskInfo state: expected 'archived', got %v", ti.State.String())
			}
			if string(ti.Result) != result {
				t.Fatalf("update result failed, expected: %v, actual:%v", "azerty", string(ti.Result))
			}
			// last error is still there
			if ti.LastErr != "test" {
				t.Fatalf("expected 'test' last error, got '%s'", ti.LastErr)
			}
		}
		assertTask(ti, "azerty")

		ti, _, err = ah.UpdateTask(eti.Queue, eti.ID, []byte("qwerty"))
		if err != nil {
			t.Fatalf("UpdateTask failed: %v", err)
		}
		assertTask(ti, "qwerty")
	}
}

func TestActiveTask_TransitionToQueue(t *testing.T) {
	const (
		customId         = "customId"
		asynchronousType = "asynchronous"
		regularType      = "regular"
		transitionType   = "transition"
	)
	queues := map[string]string{
		regularType:      "regular",
		transitionType:   "transition",
		asynchronousType: "asynchronous",
	}

	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		Queues: &QueuesConfig{
			Queues: map[string]interface{}{
				queues[regularType]:      1,
				queues[transitionType]:   1,
				queues[asynchronousType]: 1,
			},
			Priority: Lenient,
		},
		LogLevel: testLogLevel,
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, task *Task) error {
		srv.logger.Info("task handler, type: " + task.Type())
		switch task.Type() {
		case regularType:
			_, err := task.AsyncProcessor().TransitionToQueue(
				queues[transitionType],
				transitionType,
				Retention(time.Hour),
			)
			return err
		case transitionType:
			task.CallAfter(func(string, error, bool) {
				defer wg.Done()
			})
			return nil
		case asynchronousType:
			go func() {
				err := task.AsyncProcessor().TaskTransition(
					queues[transitionType],
					transitionType,
					Retention(time.Hour),
				)
				require.NoError(t, err)
			}()
			return AsynchronousTask
		default:
		}
		return errors.New("unexpected task type: " + task.Type())
	}
	_ = srv.Start(HandlerFunc(handler))

	inspect, err := NewInspectorClient(client)
	require.NoError(t, err)

	for _, taskType := range []string{regularType, asynchronousType} {
		ctx.FlushDB()
		wg.Add(1)
		_, err := client.Enqueue(NewTask(taskType, nil,
			TaskID(customId),
			Queue(queues[taskType])))
		require.NoError(t, err)

		wg.Wait()
		ti, err := inspect.GetTaskInfo(queues[transitionType], customId)
		require.NoError(t, err)
		require.Equal(t, TaskStateCompleted, ti.State)
	}

}

func TestMoveCompletedTask_ToQueue(t *testing.T) {
	const (
		customId = "customId"
	)

	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		Concurrency: 1,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Second
		}),
		LogLevel: testLogLevel,
		Queues: &QueuesConfig{
			Queues: map[string]interface{}{
				"default":   1,
				"new_queue": 1,
			},
			Priority: Lenient,
		},
	})
	defer func() {
		srv.Shutdown()
		_ = client.Close()
	}()

	var task *Task
	wg := sync.WaitGroup{}
	handler := func(ctx context.Context, t *Task) error {
		t.CallAfter(func(string, error, bool) {
			defer wg.Done()
		})
		task = t
		return nil
	}
	_ = srv.Start(HandlerFunc(handler))

	inspect, err := NewInspectorClient(client)
	require.NoError(t, err)

	ctx.FlushDB()
	wg.Add(1)
	_, err = client.Enqueue(NewTask("test", nil,
		TaskID(customId),
		Retention(time.Hour)))
	require.NoError(t, err)
	wg.Wait()

	ti, err := inspect.GetTaskInfo("default", customId)
	require.NoError(t, err)
	require.Equal(t, TaskStateCompleted, ti.State)

	_, err = task.AsyncProcessor().TransitionToQueue("new_queue", "new_type")
	// error: rqlite.moveToQueue - rqlite error: context canceled, at: rqlite.(*Connection).moveToQueue (tasks.go:1089)
	// if context is not canceled:
	// next: rqlite.moveToQueue: FAILED_PRECONDITION: cannot move to new queue an already completed task.
	require.Error(t, err)

	wg.Add(1)
	ah := srv.AsynchronousHandler()
	ti, err = ah.RequeueCompleted(
		context.Background(),
		"default",
		customId,
		"new_queue",
		"new_type",
		Retention(time.Hour))
	require.NoError(t, err)
	require.Equal(t, TaskStatePending, ti.State)
	wg.Wait()

	ti, err = inspect.GetTaskInfo("new_queue", customId)
	require.NoError(t, err)
	require.Equal(t, TaskStateCompleted, ti.State)
}

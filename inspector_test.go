// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/stretchr/testify/require"
)

func TestInspectorQueues(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		queues []string
	}{
		{queues: []string{"default"}},
		{queues: []string{"custom1", "custom2"}},
		{queues: []string{"default", "custom1", "custom2"}},
		{queues: []string{}},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		for _, qname := range tc.queues {
			ctx.InitQueue(qname)
		}
		got, err := inspector.Queues()
		if err != nil {
			t.Errorf("Queues() returned an error: %v", err)
			continue
		}
		if diff := cmp.Diff(tc.queues, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("Queues() = %v, want %v; (-want, +got)\n%s", got, tc.queues, diff)
		}
	}

}

func TestInspectorDeleteQueue(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
			active: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: false,
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			active: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m4, Score: time.Now().Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: true, // allow removing non-empty queue
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllActiveQueues(tc.active)
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if err != nil {
			t.Errorf("DeleteQueue(%q, %t) = %v, want nil",
				tc.qname, tc.force, err)
			continue
		}
		if ctx.QueueExist(tc.qname) {
			t.Errorf("%q is a member of %q", tc.qname, base.AllQueues)
		}
	}
}

func TestInspectorDeleteQueueErrorQueueNotEmpty(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "default")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "default")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			active: map[string][]*base.TaskMessage{
				"default": {m3, m4},
			},
			scheduled: map[string][]base.Z{
				"default": {},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			force: false,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllActiveQueues(tc.active)
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if !errors.Is(err, ErrQueueNotEmpty) {
			t.Errorf("DeleteQueue(%v, %t) did not return ErrQueueNotEmpty",
				tc.qname, tc.force)
		}
	}
}

func TestInspectorDeleteQueueErrorQueueNotFound(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "default")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "default")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			active: map[string][]*base.TaskMessage{
				"default": {m3, m4},
			},
			scheduled: map[string][]base.Z{
				"default": {},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "nonexistent",
			force: false,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllActiveQueues(tc.active)
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if !errors.Is(err, ErrQueueNotFound) {
			t.Errorf("DeleteQueue(%v, %t) did not return ErrQueueNotFound",
				tc.qname, tc.force)
		}
	}
}

func TestInspectorGetQueueInfo(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessage("task4", nil)
	m5 := h.NewTaskMessageWithQueue("task5", nil, "critical")
	m6 := h.NewTaskMessageWithQueue("task6", nil, "low")

	now := time.Now()
	timeCmpOpt := cmpopts.EquateApproxTime(time.Second)
	ignoreMemUsg := cmpopts.IgnoreFields(QueueInfo{}, "MemoryUsage")

	inspector := NewInspector(getClientConnOpt(t))
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		pending                         map[string][]*base.TaskMessage
		active                          map[string][]*base.TaskMessage
		scheduled                       map[string][]base.Z
		retry                           map[string][]base.Z
		archived                        map[string][]base.Z
		completed                       map[string][]base.Z
		processed                       map[string]int
		failed                          map[string]int
		oldestPendingMessageEnqueueTime map[string]time.Time
		qname                           string
		want                            *QueueInfo
		wantRqlite                      *QueueInfo
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m5},
				"low":      {m6},
			},
			active: map[string][]*base.TaskMessage{
				"default":  {m2},
				"critical": {},
				"low":      {},
			},
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m3, Score: now.Add(time.Hour).Unix()},
					{Message: m4, Score: now.Unix()},
				},
				"critical": {},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			completed: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			processed: map[string]int{
				"default":  120,
				"critical": 100,
				"low":      42,
			},
			failed: map[string]int{
				"default":  2,
				"critical": 0,
				"low":      5,
			},
			oldestPendingMessageEnqueueTime: map[string]time.Time{
				"default":  now.Add(-15 * time.Second),
				"critical": now.Add(-200 * time.Millisecond),
				"low":      now.Add(-30 * time.Second),
			},
			qname: "default",
			want: &QueueInfo{
				Queue:     "default",
				Latency:   15 * time.Second,
				Size:      4,
				Pending:   1,
				Active:    1,
				Scheduled: 2,
				Retry:     0,
				Archived:  0,
				Completed: 0,
				Processed: 120,
				Failed:    2,
				Paused:    false,
				Timestamp: now,
			},
			// PENDING(GIL): see comment in test TestCurrentStats in rqlite/inspect_test
			wantRqlite: &QueueInfo{
				Queue:     "default",
				Latency:   15 * time.Second,
				Size:      6,
				Pending:   1,
				Active:    1,
				Scheduled: 2,
				Retry:     2,
				Archived:  0,
				Processed: 120,
				Failed:    2,
				Paused:    false,
				Timestamp: now,
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllActiveQueues(tc.active)
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)
		ctx.SeedAllCompletedQueues(tc.completed)
		ctx.SeedAllProcessedQueues(tc.processed, now)
		ctx.SeedAllFailedQueues(tc.failed, now)
		for qname, enqueueTime := range tc.oldestPendingMessageEnqueueTime {
			// nothing done if enqueueTime is zero
			ctx.SeedLastPendingSince(qname, enqueueTime)
		}

		got, err := inspector.GetQueueInfo(tc.qname)
		if err != nil {
			t.Errorf("r.GetQueueInfo(%q) = %v, %v, want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}
		if brokerType == RqliteType || brokerType == SqliteType {
			if diff := cmp.Diff(tc.wantRqlite, got, timeCmpOpt, ignoreMemUsg); diff != "" {
				t.Errorf("r.GetQueueInfo(%q) = %v, %v, want %v, nil; (-want, +got)\n%s",
					tc.qname, got, err, tc.want, diff)
				continue
			}
		} else {
			if diff := cmp.Diff(tc.want, got, timeCmpOpt, ignoreMemUsg); diff != "" {
				t.Errorf("r.GetQueueInfo(%q) = %v, %v, want %v, nil; (-want, +got)\n%s",
					tc.qname, got, err, tc.want, diff)
				continue
			}
		}
	}

}

func TestInspectorHistory(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	now := time.Now()
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))

	tests := []struct {
		qname string // queue of interest
		n     int    // number of days
	}{
		{"default", 90},
		{"custom", 7},
		{"default", 1},
	}

	processedCount := func(i int) int {
		if brokerType == RqliteType || brokerType == SqliteType {
			return (i + 1) * 10
		}
		return (i + 1) * 1000
	}
	failedCount := func(i int) int {
		if brokerType == RqliteType || brokerType == SqliteType {
			return i + 1
		}
		return i + 10
	}

	for _, tc := range tests {
		ctx.FlushDB()

		ctx.InitQueue(tc.qname)
		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			ctx.SeedProcessedQueue(processedCount(i), tc.qname, ts)
			ctx.SeedFailedQueue(failedCount(i), tc.qname, ts)
		}

		got, err := inspector.History(tc.qname, tc.n)
		if err != nil {
			t.Errorf("Inspector.History(%q, %d) returned error: %v", tc.qname, tc.n, err)
			continue
		}
		if len(got) != tc.n {
			t.Errorf("Inspector.History(%q, %d) returned %d daily stats, want %d",
				tc.qname, tc.n, len(got), tc.n)
			continue
		}
		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
				Queue:     tc.qname,
				Processed: processedCount(i),
				Failed:    failedCount(i),
				Date:      now.Add(-time.Duration(i) * 24 * time.Hour),
			}
			// Allow 2 seconds difference in timestamp.
			timeCmpOpt := cmpopts.EquateApproxTime(2 * time.Second)
			if diff := cmp.Diff(want, got[i], timeCmpOpt); diff != "" {
				t.Errorf("Inspector.History %d days ago data; got %+v, want %+v; (-want,+got):\n%s",
					i, got[i], want, diff)
			}
		}
	}
}

func createPendingTask(msg *base.TaskMessage) *TaskInfo {
	return newTaskInfo(msg, base.TaskStatePending, time.Now(), nil)
}

func TestInspectorGetTaskInfo(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	m1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	m2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	m5 := h.NewTaskMessageWithQueue("task5", nil, "custom")

	now := time.Now()
	fiveMinsFromNow := now.Add(5 * time.Minute)
	oneHourFromNow := now.Add(1 * time.Hour)
	twoHoursAgo := now.Add(-2 * time.Hour)

	fixtures := struct {
		active    map[string][]*base.TaskMessage
		pending   map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
	}{
		active: map[string][]*base.TaskMessage{
			"default": {m1},
			"custom":  {},
		},
		pending: map[string][]*base.TaskMessage{
			"default": {},
			"custom":  {m5},
		},
		scheduled: map[string][]base.Z{
			"default": {{Message: m2, Score: fiveMinsFromNow.Unix()}},
			"custom":  {},
		},
		retry: map[string][]base.Z{
			"default": {},
			"custom":  {{Message: m3, Score: oneHourFromNow.Unix()}},
		},
		archived: map[string][]base.Z{
			"default": {},
			"custom":  {{Message: m4, Score: twoHoursAgo.Unix()}},
		},
	}

	ctx.SeedAllActiveQueues(fixtures.active)
	ctx.SeedAllPendingQueues(fixtures.pending)
	ctx.SeedAllScheduledQueues(fixtures.scheduled)
	ctx.SeedAllRetryQueues(fixtures.retry)
	ctx.SeedAllArchivedQueues(fixtures.archived)

	tests := []struct {
		qname string
		id    string
		want  *TaskInfo
	}{
		{
			qname: "default",
			id:    m1.ID,
			want: newTaskInfo(
				m1,
				base.TaskStateActive,
				time.Time{}, // zero value for n/a
				nil,
			),
		},
		{
			qname: "default",
			id:    m2.ID,
			want: newTaskInfo(
				m2,
				base.TaskStateScheduled,
				fiveMinsFromNow,
				nil,
			),
		},
		{
			qname: "custom",
			id:    m3.ID,
			want: newTaskInfo(
				m3,
				base.TaskStateRetry,
				oneHourFromNow,
				nil,
			),
		},
		{
			qname: "custom",
			id:    m4.ID,
			want: newTaskInfo(
				m4,
				base.TaskStateArchived,
				time.Time{}, // zero value for n/a
				nil,
			),
		},
		{
			qname: "custom",
			id:    m5.ID,
			want: newTaskInfo(
				m5,
				base.TaskStatePending,
				now,
				nil,
			),
		},
	}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	for _, tc := range tests {
		got, err := inspector.GetTaskInfo(tc.qname, tc.id)
		if err != nil {
			t.Errorf("GetTaskInfo(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		cmpOpts := []cmp.Option{
			cmp.AllowUnexported(TaskInfo{}),
			cmpopts.EquateApproxTime(2 * time.Second),
		}
		if diff := cmp.Diff(tc.want, got, cmpOpts...); diff != "" {
			t.Errorf("GetTaskInfo(%q, %q) = %v, want %v; (-want, +got)\n%s", tc.qname, tc.id, got, tc.want, diff)
		}
	}
}

func TestInspectorGetTaskInfoError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	m1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	m2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	m5 := h.NewTaskMessageWithQueue("task5", nil, "custom")

	now := time.Now()
	fiveMinsFromNow := now.Add(5 * time.Minute)
	oneHourFromNow := now.Add(1 * time.Hour)
	twoHoursAgo := now.Add(-2 * time.Hour)

	fixtures := struct {
		active    map[string][]*base.TaskMessage
		pending   map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
	}{
		active: map[string][]*base.TaskMessage{
			"default": {m1},
			"custom":  {},
		},
		pending: map[string][]*base.TaskMessage{
			"default": {},
			"custom":  {m5},
		},
		scheduled: map[string][]base.Z{
			"default": {{Message: m2, Score: fiveMinsFromNow.Unix()}},
			"custom":  {},
		},
		retry: map[string][]base.Z{
			"default": {},
			"custom":  {{Message: m3, Score: oneHourFromNow.Unix()}},
		},
		archived: map[string][]base.Z{
			"default": {},
			"custom":  {{Message: m4, Score: twoHoursAgo.Unix()}},
		},
	}

	ctx.SeedAllActiveQueues(fixtures.active)
	ctx.SeedAllPendingQueues(fixtures.pending)
	ctx.SeedAllScheduledQueues(fixtures.scheduled)
	ctx.SeedAllRetryQueues(fixtures.retry)
	ctx.SeedAllArchivedQueues(fixtures.archived)

	tests := []struct {
		qname   string
		id      string
		wantErr error
	}{
		{
			qname:   "nonexistent",
			id:      m1.ID,
			wantErr: ErrQueueNotFound,
		},
		{
			qname:   "default",
			id:      uuid.NewString(),
			wantErr: ErrTaskNotFound,
		},
	}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	for _, tc := range tests {
		info, err := inspector.GetTaskInfo(tc.qname, tc.id)
		if info != nil {
			t.Errorf("GetTaskInfo(%q, %q) returned info: %v", tc.qname, tc.id, info)
		}
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("GetTaskInfo(%q, %q) returned unexpected error: %v, want %v", tc.qname, tc.id, err, tc.wantErr)
		}
	}
}

func TestInspectorListPendingTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "critical")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "low")

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc    string
		pending map[string][]*base.TaskMessage
		qname   string
		want    []*TaskInfo
	}{
		{
			desc: "with default queue",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			want: []*TaskInfo{
				createPendingTask(m1),
				createPendingTask(m2),
			},
		},
		{
			desc: "with named queue",
			pending: map[string][]*base.TaskMessage{
				"default":  {m1, m2},
				"critical": {m3},
				"low":      {m4},
			},
			qname: "critical",
			want: []*TaskInfo{
				createPendingTask(m3),
			},
		},
		{
			desc: "with empty queue",
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			qname: "default",
			want:  []*TaskInfo(nil),
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		for q, msgs := range tc.pending {
			ctx.SeedPendingQueue(msgs, q)
		}

		got, err := inspector.ListPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListPendingTasks(%q) returned error: %v",
				tc.desc, tc.qname, err)
			continue
		}
		cmpOpts := []cmp.Option{
			cmpopts.EquateApproxTime(2 * time.Second),
			cmp.AllowUnexported(TaskInfo{}),
		}
		if diff := cmp.Diff(tc.want, got, cmpOpts...); diff != "" {
			t.Errorf("%s; ListPendingTasks(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListActiveTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc   string
		active map[string][]*base.TaskMessage
		qname  string
		want   []*TaskInfo
	}{
		{
			desc: "with a few active tasks",
			active: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			qname: "default",
			want: []*TaskInfo{
				newTaskInfo(m1, base.TaskStateActive, time.Time{}, nil),
				newTaskInfo(m2, base.TaskStateActive, time.Time{}, nil),
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllActiveQueues(tc.active)

		got, err := inspector.ListActiveTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListActiveTasks(%q) returned error: %v", tc.qname, tc.desc, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
			t.Errorf("%s; ListActiveTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createScheduledTask(z base.Z) *TaskInfo {
	return newTaskInfo(
		z.Message,
		base.TaskStateScheduled,
		time.Unix(z.Score, 0),
		nil,
	)
}

func TestInspectorListScheduledTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc      string
		scheduled map[string][]base.Z
		qname     string
		want      []*TaskInfo
	}{
		{
			desc: "with a few scheduled tasks",
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*TaskInfo{
				createScheduledTask(z3),
				createScheduledTask(z1),
				createScheduledTask(z2),
			},
		},
		{
			desc: "with empty scheduled queue",
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*TaskInfo(nil),
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)

		got, err := inspector.ListScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListScheduledTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
			t.Errorf("%s; ListScheduledTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createRetryTask(z base.Z) *TaskInfo {
	return newTaskInfo(
		z.Message,
		base.TaskStateRetry,
		time.Unix(z.Score, 0),
		nil,
	)
}

func TestInspectorListRetryTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc  string
		retry map[string][]base.Z
		qname string
		want  []*TaskInfo
	}{
		{
			desc: "with a few retry tasks",
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*TaskInfo{
				createRetryTask(z3),
				createRetryTask(z1),
				createRetryTask(z2),
			},
		},
		{
			desc: "with empty retry queue",
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*TaskInfo(nil),
		},
		// TODO(hibiken): ErrQueueNotFound when queue doesn't exist
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)

		got, err := inspector.ListRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListRetryTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
			t.Errorf("%s; ListRetryTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createArchivedTask(z base.Z) *TaskInfo {
	return newTaskInfo(
		z.Message,
		base.TaskStateArchived,
		time.Time{}, // zero value for n/a
		nil,
	)
}

func TestInspectorListArchivedTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc     string
		archived map[string][]base.Z
		qname    string
		want     []*TaskInfo
	}{
		{
			desc: "with a few archived tasks",
			archived: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by LastFailedAt.
			want: []*TaskInfo{
				createArchivedTask(z2),
				createArchivedTask(z1),
				createArchivedTask(z3),
			},
		},
		{
			desc: "with empty archived queue",
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*TaskInfo(nil),
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)

		got, err := inspector.ListArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListArchivedTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
			t.Errorf("%s; ListArchivedTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func newCompletedTaskMessage(typename, qname string, retention time.Duration, completedAt time.Time) *base.TaskMessage {
	msg := h.NewTaskMessageWithQueue(typename, nil, qname)
	msg.Retention = int64(retention.Seconds())
	msg.CompletedAt = completedAt.Unix()
	return msg
}

func createCompletedTask(z base.Z) *TaskInfo {
	return newTaskInfo(
		z.Message,
		base.TaskStateCompleted,
		time.Time{}, // zero value for n/a
		nil,         // TODO: Test with result data
	)
}

func TestInspectorListCompletedTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	now := time.Now()

	//      completed   now     retention
	//      m3  m2  m1   |      m2  m1    m3
	// -----|---|---|    +   ---|---|-----|

	m1 := newCompletedTaskMessage("task1", "default", 1*time.Hour, now.Add(-3*time.Minute))     // Expires in 57 mins
	m2 := newCompletedTaskMessage("task2", "default", 30*time.Minute, now.Add(-10*time.Minute)) // Expires in 20 mins
	m3 := newCompletedTaskMessage("task3", "default", 2*time.Hour, now.Add(-30*time.Minute))    // Expires in 90 mins
	m4 := newCompletedTaskMessage("task4", "custom", 15*time.Minute, now.Add(-2*time.Minute))   // Expires in 13 mins
	z1 := base.Z{Message: m1, Score: m1.CompletedAt + m1.Retention}
	z2 := base.Z{Message: m2, Score: m2.CompletedAt + m2.Retention}
	z3 := base.Z{Message: m3, Score: m3.CompletedAt + m3.Retention}
	z4 := base.Z{Message: m4, Score: m4.CompletedAt + m4.Retention}

	inspector := NewInspector(getClientConnOpt(t))
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		desc      string
		completed map[string][]base.Z
		qname     string
		want      []*TaskInfo
	}{
		{
			desc: "with a few completed tasks",
			completed: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by expiration time (CompletedAt + Retention).
			want: []*TaskInfo{
				createCompletedTask(z2),
				createCompletedTask(z1),
				createCompletedTask(z3),
			},
		},
		{
			desc: "with empty completed queue",
			completed: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*TaskInfo(nil),
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllCompletedQueues(tc.completed)

		got, err := inspector.ListCompletedTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListCompletedTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
			t.Errorf("%s; ListCompletedTasks(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListPagination(t *testing.T) {
	// Create 100 tasks.
	var msgs []*base.TaskMessage
	for i := 0; i <= 99; i++ {
		msgs = append(msgs,
			h.NewTaskMessage(fmt.Sprintf("task%d", i), nil))
	}
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	ctx.SeedPendingQueue(msgs, base.DefaultQueueName)
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		page     int
		pageSize int
		want     []*TaskInfo
	}{
		{
			page:     1,
			pageSize: 5,
			want: []*TaskInfo{
				createPendingTask(msgs[0]),
				createPendingTask(msgs[1]),
				createPendingTask(msgs[2]),
				createPendingTask(msgs[3]),
				createPendingTask(msgs[4]),
			},
		},
		{
			page:     3,
			pageSize: 10,
			want: []*TaskInfo{
				createPendingTask(msgs[20]),
				createPendingTask(msgs[21]),
				createPendingTask(msgs[22]),
				createPendingTask(msgs[23]),
				createPendingTask(msgs[24]),
				createPendingTask(msgs[25]),
				createPendingTask(msgs[26]),
				createPendingTask(msgs[27]),
				createPendingTask(msgs[28]),
				createPendingTask(msgs[29]),
			},
		},
	}

	for _, tc := range tests {
		got, err := inspector.ListPendingTasks("default", Page(tc.page), PageSize(tc.pageSize))
		if err != nil {
			t.Errorf("ListPendingTask('default') returned error: %v", err)
			continue
		}
		cmpOpts := []cmp.Option{
			cmpopts.EquateApproxTime(2 * time.Second),
			cmp.AllowUnexported(TaskInfo{}),
		}
		if diff := cmp.Diff(tc.want, got, cmpOpts...); diff != "" {
			t.Errorf("ListPendingTask('default') = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestInspectorListTasksQueueNotFoundError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		qname   string
		wantErr error
	}{
		{
			qname:   "nonexistent",
			wantErr: ErrQueueNotFound,
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()

		if _, err := inspector.ListActiveTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListActiveTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
		if _, err := inspector.ListPendingTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListPendingTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
		if _, err := inspector.ListScheduledTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListScheduledTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
		if _, err := inspector.ListRetryTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListRetryTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
		if _, err := inspector.ListArchivedTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListArchivedTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
		if _, err := inspector.ListCompletedTasks(tc.qname); !errors.Is(err, tc.wantErr) {
			t.Errorf("ListCompletedTasks(%q) returned error %v, want %v", tc.qname, err, tc.wantErr)
		}
	}
}

func TestInspectorDeleteAllPendingTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m4},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			qname: "custom",
			want:  1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)

		got, err := inspector.DeleteAllPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllPendingTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllPendingTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllScheduledTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)

		got, err := inspector.DeleteAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllRetryTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		want      int
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)

		got, err := inspector.DeleteAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllArchivedTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		want         int
		wantArchived map[string][]base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)

		got, err := inspector.DeleteAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllArchivedTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllArchivedTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantArchived {
			gotArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllCompletedTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	now := time.Now()

	m1 := newCompletedTaskMessage("task1", "default", 30*time.Minute, now.Add(-2*time.Minute))
	m2 := newCompletedTaskMessage("task2", "default", 30*time.Minute, now.Add(-5*time.Minute))
	m3 := newCompletedTaskMessage("task3", "default", 30*time.Minute, now.Add(-10*time.Minute))
	m4 := newCompletedTaskMessage("task4", "custom", 30*time.Minute, now.Add(-3*time.Minute))
	z1 := base.Z{Message: m1, Score: m1.CompletedAt + m1.Retention}
	z2 := base.Z{Message: m2, Score: m2.CompletedAt + m2.Retention}
	z3 := base.Z{Message: m3, Score: m3.CompletedAt + m3.Retention}
	z4 := base.Z{Message: m4, Score: m4.CompletedAt + m4.Retention}

	inspector := NewInspector(getClientConnOpt(t))
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		completed     map[string][]base.Z
		qname         string
		want          int
		wantCompleted map[string][]base.Z
	}{
		{
			completed: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantCompleted: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			completed: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantCompleted: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllCompletedQueues(tc.completed)

		got, err := inspector.DeleteAllCompletedTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllCompletedTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllCompletedTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantCompleted {
			gotCompleted := ctx.GetCompletedEntries(qname)
			if diff := cmp.Diff(want, gotCompleted, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected completed tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllPendingTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		want         int
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m3},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z1,
					z2,
					base.Z{Message: m3, Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllArchivedQueues(tc.archived)

		got, err := inspector.ArchiveAllPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllPendingTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllPendingTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllScheduledTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
			},
			archived: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllArchivedQueues(tc.archived)

		got, err := inspector.ArchiveAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllRetryTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		want         int
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
			},
			archived: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		got, err := inspector.ArchiveAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		cmpOpt := h.EquateInt64Approx(2) // allow for 2 seconds difference in Z.Score
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt, cmpOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllScheduledTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task4", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllPendingQueues(tc.pending)

		got, err := inspector.RunAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllRetryTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllPendingQueues(tc.pending)

		got, err := inspector.RunAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllArchivedTasks(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		want         int
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantArchived: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
			},
			qname: "default",
			want:  1,
			wantArchived: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantArchived: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)
		ctx.SeedAllPendingQueues(tc.pending)

		got, err := inspector.RunAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllArchivedTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllArchivedTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskDeletesPendingTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		id          string
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "default",
			id:    createPendingTask(m2).ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m3},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "custom",
			id:    createPendingTask(m3).ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)

		if err := inspector.DeleteTask(tc.qname, tc.id); err != nil {
			t.Errorf("DeleteTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}

		for qname, want := range tc.wantPending {
			got := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, got, h.SortMsgOpt); diff != "" {
				t.Errorf("unspected pending tasks in queue %q: (-want,+got):\n%s",
					qname, diff)
				continue
			}
		}
	}
}

func TestInspectorDeleteTaskDeletesScheduledTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		id            string
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			id:    createScheduledTask(z2).ID,
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)

		if err := inspector.DeleteTask(tc.qname, tc.id); err != nil {
			t.Errorf("DeleteTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
	}
}

func TestInspectorDeleteTaskDeletesRetryTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		id        string
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			id:    createRetryTask(z2).ID,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)

		if err := inspector.DeleteTask(tc.qname, tc.id); err != nil {
			t.Errorf("DeleteTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskDeletesArchivedTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		id           string
		wantArchived map[string][]base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			id:    createArchivedTask(z2).ID,
			wantArchived: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.DeleteTask(tc.qname, tc.id); err != nil {
			t.Errorf("DeleteTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		id           string
		wantErr      error
		wantArchived map[string][]base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname:   "nonexistent",
			id:      createArchivedTask(z2).ID,
			wantErr: ErrQueueNotFound,
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname:   "default",
			id:      uuid.NewString(),
			wantErr: ErrTaskNotFound,
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.DeleteTask(tc.qname, tc.id); !errors.Is(err, tc.wantErr) {
			t.Errorf("DeleteTask(%q, %q) = %v, want %v", tc.qname, tc.id, err, tc.wantErr)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsScheduledTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		id            string
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			id:    createScheduledTask(z2).ID,
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllPendingQueues(tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsRetryTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		id          string
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    createRetryTask(z2).ID,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllPendingQueues(tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTaskBy(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsArchivedTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		id           string
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "critical",
			id:    createArchivedTask(z2).ID,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)
		ctx.SeedAllPendingQueues(tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		id           string
		wantErr      error
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname:   "nonexistent",
			id:      createArchivedTask(z2).ID,
			wantErr: ErrQueueNotFound,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname:   "default",
			id:      uuid.NewString(),
			wantErr: ErrTaskNotFound,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllArchivedQueues(tc.archived)
		ctx.SeedAllPendingQueues(tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); !errors.Is(err, tc.wantErr) {
			t.Errorf("RunTask(%q, %q) = %v, want %v", tc.qname, tc.id, err, tc.wantErr)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskArchivesPendingTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	now := time.Now()
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		id           string
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m2, m3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			id:    createPendingTask(m1).ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2, m3},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m2, m3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    createPendingTask(m2).ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: m2, Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllPendingQueues(tc.pending)
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.ArchiveTask(tc.qname, tc.id); err != nil {
			t.Errorf("ArchiveTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want,+got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want,+got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskArchivesScheduledTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	now := time.Now()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		id            string
		want          string
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    createScheduledTask(z2).ID,
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{
						Message: m2,
						Score:   now.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllScheduledQueues(tc.scheduled)
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.ArchiveTask(tc.qname, tc.id); err != nil {
			t.Errorf("ArchiveTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledEntries(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskArchivesRetryTask(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	now := time.Now()

	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	inspector.rdb.SetClock(timeutil.NewSimulatedClock(now))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		id           string
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    createRetryTask(z2).ID,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{
						Message: m2,
						Score:   now.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.ArchiveTask(tc.qname, tc.id); err != nil {
			t.Errorf("ArchiveTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		cmpOpts := []cmp.Option{
			h.SortZSetEntryOpt,
			cmpopts.EquateApproxTime(2 * time.Second),
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, cmpOpts...); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskError(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		id           string
		wantErr      error
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname:   "nonexistent",
			id:      createRetryTask(z2).ID,
			wantErr: ErrQueueNotFound,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname:   "custom",
			id:      uuid.NewString(),
			wantErr: ErrTaskNotFound,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllRetryQueues(tc.retry)
		ctx.SeedAllArchivedQueues(tc.archived)

		if err := inspector.ArchiveTask(tc.qname, tc.id); !errors.Is(err, tc.wantErr) {
			t.Errorf("ArchiveTask(%q, %q) = %v, want %v", tc.qname, tc.id, err, tc.wantErr)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryEntries(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			wantArchived := ctx.GetArchivedEntries(qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

var sortSchedulerEntry = cmp.Transformer("SortSchedulerEntry", func(in []*SchedulerEntry) []*SchedulerEntry {
	out := append([]*SchedulerEntry(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Spec < out[j].Spec
	})
	return out
})

func TestInspectorSchedulerEntries(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	rdbClient := NewClientWithBroker(inspector.rdb)

	now := time.Now().UTC()
	schedulerID := "127.0.0.1:9876:abc123"

	tests := []struct {
		data []*base.SchedulerEntry // data to seed redis
		want []*SchedulerEntry
	}{
		{
			data: []*base.SchedulerEntry{
				{
					Spec:    "* * * * *",
					Type:    "foo",
					Payload: nil,
					Opts:    nil,
					Next:    now.Add(5 * time.Hour),
					Prev:    now.Add(-2 * time.Hour),
				},
				{
					Spec:    "@every 20m",
					Type:    "bar",
					Payload: h.JSON(map[string]interface{}{"fiz": "baz"}),
					Opts:    []string{`Queue("bar")`, `MaxRetry(20)`},
					Next:    now.Add(1 * time.Minute),
					Prev:    now.Add(-19 * time.Minute),
				},
			},
			want: []*SchedulerEntry{
				{
					Spec: "* * * * *",
					Task: NewTask("foo", nil),
					Opts: nil,
					Next: now.Add(5 * time.Hour),
					Prev: now.Add(-2 * time.Hour),
				},
				{
					Spec: "@every 20m",
					Task: NewTask("bar", h.JSON(map[string]interface{}{"fiz": "baz"})),
					Opts: []Option{Queue("bar"), MaxRetry(20)},
					Next: now.Add(1 * time.Minute),
					Prev: now.Add(-19 * time.Minute),
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		scheduler, ok := rdbClient.rdb.(base.Scheduler)
		require.True(t, ok)
		err := scheduler.WriteSchedulerEntries(schedulerID, tc.data, time.Minute)
		if err != nil {
			t.Fatalf("could not write data: %v", err)
		}
		got, err := inspector.SchedulerEntries()
		if err != nil {
			t.Errorf("SchedulerEntries() returned error: %v", err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Task{})
		if diff := cmp.Diff(tc.want, got, sortSchedulerEntry, ignoreOpt); diff != "" {
			t.Errorf("SchedulerEntries() = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestInspectorCancelProcessing(t *testing.T) {
	lfa := time.Now().Truncate(time.Second)

	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)

	am1 := &base.TaskMessage{}
	*am1 = *m1
	am1.ErrorMsg = context.Canceled.Error()
	am1.LastFailedAt = lfa.Unix()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()
	inspector := NewInspector(getClientConnOpt(t))
	defer func() { _ = inspector.Close() }()

	tests := []struct {
		pending map[string][]*base.TaskMessage
		id      string
		want    []*TaskInfo
		want2   []*TaskInfo
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			id: m1.ID,
			want: []*TaskInfo{
				newTaskInfo(m2, base.TaskStateActive, time.Time{}, nil),
			},
			want2: []*TaskInfo{
				newTaskInfo(am1, base.TaskStateArchived, time.Time{}, nil),
			},
		},
	}

	for _, tc := range tests {
		func() {
			ctx.FlushDB()
			ctx.SeedAllPendingQueues(tc.pending)

			server := newServer(client.rdb, Config{
				Concurrency: 10,
				RetryDelayFunc: func(n int, err error, t *Task) time.Duration {
					return time.Second
				},
				LogLevel:            testLogLevel,
				HeartBeaterInterval: time.Millisecond * 5,
			})
			defer server.Shutdown()

			done := make(chan bool, 1)
			var wg sync.WaitGroup
			for _, tasks := range tc.pending {
				for _ = range tasks {
					wg.Add(1)
				}
			}
			handler := func(ctx context.Context, _ *Task) error {
				defer wg.Done()
				select {
				case <-ctx.Done():
				case <-done:
					done <- true
				}
				return nil
			}
			_ = server.Start(HandlerFunc(handler))
			defer server.Stop()

			time.Sleep(time.Second)

			err := inspector.CancelProcessing(tc.id)
			if err != nil {
				t.Errorf("CancelProcessing(%q) returned error: %v", tc.id, err)
				return
			}

			time.Sleep(time.Second)

			got, err := inspector.ListActiveTasks("default")
			if err != nil {
				t.Errorf("ListActiveTasks() returned error: %v", err)
				return
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
				t.Errorf("ListActiveTasks() = %v, want %v; (-want,+got)\n%s",
					got, tc.want, diff)
				return
			}

			got, err = inspector.ListArchivedTasks("default")
			if err != nil {
				t.Errorf("ListArchivedTasks() returned error: %v", err)
				return
			}
			for _, t := range got {
				if !t.LastFailedAt.IsZero() {
					// Replace LastFailedAt for the sake of diff below
					t.LastFailedAt = lfa
				}
			}
			if diff := cmp.Diff(tc.want2, got, cmp.AllowUnexported(TaskInfo{})); diff != "" {
				t.Errorf("ListArchivedTasks() = %v, want %v; (-want,+got)\n%s",
					got, tc.want2, diff)
				return
			}

			done <- true
			wg.Wait()
		}()
	}
}

func TestParseOption(t *testing.T) {
	oneHourFromNow := time.Now().Add(1 * time.Hour)
	tests := []struct {
		s        string
		wantType OptionType
		wantVal  interface{}
	}{
		{`MaxRetry(10)`, MaxRetryOpt, 10},
		{`Queue("email")`, QueueOpt, "email"},
		{`Timeout(3m)`, TimeoutOpt, 3 * time.Minute},
		{Deadline(oneHourFromNow).String(), DeadlineOpt, oneHourFromNow},
		{`Unique(1h)`, UniqueOpt, 1 * time.Hour},
		{ProcessAt(oneHourFromNow).String(), ProcessAtOpt, oneHourFromNow},
		{`ProcessIn(10m)`, ProcessInOpt, 10 * time.Minute},
		{`Retention(24h)`, RetentionOpt, 24 * time.Hour},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := parseOption(tc.s)
			if err != nil {
				t.Fatalf("returned error: %v", err)
			}
			if got == nil {
				t.Fatal("returned nil")
			}
			if got.Type() != tc.wantType {
				t.Fatalf("got type %v, want type %v ", got.Type(), tc.wantType)
			}
			switch tc.wantType {
			case QueueOpt:
				gotVal, ok := got.Value().(string)
				if !ok {
					t.Fatal("returned Option with non-string value")
				}
				if gotVal != tc.wantVal.(string) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case MaxRetryOpt:
				gotVal, ok := got.Value().(int)
				if !ok {
					t.Fatal("returned Option with non-int value")
				}
				if gotVal != tc.wantVal.(int) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case TimeoutOpt, UniqueOpt, ProcessInOpt, RetentionOpt:
				gotVal, ok := got.Value().(time.Duration)
				if !ok {
					t.Fatal("returned Option with non duration value")
				}
				if gotVal != tc.wantVal.(time.Duration) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case DeadlineOpt, ProcessAtOpt:
				gotVal, ok := got.Value().(time.Time)
				if !ok {
					t.Fatal("returned Option with non time value")
				}
				if cmp.Equal(gotVal, tc.wantVal.(time.Time)) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			default:
				t.Fatalf("returned Option with unexpected type: %v", got.Type())
			}
		})
	}
}

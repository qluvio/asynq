// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/timeutil"
)

func TestForwarder(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()

	const pollInterval = time.Second
	s := newForwarder(forwarderParams{
		logger:   testLogger,
		broker:   client.rdb,
		queues:   newQueues(NewQueuesConfig(map[string]int{"default": 1, "critical": 1}), 100),
		interval: pollInterval,
	})
	t1 := h.NewTaskMessageWithQueue("gen_thumbnail", nil, "default")
	t2 := h.NewTaskMessageWithQueue("send_email", nil, "critical")
	t3 := h.NewTaskMessageWithQueue("reindex", nil, "default")
	t4 := h.NewTaskMessageWithQueue("sync", nil, "critical")
	now := time.Now()

	tests := []struct {
		initScheduled map[string][]base.Z            // scheduled queue initial state
		initRetry     map[string][]base.Z            // retry queue initial state
		initPending   map[string][]*base.TaskMessage // default queue initial state
		wait          time.Duration                  // wait duration before checking for final state
		wantScheduled map[string][]*base.TaskMessage // schedule queue final state
		wantRetry     map[string][]*base.TaskMessage // retry queue final state
		wantPending   map[string][]*base.TaskMessage // default queue final state
	}{
		{
			initScheduled: map[string][]base.Z{
				"default":  {{Message: t1, Score: now.Add(time.Hour).Unix()}},
				"critical": {{Message: t2, Score: now.Add(-2 * time.Second).Unix()}},
			},
			initRetry: map[string][]base.Z{
				"default":  {{Message: t3, Score: time.Now().Add(-500 * time.Millisecond).Unix()}},
				"critical": {},
			},
			initPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t4},
			},
			wait: pollInterval * 2,
			wantScheduled: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t3},
				"critical": {t2, t4},
			},
		},
		{
			initScheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: now.Unix()},
					{Message: t3, Score: now.Add(-500 * time.Millisecond).Unix()},
				},
				"critical": {
					{Message: t2, Score: now.Add(-2 * time.Second).Unix()},
				},
			},
			initRetry: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			initPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t4},
			},
			wait: pollInterval * 2,
			wantScheduled: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1, t3},
				"critical": {t2, t4},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()                                // clean up db before each test case.
		ctx.SeedAllScheduledQueues(tc.initScheduled) // initialize scheduled queue
		ctx.SeedAllRetryQueues(tc.initRetry)         // initialize retry queue
		ctx.SeedAllPendingQueues(tc.initPending)     // initialize default queue

		var wg sync.WaitGroup
		s.start(&wg)
		time.Sleep(tc.wait)
		s.shutdown()

		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledMessages(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q after running forwarder: (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}

		for qname, want := range tc.wantRetry {
			gotRetry := ctx.GetRetryMessages(qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q after running forwarder: (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q after running forwarder: (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
	}
}

func TestForwarderWake(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	client := NewClient(getClientConnOpt(t))
	defer func() { _ = client.Close() }()
	now := time.Now()
	client.rdb.SetClock(timeutil.NewSimulatedClock(now))

	deadlines := base.NewDeadlines(10)
	defer deadlines.Close()

	wakePrcCh := make(chan bool, 1)
	wakeFwdCh := make(chan bool, 1)
	broker := newWakingBroker(client.rdb, deadlines, wakePrcCh, wakeFwdCh)

	const pollInterval = time.Hour
	s := newForwarder(forwarderParams{
		logger:    testLogger,
		broker:    broker,
		queues:    newQueues(NewQueuesConfig(map[string]int{"default": 1, "critical": 1}), 100),
		interval:  pollInterval,
		wakeCh:    wakeFwdCh,
		wakePrcCh: wakePrcCh,
	})

	tasks := []struct {
		msg *base.TaskMessage
		pat time.Time
	}{
		{
			msg: h.NewTaskMessageWithQueue("gen_thumbnail", nil, "default"),
			pat: now.Add(time.Hour),
		},
		{
			msg: h.NewTaskMessageWithQueue("send_email", nil, "critical"),
			pat: now.Add(-2 * time.Second),
		},
		{
			msg: h.NewTaskMessageWithQueue("reindex", nil, "default"),
			pat: now.Add(-500 * time.Millisecond),
		},
		{
			msg: h.NewTaskMessageWithQueue("sync", nil, "critical"),
		},
	}

	tests := []struct {
		initScheduled []int                          // scheduled queue initial state
		initPending   []int                          // default queue initial state
		wantScheduled map[string][]*base.TaskMessage // schedule queue final state
		wantPending   map[string][]*base.TaskMessage // default queue final state
	}{
		{
			initScheduled: []int{0, 1, 2},
			initPending:   []int{3},
			wantScheduled: map[string][]*base.TaskMessage{
				"default":  {tasks[0].msg},
				"critical": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {tasks[2].msg},
				"critical": {tasks[1].msg, tasks[3].msg},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB() // clean up db before each test case.

		var wg sync.WaitGroup
		s.start(&wg)
		time.Sleep(time.Second) // wait for forwarder to start

		// initialize scheduled queue
		for _, i := range tc.initScheduled {
			task := tasks[i]
			err := broker.Schedule(context.Background(), task.msg, task.pat)
			if err != nil {
				s.shutdown()
				t.Fatal(err)
			}
		}

		// initialize default queue
		for _, i := range tc.initPending {
			task := tasks[i]
			err := broker.Enqueue(context.Background(), task.msg)
			if err != nil {
				s.shutdown()
				t.Fatal(err)
			}
		}

		s.shutdown()

		for qname, want := range tc.wantScheduled {
			gotScheduled := ctx.GetScheduledMessages(qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q after running forwarder: (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}

		for qname, want := range tc.wantPending {
			gotPending := ctx.GetPendingMessages(qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q after running forwarder: (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
	}
}

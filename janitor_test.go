// Copyright 2021 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/require"
)

func newCompletedTask(qname, tasktype string, payload []byte, completedAt time.Time) *base.TaskMessage {
	msg := h.NewTaskMessageWithQueue(tasktype, payload, qname)
	msg.CompletedAt = completedAt.Unix()
	return msg
}

func TestJanitor(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	broker, err := makeBroker(getClientConnOpt(t))
	require.NoError(t, err)
	defer func() { _ = broker.Close() }()

	const interval = 1 * time.Second
	janitor := newJanitor(janitorParams{
		logger:   testLogger,
		broker:   broker,
		queues:   newQueues(NewQueuesConfig(map[string]int{"default": 1, "custom": 1}), 100),
		interval: interval,
	})

	now := time.Now()
	hourAgo := now.Add(-1 * time.Hour)
	minuteAgo := now.Add(-1 * time.Minute)
	halfHourAgo := now.Add(-30 * time.Minute)
	halfHourFromNow := now.Add(30 * time.Minute)
	fiveMinFromNow := now.Add(5 * time.Minute)
	msg1 := newCompletedTask("default", "task1", nil, hourAgo)
	msg2 := newCompletedTask("default", "task2", nil, minuteAgo)
	msg3 := newCompletedTask("custom", "task3", nil, hourAgo)
	msg4 := newCompletedTask("custom", "task4", nil, minuteAgo)

	tests := []struct {
		completed     map[string][]base.Z // initial completed sets
		wantCompleted map[string][]base.Z // expected completed sets after janitor runs
	}{
		{
			completed: map[string][]base.Z{
				"default": {
					{Message: msg1, Score: halfHourAgo.Unix()},
					{Message: msg2, Score: fiveMinFromNow.Unix()},
				},
				"custom": {
					{Message: msg3, Score: halfHourFromNow.Unix()},
					{Message: msg4, Score: minuteAgo.Unix()},
				},
			},
			wantCompleted: map[string][]base.Z{
				"default": {
					{Message: msg2, Score: fiveMinFromNow.Unix()},
				},
				"custom": {
					{Message: msg3, Score: halfHourFromNow.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		ctx.FlushDB()
		ctx.SeedAllCompletedQueues(tc.completed)

		var wg sync.WaitGroup
		janitor.start(&wg)
		time.Sleep(2 * interval) // make sure to let janitor run at least one time
		janitor.shutdown()

		for qname, want := range tc.wantCompleted {
			got := ctx.GetCompletedEntries(qname)
			if diff := cmp.Diff(want, got, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("diff found in %q after running janitor: (-want, +got)\n%s", base.CompletedKey(qname), diff)
			}
		}
	}
}

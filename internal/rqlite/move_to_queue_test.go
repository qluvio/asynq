package rqlite

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMoveActiveToQueue verifies moving an active task to a new queue.
func TestMoveActiveToQueue(t *testing.T) {
	const (
		customID    = "customID"
		queue       = "default"
		targetQueue = "new_queue"
	)

	r := setup(t)
	defer func() { _ = r.Close() }()

	cloneWithQueue := func(t *base.TaskMessage, q string) *base.TaskMessage {
		ret := *t
		ret.Queue = q
		return &ret
	}

	makeMessage := func() *base.TaskMessage {
		return &base.TaskMessage{
			ID:      customID,
			Type:    "send_email",
			Queue:   queue,
			Timeout: 3600,
		}
	}

	tests := []struct {
		desc              string
		uniqueKeyTTL      int64
		movedUniqueKeyTTL int64
		movedProcessIn    int
		wantState         base.TaskState
	}{
		{
			desc:              "no unique key - process immediately",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "no unique key - process immediately - trying to set a unique TTL has no effect",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - setting a unique TTL of zero cancels it",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - decrease the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 1,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - increase the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "no unique key - schedule",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "no unique key - schedule - trying to set a unique TTL has no effect",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - setting a unique TTL of zero cancels it",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - decrease the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 1,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - increase the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
	}
	now := r.Now()

	for _, tc := range tests {
		//fmt.Println("running case", tc.desc)
		FlushDB(t, r.conn)
		msg := makeMessage()
		if tc.uniqueKeyTTL > 0 {
			msg.UniqueKey = "unique_key"
			msg.UniqueKeyTTL = tc.uniqueKeyTTL
		}

		msgs := map[string][]*base.TaskMessage{queue: {msg}}
		SeedAllPendingQueues(t, r, msgs)
		for _, id := range []string{customID} {
			deq, err := r.Dequeue("", queue)
			require.NoError(t, err, tc.desc)
			require.Equal(t, id, deq.Message.ID, tc.desc)
		}

		ctx := context.Background()
		msgMoved := cloneWithQueue(msg, targetQueue)
		msgMoved.UniqueKeyTTL = tc.movedUniqueKeyTTL

		processAt := now.Add(time.Duration(tc.movedProcessIn) * time.Second)
		_, err := r.MoveToQueue(ctx, queue, msgMoved, processAt, true)
		require.NoError(t, err, tc.desc)

		ti, err := r.GetTaskInfo(msgMoved.Queue, msgMoved.ID)
		require.NoError(t, err, tc.desc)
		require.Equal(t, tc.wantState, ti.State, tc.desc)

		row, err := r.conn.getTask(msgMoved.Queue, msgMoved.ID)
		require.NoError(t, err)
		ttl := time.Duration(0)
		if row.uniqueKeyDeadline > 0 {
			ttl = time.Unix(row.uniqueKeyDeadline, 0).Sub(r.Now())
		}

		expectedTTL := time.Duration(0)
		if tc.uniqueKeyTTL > 0 {
			expectedTTL = time.Duration(tc.movedUniqueKeyTTL) * time.Second
		}
		assert.Equal(t, expectedTTL, ttl, tc.desc)
	}
}

// TestMoveCompletedToQueue verifies moving a completed task to a new queue.
func TestMoveCompletedToQueue(t *testing.T) {
	const (
		customID    = "customID"
		queue       = "default"
		targetQueue = "new_queue"
	)

	r := setup(t)
	defer func() { _ = r.Close() }()

	cloneWithQueue := func(t *base.TaskMessage, q string) *base.TaskMessage {
		ret := *t
		ret.Queue = q
		return &ret
	}

	makeMessage := func() *base.TaskMessage {
		return &base.TaskMessage{
			ID:      customID,
			Type:    "send_email",
			Queue:   queue,
			Timeout: 3600,
		}
	}

	tests := []struct {
		desc              string
		uniqueKeyTTL      int64
		movedUniqueKeyTTL int64
		movedProcessIn    int
		wantState         base.TaskState
	}{
		{
			desc:              "no unique key - process immediately",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "no unique key - process immediately - trying to set a unique TTL has no effect",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - setting a unique TTL of zero cancels it",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - decrease the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 1,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "unique key - process immediately - increase the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    0,
			wantState:         base.TaskStatePending,
		},
		{
			desc:              "no unique key - schedule",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "no unique key - schedule - trying to set a unique TTL has no effect",
			uniqueKeyTTL:      0,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - setting a unique TTL of zero cancels it",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 0,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - decrease the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 1,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
		{
			desc:              "unique key - schedule - increase the unique TTL",
			uniqueKeyTTL:      10,
			movedUniqueKeyTTL: 20,
			movedProcessIn:    60,
			wantState:         base.TaskStateScheduled,
		},
	}

	for _, tc := range tests {
		//fmt.Println("running case", tc.desc)
		FlushDB(t, r.conn)
		msg := makeMessage()
		if tc.uniqueKeyTTL > 0 {
			msg.UniqueKey = "unique_key"
			msg.UniqueKeyTTL = tc.uniqueKeyTTL
		}

		msgs := map[string][]*base.TaskMessage{queue: {msg}}
		SeedAllPendingQueues(t, r, msgs)
		for _, id := range []string{customID} {
			deq, err := r.Dequeue("", queue)
			require.NoError(t, err, tc.desc)
			require.Equal(t, id, deq.Message.ID, tc.desc)
			err = r.MarkAsComplete("", deq.Message)
			require.NoError(t, err, tc.desc)
		}

		ctx := context.Background()
		msgMoved := cloneWithQueue(msg, targetQueue)
		msgMoved.UniqueKeyTTL = tc.movedUniqueKeyTTL

		processAt := r.clock.Now().Add(time.Duration(tc.movedProcessIn) * time.Second)
		_, err := r.MoveToQueue(ctx, queue, msgMoved, processAt, false)
		require.NoError(t, err, tc.desc)

		ti, err := r.GetTaskInfo(msgMoved.Queue, msgMoved.ID)
		require.NoError(t, err, tc.desc)
		require.Equal(t, tc.wantState, ti.State, tc.desc)

		row, err := r.conn.getTask(msgMoved.Queue, msgMoved.ID)
		require.NoError(t, err)
		ttl := time.Duration(0)
		if row.uniqueKeyDeadline > 0 {
			ttl = time.Unix(row.uniqueKeyDeadline, 0).Sub(r.Now())
		}

		expectedTTL := time.Duration(0)
		if tc.uniqueKeyTTL > 0 {
			expectedTTL = time.Duration(tc.movedUniqueKeyTTL) * time.Second
		}
		assert.Equal(t, expectedTTL, ttl, tc.desc)
	}
}

// TestMoveToQueueErrors verifies that an error is returned when moveToQueue is
// called for a task that is not in the expected state.
func TestMoveToQueueErrors(t *testing.T) {
	const (
		customID1   = "customID1"
		customID2   = "customID2"
		queue       = "default"
		targetQueue = "new_queue"
	)

	r := setup(t)
	defer func() { _ = r.Close() }()

	cloneWithQueue := func(t *base.TaskMessage, q string) *base.TaskMessage {
		ret := *t
		ret.Queue = q
		return &ret
	}

	t1 := &base.TaskMessage{
		ID:      customID1,
		Type:    "send_email",
		Queue:   queue,
		Timeout: 3600,
	}
	t1Moved := cloneWithQueue(t1, targetQueue)

	t2 := &base.TaskMessage{
		ID:      customID2,
		Type:    "send_email",
		Queue:   queue,
		Timeout: 3600,
	}
	t2Moved := cloneWithQueue(t2, targetQueue)

	tests := []struct {
		state            base.TaskState
		activeWantErr    bool
		completedWantErr bool
	}{
		{state: 0, activeWantErr: true, completedWantErr: true},
		{state: base.TaskStatePending, activeWantErr: true, completedWantErr: true},
		{state: base.TaskStateRetry, activeWantErr: true, completedWantErr: true},
		{state: base.TaskStateActive, activeWantErr: false, completedWantErr: true},
		{state: base.TaskStateCompleted, activeWantErr: true, completedWantErr: false},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		msgs := map[string][]*base.TaskMessage{queue: {t1, t2}}

		switch tc.state {
		case 0:
		case base.TaskStatePending:
			SeedAllPendingQueues(t, r, msgs)
		case base.TaskStateActive:
			SeedAllPendingQueues(t, r, msgs)
			for _, id := range []string{customID1, customID2} {
				deq, err := r.Dequeue("", queue)
				require.NoError(t, err)
				require.Equal(t, id, deq.Message.ID)
			}
		case base.TaskStateRetry:
			SeedAllPendingQueues(t, r, msgs)
			for _, id := range []string{customID1, customID2} {
				deq, err := r.Dequeue("", queue)
				require.NoError(t, err)
				require.Equal(t, id, deq.Message.ID)
				err = r.Retry(deq.Message, r.Now(), "", true)
				require.NoError(t, err)
			}
		case base.TaskStateCompleted:
			SeedAllPendingQueues(t, r, msgs)
			for _, id := range []string{customID1, customID2} {
				deq, err := r.Dequeue("", queue)
				require.NoError(t, err)
				require.Equal(t, id, deq.Message.ID)
				err = r.MarkAsComplete("", deq.Message)
				require.NoError(t, err)
			}
		}

		ctx := context.Background()
		_, err := r.MoveToQueue(ctx, queue, t1Moved, time.Time{}, true)
		if tc.activeWantErr {
			require.Error(t, err)
			if tc.state > 0 {
				ti, err := r.GetTaskInfo(t1.Queue, t1.ID)
				require.NoError(t, err)
				require.Equal(t, tc.state, ti.State)
			}
		} else {
			require.NoError(t, err)
			ti, err := r.GetTaskInfo(t1Moved.Queue, t1Moved.ID)
			require.NoError(t, err)
			require.Equal(t, base.TaskStatePending, ti.State)
		}

		_, err = r.MoveToQueue(ctx, queue, t2Moved, time.Time{}, false)
		if tc.completedWantErr {
			require.Error(t, err)
			if tc.state > 0 {
				ti, err := r.GetTaskInfo(t2.Queue, t2.ID)
				require.NoError(t, err)
				require.Equal(t, tc.state, ti.State)
			}
		} else {
			require.NoError(t, err)
			ti, err := r.GetTaskInfo(t2Moved.Queue, t2Moved.ID)
			require.NoError(t, err)
			require.Equal(t, base.TaskStatePending, ti.State)
		}
	}
}

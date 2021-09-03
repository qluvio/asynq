package rqlite

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/stretchr/testify/require"
)

func GetPendingMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, pending)
}

func GetActiveMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, active)
}

func GetRetryMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, retry)
}

func GetArchivedMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, archived)
}

func GetScheduledMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, scheduled)
}

func getMessages(tb testing.TB, r *RQLite, queue string, state string) []*base.TaskMessage {
	require.NotNil(tb, r.conn)
	st := fmt.Sprintf(
		"SELECT ndx,pndx,task_msg,task_timeout,task_deadline FROM "+TasksTable+
			" WHERE "+TasksTable+".queue_name='%s' "+
			" AND state='%s'",
		queue,
		state)
	qr, err := r.conn.QueryOne(st)
	require.NoError(tb, err)

	ret := make([]*base.TaskMessage, 0)
	for qr.Next() {
		deq := &dequeueRow{}
		err = qr.Scan(&deq.ndx, &deq.pndx, &deq.msg, &deq.timeout, &deq.deadline)
		require.NoError(tb, err)
		msg, err := decodeMessage([]byte(deq.msg))
		require.NoError(tb, err)
		ret = append(ret, msg)
	}
	return ret
}

func GetDeadlinesEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	st := fmt.Sprintf(
		"SELECT task_msg,deadline FROM "+TasksTable+
			" WHERE "+TasksTable+".queue_name='%s' "+
			" AND state='active'",
		queue)
	qr, err := r.conn.QueryOne(st)
	require.NoError(tb, err)

	ret := make([]base.Z, 0)
	for qr.Next() {
		deq := &dequeueResult{}
		err = qr.Scan(&deq.msg, &deq.deadline)
		require.NoError(tb, err)
		msg, err := decodeMessage([]byte(deq.msg))
		require.NoError(tb, err)
		ret = append(ret, base.Z{Score: deq.deadline, Message: msg})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Message.ID.String() < ret[j].Message.ID.String()
	})

	return ret
}

// SortDeadlineEntryOpt is an cmp.Option to sort deadlineEntry for comparing slices
var SortDeadlineEntryOpt = cmp.Transformer("SortDeadlineEntryOpt", func(in []base.Z) []base.Z {
	out := append([]base.Z(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Message.ID.String() < out[j].Message.ID.String()
	})
	return out
})

func scoredEntries(rows []*taskRow, getDeadline func(t *taskRow) int64) []base.Z {
	ret := make([]base.Z, 0, len(rows))
	for _, row := range rows {
		deadline := row.deadline
		if getDeadline != nil {
			deadline = getDeadline(row)
		}
		ret = append(ret, base.Z{
			Score:   deadline,
			Message: row.msg,
		})
	}

	return ret
}

func GetRetryEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := listTasks(r.conn, queue, retry)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.retryAt })
}

func GetArchivedEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := listTasks(r.conn, queue, archived)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.archivedAt })
}

func GetScheduledEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := listTasks(r.conn, queue, scheduled)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.scheduledAt })
}

func GetUniqueKeyTTL(tb testing.TB, r *RQLite, queue string, taskType string, taskPayload []byte) time.Duration {
	require.NotNil(tb, r.conn)
	uniqueKey := base.UniqueKey(queue, taskType, taskPayload)

	st := fmt.Sprintf(
		"SELECT ndx, queue_name, task_uuid, unique_key, unique_key_deadline, task_msg, task_timeout, task_deadline, pndx, state, scheduled_at, deadline, retry_at, done_at, failed, archived_at, cleanup_at "+
			" FROM "+TasksTable+
			" WHERE queue_name='%s' "+
			" AND unique_key='%s'", queue, uniqueKey)
	qr, err := r.conn.QueryOne(st)
	require.NoError(tb, err)
	rows, err := parseTaskRows(qr)
	require.NoError(tb, err)
	require.Equal(tb, 1, len(rows))

	return time.Second * time.Duration(rows[0].uniqueKeyDeadline-utc.Now().Unix())
}

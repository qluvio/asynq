package rqlite

import (
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/require"
)

func RandomInMemoryDbPath() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"

	for i := 0; i < 20; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
}

func GetPendingMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, pending)
}

func GetQueueStats(tb testing.TB, r *RQLite, queue string) []*base.DailyStats {
	require.NotNil(tb, r.conn)
	ret, err := r.conn.queueStats(r.Now(), queue, 0)
	require.NoError(tb, err)
	return ret
}

func GetCompletedEntries(tb testing.TB, r *RQLite, qname string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := r.conn.listTasks(qname, completed)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.retainUntil })
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

func GetCompletedMessages(tb testing.TB, r *RQLite, queue string) []*base.TaskMessage {
	return getMessages(tb, r, queue, completed)
}

func getMessages(tb testing.TB, r *RQLite, queue string, state string) []*base.TaskMessage {
	require.NotNil(tb, r.conn)
	st := Statement(
		"SELECT ndx,pndx,task_msg,task_timeout,task_deadline FROM "+r.conn.table(TasksTable)+
			" WHERE "+r.conn.table(TasksTable)+".queue_name=? "+
			" AND state=?",
		queue,
		state)
	qrs, err := r.conn.QueryStmt(r.conn.ctx(), st)
	require.NoError(tb, err)

	qr := qrs[0]
	ret := make([]*base.TaskMessage, 0)
	for qr.Next() {
		deq := &dequeueRow0{}
		err = qr.Scan(&deq.ndx, &deq.pndx, &deq.taskMsg, &deq.timeout, &deq.deadline)
		require.NoError(tb, err)
		msg, err := decodeMessage([]byte(deq.taskMsg))
		require.NoError(tb, err)
		ret = append(ret, msg)
	}
	return ret
}

func GetDeadlinesEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	st := Statement(
		"SELECT task_msg,deadline FROM "+r.conn.table(TasksTable)+
			" WHERE "+r.conn.table(TasksTable)+".queue_name=? "+
			" AND state='active'",
		queue)
	qrs, err := r.conn.QueryStmt(r.conn.ctx(), st)
	require.NoError(tb, err)

	qr := qrs[0]
	ret := make([]base.Z, 0)
	for qr.Next() {
		deq := &dequeueResult{}
		err = qr.Scan(&deq.taskMsg, &deq.deadline)
		require.NoError(tb, err)
		deq.msg, err = decodeMessage([]byte(deq.taskMsg))
		require.NoError(tb, err)
		ret = append(ret, base.Z{Score: deq.deadline, Message: deq.msg})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Message.ID < ret[j].Message.ID
	})

	return ret
}

// SortDeadlineEntryOpt is an cmp.Option to sort deadlineEntry for comparing slices
var SortDeadlineEntryOpt = cmp.Transformer("SortDeadlineEntryOpt", func(in []base.Z) []base.Z {
	out := append([]base.Z(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Message.ID < out[j].Message.ID
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
	rows, err := r.conn.listTasks(queue, retry)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.retryAt })
}

func GetArchivedEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := r.conn.listTasks(queue, archived)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.archivedAt })
}

func GetScheduledEntries(tb testing.TB, r *RQLite, queue string) []base.Z {
	require.NotNil(tb, r.conn)
	rows, err := r.conn.listTasks(queue, scheduled)
	require.NoError(tb, err)

	return scoredEntries(rows, func(r *taskRow) int64 { return r.scheduledAt })
}

func GetUniqueKeyTTL(tb testing.TB, r *RQLite, queue string, taskType string, taskPayload []byte) time.Duration {
	require.NotNil(tb, r.conn)
	uniqueKey := base.UniqueKey(queue, taskType, taskPayload)

	st := Statement(
		selectTaskRow+
			" FROM "+r.conn.table(TasksTable)+
			" WHERE queue_name=? "+
			" AND unique_key=?",
		queue,
		uniqueKey)
	qrs, err := r.conn.QueryStmt(r.conn.ctx(), st)
	require.NoError(tb, err)
	qr := qrs[0]
	rows, err := parseTaskRows(qr)
	require.NoError(tb, err)
	require.Equal(tb, 1, len(rows))

	return time.Second * time.Duration(rows[0].uniqueKeyDeadline-r.Now().Unix())
}

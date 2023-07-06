package rqlite

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/stretchr/testify/require"
)

// SeedAllPendingQueues initializes all the specified queues with the given messages.
//
// pending maps a queue name to a list of messages.
func SeedAllPendingQueues(tb testing.TB, r *RQLite, pending map[string][]*base.TaskMessage) {
	for q, msgs := range pending {
		SeedPendingQueue(tb, r, msgs, q)
	}
}

// SeedPendingQueue initializes the specified queue with the given pending messages.
func SeedPendingQueue(tb testing.TB, r *RQLite, msgs []*base.TaskMessage, qname string) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)
	for _, msg := range msgs {
		require.Equal(tb, qname, msg.Queue)
		if msg.UniqueKey != "" && msg.UniqueKeyTTL > 0 {
			err = r.EnqueueUnique(context.Background(), msg, time.Duration(msg.UniqueKeyTTL)*time.Second)
		} else {
			err = r.Enqueue(context.Background(), msg)
		}
		require.NoError(tb, err)
	}
}

func SeedAllCompletedQueues(tb testing.TB, r *RQLite, completed map[string][]base.Z, fn ...func(id string) error) {
	for q, msgs := range completed {
		SeedCompletedQueue(tb, r, msgs, q, fn...)
	}
}

func SeedCompletedQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string, fns ...func(id string) error) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	now := r.Now()
	var fn func(id string) error
	if len(fns) > 0 {
		fn = fns[0]
	}

	for _, mz := range msgs {
		msg := mz.Message
		require.Equal(tb, qname, msg.Queue)

		if msg.CompletedAt == 0 {
			msg.CompletedAt = now.Unix()
		}
		msg.Retention = mz.Score - msg.CompletedAt
		retainUntil := mz.Score
		encoded, err := encodeMessage(msg)
		require.NoError(tb, err)

		SeedPendingQueue(tb, r, []*base.TaskMessage{msg}, qname)

		// tests insist on passing completed_at in the message + score in the Z
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, done_at=?, cleanup_at=?, retain_until=? "+
				"WHERE task_uuid=?",
			active,
			encoded,
			msg.CompletedAt,
			msg.CompletedAt+int64(statsTTL),
			retainUntil,
			msg.ID)
		//st := Statement(
		//	"INSERT INTO "+r.conn.table(CompletedTasksTable)+
		//		" (queue_name, type_name, task_uuid, task_msg, done_at, retain_until) "+
		//		" VALUES (?, ?, ?, ?, ?, ?)",
		//	qname,
		//	msg.Type,
		//	msg.ID,
		//	encoded,
		//	msg.CompletedAt,
		//	retainUntil)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

		if fn != nil {
			err = fn(msg.ID)
			require.NoError(tb, err)
		}

		err = r.conn.setTaskCompleted(time.Time{}, "", msg)
		require.NoError(tb, err)

		//st = r.conn.processedStatsStatement(time.Unix(msg.CompletedAt, 0), msg.Queue)
		//wrs, err = r.conn.WriteStmt(r.conn.ctx(), st)
		//require.NoError(tb, err)
	}
}

func SeedAllDeadlines(tb testing.TB, r *RQLite, deadlines map[string][]base.Z, uniqueKeyTTL time.Duration) {
	for q, msgs := range deadlines {
		seedDeadlines(tb, r, msgs, q, uniqueKeyTTL)
	}
}

func seedDeadlines(tb testing.TB, r *RQLite, deadlines []base.Z, qname string, uniqueKeyTTL time.Duration) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)
	for _, msg := range deadlines {
		require.Equal(tb, msg.Message.Queue, qname)

		count, err := r.conn.getTaskCount(
			qname,
			"task_uuid", msg.Message.ID)
		require.NoError(tb, err)

		if count == 0 {
			if len(msg.Message.UniqueKey) > 0 {
				err = r.EnqueueUnique(context.Background(), msg.Message, uniqueKeyTTL)
			} else {
				err = r.Enqueue(context.Background(), msg.Message)
			}
			require.NoError(tb, err)
		}

		deadline := msg.Score
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET deadline=? WHERE task_uuid=?",
			deadline,
			msg.Message.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)
	}
}

// SeedAllActiveQueues fills the DB with active tasks and their deadlines
// uniqueKeyTTL: set uniqueness lock if unique key is present.
func SeedAllActiveQueues(tb testing.TB, r *RQLite, deadlines map[string][]*base.TaskMessage, insertFirst ...bool) {
	doInsertFirst := false
	if len(insertFirst) > 0 {
		doInsertFirst = insertFirst[0]
	}
	for q, msgs := range deadlines {
		SeedActiveQueue(tb, r, msgs, q, doInsertFirst)
	}
}

// SeedActiveQueue initializes the specified queue with the given active messages.
func SeedActiveQueue(tb testing.TB, r *RQLite, msgs []*base.TaskMessage, qname string, insertFirst bool) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)
	if insertFirst {
		SeedPendingQueue(tb, r, msgs, qname)
	}
	for _, msg := range msgs {
		require.Equal(tb, msg.Queue, qname)
		//var err error
		//if len(msg.Message.UniqueKey) > 0 {
		//	err = r.EnqueueUnique(msg.Message, uniqueKeyTTL)
		//} else {
		//	err = r.Enqueue(msg.Message)
		//}
		//require.NoError(tb, err)

		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state='active' WHERE task_uuid=?",
			msg.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.Equal(tb, int64(1), wrs[0].RowsAffected)
		require.NoError(tb, err, "error %v", wrs[0].Err)
	}
}

func SeedAllRetryQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z, noStats ...bool) {
	for q, msgs := range deadlines {
		SeedRetryQueue(tb, r, msgs, q, noStats...)
	}
}

// SeedRetryQueue initializes the specified queue with the given retry messages.
func SeedRetryQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string, noStats ...bool) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	for _, msg := range msgs {

		m := msg.Message
		require.Equal(tb, qname, m.Queue)

		// use enqueue, just to fill the row
		var err error
		if len(m.UniqueKey) != 0 {
			err = r.EnqueueUnique(context.Background(), m, 0)
		} else {
			err = r.Enqueue(context.Background(), m)
		}
		require.NoError(tb, err)

		retryAt := msg.Score
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state='retry', retry_at=? WHERE task_uuid=?",
			retryAt,
			msg.Message.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

		if len(noStats) == 0 || !noStats[0] {
			st = r.conn.failedStatsStatement(time.Unix(retryAt, 0), m.Queue)
			wrs, err = r.conn.WriteStmt(r.conn.ctx(), st)
			require.NoError(tb, err, "error %v", wrs[0].Err)
		}

	}
}

func SeedAllArchivedQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z) {
	for q, msgs := range deadlines {
		SeedArchivedQueue(tb, r, msgs, q)
	}
}

// SeedArchivedQueue initializes the specified queue with the given archived messages.
func SeedArchivedQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	for _, msg := range msgs {

		m := msg.Message
		require.Equal(tb, qname, m.Queue)

		// use enqueue, just to fill the row
		var err error
		if len(m.UniqueKey) != 0 {
			err = r.EnqueueUnique(context.Background(), m, 0)
		} else {
			err = r.Enqueue(context.Background(), m)
		}
		require.NoError(tb, err)

		deadline := msg.Score
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state='archived', archived_at=? WHERE task_uuid=?",
			deadline,
			msg.Message.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

		st = r.conn.failedStatsStatement(time.Unix(deadline, 0), m.Queue)
		wrs, err = r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

	}
}

func SeedAllScheduledQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z) {
	for q, msgs := range deadlines {
		SeedScheduledQueue(tb, r, msgs, q)
	}
}

// SeedScheduledQueue initializes the specified queue with the given scheduled messages.
func SeedScheduledQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	for _, msg := range msgs {

		m := msg.Message
		require.Equal(tb, qname, m.Queue)
		processAt := time.Unix(msg.Score, 0).UTC()

		var err error
		if len(m.UniqueKey) != 0 {
			err = r.ScheduleUnique(context.Background(), m, processAt, 0)
		} else {
			err = r.Schedule(context.Background(), m, processAt)
		}
		require.NoError(tb, err)

	}
}

func SeedAllProcessedQueues(tb testing.TB, r *RQLite, processed map[string]int, doneAt time.Time) {
	for q, count := range processed {
		SeedProcessedQueue(tb, r, count, q, doneAt)
	}
}

// SeedProcessedQueue initializes the specified queue with the number of processed messages.
func SeedProcessedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt time.Time) {
	seedProcessedQueue(tb, r, count, qname, doneAt, false)
}

// seedProcessedQueue initializes the specified queue with the number of processed messages.
func seedProcessedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt time.Time, failed bool) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	var st *sqlite3.Statement
	if !failed {
		st = r.conn.processedStatsStatementCount(doneAt, qname, count)
	} else {
		st = r.conn.failedStatsStatementCount(doneAt, qname, count, count)
	}

	wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
	require.NoError(tb, err, "error %v", err)
	require.True(tb, len(wrs) > 0)
	require.NoError(tb, wrs[0].Err)
}

// seedFailedQueue initializes the specified queue with the number of failed messages.
func seedFailedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt time.Time) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	st := r.conn.failedOnlyStatsStatementCount(doneAt, qname, count)
	wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
	require.NoError(tb, err, "error %v", err)
	require.True(tb, len(wrs) > 0)
	require.NoError(tb, wrs[0].Err)
}

func SeedFailedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt time.Time, failedOnly bool) {
	if failedOnly {
		seedFailedQueue(tb, r, count, qname, doneAt)
		return
	}
	seedProcessedQueue(tb, r, count, qname, doneAt, true)
}

func SeedAllFailedQueues(tb testing.TB, r *RQLite, failed map[string]int, doneAt time.Time, failedOnly bool) {
	for q, count := range failed {
		SeedFailedQueue(tb, r, count, q, doneAt, failedOnly)
	}
}

func SeedLastPendingSince(tb testing.TB, r *RQLite, qname string, enqueueTime time.Time) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)
	ctx := r.conn.ctx()

	st := Statement(
		selectTaskRow+
			" FROM "+r.conn.table(TasksTable)+
			" WHERE queue_name=? "+
			"   AND state=? "+
			" ORDER BY ndx DESC LIMIT 1",
		qname,
		pending)

	qrs, err := r.conn.QueryStmt(ctx, st)
	require.NoError(tb, err)

	rows, err := parseTaskRows(qrs[0])
	require.NoError(tb, err)
	require.Equal(tb, 1, len(rows))

	st = Statement("UPDATE "+r.conn.table(TasksTable)+" SET pending_since=? "+
		" WHERE queue_name=? AND task_uuid=? ",
		enqueueTime.UnixNano(),
		qname,
		rows[0].taskUuid)
	wrs, err := r.conn.WriteStmt(ctx, st)
	require.NoError(tb, err)
	require.Equal(tb, 1, len(wrs))
	require.Equal(tb, int64(1), wrs[0].RowsAffected)
}

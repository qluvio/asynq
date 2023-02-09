package rqlite

import (
	"testing"
	"time"

	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/hibiken/asynq/internal/utc"
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
		err := r.Enqueue(msg)
		require.NoError(tb, err)
	}
}

func SeedAllCompletedQueues(tb testing.TB, r *RQLite, completed map[string][]base.Z) {
	for q, msgs := range completed {
		SeedCompletedQueue(tb, r, msgs, q)
	}
}

func SeedCompletedQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	now := utc.Now()
	state := completed

	for _, mz := range msgs {
		msg := mz.Message
		retainUntil := mz.Score
		require.Equal(tb, qname, msg.Queue)

		if msg.CompletedAt == 0 {
			msg.CompletedAt = now.Unix()
		}
		expireAt := msg.CompletedAt + int64(statsTTL)
		encoded, err := encodeMessage(msg)
		require.NoError(tb, err)

		SeedPendingQueue(tb, r, []*base.TaskMessage{msg}, qname)
		//SeedActiveQueue(tb, r, []*base.TaskMessage{msg.Message}, qname, true)
		//err = r.MarkAsComplete("", msg)

		// tests insist on passing completed_at in the message + score in the Z
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, done_at=?, cleanup_at=?, retain_until=? "+
				"WHERE task_uuid=?",
			state,
			encoded,
			msg.CompletedAt,
			expireAt,
			retainUntil,
			msg.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

		require.NoError(tb, err)
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
				err = r.EnqueueUnique(msg.Message, uniqueKeyTTL)
			} else {
				err = r.Enqueue(msg.Message)
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

func SeedAllRetryQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z) {
	for q, msgs := range deadlines {
		SeedRetryQueue(tb, r, msgs, q)
	}
}

// SeedRetryQueue initializes the specified queue with the given retry messages.
func SeedRetryQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	for _, msg := range msgs {

		m := msg.Message
		require.Equal(tb, qname, m.Queue)

		// use enqueue, just to fill the row
		var err error
		if len(m.UniqueKey) != 0 {
			err = r.EnqueueUnique(m, 0)
		} else {
			err = r.Enqueue(m)
		}
		require.NoError(tb, err)

		retryAt := msg.Score
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state='retry', retry_at=? WHERE task_uuid=?",
			retryAt,
			msg.Message.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)
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
			err = r.EnqueueUnique(m, 0)
		} else {
			err = r.Enqueue(m)
		}
		require.NoError(tb, err)

		deadline := msg.Score
		st := Statement(
			"UPDATE "+r.conn.table(TasksTable)+" SET state='archived', archived_at=? WHERE task_uuid=?",
			deadline,
			msg.Message.ID)
		wrs, err := r.conn.WriteStmt(r.conn.ctx(), st)
		require.NoError(tb, err, "error %v", wrs[0].Err)

		require.NoError(tb, err)

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
			err = r.ScheduleUnique(m, processAt, 0)
		} else {
			err = r.Schedule(m, processAt)
		}
		require.NoError(tb, err)

	}
}

func SeedAllProcessedQueues(tb testing.TB, r *RQLite, processed map[string]int, doneAt int64) {
	for q, count := range processed {
		SeedProcessedQueue(tb, r, count, q, doneAt)
	}
}

// SeedProcessedQueue initializes the specified queue with the number of processed messages.
func SeedProcessedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt int64) {
	seedProcessedQueue(tb, r, count, qname, doneAt, false)
}

// seedProcessedQueue initializes the specified queue with the number of processed messages.
func seedProcessedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt int64, failed bool) {
	err := r.conn.EnsureQueue(qname)
	require.NoError(tb, err)

	stmts := make([]*sqlite3.Statement, 0, count)
	for i := 0; i < count; i++ {

		task := h.NewTaskMessage("", nil)
		em, err := encodeMessage(task)
		require.NoError(tb, err)
		var st *sqlite3.Statement
		if !failed {
			st = Statement(
				"INSERT INTO "+r.conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, pndx, state, done_at) "+
					"VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+r.conn.table(TasksTable)+")+1, ?, ?)",
				qname,
				task.Type,
				task.ID,
				task.ID,
				em,
				0,
				0,
				processed,
				doneAt)
		} else {
			st = Statement(
				"INSERT INTO "+r.conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, pndx, state, done_at, failed) "+
					"VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+r.conn.table(TasksTable)+")+1, ?, ?, ?)",
				qname,
				task.Type,
				task.ID,
				task.ID,
				em,
				0,
				0,
				retry,
				doneAt,
				true)
		}
		stmts = append(stmts, st)
	}

	if len(stmts) > 0 {
		_, err = r.conn.WriteStmt(r.conn.ctx(), stmts...)
		require.NoError(tb, err, "error %v", err)
	}
}

func SeedFailedQueue(tb testing.TB, r *RQLite, count int, qname string, doneAt int64) {
	seedProcessedQueue(tb, r, count, qname, doneAt, true)
}

func SeedAllFailedQueues(tb testing.TB, r *RQLite, failed map[string]int, doneAt int64) {
	for q, count := range failed {
		SeedFailedQueue(tb, r, count, q, doneAt)
	}
}

package rqlite

import (
	"fmt"
	"testing"
	"time"

	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
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
	err := EnsureQueue(r.conn, qname)
	require.NoError(tb, err)
	for _, msg := range msgs {
		require.Equal(tb, qname, msg.Queue)
		err := r.Enqueue(msg)
		require.NoError(tb, err)
	}
}

func SeedAllDeadlines(tb testing.TB, r *RQLite, deadlines map[string][]base.Z, uniqueKeyTTL time.Duration) {
	for q, msgs := range deadlines {
		seedDeadlines(tb, r, msgs, q, uniqueKeyTTL)
	}
}

func seedDeadlines(tb testing.TB, r *RQLite, deadlines []base.Z, qname string, uniqueKeyTTL time.Duration) {
	err := EnsureQueue(r.conn, qname)
	require.NoError(tb, err)
	for _, msg := range deadlines {
		require.Equal(tb, msg.Message.Queue, qname)

		count, err := getTaskCount(
			r.conn,
			qname,
			fmt.Sprintf("task_uuid='%s'", msg.Message.ID.String()))
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
		st := fmt.Sprintf(
			"UPDATE "+TasksTable+" SET deadline=%d WHERE task_uuid='%s'",
			deadline,
			msg.Message.ID.String())
		wr, err := r.conn.WriteOne(st)
		require.NoError(tb, err, "error %v", wr.Err)
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
	err := EnsureQueue(r.conn, qname)
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

		st := fmt.Sprintf(
			"UPDATE "+TasksTable+" SET state='active' WHERE task_uuid='%s'",
			msg.ID.String())
		wr, err := r.conn.WriteOne(st)
		require.Equal(tb, int64(1), wr.RowsAffected)
		require.NoError(tb, err, "error %v", wr.Err)
	}
}

func SeedAllRetryQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z) {
	for q, msgs := range deadlines {
		SeedRetryQueue(tb, r, msgs, q)
	}
}

// SeedRetryQueue initializes the specified queue with the given retry messages.
func SeedRetryQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := EnsureQueue(r.conn, qname)
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
		st := fmt.Sprintf(
			"UPDATE "+TasksTable+" SET state='retry', retry_at=%d WHERE task_uuid='%s'",
			retryAt,
			msg.Message.ID.String())
		wr, err := r.conn.WriteOne(st)
		require.NoError(tb, err, "error %v", wr.Err)
	}
}

func SeedAllArchivedQueues(tb testing.TB, r *RQLite, deadlines map[string][]base.Z) {
	for q, msgs := range deadlines {
		SeedArchivedQueue(tb, r, msgs, q)
	}
}

// SeedArchivedQueue initializes the specified queue with the given archived messages.
func SeedArchivedQueue(tb testing.TB, r *RQLite, msgs []base.Z, qname string) {
	err := EnsureQueue(r.conn, qname)
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
		st := fmt.Sprintf(
			"UPDATE "+TasksTable+" SET state='archived', archived_at=%d WHERE task_uuid='%s'",
			deadline,
			msg.Message.ID.String())
		wr, err := r.conn.WriteOne(st)
		require.NoError(tb, err, "error %v", wr.Err)

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
	err := EnsureQueue(r.conn, qname)
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
	err := EnsureQueue(r.conn, qname)
	require.NoError(tb, err)

	stmts := make([]string, 0, count)
	for i := 0; i < count; i++ {

		task := h.NewTaskMessage("", nil)
		st, err := encodeMessage(task)
		require.NoError(tb, err)
		if !failed {
			st = fmt.Sprintf(
				"INSERT INTO "+TasksTable+"(queue_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, pndx, state, done_at) "+
					"VALUES ('%s', '%s', '%s', '%s', %d, %d, (SELECT COALESCE(MAX(pndx),0) FROM "+TasksTable+")+1, '%s', %d)",
				qname,
				task.ID.String(),
				task.ID.String(),
				st,
				0,
				0,
				processed,
				doneAt)
		} else {
			st = fmt.Sprintf(
				"INSERT INTO "+TasksTable+"(queue_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, pndx, state, done_at, failed) "+
					"VALUES ('%s', '%s', '%s', '%s', %d, %d, (SELECT COALESCE(MAX(pndx),0) FROM "+TasksTable+")+1, '%s', %d, %v)",
				qname,
				task.ID.String(),
				task.ID.String(),
				st,
				0,
				0,
				retry,
				doneAt,
				true)
		}
		stmts = append(stmts, st)
	}

	if len(stmts) > 0 {
		_, err = r.conn.Write(stmts)
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

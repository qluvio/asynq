package rqlite

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

type taskRow struct {
	ndx               int64
	queueName         string
	typeName          string
	taskUuid          string
	uniqueKey         string
	uniqueKeyDeadline int64
	taskMsg           string
	taskTimeout       int64
	taskDeadline      int64
	pndx              int64
	state             string
	scheduledAt       int64
	deadline          int64
	retryAt           int64
	doneAt            int64
	failed            bool
	archivedAt        int64
	cleanupAt         int64
	retainUntil       int64
	sid               string
	affinityTimeout   int64
	recurrent         bool
	result            string
	pendingSince      int64
	msg               *base.TaskMessage
}

const selectTaskRow = "SELECT ndx, queue_name, type_name, task_uuid, unique_key, unique_key_deadline, task_msg, task_timeout, task_deadline, pndx, state, scheduled_at, deadline, retry_at, done_at, failed, archived_at, cleanup_at, retain_until, sid, affinity_timeout, recurrent, result, pending_since "

func parseTaskRows(qr sqlite3.QueryResult) ([]*taskRow, error) {
	op := errors.Op("rqlite.parseTaskRows")

	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*taskRow, 0)

	for qr.Next() {
		s := &taskRow{}
		err := qr.Scan(
			&s.ndx,
			&s.queueName,
			&s.typeName,
			&s.taskUuid,
			&s.uniqueKey,
			&s.uniqueKeyDeadline,
			&s.taskMsg,
			&s.taskTimeout,
			&s.taskDeadline,
			&s.pndx,
			&s.state,
			&s.scheduledAt,
			&s.deadline,
			&s.retryAt,
			&s.doneAt,
			&s.failed,
			&s.archivedAt,
			&s.cleanupAt,
			&s.retainUntil,
			&s.sid,
			&s.affinityTimeout,
			&s.recurrent,
			&s.result,
			&s.pendingSince)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		m, err := decodeMessage([]byte(s.taskMsg))
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		s.msg = m
		ret = append(ret, s)
	}
	return ret, nil
}

func (conn *Connection) listTasks(queue string, state string) ([]*taskRow, error) {
	return conn.listTasksPaged(queue, state, nil, "")
}

func (conn *Connection) listTasksPaged(queue string, state string, page *base.Pagination, orderBy string) ([]*taskRow, error) {
	op := errors.Op("listTasks")
	st := Statement(
		selectTaskRow+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? "+
			" AND state=? ",
		queue,
		state)
	if page != nil {
		if len(orderBy) == 0 {
			orderBy = "ndx"
		}
		st = st.Append(fmt.Sprintf(" ORDER BY %s LIMIT %d OFFSET %d", orderBy, page.Size, page.Start()))
	}
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRError(op, qrs[0], err, st)
	}
	return parseTaskRows(qrs[0])
}

func (conn *Connection) getTask(queue string, id string) (*taskRow, error) {
	op := errors.Op("rqlite.getTask")
	st := Statement(
		selectTaskRow+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? "+
			" AND task_uuid=?",
		queue,
		id)
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRError(op, qrs[0], err, st)
	}
	rows, err := parseTaskRows(qrs[0])
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	switch len(rows) {
	case 0:
		return nil, errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: queue, ID: id})
	case 1:
		return rows[0], nil
	default:
		return nil, errors.E(op, errors.Internal,
			fmt.Sprintf("unexpected result count: %d (expected 1), statement: %s", len(rows), st))
	}
}

func (conn *Connection) getTaskCount(queue string, andWhere, whereValue string) (int64, error) {
	op := errors.Op("rqlite.getTaskCount")
	st := Statement(
		"SELECT COUNT(*) "+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? ",
		queue)
	if len(andWhere) > 0 {
		st = st.Append(fmt.Sprintf(" AND %s=?", andWhere), whereValue)
	}

	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteRError(op, qrs[0], err, st)
	}
	count := int64(0)
	qrs[0].Next()
	err = qrs[0].Scan(&count)
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	return count, nil
}

type dequeueRow struct {
	uuid     string
	ndx      int64
	pndx     int64
	taskMsg  string
	timeout  int64
	deadline int64
	msg      *base.TaskMessage
}

// We cannot issue a select and an update in the same transaction with rqlite
// Thus we first get the index of the row, then try update this row
func (conn *Connection) getPending(now time.Time, serverID string, queue string) (*dequeueRow, error) {
	op := errors.Op("rqlite.getPending")

	st := Statement(
		"SELECT task_uuid,ndx,pndx,task_msg,task_timeout,task_deadline FROM "+conn.table(TasksTable)+
			" INNER JOIN "+conn.table(QueuesTable)+
			" ON "+conn.table(QueuesTable)+".queue_name="+conn.table(TasksTable)+".queue_name"+
			" WHERE "+conn.table(QueuesTable)+".queue_name=? "+
			" AND pndx=(SELECT COALESCE(MIN(pndx),0) FROM "+conn.table(TasksTable)+" WHERE state='pending' AND queue_name=?)"+
			" AND "+conn.table(QueuesTable)+".state='active'",
		queue,
		queue)

	st.Append(" AND ("+
		""+conn.table(TasksTable)+".sid IS NULL "+
		"OR ("+conn.table(TasksTable)+".sid=?"+
		"  AND ("+conn.table(TasksTable)+".affinity_timeout>=0 "+
		"    OR ("+conn.table(TasksTable)+".affinity_timeout<0 AND ("+
		"        "+conn.table(TasksTable)+".archived_at-"+conn.table(TasksTable)+".affinity_timeout)<=?))) "+
		"OR (("+conn.table(TasksTable)+".done_at+"+conn.table(TasksTable)+".affinity_timeout)<=? "+
		"   AND "+conn.table(TasksTable)+".sid!=?)"+
		")",
		serverID,
		now.Unix(),
		now.Unix(),
		serverID)

	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRError(op, qrs[0], err, st)
	}

	if len(qrs) == 0 || qrs[0].NumRows() == 0 {
		// no row
		return nil, nil
	}

	qr := qrs[0]
	if qr.NumRows() > 1 {
		// PENDING(GIL): take the first anyway because once we're in this state
		//               this cannot change anymore !
		//return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite - more than one row selected: %d", qr.NumRows()))
		if slog != nil {
			slog.Warn(op, "rqlite - more than one row selected: ", qr.NumRows())
		} else {
			fmt.Println(op, "rqlite - more than one row selected: ", qr.NumRows())
		}
	}

	qr.Next()
	deq := &dequeueRow{}
	err = qr.Scan(&deq.uuid, &deq.ndx, &deq.pndx, &deq.taskMsg, &deq.timeout, &deq.deadline)
	if err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
	}
	deq.msg, err = decodeMessage([]byte(deq.taskMsg))
	if err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
	}

	return deq, nil
}

func (conn *Connection) deleteTasks(queue string, state string) (int64, error) {
	op := errors.Op("rqlite.deleteTasks")

	st := Statement(
		"DELETE FROM "+conn.table(TasksTable)+" WHERE queue_name=? AND state=?",
		queue,
		state)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}

	return wrs[0].RowsAffected, nil
}

func (conn *Connection) deleteTask(queue string, taskid string) (int64, error) {
	op := errors.Op("rqlite.deleteTask")

	st := Statement(
		"DELETE FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND task_uuid=? AND state!='active'",
		queue,
		taskid)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}
	ret := wrs[0].RowsAffected

	// enforce conventional return values for inspector
	if ret == 0 {
		qs, err := conn.getTask(queue, taskid)
		if err == nil && qs != nil {
			ret = -1
		}
	}

	return ret, nil
}

func (conn *Connection) setTaskPending(queue string, taskid string) (int64, error) {
	op := errors.Op("rqlite.setTaskPending")
	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET state='pending', deadline=NULL "+
			" WHERE queue_name=? AND task_uuid=? AND state != 'pending' AND state!='active'",
		queue,
		taskid)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}

	ret := wrs[0].RowsAffected
	// enforce conventional return values for inspector
	if ret == 0 {
		qs, err := conn.getTask(queue, taskid)
		if err == nil && qs != nil {
			switch qs.state {
			case pending:
				ret = -2
			case active:
				ret = -1
			}
		}
	}
	return ret, nil
}

func (conn *Connection) setPending(queue string, state string) (int64, error) {
	op := errors.Op("rqlite.setPending")
	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET state='pending', deadline=NULL "+
			" WHERE queue_name=? AND state=?",
		queue,
		state)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}
	return wrs[0].RowsAffected, nil
}

func (conn *Connection) setArchived(now time.Time, queue string, state string) (int64, error) {
	op := errors.Op("rqlite.setArchived")

	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET state='archived', "+
			" unique_key_deadline=NULL, affinity_timeout=0, "+
			" archived_at=?, cleanup_at=? "+
			" WHERE queue_name=? AND state=?",
		now.Unix(),
		now.AddDate(0, 0, conn.config.ArchivedExpirationInDays).Unix(),
		queue,
		state)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}
	return wrs[0].RowsAffected, nil
}

func (conn *Connection) setTaskArchived(now time.Time, queue string, taskid string) (int64, error) {
	op := errors.Op("rqlite.setTaskArchived")

	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET state='archived', "+
			" archived_at=?, cleanup_at=? "+
			" WHERE queue_name=? AND task_uuid=? AND state != 'archived' AND state!='active'",
		now.Unix(),
		now.AddDate(0, 0, conn.config.ArchivedExpirationInDays).Unix(),
		queue,
		taskid)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}

	ret := wrs[0].RowsAffected
	// enforce conventional return values for inspector
	if ret == 0 {
		qs, err := conn.getTask(queue, taskid)
		if err == nil && qs != nil {
			switch qs.state {
			case active:
				ret = -2
			case archived:
				ret = -1
			}
		}
	}
	return ret, nil
}

// enqueueMessages insert the given task messages (and put them in state 'pending').
// notes:
// * pending_since is unix nano (whereas all other time fields are unix)
// * with rqlite the json serialization makes large number values be decoded as
// float on the rqlite side leading to +/- 1micro inaccuracy
func (conn *Connection) enqueueMessages(ctx context.Context, now time.Time, msgs ...*base.MessageBatch) error {
	op := errors.Op("rqlite.enqueueMessages")
	if len(msgs) == 0 {
		return nil
	}

	queues := make(map[string]bool)
	stmts := make([]*sqlite3.Statement, 0, len(msgs)*2)
	msgNdx := make([]int, 0, len(msgs))

	for _, bmsg := range msgs {
		msg := bmsg.Msg
		if !queues[msg.Queue] {
			stmts = append(stmts, conn.ensureQueueStatement(msg.Queue))
			queues[msg.Queue] = true
		}
		encoded, err := encodeMessage(msg)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
		}
		msgNdx = append(msgNdx, len(stmts))
		stmts = append(stmts, Statement(
			"INSERT INTO "+conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, recurrent, pndx, state, pending_since) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?, ?)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			msg.Recurrent,
			pending,
			now.UnixNano())) // pending_since is unix nano (whereas all other time fields are unix)
	}

	wrs, err := conn.WriteStmt(ctx, stmts...)
	var err0 error
	if err != nil {
		err0 = NewRqliteWsError(op, wrs, err, stmts)
	}
	if len(wrs) != len(stmts) {
		return NewRqliteWsError(
			op,
			wrs,
			errors.New(fmt.Sprintf("unexpected result count: %d, expected: %d", len(wrs), len(stmts))),
			stmts,
		)
	}

	allErrors := make(map[int]error)
	for i, ndx := range msgNdx {
		if wrs[ndx].RowsAffected == 0 {
			if wrs[ndx].Err != nil && strings.Contains(wrs[ndx].Err.Error(), "UNIQUE constraint failed") {
				err = errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
			} else {
				err = errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
			}
		} else {
			err = expectOneRowUpdated(op, wrs[ndx], stmts[ndx], true)
		}
		if err != nil {
			msgs[i].Err = err
			allErrors[i] = err
		}
	}

	if len(allErrors) > 0 {
		//if len(msgs) == 1 {
		//	return allErrors[0]
		//}
		return &errors.BatchError{Errors: allErrors}
	}
	if err0 != nil {
		return err0
	}
	return nil
}

// enqueueUniqueMessages inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (conn *Connection) enqueueUniqueMessages(ctx context.Context, now time.Time, msgs ...*base.MessageBatch) error {
	op := errors.Op("rqlite.enqueueUniqueMessages")
	if len(msgs) == 0 {
		return nil
	}

	queues := make(map[string]bool)
	stmts := make([]*sqlite3.Statement, 0, len(msgs)*2)
	msgNdx := make([]int, 0, len(msgs))

	for _, bmsg := range msgs {
		msg := bmsg.Msg
		if !queues[msg.Queue] {
			stmts = append(stmts, conn.ensureQueueStatement(msg.Queue))
			queues[msg.Queue] = true
		}
		if bmsg.Msg.UniqueKeyTTL == 0 {
			// flaky tests
			bmsg.Msg.UniqueKeyTTL = int64(bmsg.UniqueTTL.Seconds())
		}
		encoded, err := encodeMessage(msg)
		if err != nil {
			return errors.E(op, errors.Internal, "cannot encode task message: %v", err)
		}
		uniqueKeyExpireAt := now.Add(bmsg.UniqueTTL).Unix()
		msgNdx = append(msgNdx, len(stmts))
		uniqueKeyExpirationLimit := now.Unix()
		if bmsg.ForceUnique {
			uniqueKeyExpirationLimit = math.MaxInt64
		}

		// if the unique_key_deadline is expired we ignore the constraint
		// https://www.sqlite.org/lang_UPSERT.html
		stmts = append(stmts, Statement(
			"INSERT INTO "+conn.table(TasksTable)+" (queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, unique_key_deadline, recurrent, pndx, state, pending_since)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?, ?) "+
				" ON CONFLICT(unique_key) DO UPDATE SET "+
				"   queue_name=excluded.queue_name, "+
				"   type_name=excluded.type_name, "+
				"   task_uuid=excluded.task_uuid, "+
				"   unique_key=excluded.unique_key, "+
				"   task_msg=excluded.task_msg, "+
				"   task_timeout=excluded.task_timeout, "+
				"   task_deadline=excluded.task_deadline, "+
				"   unique_key_deadline=excluded.unique_key_deadline, "+
				"   recurrent=excluded.recurrent, "+
				"   pndx=excluded.pndx, "+
				"   state=excluded.state, "+
				"   pending_since=excluded.pending_since "+
				" WHERE ("+conn.table(TasksTable)+".unique_key_deadline<=? "+
				"    OR "+conn.table(TasksTable)+".unique_key_deadline IS NULL)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.UniqueKey,
			encoded,
			msg.Timeout,
			msg.Deadline,
			uniqueKeyExpireAt,
			msg.Recurrent,
			pending,
			now.UnixNano(), // pending_since is unix nano (whereas all other time fields are unix)
			uniqueKeyExpirationLimit))

		//stmts = append(stmts, Statement(
		//	"INSERT INTO "+conn.table(TasksTable)+" (queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, unique_key_deadline, affinity_timeout, recurrent, pndx, state, sid)"+
		//		" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?, NULL) "+
		//		" ON CONFLICT(unique_key) DO UPDATE SET "+
		//		"   queue_name=excluded.queue_name, "+
		//		"   type_name=excluded.type_name, "+
		//		"   task_uuid=excluded.task_uuid, "+
		//		"   unique_key=excluded.unique_key, "+
		//		"   task_msg=excluded.task_msg, "+
		//		"   task_timeout=excluded.task_timeout, "+
		//		"   task_deadline=excluded.task_deadline, "+
		//		"   unique_key_deadline=excluded.unique_key_deadline, "+
		//		"   affinity_timeout=excluded.affinity_timeout, "+
		//		"   recurrent=excluded.recurrent, "+
		//		"   pndx=excluded.pndx, "+
		//		"   state=excluded.state, "+
		//		"   sid=excluded.sid"+
		//		" WHERE "+conn.table(TasksTable)+".unique_key_deadline<=?",
		//	msg.Queue,
		//	msg.Type,
		//	msg.ID.String(),
		//	msg.UniqueKey,
		//	encoded,
		//	msg.Timeout,
		//	msg.Deadline,
		//	uniqueKeyExpireAt,
		//	msg.ServerAffinity,
		//	msg.Recurrent,
		//	pending,
		//	uniqueKeyExpirationLimit))
	}

	wrs, err := conn.WriteStmt(ctx, stmts...)
	var err0 error
	if err != nil {
		err0 = NewRqliteWsError(op, wrs, err, stmts)
	}
	if len(wrs) != len(stmts) {
		return NewRqliteWsError(
			op,
			wrs,
			errors.New(fmt.Sprintf("unexpected result count: %d, expected: %d", len(wrs), len(stmts))),
			stmts,
		)
	}

	allErrors := make(map[int]error)
	for i, ndx := range msgNdx {
		if wrs[ndx].RowsAffected == 0 {
			if wrs[ndx].Err != nil && strings.Contains(wrs[ndx].Err.Error(), "UNIQUE constraint failed") {
				err = errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
			} else {
				err = errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
			}
			msgs[i].Err = err
			allErrors[i] = err
		}
	}

	if len(allErrors) > 0 {
		//if len(msgs) == 1 {
		//	return allErrors[0]
		//}
		return &errors.BatchError{Errors: allErrors}
	}
	if err0 != nil {
		return err0
	}
	return nil
}

type dequeueResult struct {
	taskMsg  string
	deadline int64
	msg      *base.TaskMessage
}

func (conn *Connection) setTaskActive(ndx int64, serverID string, serverAffinity, deadline int64) (bool, error) {
	op := errors.Op("rqlite.setTaskActive")

	var st *sqlite3.Statement
	if len(serverID) > 0 {
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='active', pending_since=NULL, sid=?, affinity_timeout=?, deadline=? WHERE state='pending' AND ndx=?",
			serverID,
			serverAffinity,
			deadline,
			ndx)
	} else {
		// PENDING(GIL): rqlite api does not support null on insert or update with parameterized statement
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='active', pending_since=NULL, affinity_timeout=?, deadline=? WHERE state='pending' AND ndx=?",
			serverAffinity,
			deadline,
			ndx)
	}
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return false, NewRqliteWError(op, wrs[0], err, st)
	}
	// if the row was changed (by another rqlite) no row is affected
	return wrs[0].RowsAffected == 1, nil
}

// dequeueMessage finds a processable task in the given queue.
// The task state is set to active and its deadline is computed by inspecting
// Timout and Deadline fields.
//
// Returns nil if no processable task is found in the given queue.
// Returns a dequeueResult instance if a task is found.
func (conn *Connection) dequeueMessage(now time.Time, serverID string, qname string) (*dequeueResult, error) {

	nowu := now.Unix()
	for {
		row, err := conn.getPending(now, serverID, qname)
		if err != nil {
			return nil, err
		}

		// no pending row
		if row == nil {
			return nil, nil
		}

		var score int64
		if row.timeout != 0 && row.deadline != 0 {
			score = int64(math.Min(float64(nowu+row.timeout), float64(row.deadline)))
		} else if row.timeout != 0 {
			score = nowu + row.timeout
		} else if row.deadline != 0 {
			score = row.deadline
		} else {
			return nil, errors.E(errors.Op("rqlite.dequeueMessage"), errors.Internal, "asynq internal error: both timeout and deadline are not set")
		}

		ok, err := conn.setTaskActive(row.ndx, serverID, row.msg.ServerAffinity, score)
		if err != nil {
			return nil, err
		}

		if ok {
			return &dequeueResult{
				taskMsg:  row.taskMsg,
				deadline: score,
				msg:      row.msg,
			}, nil
		}
	}
}

// setTaskDone removes the task from active queue to mark the task as done and
// set its state to 'processed'.
// It removes a uniqueness lock acquired by the task, if any.
func (conn *Connection) setTaskDone(now time.Time, serverID string, msg *base.TaskMessage) error {
	op := errors.Op("rqlite.setTaskDone")
	state := processed

	expireAt := now.Add(statsTTL)

	var st *sqlite3.Statement
	if len(msg.UniqueKey) > 0 {
		if len(serverID) > 0 {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, unique_key_deadline=NULL, sid=?, done_at=?, cleanup_at=?, unique_key=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				serverID,
				now.Unix(),
				expireAt.Unix(),
				msg.ID,
				msg.ID)
		} else {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, unique_key_deadline=NULL, done_at=?, cleanup_at=?, unique_key=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				now.Unix(),
				expireAt.Unix(),
				msg.ID,
				msg.ID)
		}
	} else {
		if len(serverID) > 0 {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, sid=?, done_at=?, cleanup_at=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				serverID,
				now.Unix(),
				expireAt.Unix(),
				msg.ID)
		} else {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, done_at=?, cleanup_at=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				now.Unix(),
				expireAt.Unix(),
				msg.ID)
		}
	}

	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWError(op, wrs[0], err, st)
	}
	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs[0], st, len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	return nil
}

// setTaskCompleted removes the task from active queue to mark the task as done and
// set its state to 'completed'.
// It removes a uniqueness lock acquired by the task, if any.
func (conn *Connection) setTaskCompleted(now time.Time, serverID string, msg *base.TaskMessage) error {
	op := errors.Op("rqlite.setTaskCompleted")
	state := completed

	msg.CompletedAt = now.Unix()

	expireAt := now.Add(statsTTL)
	retainUntil := now.Add(time.Second * time.Duration(msg.Retention))
	// re-encode message to have completedAt
	encoded, err := encodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	var st *sqlite3.Statement
	if len(msg.UniqueKey) > 0 {
		if len(serverID) > 0 {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, unique_key_deadline=NULL, sid=?, done_at=?, cleanup_at=?, retain_until=?, unique_key=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				encoded,
				serverID,
				now.Unix(),
				expireAt.Unix(),
				retainUntil.Unix(),
				msg.ID,
				msg.ID)
		} else {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, unique_key_deadline=NULL, done_at=?, cleanup_at=?, retain_until=?, unique_key=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				encoded,
				now.Unix(),
				expireAt.Unix(),
				retainUntil.Unix(),
				msg.ID,
				msg.ID)
		}
	} else {
		if len(serverID) > 0 {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, sid=?, done_at=?, cleanup_at=?, retain_until=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				encoded,
				serverID,
				now.Unix(),
				expireAt.Unix(),
				retainUntil.Unix(),
				msg.ID)
		} else {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state=?, task_msg=?, deadline=NULL, done_at=?, cleanup_at=?, retain_until=? "+
					"WHERE task_uuid=? AND state='active'",
				state,
				encoded,
				now.Unix(),
				expireAt.Unix(),
				retainUntil.Unix(),
				msg.ID)
		}
	}

	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWError(op, wrs[0], err, st)
	}
	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs[0], st, len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	return nil
}

// deleteExpiredCompletedTasks deletes completed tasks whose retention period
// expired to the archived set.
func (conn *Connection) deleteExpiredCompletedTasks(now time.Time, qname string) error {
	op := errors.Op("rqlite.deleteExpiredCompletedTasks")
	state := completed

	stmt := Statement(
		"DELETE FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND state=? AND retain_until<?",
		qname,
		state,
		now.Unix())
	wrs, err := conn.WriteStmt(conn.ctx(), stmt)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{stmt})
	}
	return nil
}

func (conn *Connection) writeTaskResult(qname, taskID string, data []byte, active bool) (int, error) {
	op := errors.Op("rqlite.writeTaskResult")
	andState := " AND state='active' "
	if !active {
		// for tests
		andState = ""
	}

	var st *sqlite3.Statement
	if len(data) == 0 {
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET result=NULL "+
				" WHERE queue_name=?"+andState+" AND task_uuid=?",
			qname,
			taskID)
	} else {
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET result=? "+
				" WHERE queue_name=?"+andState+" AND task_uuid=?",
			base64.StdEncoding.EncodeToString(data),
			qname,
			taskID)
	}
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}

	err = expectOneRowUpdated(op, wrs[0], st, true)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// Requeue moves the task from active to pending in the specified queue.
//   - serverID is the ID of the server
//   - aborted is true when re-queuing occurs because the server is stopping and
//     false for recurrent tasks
//
// The server ID is not updated when aborting.
// The unique key deadline is moved to (now + unique key TTL) for recurrent tasks
// with a unique key.
func (conn *Connection) requeueTask(now time.Time, serverID string, msg *base.TaskMessage, aborted bool) error {
	op := errors.Op("rqlite.requeueTask")

	var st *sqlite3.Statement
	if aborted {
		// server is stopping: just push back to pending
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='pending', deadline=NULL,"+
				" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+ // changed pndx=
				" WHERE queue_name=? AND state='active' AND task_uuid=?",
			msg.Queue,
			msg.ID)
	} else {
		scheduledAt := int64(0)
		state := pending
		// recurrent task:
		// - if the server id is provided, update 'sid'
		// - if the task has a unique key, move the unique key deadline
		uniqueKeyExpireAt := now.Add(time.Second * time.Duration(msg.UniqueKeyTTL))

		scheduledAtReq := ""
		scheduledAtPos := 0
		if msg.ReprocessAfter > 0 {
			state = scheduled
			scheduledAt = now.Add(time.Second * time.Duration(msg.ReprocessAfter)).Unix()
			scheduledAtReq = "scheduled_at=?,"
		}

		// note: make separate requests when length of server id is zero because
		//       rqlite does not support nil in parameterized request.

		if len(serverID) > 0 {
			if len(msg.UniqueKey) > 0 {
				scheduledAtPos = 4
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, sid=?, done_at=?, unique_key_deadline=?, "+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					serverID,
					now.Unix(),
					uniqueKeyExpireAt.Unix(),

					msg.Queue,
					msg.ID)
			} else {
				scheduledAtPos = 3
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, sid=?, done_at=?,"+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					serverID,
					now.Unix(),

					msg.Queue,
					msg.ID)
			}
		} else {
			if len(msg.UniqueKey) > 0 {
				scheduledAtPos = 3
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, sid=NULL, done_at=?, unique_key_deadline=?,"+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					now.Unix(),
					uniqueKeyExpireAt.Unix(),

					msg.Queue,
					msg.ID)
			} else {
				scheduledAtPos = 2
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL, sid=NULL, done_at=?,"+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					now.Unix(),

					msg.Queue,
					msg.ID)
			}
		}

		// insert the scheduledAt parameter if needed
		if msg.ReprocessAfter > 0 {
			st.Parameters = append(st.Parameters, nil /* use the zero value of the element type */)
			copy(st.Parameters[scheduledAtPos+1:], st.Parameters[scheduledAtPos:])
			st.Parameters[scheduledAtPos] = scheduledAt
		}

	}
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWError(op, wrs[0], err, st)
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs[0], st, len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) scheduleTasks(ctx context.Context, msgs ...*base.MessageBatch) error {
	op := errors.Op("rqlite.scheduleTasks")

	if len(msgs) == 0 {
		return nil
	}

	queues := make(map[string]bool)
	stmts := make([]*sqlite3.Statement, 0, len(msgs)*2)
	msgNdx := make([]int, 0, len(msgs))

	for _, bmsg := range msgs {
		msg := bmsg.Msg
		if !queues[msg.Queue] {
			stmts = append(stmts, conn.ensureQueueStatement(msg.Queue))
			queues[msg.Queue] = true
		}

		encoded, err := encodeMessage(msg)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
		}

		// NOTE: set 'pndx' in order to preserve order of insertion: when the task
		//       is put in pending state it keeps it.
		msgNdx = append(msgNdx, len(stmts))
		stmts = append(stmts, Statement(
			"INSERT INTO "+conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, scheduled_at, affinity_timeout, recurrent, pndx, state) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			bmsg.ProcessAt.UTC().Unix(),
			msg.ServerAffinity,
			msg.Recurrent,
			scheduled))
	}

	wrs, err := conn.WriteStmt(ctx, stmts...)
	var err0 error
	if err != nil {
		err0 = NewRqliteWsError(op, wrs, err, stmts)
	}
	if len(wrs) != len(stmts) {
		return NewRqliteWsError(
			op,
			wrs,
			errors.New(fmt.Sprintf("unexpected result count: %d, expected: %d", len(wrs), len(stmts))),
			stmts,
		)
	}

	allErrors := make(map[int]error)
	for i, ndx := range msgNdx {
		if wrs[ndx].RowsAffected == 0 {
			if wrs[ndx].Err != nil && strings.Contains(wrs[ndx].Err.Error(), "UNIQUE constraint failed") {
				err = errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
			} else {
				err = errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
			}
		} else {
			err = expectOneRowUpdated(op, wrs[ndx], stmts[ndx], true)
		}
		if err != nil {
			msgs[i].Err = err
			allErrors[i] = err
		}
	}

	if len(allErrors) > 0 {
		//if len(msgs) == 1 {
		//	return allErrors[0]
		//}
		return &errors.BatchError{Errors: allErrors}
	}
	if err0 != nil {
		return err0
	}
	return nil
}

func (conn *Connection) scheduleUniqueTasks(ctx context.Context, now time.Time, msgs ...*base.MessageBatch) error {
	op := errors.Op("rqlite.scheduleUniqueTasks")

	if len(msgs) == 0 {
		return nil
	}

	queues := make(map[string]bool)
	stmts := make([]*sqlite3.Statement, 0, len(msgs)*2)
	msgNdx := make([]int, 0, len(msgs))

	for _, bmsg := range msgs {
		msg := bmsg.Msg
		if !queues[msg.Queue] {
			stmts = append(stmts, conn.ensureQueueStatement(msg.Queue))
			queues[msg.Queue] = true
		}
		if bmsg.Msg.UniqueKeyTTL == 0 {
			bmsg.Msg.UniqueKeyTTL = int64(bmsg.UniqueTTL.Seconds())
		}
		encoded, err := encodeMessage(msg)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode task message: %v", err))
		}
		msgNdx = append(msgNdx, len(stmts))
		uniqueKeyExpirationLimit := now.Unix()
		if bmsg.ForceUnique {
			uniqueKeyExpirationLimit = math.MaxInt64
		}

		stmts = append(stmts, Statement(
			"INSERT INTO "+conn.table(TasksTable)+" (queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, unique_key_deadline, scheduled_at, affinity_timeout, recurrent, pndx, state)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?) "+
				" ON CONFLICT(unique_key) DO UPDATE SET"+
				"   queue_name=excluded.queue_name, "+
				"   type_name=excluded.type_name, "+
				"   task_uuid=excluded.task_uuid, "+
				"   unique_key=excluded.unique_key, "+
				"   task_msg=excluded.task_msg, "+
				"   task_timeout=excluded.task_timeout, "+
				"   task_deadline=excluded.task_deadline, "+
				"   unique_key_deadline=excluded.unique_key_deadline, "+
				"   scheduled_at=excluded.scheduled_at, "+
				"   affinity_timeout=excluded.affinity_timeout, "+
				"   recurrent=excluded.recurrent, "+
				"   pndx=excluded.pndx, "+
				"   state=excluded.state"+
				" WHERE ("+conn.table(TasksTable)+".unique_key_deadline<=? "+
				"    OR "+conn.table(TasksTable)+".unique_key_deadline IS NULL)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.UniqueKey,
			encoded,
			msg.Timeout,
			msg.Deadline,
			now.Add(bmsg.UniqueTTL).Unix(),
			bmsg.ProcessAt.UTC().Unix(),
			msg.ServerAffinity,
			msg.Recurrent,
			scheduled,
			uniqueKeyExpirationLimit))
	}

	wrs, err := conn.WriteStmt(ctx, stmts...)
	var err0 error
	if err != nil {
		err0 = NewRqliteWsError(op, wrs, err, stmts)
	}
	if len(wrs) != len(stmts) {
		return NewRqliteWsError(
			op,
			wrs,
			errors.New(fmt.Sprintf("unexpected result count: %d, expected: %d", len(wrs), len(stmts))),
			stmts,
		)
	}

	allErrors := make(map[int]error)
	for i, ndx := range msgNdx {
		if wrs[ndx].RowsAffected == 0 {
			if wrs[ndx].Err != nil && strings.Contains(wrs[ndx].Err.Error(), "UNIQUE constraint failed") {
				err = errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
			} else {
				err = errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
			}
		}
		if err != nil {
			msgs[i].Err = err
			allErrors[i] = err
		}
	}

	if len(allErrors) > 0 {
		//if len(msgs) == 1 {
		//	return allErrors[0]
		//}
		return &errors.BatchError{Errors: allErrors}
	}

	// make sure we don't swallow the error
	if err0 != nil {
		return err0
	}
	return nil
}

func (conn *Connection) retryTask(now time.Time, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	op := errors.Op("rqlite.retryCmd")

	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := encodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET task_msg=?, state='retry', "+
			" sid=NULL, done_at=?, retry_at=?, failed=?, cleanup_at=? "+
			" WHERE queue_name=? AND state='active' AND task_uuid=?",
		encoded,
		now.Unix(),
		processAt.UTC().Unix(),
		isFailure,
		now.Add(statsTTL).Unix(), //expireAt
		msg.Queue,
		msg.ID)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if len(wrs) == 0 {
		wrs = append(wrs, sqlite3.WriteResult{Err: err})
	}
	if err != nil {
		return NewRqliteWError(op, wrs[0], err, st)
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs[0], st, len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) archiveTask(now time.Time, msg *base.TaskMessage, errMsg string) error {
	op := errors.Op("rqlite.archiveTask")
	state := active

	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := encodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	cutoff := now.AddDate(0, 0, -conn.config.ArchivedExpirationInDays)

	affinity := " affinity_timeout=0, "
	if len(errMsg) > 0 {
		affinity = " affinity_timeout=-affinity_timeout, "
	}
	//PENDING(GIL) - TODO: add cleanup to respect max number of tasks in archive
	_ = conn.config.MaxArchiveSize
	stmts := []*sqlite3.Statement{
		Statement(
			"DELETE FROM "+conn.table(TasksTable)+
				" WHERE archived_at<? OR cleanup_at<?",
			cutoff.Unix(),
			now.Unix()),
		Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='archived', task_msg=?, "+affinity+
				" archived_at=?, cleanup_at=?, failed=?, unique_key_deadline=NULL "+
				" WHERE queue_name=? AND state=? AND task_uuid=?",
			encoded,
			now.Unix(),
			now.Add(statsTTL).Unix(), //expireAt
			len(errMsg) > 0,
			msg.Queue,
			state,
			msg.ID),
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs[1], stmts[1], len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}

	return nil
}

func (conn *Connection) forwardTasks(now time.Time, qname, src string) (int, error) {
	op := errors.Op("rqlite.forwardTask")

	srcAt := src + "_at"
	st := Statement(
		"UPDATE "+conn.table(TasksTable)+" SET state='pending' "+
			" WHERE queue_name=? AND state=? AND "+srcAt+"<?",
		qname,
		src,
		now.Unix())
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWError(op, wrs[0], err, st)
	}

	if len(wrs) == 0 {
		return 0, nil
	}
	return int(wrs[0].RowsAffected), nil
}

// KEYS[0] -> queue name
// ARGV[0] -> deadline unix timestamp
func (conn *Connection) listDeadlineExceededTasks(qname string, deadline time.Time) ([]*base.TaskMessage, error) {
	op := errors.Op("rqlite.listDeadlineExceededTasks")

	st := Statement(
		"SELECT task_msg FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND deadline<=?",
		qname,
		deadline.Unix())
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRError(op, qrs[0], err, st)
	}

	if len(qrs) == 0 || qrs[0].NumRows() == 0 {
		return nil, nil
	}

	qr := qrs[0]
	data := make([]string, 0, qr.NumRows())
	for qr.Next() {
		s := ""
		err = qr.Scan(&s)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		data = append(data, s)
	}

	ret := make([]*base.TaskMessage, 0, len(data))
	for _, s := range data {
		msg, err := decodeMessage([]byte(s))
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		ret = append(ret, msg)
	}
	return ret, nil
}
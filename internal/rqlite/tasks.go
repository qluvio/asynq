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
	ndx                int64
	queueName          string
	typeName           string
	taskUuid           string
	uniqueKey          string
	uniqueKeyDeadline  int64
	taskMsg            string
	taskTimeout        int64
	taskDeadline       int64
	srvAffinityTimeout int64
	pndx               int64
	state              string
	scheduledAt        int64
	deadline           int64
	retryAt            int64
	doneAt             int64
	failed             bool
	archivedAt         int64
	cleanupAt          int64
	retainUntil        int64
	sid                string
	affinityTimeout    int64
	recurrent          bool
	result             string
	pendingSince       int64
	msg                *base.TaskMessage
}

const selectTaskRow = "SELECT ndx, queue_name, type_name, task_uuid, unique_key, unique_key_deadline, task_msg, task_timeout, task_deadline, server_affinity, pndx, state, scheduled_at, deadline, retry_at, done_at, failed, archived_at, cleanup_at, retain_until, sid, affinity_timeout, recurrent, result, pending_since "

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
			&s.srvAffinityTimeout,
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

	if state == completed {
		return conn.listCompletedTasksPaged(queue, page, orderBy)
	}

	op := errors.Op("listTasks")
	st := Statement(
		selectTaskRow+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? "+
			" AND state=? ",
		queue,
		state)
	if page != nil {
		if len(orderBy) == 0 || page.StartAfterUuid != "" {
			// if option startAfter is used we order by insertion order
			orderBy = "ndx"
		}
		if page.StartAfterUuid != "" {
			st = st.Append("AND ndx > (SELECT ndx "+
				" FROM "+conn.table(TasksTable)+
				" WHERE task_uuid = ?)", page.StartAfterUuid)
		}
		st = st.Append(fmt.Sprintf(" ORDER BY %s LIMIT %d OFFSET %d", orderBy, page.Size, page.Start()))

	} else {
		if len(orderBy) == 0 {
			orderBy = "ndx"
		}
		st = st.Append(fmt.Sprintf(" ORDER BY %s", orderBy))
	}
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	return parseTaskRows(qrs[0])
}

// listAllTasks is a debug function to list all tasks in the given queue
func (conn *Connection) listAllTasks(queue string) ([]*taskRow, error) {
	op := errors.Op("listAllTasks")
	st := Statement(
		selectTaskRow+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? ORDER BY 'ndx'",
		queue)
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
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
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
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
		return 0, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	count := int64(0)
	qrs[0].Next()
	err = qrs[0].Scan(&count)
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	return count, nil
}

func (conn *Connection) deleteTasks(queue string, state string) (int64, error) {
	op := errors.Op("rqlite.deleteTasks")

	if state == completed {
		return conn.deleteCompletedTasks(queue)
	}

	st := Statement(
		"DELETE FROM "+conn.table(TasksTable)+" WHERE queue_name=? AND state=?",
		queue,
		state)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}
	ret := wrs[0].RowsAffected

	if ret == 0 {
		// look into completed
		ret2, err2 := conn.deleteCompletedTask(queue, taskid)
		if ret2 == 1 && err2 == nil {
			ret = 1
		}
	}

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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
			"INSERT INTO "+conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, server_affinity, recurrent, pndx, state, pending_since) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?, ?)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			msg.ServerAffinity,
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
			err = expectOneRowUpdated(op, wrs, ndx, stmts[ndx], true)
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
			"INSERT INTO "+conn.table(TasksTable)+" (queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, server_affinity, unique_key_deadline, recurrent, pndx, state, pending_since)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?, ?) "+
				" ON CONFLICT(unique_key) DO UPDATE SET "+
				"   queue_name=excluded.queue_name, "+
				"   type_name=excluded.type_name, "+
				"   task_uuid=excluded.task_uuid, "+
				"   unique_key=excluded.unique_key, "+
				"   task_msg=excluded.task_msg, "+
				"   task_timeout=excluded.task_timeout, "+
				"   task_deadline=excluded.task_deadline, "+
				"   server_affinity=excluded.server_affinity, "+
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
			msg.ServerAffinity,
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

// setTaskDone removes the task from active queue to mark the task as done and
// set its state to 'processed'.
// It removes a uniqueness lock acquired by the task, if any.
func (conn *Connection) setTaskDone(now time.Time, serverID string, msg *base.TaskMessage) error {
	op := errors.Op("rqlite.setTaskDone")

	stmts := []*sqlite3.Statement{
		conn.processedStatsStatement(now, msg.Queue),
		Statement(
			"DELETE FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND state='active'",
			msg.ID),
	}

	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}

	err = expectOneRowUpdated(op, wrs, 0, stmts[0], true)
	if err != nil {
		return err
	}
	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs, 1, stmts[1], len(msg.UniqueKey) == 0)
	if err != nil {
		return err
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}

	err = expectOneRowUpdated(op, wrs, 0, st, true)
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
		if len(serverID) > 0 {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state='pending', deadline=NULL, pending_since=?, "+
					" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, "+ // changed pndx=
					" sid=(SELECT sid FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND sid=? AND affinity_timeout > 0)"+
					" WHERE queue_name=? AND state='active' AND task_uuid=?",
				now.UnixNano(),
				msg.ID,
				serverID,
				msg.Queue,
				msg.ID)
		} else {
			st = Statement(
				"UPDATE "+conn.table(TasksTable)+" SET state='pending', deadline=NULL, pending_since=?, "+
					" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, "+ // changed pndx=
					" sid=(SELECT sid FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND affinity_timeout > 0)"+
					" WHERE queue_name=? AND state='active' AND task_uuid=?",
				now.UnixNano(),
				msg.ID,
				msg.Queue,
				msg.ID)
		}
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
				scheduledAtPos = 5
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL,"+
						" sid=(SELECT sid FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND sid=? AND affinity_timeout > 0),"+
						" done_at=?, unique_key_deadline=?, "+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					msg.ID,
					serverID,
					now.Unix(),
					uniqueKeyExpireAt.Unix(),
					msg.Queue,
					msg.ID)
			} else {
				scheduledAtPos = 4
				st = Statement(
					"UPDATE "+conn.table(TasksTable)+" SET state=?, deadline=NULL,"+
						" sid=(SELECT sid FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND sid=? AND affinity_timeout > 0),"+
						" done_at=?,"+scheduledAtReq+
						" pndx=(SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1 "+
						" WHERE queue_name=? AND state='active' AND task_uuid=?",
					state,
					msg.ID,
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
			st.Arguments = append(st.Arguments, nil /* use the zero value of the element type */)
			copy(st.Arguments[scheduledAtPos+1:], st.Arguments[scheduledAtPos:])
			st.Arguments[scheduledAtPos] = scheduledAt
		}

	}
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs, 0, st, len(msg.UniqueKey) == 0)
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
			"INSERT INTO "+conn.table(TasksTable)+"(queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, server_affinity, scheduled_at, affinity_timeout, recurrent, pndx, state) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?)",
			msg.Queue,
			msg.Type,
			msg.ID,
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			msg.ServerAffinity,
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
			err = expectOneRowUpdated(op, wrs, ndx, stmts[ndx], true)
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
			"INSERT INTO "+conn.table(TasksTable)+" (queue_name, type_name, task_uuid, unique_key, task_msg, task_timeout, task_deadline, server_affinity, unique_key_deadline, scheduled_at, affinity_timeout, recurrent, pndx, state)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(pndx),0) FROM "+conn.table(TasksTable)+")+1, ?) "+
				" ON CONFLICT(unique_key) DO UPDATE SET"+
				"   queue_name=excluded.queue_name, "+
				"   type_name=excluded.type_name, "+
				"   task_uuid=excluded.task_uuid, "+
				"   unique_key=excluded.unique_key, "+
				"   task_msg=excluded.task_msg, "+
				"   task_timeout=excluded.task_timeout, "+
				"   task_deadline=excluded.task_deadline, "+
				"   server_affinity=excluded.server_affinity, "+
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
			msg.ServerAffinity,
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

	stmts := []*sqlite3.Statement{
		Statement(
			"UPDATE "+conn.table(TasksTable)+" SET task_msg=?, state='retry', "+
				" sid=NULL, deadline=NULL, done_at=?, retry_at=?, failed=?, cleanup_at=? "+
				" WHERE queue_name=? AND state='active' AND task_uuid=?",
			encoded,
			now.Unix(),
			processAt.UTC().Unix(),
			isFailure,
			now.Add(statsTTL).Unix(), //expireAt
			msg.Queue,
			msg.ID),
	}
	if isFailure {
		stmts = append(stmts, conn.failedStatsStatement(now, msg.Queue))
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if len(wrs) == 0 {
		wrs = append(wrs, sqlite3.WriteResult{Err: err})
	}
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs, 0, stmts[0], len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	if isFailure {
		err = expectOneRowUpdated(op, wrs, 1, stmts[1], true)
		if err != nil {
			return err
		}
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
		conn.failedStatsStatement(now, msg.Queue),
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}

	// with uniqueKey the unique lock may have been forced to put the task
	// again in state 'pending' - see enqueueUniqueMessages
	err = expectOneRowUpdated(op, wrs, 1, stmts[1], len(msg.UniqueKey) == 0)
	if err != nil {
		return err
	}
	err = expectOneRowUpdated(op, wrs, 2, stmts[2], true)
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
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
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
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
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

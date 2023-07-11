package rqlite

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

const selectCompletedTaskRow = "SELECT ndx, queue_name, type_name, task_uuid, task_msg, deadline, done_at, retain_until, sid, result "

func parseCompletedTaskRows(qr sqlite3.QueryResult) ([]*taskRow, error) {
	op := errors.Op("rqlite.parseCompletedTaskRows")

	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*taskRow, 0)

	for qr.Next() {
		s := &taskRow{
			state: completed,
		}
		err := qr.Scan(
			&s.ndx,
			&s.queueName,
			&s.typeName,
			&s.taskUuid,
			&s.taskMsg,
			&s.deadline,
			&s.doneAt,
			&s.retainUntil,
			&s.sid,
			&s.result)
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

func (conn *Connection) deleteCompletedTasks(queue string) (int64, error) {
	op := errors.Op("rqlite.deleteCompletedTasks")

	st := Statement(
		"DELETE FROM "+conn.table(CompletedTasksTable)+" WHERE queue_name=? ",
		queue)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}

	return wrs[0].RowsAffected, nil
}

func (conn *Connection) deleteCompletedTask(queue string, taskid string) (int64, error) {
	op := errors.Op("rqlite.deleteTask")

	st := Statement(
		"DELETE FROM "+conn.table(CompletedTasksTable)+
			" WHERE queue_name=? AND task_uuid=? ",
		queue,
		taskid)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return 0, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}
	ret := wrs[0].RowsAffected

	// returning ret should be enough to respect conventional return values for inspector
	return ret, nil
}

// deleteExpiredCompletedTasks deletes completed tasks whose retention period
// expired to the archived set.
func (conn *Connection) deleteExpiredCompletedTasks(now time.Time, qname string) error {
	op := errors.Op("rqlite.deleteExpiredCompletedTasks")

	stmt := Statement(
		"DELETE FROM "+conn.table(CompletedTasksTable)+
			" WHERE queue_name=? AND retain_until<?",
		qname,
		now.Unix())
	wrs, err := conn.WriteStmt(conn.ctx(), stmt)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{stmt})
	}
	return nil
}

// listCompletedTasksPaged lists completed tasks.
func (conn *Connection) listCompletedTasksPaged(queue string, page *base.Pagination, orderBy string) ([]*taskRow, error) {
	op := errors.Op("listCompletedTasks")
	st := Statement(
		selectCompletedTaskRow+
			" FROM "+conn.table(CompletedTasksTable)+
			" WHERE queue_name=? ",
		queue)
	if page != nil {
		if len(orderBy) == 0 || page.StartAfterUuid != "" {
			// if option startAfter is used we order by insertion order
			orderBy = "ndx"
		}
		if page.StartAfterUuid != "" {
			st = st.Append("AND ndx > (SELECT ndx "+
				" FROM "+conn.table(CompletedTasksTable)+
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
	return parseCompletedTaskRows(qrs[0])
}

func (conn *Connection) getCompletedTask(queue string, id string) (*taskRow, error) {
	op := errors.Op("rqlite.getCompletedTask")
	st := Statement(
		selectCompletedTaskRow+
			" FROM "+conn.table(CompletedTasksTable)+
			" WHERE queue_name=? "+
			" AND task_uuid=?",
		queue,
		id)
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	rows, err := parseCompletedTaskRows(qrs[0])
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

func (conn *Connection) getCompletedTaskInfo(now time.Time, qname string, taskId string) (*base.TaskInfo, error) {
	var op errors.Op = "getCompletedTaskInfo"
	tr, err := conn.getCompletedTask(qname, taskId)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return getTaskInfo(op, now, tr)
}

// setTaskCompleted removes the task from active queue to mark the task as done
// and set its state to 'completed' by removing the task from the tasks table and
// inserting it into the completed_tasks table.
func (conn *Connection) setTaskCompleted(now time.Time, serverID string, msg *base.TaskMessage) error {
	op := errors.Op("rqlite.setTaskCompleted")

	if now.IsZero() {
		if msg.CompletedAt != 0 {
			now = time.Unix(msg.CompletedAt, 0)
		} else {
			now = time.Now()
		}
	}
	msg.CompletedAt = now.Unix()
	retainUntil := now.Add(time.Second * time.Duration(msg.Retention))
	// re-encode message to have completedAt
	encoded, err := encodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	var st *sqlite3.Statement
	if len(serverID) > 0 {
		st = Statement(
			"INSERT INTO "+conn.table(CompletedTasksTable)+
				" (ndx, deadline, result, queue_name, type_name, task_uuid, task_msg, done_at, retain_until, sid) "+
				" SELECT ndx, deadline, result, ?, ?, ?, ?, ?, ?, ? "+
				"   FROM "+conn.table(TasksTable)+
				"   WHERE task_uuid=? AND state='active' ",
			msg.Queue,
			msg.Type,
			msg.ID,
			encoded,
			msg.CompletedAt,
			retainUntil.Unix(),
			serverID,
			msg.ID)
	} else {
		st = Statement(
			"INSERT INTO "+conn.table(CompletedTasksTable)+
				" (ndx, deadline, result, queue_name, type_name, task_uuid, task_msg, done_at, retain_until) "+
				" SELECT ndx, deadline, result, ?, ?, ?, ?, ?, ? "+
				"   FROM "+conn.table(TasksTable)+
				"   WHERE task_uuid=? AND state='active' ",
			msg.Queue,
			msg.Type,
			msg.ID,
			encoded,
			now.Unix(),
			retainUntil.Unix(),
			msg.ID)
	}
	delSt := Statement(
		"DELETE FROM "+conn.table(TasksTable)+" WHERE task_uuid=? AND state='active'",
		msg.ID)

	stmts := []*sqlite3.Statement{
		st,
		conn.processedStatsStatement(now, msg.Queue),
		delSt,
	}

	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}

	err = expectOneRowUpdated(op, wrs, 0, stmts[0], true)
	if err != nil {
		return err
	}
	err = expectOneRowUpdated(op, wrs, 1, stmts[1], true)
	if err != nil {
		return err
	}
	err = expectOneRowUpdated(op, wrs, 2, stmts[2], true)
	if err != nil {
		return err
	}
	return nil
}

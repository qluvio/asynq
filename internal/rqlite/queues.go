package rqlite

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

type queueRow struct {
	queueName string
	state     string
}

func (conn *Connection) ensureQueueStatement(queue string) *sqlite3.Statement {
	return Statement(
		"INSERT INTO "+conn.table(QueuesTable)+" (queue_name, state) VALUES(?, 'active') "+
			" ON CONFLICT(queue_name) DO NOTHING;",
		queue)
}

func (conn *Connection) EnsureQueue(queue string) error {
	st := conn.ensureQueueStatement(queue)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWsError("EnsureQueue", wrs, err, []*sqlite3.Statement{st})
	}
	return nil
}

func (conn *Connection) GetQueue(qname string) (*queueRow, error) {
	var op errors.Op = "getQueue"

	st := Statement(
		"SELECT queue_name,state FROM "+conn.table(QueuesTable)+" WHERE queue_name=?",
		qname)
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	if len(qrs) == 0 || qrs[0].NumRows() == 0 {
		return nil, nil
	}
	res := qrs[0]
	if res.NumRows() > 1 {
		return nil, errors.E(op, fmt.Sprintf("multiple queues: [%s], res: %v", qname, res))
	}
	q := &queueRow{}
	res.Next()
	err = res.Scan(&q.queueName, &q.state)
	if err != nil {
		return nil, errors.E(op, err)
	}

	return q, nil
}

func (conn *Connection) pauseQueue(queue string, b bool) error {
	op := errors.Op("pauseQueue")
	val := paused
	if !b {
		val = active
	}
	st := Statement("UPDATE "+conn.table(QueuesTable)+" SET state=? "+
		" WHERE queue_name=? AND state!=? ",
		val,
		queue,
		val)
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}
	switch wrs[0].RowsAffected {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, fmt.Sprintf("queue %v not changed to %s", queue, val))
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("queue %v:%d rows changed to %s", queue, wrs[0].RowsAffected, val))
	}
}

func (conn *Connection) removeQueue(queue string, force bool) (int64, error) {
	op := errors.Op("removeQueue")

	st := Statement(
		"DELETE FROM "+conn.table(QueuesTable)+
			" WHERE queue_name=? AND (SELECT COUNT(*) FROM "+conn.table(TasksTable)+
			" WHERE "+conn.table(TasksTable)+".queue_name=?)=0",
		queue,
		queue)
	if force {
		// fail if there's still any active task
		st = Statement(
			"DELETE FROM "+conn.table(QueuesTable)+
				" WHERE queue_name=? AND (SELECT COUNT(*) FROM "+conn.table(TasksTable)+
				" WHERE "+conn.table(TasksTable)+".queue_name=? AND "+conn.table(TasksTable)+".state='active')=0",
			queue,
			queue)
	}
	stmts := []*sqlite3.Statement{
		st,
		Statement(
			"DELETE FROM "+conn.table(TasksTable)+
				" WHERE queue_name=? AND NOT EXISTS (SELECT queue_name FROM "+conn.table(QueuesTable)+
				" WHERE "+conn.table(QueuesTable)+".queue_name=?)",
			queue,
			queue),
	}

	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return 0, NewRqliteWsError(op, wrs, err, stmts)
	}

	ret := wrs[0].RowsAffected
	if ret > 1 {
		// something really wrong there
		return ret, errors.E(op, "multiple rows updated ("+st.String()+")")
	}
	// enforce conventional return values for inspector
	if ret == 0 {
		qs, err := conn.listQueues(queue)
		if err == nil && len(qs) > 0 {
			ret = -1
			if force {
				ret = -2
			}
		}
	}
	return ret, nil
}

func (conn *Connection) listQueues(queue ...string) ([]*queueRow, error) {
	op := errors.Op("listQueues")
	st := Statement("SELECT queue_name,state" +
		" FROM " + conn.table(QueuesTable) + " ")
	if len(queue) > 0 {
		st = st.Append(" WHERE queue_name=? ", queue[0])
	}

	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}

	qr := qrs[0]
	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*queueRow, 0)

	for qr.Next() {
		s := &queueRow{}
		err = qr.Scan(
			&s.queueName,
			&s.state)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		ret = append(ret, s)
	}
	return ret, nil
}

func (conn *Connection) currentStats(now time.Time, queue string) (*base.Stats, error) {
	op := errors.Op("currentStats")
	stmts := []*sqlite3.Statement{
		Statement(
			"SELECT queue_name, state "+
				" FROM "+conn.table(QueuesTable)+" WHERE queue_name=? ", queue),
		Statement(
			selectTaskRow+
				" FROM "+conn.table(TasksTable)+
				" WHERE queue_name=? ", queue),
		Statement(
			"SELECT COUNT(*)"+
				" FROM "+conn.table(CompletedTasksTable)+
				" WHERE queue_name=? ", queue),
		conn.queueDayStatsStatement(queue, now),
	}

	qrs, err := conn.QueryStmt(conn.ctx(), stmts...)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, stmts)
	}
	err = expectQueryResultCount(op, 4, qrs)
	if err != nil {
		return nil, err
	}

	var ret *base.Stats
	if qrs[0].Next() {
		ret = &base.Stats{
			Timestamp: now.UTC(),
		}
		state := ""
		err = qrs[0].Scan(
			&ret.Queue,
			&state)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		ret.Paused = state == paused

		oldestPending := now.UnixNano()
		tasks, err := parseTaskRows(qrs[1])
		if err != nil {
			return nil, err
		}
		for _, task := range tasks {
			switch task.state {
			case pending:
				ret.Pending++
				if task.pendingSince < oldestPending {
					oldestPending = task.pendingSince
				}
			case active:
				ret.Active++
			case scheduled:
				ret.Scheduled++
			case retry:
				ret.Retry++
			case archived:
				ret.Archived++
			case completed:
				ret.Completed++
			}
		}
		ret.Latency = now.Sub(time.Unix(0, oldestPending))

		if qrs[2].Next() {
			completed := 0
			err := qrs[2].Scan(&completed)
			if err != nil {
				return nil, err
			}
			ret.Completed += completed
		}

		// processed are not included in size
		ret.Size = ret.Pending + ret.Active + ret.Scheduled + ret.Retry + ret.Archived + ret.Completed

		ds, err := parseDailyStatsRow(qrs[3])
		ret.Processed = ds.Processed
		ret.Failed = ds.Failed

	} else if qrs[1].Next() {
		// return error ?
		slog.Warn(fmt.Sprintf("found %d tasks in for non existent queues '%s'",
			qrs[1].NumRows(),
			queue))
	}
	return ret, nil
}

func (conn *Connection) historicalStats(now time.Time, queue string, ndays int) ([]*base.DailyStats, error) {
	return conn.queueStats(now, queue, ndays)
}

func (conn *Connection) getTaskInfo(now time.Time, qname string, taskid string) (*base.TaskInfo, error) {
	var op errors.Op = "getTaskInfo"

	tr, err := conn.getTask(qname, taskid)
	if err == nil {
		return getTaskInfo(op, now, tr)
	} else if !errors.IsTaskNotFound(err) {
		return nil, errors.E(op, errors.Internal, err)
	}
	// fall back to completed if not found
	ret, err2 := conn.getCompletedTaskInfo(now, qname, taskid)
	if err2 != nil {
		if errors.IsTaskNotFound(err2) {
			// return the initial 'not found' error
			return nil, errors.E(op, errors.Internal, err)
		}
		// last error is more exotic: return it
		return nil, errors.E(op, errors.Internal, err2)
	}
	return ret, nil
}

func getTaskInfo(op errors.Op, now time.Time, tr *taskRow) (*base.TaskInfo, error) {

	state, err := base.TaskStateFromString(tr.state)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	nextProcessAt := time.Time{}
	switch tr.state {
	case pending:
		nextProcessAt = now
	case scheduled:
		nextProcessAt = time.Unix(tr.scheduledAt, 0).UTC()
	case retry:
		nextProcessAt = time.Unix(tr.retryAt, 0).UTC()
	}
	nextProcessAt = nextProcessAt.UTC()

	var result []byte
	if tr.result != "" {
		var err error
		result, err = base64.StdEncoding.DecodeString(tr.result)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
	}

	return &base.TaskInfo{
		Message:       tr.msg,
		State:         state,
		NextProcessAt: nextProcessAt,
		Result:        result,
	}, nil
}

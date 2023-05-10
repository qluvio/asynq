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
			" WHERE queue_name=? AND (SELECT COUNT(*) FROM "+conn.table(conn.table(TasksTable))+
			" WHERE "+conn.table(TasksTable)+".queue_name=?)=0",
		queue,
		queue)
	if force {
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
	st := Statement("SELECT queue_name, state " +
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
	}
	qrs, err := conn.QueryStmt(conn.ctx(), stmts...)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, stmts)
	}
	err = expectQueryResultCount(op, 2, qrs)
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
				if task.failed {
					ret.Failed++
				}
			case archived:
				ret.Archived++
				if task.failed {
					ret.Failed++
				}
			case completed:
				ret.Completed++
				ret.Processed++
			case processed:
				ret.Processed++
			}
		}
		ret.Latency = now.Sub(time.Unix(0, oldestPending))
		// processed are not included in size
		ret.Size = ret.Pending + ret.Active + ret.Scheduled + ret.Retry + ret.Archived
		ret.Processed += ret.Failed

	} else if qrs[1].Next() {
		// return error ?
		slog.Warn(fmt.Sprintf("found %d tasks in for non existent queues '%s'",
			qrs[1].NumRows(),
			queue))
	}
	return ret, nil
}

func (conn *Connection) historicalStats(now time.Time, queue string, ndays int) ([]*base.DailyStats, error) {
	op := errors.Op("historicalStats")
	const day = 24 * time.Hour

	stmts := make([]*sqlite3.Statement, 0, ndays*3)
	last := now.Unix()

	for i := 0; i < ndays; i++ {
		first := now.Add(-time.Duration(i+1) * day).Unix()

		// processed
		stmts = append(stmts, Statement("SELECT COUNT(*) "+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND state='processed' AND done_at>? AND done_at<=?",
			queue,
			first,
			last))
		// retry/failed
		stmts = append(stmts, Statement("SELECT COUNT(*) "+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND state='retry' AND failed=true AND done_at>? AND done_at<=?",
			queue,
			first,
			last))
		// archived/failed
		stmts = append(stmts, Statement("SELECT COUNT(*) "+
			" FROM "+conn.table(TasksTable)+
			" WHERE queue_name=? AND state='archived' AND failed=true AND archived_at>? AND archived_at<=?",
			queue,
			first,
			last))
		last = first
	}

	qrs, err := conn.QueryStmt(conn.ctx(), stmts...)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, stmts)
	}
	err = expectQueryResultCount(op, ndays*3, qrs)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	ret := make([]*base.DailyStats, 0, ndays)
	for i := 0; i < ndays; i++ {
		ts := now.Add(-time.Duration(i) * 24 * time.Hour)
		ndx := i * 3

		nbProcessed := 0
		qrs[ndx].Next()
		err = qrs[ndx].Scan(&nbProcessed)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}

		nbRetry := 0
		qrs[ndx+1].Next()
		err = qrs[ndx+1].Scan(&nbRetry)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}

		nbArchived := 0
		qrs[ndx+2].Next()
		err = qrs[ndx+2].Scan(&nbArchived)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}

		ret = append(ret, &base.DailyStats{
			Queue:     queue,
			Processed: nbProcessed,
			Failed:    nbArchived + nbRetry,
			Time:      ts,
		})
	}
	return ret, nil
}

func (conn *Connection) getTaskInfo(now time.Time, qname string, taskid string) (*base.TaskInfo, error) {
	var op errors.Op = "getTaskInfo"

	tr, err := conn.getTask(qname, taskid)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return getTaskInfo(op, now, tr)
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

package rqlite

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/rqlite/gorqlite"
)

type queueRow struct {
	queueName string
	state     string
}

func ensureQueueStatement(queue string) string {
	return fmt.Sprintf(
		"INSERT INTO "+QueuesTable+" (queue_name, state) VALUES('%s', 'active') "+
			" ON CONFLICT(queue_name) DO NOTHING;",
		queue)
}

func EnsureQueue(conn *gorqlite.Connection, queue string) error {
	st := ensureQueueStatement(queue)
	res, err := conn.WriteOne(st)
	if err != nil {
		return NewRqliteWError("EnsureQueue", res, err, st)
	}
	return nil
}

func GetQueue(conn *gorqlite.Connection, qname string) (*queueRow, error) {
	var op errors.Op = "getQueue"

	st := fmt.Sprintf(
		"SELECT queue_name,state FROM "+QueuesTable+" WHERE queue_name='%s'",
		qname)
	res, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError("getQueue", res, err, st)
	}
	if res.NumRows() > 1 {
		return nil, errors.E(op, fmt.Sprintf("multiple queues: [%s], res: %v", qname, res))
	}
	if res.NumRows() == 0 {
		return nil, nil
	}
	q := &queueRow{}
	res.Next()
	err = res.Scan(&q.queueName, &q.state)
	if err != nil {
		return nil, errors.E(op, err)
	}

	return q, nil
}

func pauseQueue(conn *gorqlite.Connection, queue string, b bool) error {
	op := errors.Op("pauseQueue")
	val := paused
	if !b {
		val = active
	}
	st := fmt.Sprintf("UPDATE "+QueuesTable+" SET state='%s' "+
		" WHERE queue_name='%s' AND state!='%s' ",
		val,
		queue,
		val)
	wr, err := conn.WriteOne(st)
	if err != nil {
		return NewRqliteWError(op, wr, err, st)
	}
	switch wr.RowsAffected {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, fmt.Sprintf("queue %v not changed to %s", queue, val))
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("queue %v:%d rows changed to %s", queue, wr.RowsAffected, val))
	}
}

func removeQueue(conn *gorqlite.Connection, queue string, force bool) (int64, error) {
	op := errors.Op("removeQueue")

	st := fmt.Sprintf(
		"DELETE FROM "+QueuesTable+
			" WHERE queue_name='%s' AND (SELECT COUNT(*) FROM "+TasksTable+
			" WHERE "+TasksTable+".queue_name='%s')=0",
		queue,
		queue)
	if force {
		st = fmt.Sprintf(
			"DELETE FROM "+QueuesTable+
				" WHERE queue_name='%s' AND (SELECT COUNT(*) FROM "+TasksTable+
				" WHERE "+TasksTable+".queue_name='%s' AND "+TasksTable+".state='active')=0",
			queue,
			queue)
	}
	stmts := []string{
		st,
		fmt.Sprintf(
			"DELETE FROM "+TasksTable+
				" WHERE queue_name='%s' AND NOT EXISTS (SELECT queue_name FROM "+QueuesTable+
				" WHERE "+QueuesTable+".queue_name='%s')",
			queue,
			queue),
	}

	wrs, err := conn.Write(stmts)
	if err != nil {
		return 0, NewRqliteWsError(op, wrs, err, stmts)
	}

	ret := wrs[0].RowsAffected
	if ret > 1 {
		// something really wrong there
		return ret, errors.E(op, "multiple rows updated ("+st+")")
	}
	// enforce conventional return values for inspector
	if ret == 0 {
		qs, err := listQueues(conn, queue)
		if err == nil && len(qs) > 0 {
			ret = -1
			if force {
				ret = -2
			}
		}
	}
	return ret, nil
}

func listQueues(conn *gorqlite.Connection, queue ...string) ([]*queueRow, error) {
	op := errors.Op("listQueues")
	st := "SELECT queue_name, state " +
		" FROM " + QueuesTable
	if len(queue) > 0 {
		st += fmt.Sprintf(" WHERE queue_name='%s' ", queue[0])
	}

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}

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

func currentStats(conn *gorqlite.Connection, queue string) (*base.Stats, error) {
	op := errors.Op("currentStats")
	stmts := []string{
		fmt.Sprintf(
			"SELECT queue_name, state "+
				" FROM "+QueuesTable+" WHERE queue_name='%s' ", queue),
		fmt.Sprintf(
			"SELECT ndx, queue_name, task_uuid, unique_key, unique_key_deadline, task_msg, task_timeout, task_deadline, pndx, state, scheduled_at, deadline, retry_at, done_at, failed, archived_at, cleanup_at "+
				" FROM "+TasksTable+
				" WHERE queue_name='%s' ", queue),
	}
	qrs, err := conn.Query(stmts)
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
			Timestamp: time.Now().UTC(),
		}
		state := ""
		err = qrs[0].Scan(
			&ret.Queue,
			&state)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		ret.Paused = state == paused

		tasks, err := parseTaskRows(qrs[1])
		if err != nil {
			return nil, err
		}
		for _, task := range tasks {
			switch task.state {
			case pending:
				ret.Pending++
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
			case processed:
				ret.Processed++
			}
		}
		// processed are not included
		ret.Size = ret.Pending + ret.Active + ret.Scheduled + ret.Retry + ret.Archived

	} else if qrs[1].Next() {
		// return error ?
		slog.Warn(fmt.Sprintf("found %d tasks in for non existent queues '%s'",
			qrs[1].NumRows(),
			queue))
	}
	return ret, nil
}

func historicalStats(conn *gorqlite.Connection, queue string, ndays int) ([]*base.DailyStats, error) {
	op := errors.Op("historicalStats")
	const day = 24 * time.Hour

	stmts := make([]string, 0, ndays*3)
	now := utc.Now()
	last := now.Unix()

	for i := 0; i < ndays; i++ {
		first := now.Add(-time.Duration(i+1) * day).Unix()

		// processed
		stmts = append(stmts, fmt.Sprintf("SELECT COUNT(*) "+
			" FROM "+TasksTable+
			" WHERE queue_name='%s' AND state='processed' AND done_at>%d AND done_at<=%d",
			queue,
			first,
			last))
		// retry/failed
		stmts = append(stmts, fmt.Sprintf("SELECT COUNT(*) "+
			" FROM "+TasksTable+
			" WHERE queue_name='%s' AND state='retry' AND failed=true AND done_at>%d AND done_at<=%d",
			queue,
			first,
			last))
		// archived/failed
		stmts = append(stmts, fmt.Sprintf("SELECT COUNT(*) "+
			" FROM "+TasksTable+
			" WHERE queue_name='%s' AND state='archived' AND failed=true AND archived_at>%d AND archived_at<=%d",
			queue,
			first,
			last))
		last = first
	}

	qrs, err := conn.Query(stmts)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, stmts)
	}
	err = expectQueryResultCount(op, ndays*3, qrs)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	ret := make([]*base.DailyStats, 0, ndays)
	for i := 0; i < ndays; i++ {
		ts := now.Add(-time.Duration(i) * 24 * time.Hour).Time
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

func getTaskInfo(conn *gorqlite.Connection, qname string, taskid uuid.UUID) (*base.TaskInfo, error) {
	var op errors.Op = "getTaskInfo"

	tr, err := getTask(conn, qname, taskid.String())
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	var state base.TaskState
	if tr.state != processed {
		// PENDING(GIL): 'processed' is not in base.TaskState
		state, err = base.TaskStateFromString(tr.state)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
	}

	nextProcessAt := time.Time{}
	switch tr.state {
	case pending:
		nextProcessAt = utc.Now().Time
	case scheduled:
		nextProcessAt = time.Unix(tr.scheduledAt, 0).UTC()
	case retry:
		nextProcessAt = time.Unix(tr.retryAt, 0).UTC()
	}

	return &base.TaskInfo{
		Message:       tr.msg,
		State:         state,
		NextProcessAt: nextProcessAt,
	}, nil
}

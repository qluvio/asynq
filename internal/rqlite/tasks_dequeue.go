package rqlite

import (
	"fmt"
	"math"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

type dequeueResult struct {
	taskMsg  string
	deadline int64
	msg      *base.TaskMessage
}

/*
UPDATE asynq_tasks
SET state='active',
    pending_since=NULL,
    affinity_timeout=affinity_timeout,
    deadline=iif(task_deadline=0, task_timeout+1686677036, task_deadline)
WHERE state='pending'
  AND (task_uuid,
       ndx,
       pndx,
       task_msg,
       task_timeout,
       task_deadline)=
    (SELECT task_uuid,
            ndx,
            pndx,
            task_msg,
            task_timeout,
            task_deadline
     FROM asynq_tasks
     INNER JOIN asynq_queues ON asynq_queues.queue_name=asynq_tasks.queue_name
     WHERE asynq_queues.queue_name='default'
       AND pndx= (SELECT COALESCE(MIN(pndx), 0) FROM asynq_tasks WHERE state='pending' AND queue_name='default')
       AND asynq_queues.state='active'
       AND (asynq_tasks.sid IS NULL
            OR (asynq_tasks.sid=''
                AND (asynq_tasks.affinity_timeout>=0
                     OR (asynq_tasks.affinity_timeout<0
                         AND (asynq_tasks.archived_at-asynq_tasks.affinity_timeout)<=1686677036)))
            OR ((asynq_tasks.done_at+asynq_tasks.affinity_timeout)<=1686677036
                AND asynq_tasks.sid!='')))
RETURNING task_uuid,ndx,pndx,task_msg,deadline;
*/

// dequeueMessage finds a processable task in the given queue.
// The task state is set to active and its deadline is computed by inspecting
// Timout and Deadline fields.
//
// Returns nil if no processable task is found in the given queue.
// Returns a dequeueResult instance if a task is found.
//
// dequeueMessage uses the sqlite syntax using the RETURNING clause
// as described here: https://www.sqlite.org/lang_returning.html
func (conn *Connection) dequeueMessage(now time.Time, serverID string, qname string) (*dequeueResult, error) {
	op := errors.Op("rqlite.dequeueMessage")

	nowUnix := now.Unix()
	var st *sqlite3.Statement

	getPending := "(task_uuid,ndx,pndx,task_msg,task_timeout,task_deadline)=" +
		"(SELECT task_uuid,ndx,pndx,task_msg,task_timeout,task_deadline FROM " + conn.table(TasksTable) +
		" INNER JOIN " + conn.table(QueuesTable) +
		" ON " + conn.table(QueuesTable) + ".queue_name=" + conn.table(TasksTable) + ".queue_name" +
		" WHERE " + conn.table(QueuesTable) + ".queue_name=? " +
		" AND " + conn.table(TasksTable) + ".state='pending' " +
		" AND pndx=(SELECT COALESCE(MIN(pndx),0) FROM " + conn.table(TasksTable) + " WHERE state='pending' AND queue_name=?)" +
		" AND " + conn.table(QueuesTable) + ".state='active'" +
		" AND (" +
		"    " + conn.table(TasksTable) + ".sid IS NULL " +
		"    OR (" + conn.table(TasksTable) + ".sid=?" +
		"       AND (" + conn.table(TasksTable) + ".affinity_timeout>=0 " +
		"       OR (" + conn.table(TasksTable) + ".affinity_timeout<0 AND (" +
		"           " + conn.table(TasksTable) + ".archived_at-" + conn.table(TasksTable) + ".affinity_timeout)<=?))) " +
		" OR ((" + conn.table(TasksTable) + ".done_at+" + conn.table(TasksTable) + ".affinity_timeout)<=? " +
		"     AND " + conn.table(TasksTable) + ".sid!=?)))"
	returning := " RETURNING task_uuid,ndx,pndx,task_msg,deadline"

	if len(serverID) > 0 {
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='active', pending_since=NULL, sid=?, "+
				"affinity_timeout=server_affinity, "+
				"deadline=iif(task_deadline=0,task_timeout+?,task_deadline) "+
				"WHERE "+conn.table(TasksTable)+".state='pending' AND "+getPending+returning,
			serverID,
			nowUnix,
			qname,
			qname,
			serverID,
			nowUnix,
			nowUnix,
			serverID)
	} else {
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='active', pending_since=NULL, "+
				"affinity_timeout=server_affinity, "+
				"deadline=iif(task_deadline=0,task_timeout+?,task_deadline) "+
				"WHERE "+conn.table(TasksTable)+".state='pending' AND "+getPending+returning,
			nowUnix,
			qname,
			qname,
			serverID,
			nowUnix,
			nowUnix,
			serverID)
	}
	st = st.WithReturning(true)
	qrs, err := conn.RequestStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRqError(op, qrs, err, []*sqlite3.Statement{st})
	}
	if len(qrs) == 0 {
		// no row
		return nil, nil
	}

	qr := qrs[0]
	if qr.Err != nil {
		return nil, NewRqliteRqError(op, qrs, qr.Err, []*sqlite3.Statement{st})
	}
	if qr.Query.NumRows() == 0 {
		// no row
		return nil, nil
	}

	if qr.Query.NumRows() > 1 {
		// PENDING(GIL): take the first anyway because once we're in this state
		//               this cannot change anymore !
		//return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite - more than one row selected: %d", qr.NumRows()))
		if slog != nil {
			slog.Warn(op, "rqlite - more than one row selected: ", qr.Query.NumRows())
		} else {
			fmt.Println(op, "rqlite - more than one row selected: ", qr.Query.NumRows())
		}
	}

	qr.Query.Next()
	row := &dequeueRow{}
	err = qr.Query.Scan(&row.uuid, &row.ndx, &row.pndx, &row.taskMsg, &row.deadline)
	if err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
	}
	row.msg, err = decodeMessage([]byte(row.taskMsg))
	if err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
	}
	return &dequeueResult{
		taskMsg:  row.taskMsg,
		deadline: row.deadline,
		msg:      row.msg,
	}, nil
}

type dequeueRow struct {
	uuid     string
	ndx      int64
	pndx     int64
	taskMsg  string
	deadline int64
	msg      *base.TaskMessage
}

//
// ----- old impl -----
//

type dequeueRow0 struct {
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
func (conn *Connection) getPending(now time.Time, serverID string, queue string) (*dequeueRow0, error) {
	op := errors.Op("rqlite.getPending")

	st := Statement(
		"SELECT task_uuid,ndx,pndx,task_msg,task_timeout,task_deadline FROM "+conn.table(TasksTable)+
			" INNER JOIN "+conn.table(QueuesTable)+
			" ON "+conn.table(QueuesTable)+".queue_name="+conn.table(TasksTable)+".queue_name"+
			" WHERE "+conn.table(QueuesTable)+".queue_name=? "+
			" AND "+conn.table(TasksTable)+".state='pending' "+
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
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
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
	deq := &dequeueRow0{}
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
		st = Statement(
			"UPDATE "+conn.table(TasksTable)+" SET state='active', pending_since=NULL, affinity_timeout=?, deadline=? WHERE state='pending' AND ndx=?",
			serverAffinity,
			deadline,
			ndx)
	}
	wrs, err := conn.WriteStmt(conn.ctx(), st)
	if err != nil {
		return false, NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}
	if len(wrs) == 0 {
		// no row update
		return false, nil
	}
	// if the row was changed (by another rqlite) no row is affected
	return wrs[0].RowsAffected == 1, nil
}

// dequeueMessage0 is the initial implementation of dequeueMessage: it first
// finds a pending task and then marks it active.
//
// dequeueMessage0 finds a processable task in the given queue.
// The task state is set to active and its deadline is computed by inspecting
// Timout and Deadline fields.
//
// Returns nil if no processable task is found in the given queue.
// Returns a dequeueResult instance if a task is found.
func (conn *Connection) dequeueMessage0(now time.Time, serverID string, qname string) (*dequeueResult, error) {

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

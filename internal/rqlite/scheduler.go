package rqlite

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

type schedulerRow struct {
	schedulerId    string
	expireAt       int64
	schedulerEntry string
	entry          *base.SchedulerEntry
}

func (conn *Connection) listSchedulerEntries(where string, whereParams ...interface{}) ([]*schedulerRow, error) {
	op := errors.Op("listSchedulerEntries")

	st := Statement("SELECT scheduler_id, expire_at, scheduler_entry" +
		" FROM " + conn.table(SchedulersTable) + " ")
	if len(where) > 0 {
		st = st.Append(" WHERE "+where, whereParams...)
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
	ret := make([]*schedulerRow, 0)

	for qr.Next() {
		s := &schedulerRow{}
		err = qr.Scan(
			&s.schedulerId,
			&s.expireAt,
			&s.schedulerEntry)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		sv, err := decodeSchedulerEntry([]byte(s.schedulerEntry))
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		s.entry = sv
		ret = append(ret, s)
	}
	return ret, nil
}

type schedulerEnqueueEventRow struct {
	ndx          int
	uuid         string
	taskId       string
	enqueuedAt   int64
	enqueueEvent string
	event        *base.SchedulerEnqueueEvent
}

func parseSchedulerEnqueueEvents(qr sqlite3.QueryResult) ([]*schedulerEnqueueEventRow, error) {
	op := errors.Op("parseSchedulerEnqueueEvents")
	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*schedulerEnqueueEventRow, 0)

	for qr.Next() {
		s := &schedulerEnqueueEventRow{}
		err := qr.Scan(
			&s.ndx,
			&s.uuid,
			&s.taskId,
			&s.enqueuedAt,
			&s.enqueueEvent)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		ev, err := decodeSchedulerEnqueueEvent([]byte(s.enqueueEvent))
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		s.event = ev
		ret = append(ret, s)
	}
	return ret, nil
}

func (conn *Connection) listSchedulerEnqueueEvents(entryID string, page base.Pagination) ([]*schedulerEnqueueEventRow, error) {
	op := errors.Op("listSchedulerEnqueueEvents")
	// most recent events first
	st := Statement(
		"SELECT ndx, uuid, task_id, enqueued_at, scheduler_enqueue_event FROM "+conn.table(SchedulerHistoryTable)+
			" WHERE uuid=? "+
			" ORDER BY -enqueued_at LIMIT ? OFFSET ?",
		entryID,
		page.Size,
		page.Start())

	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	return parseSchedulerEnqueueEvents(qrs[0])
}

func (conn *Connection) listAllSchedulerEnqueueEvents(entryID string) ([]*schedulerEnqueueEventRow, error) {
	op := errors.Op("listAllSchedulerEnqueueEvents")
	st := Statement(
		"SELECT ndx, uuid, task_id, enqueued_at, scheduler_enqueue_event FROM "+conn.table(SchedulerHistoryTable)+
			" WHERE uuid=? "+
			" ORDER BY ndx ",
		entryID)

	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{st})
	}
	return parseSchedulerEnqueueEvents(qrs[0])
}

func (conn *Connection) writeSchedulerEntries(now time.Time, schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	op := errors.Op("rqlite.writeSchedulerEntries")
	if len(entries) == 0 {
		return nil
	}

	exp := now.Add(ttl)
	args := make([]string, 0, len(entries))

	for _, e := range entries {
		bytes, err := encodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}

	stmts := make([]*sqlite3.Statement, 0, len(args)-1)
	for i := 0; i < len(args); i++ {
		stmts = append(stmts, Statement(
			"INSERT INTO "+conn.table(SchedulersTable)+"(scheduler_id, expire_at, scheduler_entry) "+
				"VALUES (?, ?, ?)",
			schedulerID,
			exp.Unix(),
			args[i]))
	}

	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}
	return nil
}

func (conn *Connection) clearSchedulerEntries(schedulerID string) error {
	var op errors.Op = "rqlite.clearSchedulerEntries"

	stmt := Statement(
		"DELETE FROM "+conn.table(SchedulersTable)+" WHERE scheduler_id=?",
		schedulerID)
	wrs, err := conn.WriteStmt(conn.ctx(), stmt)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{stmt})
	}
	return nil
}

func (conn *Connection) clearSchedulerHistory(entryID string) error {
	stmt := Statement("DELETE FROM "+conn.table(SchedulerHistoryTable)+" WHERE uuid=?",
		entryID)
	wrs, err := conn.WriteStmt(conn.ctx(), stmt)
	if err != nil {
		return NewRqliteWsError("rqlite.clearSchedulerHistory", wrs, err, []*sqlite3.Statement{stmt})
	}
	return nil
}

func (conn *Connection) recordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	op := errors.Op("rqlite.recordSchedulerEnqueueEvent")

	data, err := encodeSchedulerEnqueueEvent(event)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode scheduler enqueue event: %v", err))
	}

	stmts := []*sqlite3.Statement{
		Statement(
			"INSERT INTO "+conn.table(SchedulerHistoryTable)+"(uuid, task_id, enqueued_at, scheduler_enqueue_event) "+
				"VALUES(?, ?, ?, ?) ",
			entryID,
			event.TaskID,
			event.EnqueuedAt.UTC().Unix(),
			data),
		Statement(
			"DELETE FROM "+conn.table(SchedulerHistoryTable)+
				" WHERE uuid=? "+
				" AND ndx IN "+"(SELECT ndx FROM "+conn.table(SchedulerHistoryTable)+
				" WHERE uuid=? ORDER BY -enqueued_at "+
				"   LIMIT (SELECT COUNT(*) FROM "+conn.table(SchedulerHistoryTable)+" WHERE uuid=?) "+
				"   OFFSET ?)",
			entryID,
			entryID,
			entryID,
			schedulerHistoryMaxEvents),
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, stmts)
	}
	return nil
}

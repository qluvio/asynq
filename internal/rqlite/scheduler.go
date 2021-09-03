package rqlite

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/rqlite/gorqlite"
)

type schedulerRow struct {
	schedulerId    string
	expireAt       int64
	schedulerEntry string
	entry          *base.SchedulerEntry
}

func listSchedulerEntries(conn *gorqlite.Connection, where string) ([]*schedulerRow, error) {
	op := errors.Op("listSchedulerEntries")

	st := "SELECT scheduler_id, expire_at, scheduler_entry" +
		" FROM " + SchedulersTable
	if len(where) > 0 {
		st += where
	}

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}

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

func parseSchedulerEnqueueEvents(qr gorqlite.QueryResult) ([]*schedulerEnqueueEventRow, error) {
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

func listSchedulerEnqueueEvents(conn *gorqlite.Connection, entryID string, page base.Pagination) ([]*schedulerEnqueueEventRow, error) {
	op := errors.Op("listSchedulerEnqueueEvents")
	// most recent events first
	st := fmt.Sprintf(
		"SELECT ndx, uuid, task_id, enqueued_at, scheduler_enqueue_event FROM "+SchedulerHistoryTable+
			" WHERE uuid='%s' "+
			" ORDER BY -enqueued_at LIMIT %d OFFSET %d",
		entryID,
		page.Size,
		page.Start())

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}
	return parseSchedulerEnqueueEvents(qr)
}

func listAllSchedulerEnqueueEvents(conn *gorqlite.Connection, entryID string) ([]*schedulerEnqueueEventRow, error) {
	op := errors.Op("listAllSchedulerEnqueueEvents")
	st := fmt.Sprintf(
		"SELECT ndx, uuid, task_id, enqueued_at, scheduler_enqueue_event FROM "+SchedulerHistoryTable+
			" WHERE uuid='%s' "+
			" ORDER BY ndx ",
		entryID)

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}
	return parseSchedulerEnqueueEvents(qr)
}

func writeSchedulerEntries(conn *gorqlite.Connection, schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	op := errors.Op("rqlite.writeSchedulerEntries")
	if len(entries) == 0 {
		return nil
	}

	exp := utc.Now().Add(ttl)
	args := make([]string, 0, len(entries))

	for _, e := range entries {
		bytes, err := encodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}

	stmts := make([]string, 0, len(args)-1)
	for i := 0; i < len(args); i++ {
		stmts = append(stmts, fmt.Sprintf(
			"INSERT INTO "+SchedulersTable+"(scheduler_id, expire_at, scheduler_entry) "+
				"VALUES ('%s', %d, '%s')",
			schedulerID,
			exp.Unix(),
			args[i]))
	}

	wr, err := conn.Write(stmts)
	if err != nil {
		return NewRqliteWsError(op, wr, err, stmts)
	}
	return nil
}

func clearSchedulerEntries(conn *gorqlite.Connection, schedulerID string) error {
	var op errors.Op = "rqlite.clearSchedulerEntries"

	stmt := fmt.Sprintf(
		"DELETE FROM "+SchedulersTable+" WHERE scheduler_id='%s'",
		schedulerID)
	wr, err := conn.WriteOne(stmt)
	if err != nil {
		return NewRqliteWError(op, wr, err, stmt)
	}
	return nil
}

func clearSchedulerHistory(conn *gorqlite.Connection, entryID string) error {
	stmt := fmt.Sprintf("DELETE FROM "+SchedulerHistoryTable+" WHERE uuid='%s'",
		entryID)
	wr, err := conn.WriteOne(stmt)
	if err != nil {
		return NewRqliteWError(errors.Op("rqlite.clearSchedulerHistory"), wr, err, stmt)
	}
	return nil
}

func recordSchedulerEnqueueEvent(conn *gorqlite.Connection, entryID string, event *base.SchedulerEnqueueEvent) error {
	op := errors.Op("rqlite.recordSchedulerEnqueueEvent")

	data, err := encodeSchedulerEnqueueEvent(event)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode scheduler enqueue event: %v", err))
	}

	stmts := []string{
		fmt.Sprintf(
			"INSERT INTO "+SchedulerHistoryTable+"(uuid, task_id, enqueued_at, scheduler_enqueue_event) "+
				"VALUES('%s', '%s', %d, '%s') ",
			entryID,
			event.TaskID,
			event.EnqueuedAt.UTC().Unix(),
			data),
		fmt.Sprintf(
			"DELETE FROM "+SchedulerHistoryTable+
				" WHERE uuid='%s' "+
				" AND ndx IN "+"(SELECT ndx FROM "+SchedulerHistoryTable+
				" WHERE uuid='%s' ORDER BY -enqueued_at "+
				"   LIMIT (SELECT COUNT(*) FROM "+SchedulerHistoryTable+" WHERE uuid='%s') "+
				"   OFFSET %d)",
			entryID,
			entryID,
			entryID,
			maxEvents),
	}
	wr, err := conn.Write(stmts)
	if err != nil {
		return NewRqliteWsError(op, wr, err, stmts)
	}
	return nil
}

package rqlite

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
)

func dayOf(now time.Time, deltaDays ...int) time.Time {
	y, m, d := now.Date()
	if len(deltaDays) > 0 && deltaDays[0] != 0 {
		d += deltaDays[0]
	}
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// failedStatsStatement produces a statement to increase processed and failed when a
// task is put into state retry or archive.
// In RDB
//
//	processed key: done, complete, retry, archive
//	failed key: retry, archive
func (conn *Connection) failedStatsStatement(now time.Time, qname string) *sqlite3.Statement {
	return conn.failedStatsStatementCount(now, qname, 1, 1)
}
func (conn *Connection) failedStatsStatementCount(now time.Time, qname string, processed, failed int) *sqlite3.Statement {
	day := dayOf(now)
	return Statement(
		"INSERT INTO "+conn.table(StatsTable)+" (day,ts,queue_name,processed,failed) VALUES (?,?,?,?,?) "+
			"ON CONFLICT(day,queue_name) "+
			"DO UPDATE SET (processed,failed)=(excluded.processed+processed,excluded.failed+failed)",
		day.Format(dayFormat),
		day.Unix(),
		qname,
		processed,
		failed)
}

// failedOnlyStatsStatementCount only increases the failed count (for tests)
func (conn *Connection) failedOnlyStatsStatementCount(now time.Time, qname string, failed int) *sqlite3.Statement {
	day := dayOf(now)
	return Statement(
		"INSERT INTO "+conn.table(StatsTable)+" (day,ts,queue_name,failed) VALUES (?,?,?,?) "+
			"ON CONFLICT(day,queue_name) "+
			"DO UPDATE SET (failed)=(excluded.failed+failed)",
		day.Format(dayFormat),
		day.Unix(),
		qname,
		failed)
}

// processedStatsStatement produces a statement to increase processed when a task is
// done (processed) or marked as completed.
func (conn *Connection) processedStatsStatement(now time.Time, qname string) *sqlite3.Statement {
	return conn.processedStatsStatementCount(now, qname, 1)
}

func (conn *Connection) processedStatsStatementCount(now time.Time, qname string, processed int) *sqlite3.Statement {
	day := dayOf(now)
	return Statement(
		"INSERT INTO "+conn.table(StatsTable)+" (day,ts,queue_name,processed) VALUES (?,?,?,?) "+
			"ON CONFLICT(day,queue_name) DO UPDATE SET processed=processed+excluded.processed",
		day.Format(dayFormat),
		day.Unix(),
		qname,
		processed)
}

func (conn *Connection) queueStats(now time.Time, queue string, ndays int) ([]*base.DailyStats, error) {
	op := errors.Op("rqlite.queueStats")
	var st *sqlite3.Statement
	if ndays == 0 {
		st = Statement(
			"SELECT queue_name,day,ts,processed,failed FROM "+conn.table(StatsTable)+
				" WHERE "+conn.table(StatsTable)+".queue_name=? ",
			queue)
	} else {
		firstday := dayOf(now, -ndays)
		lastday := dayOf(now)
		st = Statement(
			"SELECT queue_name,day,ts,processed,failed FROM "+conn.table(StatsTable)+
				" WHERE "+conn.table(StatsTable)+".queue_name=?"+
				" AND "+conn.table(StatsTable)+".ts>=?"+
				" AND "+conn.table(StatsTable)+".ts<=?",
			queue,
			firstday.Unix(),
			lastday.Unix())
	}
	qrs, err := conn.QueryStmt(conn.ctx(), st)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	qr := qrs[0]
	ret := ([]*base.DailyStats)(nil)
	for qr.Next() {
		ds := &base.DailyStats{}
		var ts int64
		day := ""
		err = qr.Scan(&ds.Queue, &day, &ts, &ds.Processed, &ds.Failed)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		ds.Time = time.Unix(ts, 0).UTC()
		ret = append(ret, ds)
	}
	return ret, nil
}

func (conn *Connection) queueDayStatsStatement(queue string, now time.Time) *sqlite3.Statement {
	day := dayOf(now)
	return Statement(
		"SELECT queue_name,day,ts,processed,failed FROM "+conn.table(StatsTable)+
			" WHERE "+conn.table(StatsTable)+".queue_name=? "+
			" AND "+conn.table(StatsTable)+".day=? ",
		queue,
		day.Format(dayFormat))
}

func parseDailyStatsRow(qr sqlite3.QueryResult) (*base.DailyStats, error) {
	op := errors.Op("rqlite.parseDailyStatsRows")
	// no row
	if qr.NumRows() == 0 {
		return &base.DailyStats{}, nil
	}
	if qr.NumRows() != 1 {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("unexpected result count: %d", qr.NumRows()))
	}
	ret := &base.DailyStats{}
	for qr.Next() {
		var ts int64
		day := ""
		err := qr.Scan(&ret.Queue, &day, &ts, &ret.Processed, &ret.Failed)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		ret.Time = time.Unix(ts, 0).UTC()
	}
	return ret, nil
}

func (conn *Connection) queueDayStats(queue string, now time.Time) (*base.DailyStats, error) {
	op := errors.Op("rqlite.queueDayStats")

	qrs, err := conn.QueryStmt(
		conn.ctx(),
		conn.queueDayStatsStatement(queue, now))
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	return parseDailyStatsRow(qrs[0])
}

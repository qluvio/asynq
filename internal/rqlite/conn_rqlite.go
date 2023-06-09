package rqlite

import (
	"context"
	"net/http"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/rqlite/gorqlite"
)

type RQLiteConnection struct {
	conn *gorqlite.Connection
}

func NewRQLiteConnection(ctx context.Context, config *Config, httpClient *http.Client, logger log.Base) (*Connection, error) {
	op := errors.Op("open")

	type Tracer interface {
		Tracef(format string, args ...interface{})
	}
	if tracer, ok := logger.(Tracer); ok {
		gorqlite.WithTracer(tracer)
	}

	rqliteConn, err := gorqlite.OpenContext(ctx, config.RqliteUrl, httpClient)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	err = rqliteConn.SetConsistencyLevel(config.ConsistencyLevel)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	return &Connection{
		DbConnection: &RQLiteConnection{conn: rqliteConn},
		config:       config,
	}, nil
}

func (c *RQLiteConnection) PingContext(ctx context.Context) error {
	_, err := c.conn.Leader(ctx)
	return err
}

func (c *RQLiteConnection) Close() {
	c.conn.Close()
}

func (c *RQLiteConnection) QueryStmt(ctx context.Context, stmts ...*sqlite3.Statement) ([]sqlite3.QueryResult, error) {
	ret, err := c.conn.QueryStmt(ctx, Statements(stmts).Rqlite()...)
	return fromRQQueryResults(ret), err
}

func (c *RQLiteConnection) WriteStmt(ctx context.Context, stmts ...*sqlite3.Statement) ([]sqlite3.WriteResult, error) {
	wrs, err := c.conn.WriteStmt(ctx, Statements(stmts).Rqlite()...)
	return fromRQWriteResults(wrs), err
}

// statements

func toRqlite(s *sqlite3.Statement) *gorqlite.Statement {
	return (*gorqlite.Statement)(s)
}

type Statements []*sqlite3.Statement

func (s Statements) Rqlite() []*gorqlite.Statement {
	switch len(s) {
	case 0:
		return nil
	case 1:
		return []*gorqlite.Statement{toRqlite(s[0])}
	}
	ret := make([]*gorqlite.Statement, 0, len(s))
	for _, st := range s {
		ret = append(ret, toRqlite(st))
	}
	return ret
}

// write

func fromRQWriteResult(wr gorqlite.WriteResult) sqlite3.WriteResult {
	return sqlite3.WriteResult{
		Err:          wr.Err,
		Timing:       wr.Timing,
		RowsAffected: wr.RowsAffected,
		LastInsertID: wr.LastInsertID,
	}
}

func fromRQWriteResults(wrs []gorqlite.WriteResult) []sqlite3.WriteResult {
	ret := make([]sqlite3.WriteResult, 0, len(wrs))
	for _, wr := range wrs {
		ret = append(ret, fromRQWriteResult(wr))
	}
	return ret
}

// query

func fromRQQueryResult(qr gorqlite.QueryResult) sqlite3.QueryResult {
	return &rqQueryResult{QueryResult: &qr}
}

func fromRQQueryResults(qrs []gorqlite.QueryResult) []sqlite3.QueryResult {
	ret := make([]sqlite3.QueryResult, 0, len(qrs))
	for _, qr := range qrs {
		ret = append(ret, fromRQQueryResult(qr))
	}
	return ret
}

var _ sqlite3.QueryResult = (*rqQueryResult)(nil)

type rqQueryResult struct {
	*gorqlite.QueryResult
}

func (r *rqQueryResult) Err() error {
	return r.QueryResult.Err
}

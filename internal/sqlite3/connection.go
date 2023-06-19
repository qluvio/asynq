package sqlite3

import (
	"context"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3/command"
	"github.com/hibiken/asynq/internal/sqlite3/db"
	"github.com/hibiken/asynq/internal/sqlite3/encoding"
	"github.com/mattn/go-sqlite3"
)

type SQLiteConnection struct {
	mu        sync.Mutex // protect db
	db        *db.DB     // the actual connection
	retryBusy bool       // retry 'execute' on sqlite3.ErrBusy error: 'database is locked'
}

func NewSQLiteConnection(db *db.DB, retryBusy bool) *SQLiteConnection {
	ret := &SQLiteConnection{
		db:        db,
		retryBusy: retryBusy,
	}
	return ret
}

func (c *SQLiteConnection) PingContext(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *SQLiteConnection) Close() {
	_ = c.db.Close()
}

func newRequest(stmts []*Statement) *command.Request {
	statements := make([]*command.Statement, 0, len(stmts))
	for _, st := range stmts {
		params := make([]*command.Parameter, 0, len(st.Arguments))
		for _, p := range st.Arguments {
			params = append(params, &command.Parameter{
				Value: p,
			})
		}
		statements = append(statements, &command.Statement{
			Sql:        st.Query,
			Parameters: params,
			Returning:  st.Returning,
		})
	}
	return &command.Request{
		Transaction: true,
		Statements:  statements,
	}
}

func (c *SQLiteConnection) QueryContext(ctx context.Context, req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	// no lock: db uses a 'read' connection
	return c.db.QueryContext(ctx, req, xTime)
}

func (c *SQLiteConnection) QueryStmt(ctx context.Context, stmts ...*Statement) ([]QueryResult, error) {
	req := newRequest(stmts)
	qrows, err := c.QueryContext(ctx, req, true)
	if err != nil {
		return nil, err
	}

	ret := make([]QueryResult, len(qrows))
	for j := range qrows {
		rows, err := encoding.NewRowsFromQueryRows(qrows[j])
		if err != nil {
			return nil, err
		}
		ret[j] = newQueryResult(rows)
	}

	return ret, nil
}

func (c *SQLiteConnection) ExecuteContext(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	attempt := 0
	// uncomment for troubleshooting
	//t0 := time.Now()

	// We are protected by taking the lock for concurrent calls using this
	// connection, but not against concurrent calls made with another connection.
	// Hence, retry write attempts that failed with error: SQLITE_BUSY - 05, with
	// message 'database is locked'.
	//
	// Note that all constructors of db.DB open their internal write connections
	// with option '_txlock=immediate' which is expected to provide a failure
	// upfront (and not after executing some statements). Also function newRequest
	// (in this file) always uses Transaction: true

	for {
		attempt++
		wrs, err := c.db.ExecuteContext(ctx, req, xTime)
		if sqliteErr, ok := err.(sqlite3.Error); ok &&
			sqliteErr.Code == sqlite3.ErrBusy &&
			attempt <= 20 &&
			c.retryBusy {
			// 99% of errors (in a unit-test) were eliminated with a 5ms delay
			// but half of them required 5 retries or more.
			// Also: using an exponential backoff did not help at all.
			time.Sleep(time.Millisecond * 5)
			continue
		}
		// uncomment for troubleshooting
		//if attempt > 1 {
		//	fmt.Println("ExecuteContext", "attempt", attempt, "d", time.Now().Sub(t0), "err", err)
		//}
		return wrs, err
	}
}

func (c *SQLiteConnection) WriteStmt(ctx context.Context, stmts ...*Statement) ([]WriteResult, error) {
	req := newRequest(stmts)
	wrs, err := c.ExecuteContext(ctx, req, true)
	if err != nil {
		return nil, err
	}
	ret := make([]WriteResult, len(wrs))
	for i, wr := range wrs {
		var werr error
		if wr.Error != "" {
			werr = errors.New(wr.Error)
		}
		ret[i] = WriteResult{
			Err:          werr,
			Timing:       wr.Time,
			RowsAffected: wr.RowsAffected,
			LastInsertID: wr.LastInsertId,
		}
	}
	return ret, nil
}

func (c *SQLiteConnection) RequestContext(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	attempt := 0
	// uncomment for troubleshooting
	//t0 := time.Now()

	// We are protected by taking the lock for concurrent calls using this
	// connection, but not against concurrent calls made with another connection.
	// Hence, retry write attempts that failed with error: SQLITE_BUSY - 05, with
	// message 'database is locked'.
	//
	// Note that all constructors of db.DB open their internal write connections
	// with option '_txlock=immediate' which is expected to provide a failure
	// upfront (and not after executing some statements). Also function newRequest
	// (in this file) always uses Transaction: true

	for {
		attempt++
		eqrs, err := c.db.RequestContext(ctx, req, xTime)
		if sqliteErr, ok := err.(sqlite3.Error); ok &&
			sqliteErr.Code == sqlite3.ErrBusy &&
			attempt <= 20 &&
			c.retryBusy {
			// 99% of errors (in a unit-test) were eliminated with a 5ms delay
			// but half of them required 5 retries or more.
			// Also: using an exponential backoff did not help at all.
			time.Sleep(time.Millisecond * 5)
			continue
		}
		// uncomment for troubleshooting
		//if attempt > 1 {
		//	fmt.Println("ExecuteContext", "attempt", attempt, "d", time.Now().Sub(t0), "err", err)
		//}
		return eqrs, err
	}
}

func (c *SQLiteConnection) RequestStmt(ctx context.Context, stmts ...*Statement) ([]RequestResult, error) {
	req := newRequest(stmts)
	eqrs, err := c.RequestContext(ctx, req, true)
	if err != nil {
		return nil, err
	}

	ret := make([]RequestResult, len(eqrs))
	for j, eqr := range eqrs {
		if eqr.GetError() != "" {
			ret[j] = RequestResult{
				Err: errors.New(eqr.GetError()),
			}
			continue
		}
		if eqr.GetE() == nil && eqr.GetQ() == nil {
			ret[j] = RequestResult{
				Err: errors.New("no result (internal error)"),
			}
			continue
		}
		if eqr.GetE() != nil {
			wr := eqr.GetE()
			var err error
			if wr.Error != "" { // should not happen (should have been reported on RequestResult.Err)
				err = errors.New(wr.Error)
			}
			ret[j] = RequestResult{
				Write: WriteResult{
					Err:          err,
					Timing:       wr.Time,
					RowsAffected: wr.RowsAffected,
					LastInsertID: wr.LastInsertId,
				},
			}
		} else {
			rows, err := encoding.NewRowsFromQueryRows(eqr.GetQ())
			if err != nil {
				return nil, err
			}
			ret[j] = RequestResult{
				Query: newQueryResult(rows),
			}
		}
	}

	return ret, nil
}

package sqlite3

import (
	"context"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3/command"
	"github.com/hibiken/asynq/internal/sqlite3/db"
	"github.com/hibiken/asynq/internal/sqlite3/encoding"
)

type SQLiteConnection struct {
	db *db.DB
}

func NewSQLiteConnection(db *db.DB /*, config *Config*/) *SQLiteConnection {
	ret := &SQLiteConnection{
		db: db,
		/*config:       config,*/
	}
	/*ret.buildTables()*/
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
		params := make([]*command.Parameter, 0, len(st.Parameters))
		for _, p := range st.Parameters {
			params = append(params, &command.Parameter{
				Value: p,
			})
		}
		statements = append(statements, &command.Statement{
			Sql:        st.Sql,
			Parameters: params,
		})
	}
	return &command.Request{
		Transaction: true, // PENDING(GIL): config
		Statements:  statements,
	}
}

func (c *SQLiteConnection) QueryStmt(ctx context.Context, stmts ...*Statement) ([]QueryResult, error) {
	// PENDING(GIL): add context to parameters of db.Query
	qrows, err := c.db.Query(newRequest(stmts), true) // PENDING(GIL): config
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

func (c *SQLiteConnection) WriteStmt(ctx context.Context, stmts ...*Statement) ([]WriteResult, error) {
	// PENDING(GIL): add context to parameters of db.Execute
	wrs, err := c.db.Execute(newRequest(stmts), true) // PENDING(GIL): config
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

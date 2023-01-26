package rqlite

import (
	"context"

	"github.com/hibiken/asynq/internal/sqlite3"
)

type DbConnection interface {
	QueryStmt(ctx context.Context, sqlStatements ...*sqlite3.Statement) (results []sqlite3.QueryResult, err error)
	WriteStmt(ctx context.Context, sqlStatements ...*sqlite3.Statement) (results []sqlite3.WriteResult, err error)
	PingContext(ctx context.Context) error
	Close()
}

type Connection struct {
	DbConnection
	config     *Config
	tableNames map[string]string
	tables     map[string]string
}

func (conn *Connection) ctx() context.Context {
	return context.Background()
}

func Statement(sql string, params ...interface{}) *sqlite3.Statement {
	ret := sqlite3.NewStatement(sql, params...)
	//if len(ret.Warning) > 0 {
	//	panic(ret.Warning)
	//}
	return ret
}

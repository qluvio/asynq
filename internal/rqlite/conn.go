package rqlite

import (
	"context"
	"net/http"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
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
	indexes    map[string][]string
}

func newConnection(ctx context.Context, config *Config, httpClient *http.Client, logger log.Base) (*Connection, error) {
	op := errors.Op("newConnection")

	var err error
	var conn *Connection
	switch config.Type {
	case rqliteType:
		conn, err = NewRQLiteConnection(ctx, config, httpClient, logger)
	case sqliteType:
		conn, err = NewSQLiteConnection(ctx, config)
	}
	if err != nil {
		return nil, err
	}

	// build internal table names
	conn.buildTables()

	_, err = conn.CreateTablesIfNotExist()
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	err = conn.CreateIndexes()
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}

	return conn, nil
}

func (conn *Connection) ctx() context.Context {
	return context.Background()
}

func Statement(sql string, params ...interface{}) *sqlite3.Statement {
	return sqlite3.NewStatement(sql, params...)
}

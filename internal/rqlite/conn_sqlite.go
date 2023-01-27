package rqlite

import (
	"context"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/hibiken/asynq/internal/sqlite3/db"
)

func NewSQLiteConnection(ctx context.Context, config *Config) (*Connection, error) {
	op := errors.Op("open")

	var err error
	var conn *db.DB
	switch config.SqliteInMemory {
	case false:
		conn, err = db.OpenContext(ctx, config.SqliteDbPath, false)
	case true:
		conn, err = db.OpenInMemoryPath(config.SqliteDbPath, false)
	}
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	ret := newSQLiteConnection(conn, config)
	_, err = ret.CreateTablesIfNotExist()
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return ret, nil
}

func newSQLiteConnection(conn *db.DB, config *Config) *Connection {
	ret := &Connection{
		DbConnection: sqlite3.NewSQLiteConnection(conn),
		config:       config,
	}
	ret.buildTables()
	return ret
}

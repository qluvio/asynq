package rqlite

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/sqlite3"
)

type DbConnection interface {
	QueryStmt(ctx context.Context, sqlStatements ...*sqlite3.Statement) (results []sqlite3.QueryResult, err error)
	WriteStmt(ctx context.Context, sqlStatements ...*sqlite3.Statement) (results []sqlite3.WriteResult, err error)
	RequestStmt(ctx context.Context, sqlStatements ...*sqlite3.Statement) (results []sqlite3.RequestResult, err error)
	PingContext(ctx context.Context) error
	Close()
}

func Statement(sql string, params ...interface{}) *sqlite3.Statement {
	return sqlite3.NewStatement(sql, params...)
}

type Connection struct {
	DbConnection
	config *Config
	tables map[string]*TableDef
}

func newConnection(ctx context.Context, config *Config, httpClient *http.Client, logger log.Base) (*Connection, error) {
	op := errors.Op("newConnection")

	var err error
	var conn *Connection
	switch config.Type {
	case rqliteType:
		conn, err = NewRQLiteConnection(ctx, config, httpClient, logger)
	case sqliteType:
		conn, err = NewSQLiteConnection(ctx, config, logger)
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

func (conn *Connection) createTables(tablesDef map[string]*TableDef) error {
	stmts := make([]*sqlite3.Statement, 0)
	tables := make([]string, 0)
	for _, t := range tablesDef {
		tables = append(tables, t.CreateStmt)
	}
	sort.Strings(tables)
	for _, stmt := range tables {
		stmts = append(stmts, Statement(stmt))
	}

	verStmt := fmt.Sprintf(InsertVersionStmtFmt, conn.table(VersionTable))
	stmts = append(stmts, Statement(verStmt, Version, time.Now().Unix()))
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("CreateTables", wrs, err, stmts)
	}

	return nil
}

func (conn *Connection) createIndexes(tablesDef map[string]*TableDef) error {
	stmts := make([]*sqlite3.Statement, 0)
	indexes := make([]string, 0)
	for _, table := range tablesDef {
		indexes = append(indexes, table.CreateIndexStmts...)
	}
	sort.Strings(indexes)
	for _, stmt := range indexes {
		stmts = append(stmts, Statement(stmt))
	}

	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("CreateIndexes", wrs, err, stmts)
	}

	return nil
}

func (conn *Connection) dropTables(tablesDef map[string]*TableDef) error {
	stmts := make([]*sqlite3.Statement, 0)
	for _, ctor := range tablesDef {
		stmts = append(stmts, Statement("DROP TABLE IF EXISTS "+ctor.Name))
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("PurgeTables", wrs, err, stmts)
	}
	return nil
}

func (conn *Connection) purgeTables(tablesDef map[string]*TableDef, except ...string) error {
	stmts := make([]*sqlite3.Statement, 0)

	verTable := ""
	if len(except) > 0 && except[0] != "" {
		verTable = conn.table(except[0])
	}
	for _, ctor := range tablesDef {
		if ctor.Name == verTable {
			continue
		}
		stmts = append(stmts, Statement(fmt.Sprintf("DELETE FROM '%s' ", ctor.Name)))
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("PurgeTables", wrs, err, stmts)
	}
	return nil
}

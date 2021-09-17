package rqlite

import (
	"context"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/rqlite/gorqlite"
)

var AllTables = map[string]string{
	QueuesTable:           CreateQueuesTable,
	TasksTable:            CreateTasksTable,
	ServersTable:          CreateServersTable,
	WorkersTable:          CreateWorkersTable,
	SchedulersTable:       CreateSchedulersTable,
	SchedulerHistoryTable: CreateSchedulerHistoryTable,
	CancellationTable:     CreateCancellationTable,
	VersionTable:          CreateVersionTable,
}

const (
	Version            = "1.0.0"
	VersionTable       = "asynq_schema_version"
	CreateVersionTable = `CREATE TABLE ` + VersionTable + ` (
	version text not null primary key,
	ts integer
)`
	InsertVersionStmt = "INSERT INTO " + VersionTable + " (version, ts) VALUES(?, ?) "

	QueuesTable       = "asynq_queues"
	CreateQueuesTable = `CREATE TABLE ` + QueuesTable + ` (
	queue_name text not null primary key, 
	state      text not null
)`
	active    = "active"
	paused    = "paused"
	pending   = "pending"
	scheduled = "scheduled"
	retry     = "retry"
	archived  = "archived"
	processed = "processed"

	TasksTable       = "asynq_tasks"
	CreateTasksTable = `CREATE TABLE ` + TasksTable + ` (
	ndx                    integer not null primary key,
	queue_name             text not null,
	task_uuid              text not null unique,
	unique_key             text not null unique, 
	unique_key_deadline    integer,
	task_msg               text,
	task_timeout           integer,
	task_deadline          integer,
	pndx                   integer default 0,
	state                  text not null,
	scheduled_at           integer,
	deadline               integer,
	retry_at               integer,
	done_at                integer,
	failed                 boolean,
	archived_at            integer,
	cleanup_at             integer
)`

	ServersTable       = "asynq_servers"
	CreateServersTable = `CREATE TABLE ` + ServersTable + ` (
	sid         text not null unique,
	pid         integer not null,
	host        text not null,
	expire_at   integer,
	server_info text not null
)`

	WorkersTable       = "asynq_workers"
	CreateWorkersTable = `CREATE TABLE ` + WorkersTable + ` (
	sid         text not null,
	task_uuid   text not null,
	expire_at   integer,
	worker_info text not null
)`
	SchedulersTable       = "asynq_schedulers"
	CreateSchedulersTable = `CREATE TABLE ` + SchedulersTable + ` (
	scheduler_id    text not null,
	expire_at       integer,
	scheduler_entry text not null
)`
	SchedulerHistoryTable       = "asynq_scheduler_history"
	CreateSchedulerHistoryTable = `CREATE TABLE ` + SchedulerHistoryTable + ` (
	ndx                     integer not null primary key,
	uuid                    text not null,
	task_id                 text not null,
	enqueued_at             integer,
	scheduler_enqueue_event text not null
)`
	CancellationTable       = "asynq_cancel"
	CreateCancellationTable = `CREATE TABLE ` + CancellationTable + ` (
	ndx                     integer not null primary key,
	uuid                    text not null,
	cancelled_at            integer
)`
)

func CreateTablesIfNotExist(conn *gorqlite.Connection) (bool, error) {
	op := errors.Op("CreateTablesIfNotExist")

	get := Statement("SELECT COUNT(*) FROM " + VersionTable)
	qrs, err := conn.QueryStmt(context.Background(), get)
	if err != nil && (qrs[0].Err == nil || !strings.Contains(qrs[0].Err.Error(), "no such table:")) {
		return false, errors.E(op, errors.Internal, NewRqliteRError(op, qrs[0], err, get))
	}
	if qrs[0].NumRows() > 0 {
		return false, nil
	}
	err = CreateTables(conn)
	if err != nil {
		return false, err
	}
	return true, nil
}

func CreateTables(conn *gorqlite.Connection) error {
	op := errors.Op("CreateTables")

	stmts := make([]*gorqlite.Statement, 0)
	for _, stmt := range AllTables {
		stmts = append(stmts, Statement(stmt))
	}
	stmts = append(stmts, Statement(InsertVersionStmt, Version, time.Now().Unix()))
	_, err := conn.WriteStmt(context.Background(), stmts...)
	if err != nil {
		return errors.E(op, errors.Internal, err)
	}
	return nil
}

// DropTables deletes all the tables.
func DropTables(conn *gorqlite.Connection) error {
	stmts := make([]string, 0)
	for table := range AllTables {
		stmts = append(stmts, "DROP TABLE IF EXISTS "+table)
	}
	_, err := conn.Write(stmts)
	return err
}

func Statement(sql string, params ...interface{}) *gorqlite.Statement {
	return gorqlite.NewStatement(sql, params...)
}

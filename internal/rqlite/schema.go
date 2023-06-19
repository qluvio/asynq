package rqlite

import (
	"fmt"
	"sort"
	"time"

	"github.com/hibiken/asynq/internal/sqlite3"
)

var AllTables = map[string]string{
	QueuesTable:           CreateQueuesTableFmt,
	TasksTable:            CreateTasksTableFmt,
	ServersTable:          CreateServersTableFmt,
	WorkersTable:          CreateWorkersTableFmt,
	SchedulersTable:       CreateSchedulersTableFmt,
	SchedulerHistoryTable: CreateSchedulerHistoryTableFmt,
	CancellationTable:     CreateCancellationTableFmt,
	VersionTable:          CreateVersionTableFmt,
	StatsTable:            CreateStatsTableFmt,
}

type IndexCtor struct {
	Name   string
	Format string
}

var AllIndexes = map[string][]IndexCtor{
	TasksTable: {CreateTasksIndexesFmt},
}

const (
	Version               = "1.0.0"
	VersionTable          = "asynq_schema_version"
	CreateVersionTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	version text not null primary key,
	ts integer
)`
	InsertVersionStmtFmt = "INSERT INTO %s (version, ts) VALUES(?, ?) " +
		" ON CONFLICT(version) DO NOTHING;"

	QueuesTable          = "asynq_queues"
	CreateQueuesTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	queue_name      text not null primary key, 
	state           text not null
)`
	active    = "active"
	paused    = "paused"
	pending   = "pending"
	scheduled = "scheduled"
	retry     = "retry"
	archived  = "archived"
	completed = "completed"

	TasksTable          = "asynq_tasks"
	CreateTasksTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	ndx                     integer not null primary key,
	queue_name              text not null,
	type_name               text not null,
	task_uuid               text not null unique,
	unique_key              text not null unique, 
	unique_key_deadline     integer,
	task_msg                text,
	task_timeout            integer,
	task_deadline           integer,
	server_affinity         integer,
	pndx                    integer default 0,
	state                   text not null,
	scheduled_at            integer,
	deadline                integer,
	retry_at                integer,
	done_at                 integer,
	failed                  boolean,
	archived_at             integer,
	cleanup_at              integer,
	retain_until            integer,
	sid                     text,
	affinity_timeout        integer,
	recurrent               boolean,
	result                  text,
	pending_since           integer 
)`

	ServersTable          = "asynq_servers"
	CreateServersTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	sid         text not null unique,
	pid         integer not null,
	host        text not null,
	expire_at   integer,
	server_info text not null
)`

	WorkersTable          = "asynq_workers"
	CreateWorkersTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	sid         text not null,
	task_uuid   text not null,
	expire_at   integer,
	worker_info text not null
)`
	SchedulersTable          = "asynq_schedulers"
	CreateSchedulersTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	scheduler_id    text not null,
	expire_at       integer,
	scheduler_entry text not null
)`
	SchedulerHistoryTable          = "asynq_scheduler_history"
	CreateSchedulerHistoryTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	ndx                     integer not null primary key,
	uuid                    text not null,
	task_id                 text not null,
	enqueued_at             integer,
	scheduler_enqueue_event text not null
)`
	CancellationTable          = "asynq_cancel"
	CreateCancellationTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	ndx                     integer not null primary key,
	uuid                    text not null,
	cancelled_at            integer
)`

	StatsTable          = "asynq_stats"
	CreateStatsTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	day                    text not null, 
	ts                     integer,  
	queue_name             text not null,
	processed              integer DEFAULT 0,
	failed                 integer DEFAULT 0,
    PRIMARY KEY (day, queue_name)                              
)`
)

var (
	CreateTasksIndexesFmt = IndexCtor{
		Name:   "idx_%s_queue_and_state",
		Format: `CREATE INDEX IF NOT EXISTS %s ON %s (queue_name,state);`,
	}
)

func (conn *Connection) buildTables() {
	tables := make(map[string]string)
	tableNames := make(map[string]string)
	for table, ctorFmt := range AllTables {
		t := conn.config.TablesPrefix + table
		tables[t] = fmt.Sprintf(ctorFmt, t)
		tableNames[table] = t
	}
	indexes := make(map[string][]string)
	for table, ndxCtorFmts := range AllIndexes {
		tableIndexes := []string(nil)
		t := conn.config.TablesPrefix + table
		for _, ndxCtorFmt := range ndxCtorFmts {
			name := fmt.Sprintf(ndxCtorFmt.Name, t)
			tableIndexes = append(tableIndexes, fmt.Sprintf(ndxCtorFmt.Format, name, t))
		}
		indexes[t] = tableIndexes
	}
	conn.tables = tables
	conn.tableNames = tableNames
	conn.indexes = indexes
}

func (conn *Connection) AllTables() map[string]string {
	return conn.tables
}

func (conn *Connection) AllIndexes() map[string][]string {
	return conn.indexes
}

func (conn *Connection) table(name string) string {
	return conn.tableNames[name]
}

// CreateTablesIfNotExist returns true if tables were created, false if they were not.
func (conn *Connection) CreateTablesIfNotExist() (bool, error) {

	//
	// commented until a migration strategy is in place
	//

	//op := errors.Op("CreateTablesIfNotExist")

	//get := Statement("SELECT COUNT(*) FROM " + conn.table(VersionTable))
	//qrs, err := conn.QueryStmt(conn.ctx(), get)
	//if err != nil {
	//	if len(qrs) == 0 {
	//		return false, errors.E(op, errors.Internal, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{get}))
	//	}
	//	if qrs[0].Err() == nil || !strings.Contains(qrs[0].Err().Error(), "no such table:") {
	//		return false, errors.E(op, errors.Internal, NewRqliteRsError(op, qrs, err, []*sqlite3.Statement{get}))
	//	}
	//}
	//if len(qrs) == 0 {
	//	return false, errors.E(op, errors.Internal, NewRqliteRsError(op, qrs, nil, []*sqlite3.Statement{get}))
	//}
	//if qrs[0].NumRows() > 0 {
	//	return false, nil
	//}
	err := conn.CreateTables()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (conn *Connection) CreateTables() error {
	stmts := make([]*sqlite3.Statement, 0)
	tables := make([]string, 0)
	for _, stmt := range conn.AllTables() {
		tables = append(tables, stmt)
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

func (conn *Connection) CreateIndexes() error {
	stmts := make([]*sqlite3.Statement, 0)
	indexes := make([]string, 0)
	for _, stmt := range conn.AllIndexes() {
		indexes = append(indexes, stmt...)
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

// DropTables deletes all the tables.
func (conn *Connection) DropTables() error {
	stmts := make([]*sqlite3.Statement, 0)
	for table := range conn.AllTables() {
		stmts = append(stmts, Statement("DROP TABLE IF EXISTS "+table))
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("PurgeTables", wrs, err, stmts)
	}
	return nil
}

// PurgeTables purges data from all tables, except the version table.
func (conn *Connection) PurgeTables() error {
	stmts := make([]*sqlite3.Statement, 0)
	verTable := conn.table(VersionTable)
	for table := range conn.AllTables() {
		if table == verTable {
			continue
		}
		stmts = append(stmts, Statement(fmt.Sprintf("DELETE FROM '%s' ", table)))
	}
	wrs, err := conn.WriteStmt(conn.ctx(), stmts...)
	if err != nil {
		return NewRqliteWsError("PurgeTables", wrs, err, stmts)
	}
	return nil
}

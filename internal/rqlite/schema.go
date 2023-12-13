package rqlite

import (
	"fmt"
	"strings"
)

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
	ndx                     integer not null primary key AUTOINCREMENT,
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

	CompletedTasksTable          = "asynq_completed_tasks"
	CreateCompletedTasksTableFmt = `CREATE TABLE IF NOT EXISTS %s (
	ndx                     integer not null primary key,
	queue_name              text not null,
	type_name               text not null,
	task_uuid               text not null unique,
	task_msg                text,
	deadline                integer,
	done_at                 integer,
	retain_until            integer,
	sid                     text,
	result                  text
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

type TableCtor struct {
	NameFmt   string
	CreateFmt string
	Indexes   []IndexCtor
}

type IndexCtor struct {
	NameFmt   string
	CreateFmt string
}

var (
	CreateTasksIndexesFmt = IndexCtor{
		NameFmt:   "idx_%s_queue_and_state",
		CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (queue_name,state);`,
	}
	CreateTasksQueueIndexesFmt = IndexCtor{
		NameFmt:   "idx_%s_queue",
		CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (queue_name);`,
	}
	//CreateTasksStateIndexesFmt = IndexCtor{
	//	NameFmt:   "idx_%s_state",
	//	CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (state);`,
	//}
	CreateCompletedTasksQueueUuidIndexesFmt = IndexCtor{
		NameFmt:   "idx_%s_queue_and_uuid",
		CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (queue_name,task_uuid);`,
	}
	CreateCompletedTasksUuidIndexesFmt = IndexCtor{
		NameFmt:   "idx_%s_uuid",
		CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (task_uuid);`,
	}
	CreateCompletedTasksQueueRetainUntilIndexesFmt = IndexCtor{
		NameFmt:   "idx_%s_queue_retain_until",
		CreateFmt: `CREATE INDEX IF NOT EXISTS %s ON %s (queue_name,retain_until);`,
	}

	AllTables = map[string]TableCtor{
		QueuesTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateQueuesTableFmt,
		},
		TasksTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateTasksTableFmt,
			Indexes: []IndexCtor{
				CreateTasksIndexesFmt,
				CreateTasksQueueIndexesFmt,
			},
		},
		CompletedTasksTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateCompletedTasksTableFmt,
			Indexes: []IndexCtor{
				CreateCompletedTasksQueueUuidIndexesFmt,
				CreateCompletedTasksUuidIndexesFmt,
				CreateCompletedTasksQueueRetainUntilIndexesFmt,
			},
		},
		ServersTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateServersTableFmt,
		},
		WorkersTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateWorkersTableFmt,
		},
		SchedulersTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateSchedulersTableFmt,
		},
		SchedulerHistoryTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateSchedulerHistoryTableFmt,
		},
		CancellationTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateCancellationTableFmt,
		},
		VersionTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateVersionTableFmt,
		},
		StatsTable: {
			NameFmt:   "%s_%s",
			CreateFmt: CreateStatsTableFmt,
		},
	}
)

type TableDef struct {
	Name             string
	CreateStmt       string
	CreateIndexStmts []string
}

func buildTables(allTables map[string]TableCtor, tablesPrefix string) map[string]*TableDef {

	ret := make(map[string]*TableDef)

	// config may use 'prefix_'
	if strings.HasSuffix(tablesPrefix, "_") {
		tablesPrefix = tablesPrefix[:len(tablesPrefix)-1]
	}

	for name, ctor := range allTables {
		tableName := fmt.Sprintf(ctor.NameFmt, tablesPrefix, name)
		if strings.HasPrefix(tableName, "_") {
			tableName = tableName[1:]
		}
		createTable := fmt.Sprintf(ctor.CreateFmt, tableName)
		indexes := []string(nil)
		for _, ndx := range ctor.Indexes {
			indexName := fmt.Sprintf(ndx.NameFmt, tableName)
			indexes = append(indexes, fmt.Sprintf(ndx.CreateFmt, indexName, tableName))
		}

		ret[name] = &TableDef{
			Name:             tableName,
			CreateStmt:       createTable,
			CreateIndexStmts: indexes,
		}
	}

	return ret
}

func (conn *Connection) buildTables() {
	conn.tables = buildTables(AllTables, conn.config.TablesPrefix)
}

func (conn *Connection) AllTables() map[string]*TableDef {
	return conn.tables
}

func (conn *Connection) table(name string) string {
	t := conn.tables[name]
	if t == nil {
		return ""
	}
	return t.Name
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
	return conn.createTables(conn.AllTables())
}

func (conn *Connection) CreateIndexes() error {
	return conn.createIndexes(conn.AllTables())
}

// DropTables deletes all the tables.
func (conn *Connection) DropTables() error {
	return conn.dropTables(conn.AllTables())
}

// PurgeTables purges data from all tables, except the version table.
func (conn *Connection) PurgeTables() error {
	return conn.purgeTables(conn.AllTables(), VersionTable)
}

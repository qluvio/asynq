package asynq

import (
	"net/http"
	"time"

	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

type RqliteConfig struct {
	Type                  string        `json:"type"`                        // rqlite | sqlite
	SqliteDbPath          string        `json:"db_path,omitempty"`           // sqlite: DB path
	SqliteInMemory        bool          `json:"sqlite_in_memory,omitempty"`  // sqlite: true to use in-memory DB
	SqliteTracing         bool          `json:"sqlite_tracing,omitempty"`    // sqlite: true to trace sql requests execution
	SqliteFKEnabled       bool          `json:"sqlite_fk_enabled"`           // sqlite: true to enable foreign keys constraints (default is false)
	SqliteDisableWall     bool          `json:"sqlite_disable_wall"`         // sqlite: true to disable wall mode with on disk db (default is false)
	SqliteSynchronousMode string        `json:"sqlite_synchronous_mode"`     // sqlite: synchronous mode (OFF | NORMAL | FULL | EXTRA) (default is NORMAL)
	RqliteUrl             string        `json:"rqlite_url,omitempty"`        // Rqlite server url, e.g. http://localhost:4001.
	ConsistencyLevel      string        `json:"consistency_level,omitempty"` // consistency level: none | weak| strong
	TablesPrefix          string        `json:"tables_prefix,omitempty"`     // tables prefix
	PubsubPollingInterval time.Duration `json:"pubsub_polling_interval"`     // cancellation pub-sub polling period
}

func (c *RqliteConfig) InitDefaults() *RqliteConfig {
	c.SqliteSynchronousMode = "NORMAL"
	c.ConsistencyLevel = "strong"
	c.PubsubPollingInterval = rqlite.PubsubPollingInterval
	return c
}

func (c *RqliteConfig) make() *rqlite.Config {
	ret := (&rqlite.Config{}).InitDefaults()
	// PENDING(GIL): I'm following what exists, but configs should be defined in
	//  their own package and be accessed from both package 'asynq' and internal/* ...
	ret.Type = c.Type
	ret.SqliteDbPath = c.SqliteDbPath
	ret.SqliteInMemory = c.SqliteInMemory
	ret.SqliteTracing = c.SqliteTracing
	ret.SqliteFKEnabled = c.SqliteFKEnabled
	ret.SqliteDisableWall = c.SqliteDisableWall
	ret.SqliteSynchronousMode = c.SqliteSynchronousMode
	ret.RqliteUrl = c.RqliteUrl
	if len(c.ConsistencyLevel) > 0 {
		ret.ConsistencyLevel = c.ConsistencyLevel
	}
	ret.TablesPrefix = c.TablesPrefix
	//ret.MaxArchiveSize = c.MaxArchiveSize
	//ret.ArchivedExpirationInDays = c.ArchivedExpirationInDays
	//ret.ArchiveTTL = c.StatsTTL
	//ret.SchedulerHistoryMaxEvents = c.SchedulerHistoryMaxEvents
	ret.PubsubPollingInterval = c.PubsubPollingInterval
	return ret
}

type RqliteConnOpt struct {
	Config     *RqliteConfig
	HttpClient *http.Client
	Log        log.Base
}

func (o RqliteConnOpt) MakeClient() interface{} {
	return rqlite.NewRQLite(o.Config.make(), o.HttpClient, o.Log)
}

func (o RqliteConnOpt) Logger() Logger {
	return o.Log
}

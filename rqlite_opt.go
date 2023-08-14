package asynq

import (
	"net/http"

	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

// RqliteConfig exports rqlite.Config
type RqliteConfig = rqlite.Config
type RQLiteConnConfig = rqlite.RQLiteConnConfig
type SQLiteConnConfig = rqlite.SQLiteConnConfig

func NewRqliteConfig() *RqliteConfig {
	return (&RqliteConfig{}).InitDefaults()
}

type RqliteConnOpt struct {
	Config     *RqliteConfig
	HttpClient *http.Client
	Log        log.Base
}

func (o RqliteConnOpt) MakeClient() interface{} {
	return rqlite.NewRQLite(o.Config, o.HttpClient, o.Log)
}

func (o RqliteConnOpt) Logger() Logger {
	return o.Log
}

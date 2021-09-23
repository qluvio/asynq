package asynq

import (
	"net/http"

	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

type RqliteConfig struct {
	RqliteUrl        string `json:"rqlite_url"`                  // Rqlite server url, e.g. http://localhost:4001.
	ConsistencyLevel string `json:"consistency_level,omitempty"` // consistency level: none | weak| strong
	TablesPrefix     string `json:"tables_prefix,omitempty"`     // tables prefix
}

func (c RqliteConfig) InitDefaults() {
	c.ConsistencyLevel = "strong"
}

func (c RqliteConfig) make() rqlite.Config {
	ret := (&rqlite.Config{}).InitDefaults()
	// PENDING(GIL): I'm following what exists, but configs should be defined in
	//  their own package and be accessed from both package 'asynq' and internal/* ...
	ret.RqliteUrl = c.RqliteUrl
	if len(c.ConsistencyLevel) > 0 {
		ret.ConsistencyLevel = c.ConsistencyLevel
	}
	ret.TablesPrefix = c.TablesPrefix
	return *ret
}

type RqliteConnOpt struct {
	Config     RqliteConfig
	HttpClient *http.Client
	Log        log.Base
}

func (o RqliteConnOpt) MakeClient() interface{} {
	return rqlite.NewRQLite(o.Config.make(), o.HttpClient, o.Log)
}

func (o RqliteConnOpt) Logger() Logger {
	return o.Log
}

package asynq

import (
	"net/http"

	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

type RqliteConfig struct {
	RqliteUrl        string `json:"rqlite_url"`                  // Rqlite server url, e.g. http://localhost:4001.
	ConsistencyLevel string `json:"consistency_level,omitempty"` // consistency level: none | weak| strong
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
	return *ret
}

type RqliteConnOpt struct {
	Config     RqliteConfig
	HttpClient *http.Client
	Logger     log.Base
}

func (o RqliteConnOpt) MakeClient() interface{} {
	return rqlite.NewRQLite(o.Config.make(), o.HttpClient, o.Logger)
}

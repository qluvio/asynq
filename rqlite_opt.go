package asynq

import (
	"net/http"

	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

type RqliteConnOpt struct {
	// Rqlite server url, e.g. http://localhost:4001.
	Config     rqlite.Config
	HttpClient *http.Client
	Logger     log.Base
}

func (o RqliteConnOpt) MakeClient() interface{} {

	return rqlite.NewRQLite(o.Config, o.HttpClient, o.Logger)
}

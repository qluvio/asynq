// Package sqlite provides the implementation for connecting to sqlite. The rest
// of the Asynq implementation for sqlite is reused from the rqlite package.
package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/hibiken/asynq/internal/sqlite3/db"
)

type Config struct {
	DbPath          string `json:"db_path,omitempty"`
	InMemory        bool   `json:"in_memory,omitempty"`
	FkEnabled       bool   `json:"fk_enabled,omitempty"`
	DisableWal      bool   `json:"disable_wal,omitempty"`
	SynchronousMode string `json:"synchronous_mode,omitempty"`
	Tracing         bool   `json:"tracing,omitempty"`
}

func (c *Config) InitDefaults() *Config {
	c.SynchronousMode = "NORMAL"
	return c
}

func (c *Config) Validate() error {
	switch strings.ToUpper(c.SynchronousMode) {
	case "OFF", "NORMAL", "FULL", "EXTRA":
	case "":
		c.SynchronousMode = "NORMAL"
	default:
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition,
			fmt.Sprintf("invalid synchronous mode: %s", c.SynchronousMode))
	}
	return nil
}

// NewSQLiteConnection returns an initialized *sqlite3.SQLiteConnection or an error
func NewSQLiteConnection(
	ctx context.Context,
	cfg Config,
	logger log.Base) (*sqlite3.SQLiteConnection, error) {

	op := errors.Op("NewSQLiteConnection")

	var err error
	var conn *db.DB
	switch cfg.InMemory {
	case false:
		conn, err = db.OpenContext(ctx, cfg.DbPath, cfg.FkEnabled, !cfg.DisableWal, cfg.SynchronousMode)
	case true:
		conn, err = db.OpenInMemoryPath(cfg.DbPath, cfg.FkEnabled)
	}
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return sqlite3.NewSQLiteConnection(conn, true, logger, cfg.Tracing), nil
}

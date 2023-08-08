// Package sqlite provides the implementation for connecting to sqlite. The rest
// of the Asynq implementation for sqlite is reused from the rqlite package.
package sqlite

import (
	"context"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/hibiken/asynq/internal/sqlite3/db"
)

/*
type SQLiteConnectionConfig struct {
	DbPath          string
	InMemory        bool
	FkEnabled       bool
	WalMode         bool
	SynchronousMode string
	Tracing         bool
}
*/

// NewSQLiteConnection returns an initialized *sqlite3.SQLiteConnection or an error
func NewSQLiteConnection(
	ctx context.Context,
	dbPath string,
	inMemory bool,
	logger log.Base,
	tracing bool,
	fkEnabled bool,
	walEnabled bool,
	synchronousMode string) (*sqlite3.SQLiteConnection, error) {

	op := errors.Op("NewSQLiteConnection")

	var err error
	var conn *db.DB
	switch inMemory {
	case false:
		conn, err = db.OpenContext(ctx, dbPath, fkEnabled, walEnabled, synchronousMode)
	case true:
		conn, err = db.OpenInMemoryPath(dbPath, fkEnabled)
	}
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return sqlite3.NewSQLiteConnection(conn, true, logger, tracing), nil
}

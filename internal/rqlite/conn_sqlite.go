package rqlite

import (
	"context"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite"
)

func NewSQLiteConnection(ctx context.Context, config *Config) (*Connection, error) {
	op := errors.Op("open")

	dbConnection, err := sqlite.NewSQLiteConnection(ctx, config.SqliteDbPath, config.SqliteInMemory)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return &Connection{
		DbConnection: dbConnection,
		config:       config,
	}, nil
}
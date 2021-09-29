package asynq

import (
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/rqlite"
)

// makeBroker returns a base.Broker instance given a client connection option.
func makeBroker(r ClientConnOpt) (base.Broker, error) {
	c := r.MakeClient()

	switch cl := c.(type) {
	case redis.UniversalClient:
		return rdb.NewRDB(cl), nil
	case *rqlite.RQLite:
		return cl, nil
	default:
		return nil, errors.E(errors.Op("makeBroker"), errors.Internal, fmt.Sprintf("asynq: unsupported ClientConnOpt type %T", r))
	}
}

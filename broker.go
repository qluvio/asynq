package asynq

import (
	"context"
	"fmt"
	"time"

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

// newWakingBroker returns a base.Broker wrapper that sends wake signals on the specified channels when tasks are
// expected to be moved into pending state, i.e. become ready for processing.
func newWakingBroker(broker base.Broker, deadlines *base.Deadlines, wakePrcCh chan<- bool, wakeFwdCh chan<- bool) base.Broker {
	return &wakingBroker{Broker: broker, deadlines: deadlines, wakePrcCh: wakePrcCh, wakeFwdCh: wakeFwdCh}
}

type wakingBroker struct {
	base.Broker
	deadlines *base.Deadlines
	wakePrcCh chan<- bool
	wakeFwdCh chan<- bool
}

func (b *wakingBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	err := b.Broker.Enqueue(ctx, msg)
	if err == nil {
		b.wakePrc()
	}
	return err
}

func (b *wakingBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration, forceUnique ...bool) error {
	err := b.Broker.EnqueueUnique(ctx, msg, ttl, forceUnique...)
	if err == nil {
		b.wakePrc()
	}
	return err
}

func (b *wakingBroker) Requeue(serverID string, msg *base.TaskMessage, aborted bool) error {
	err := b.Broker.Requeue(serverID, msg, aborted)
	if err == nil && !aborted {
		if msg.ReprocessAfter <= 0 {
			b.wakePrc()
		} else {
			b.wakeFwdAt(time.Now().Add(time.Second * time.Duration(msg.ReprocessAfter)))
		}
	}
	return err
}
func (b *wakingBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	err := b.Broker.Schedule(ctx, msg, processAt)
	if err == nil {
		b.wakeFwdAt(processAt)
	}
	return err
}
func (b *wakingBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration, forceUnique ...bool) error {
	err := b.Broker.ScheduleUnique(ctx, msg, processAt, ttl, forceUnique...)
	if err == nil {
		b.wakeFwdAt(processAt)
	}
	return err
}
func (b *wakingBroker) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	err := b.Broker.Retry(msg, processAt, errMsg, isFailure)
	if err == nil {
		b.wakeFwdAt(processAt)
	}
	return err
}
func (b *wakingBroker) EnqueueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	err := b.Broker.EnqueueBatch(ctx, msgs...)
	if err == nil {
		b.wakePrc()
	}
	return err
}
func (b *wakingBroker) EnqueueUniqueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	err := b.Broker.EnqueueUniqueBatch(ctx, msgs...)
	if err == nil {
		b.wakePrc()
	}
	return err
}
func (b *wakingBroker) ScheduleBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	err := b.Broker.ScheduleBatch(ctx, msgs...)
	if err == nil {
		for _, msg := range msgs {
			b.wakeFwdAt(msg.ProcessAt)
		}
	}
	return err
}
func (b *wakingBroker) ScheduleUniqueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	err := b.Broker.ScheduleUniqueBatch(ctx, msgs...)
	if err == nil {
		for _, msg := range msgs {
			b.wakeFwdAt(msg.ProcessAt)
		}
	}
	return err
}

func (b *wakingBroker) wakePrc() {
	select {
	case b.wakePrcCh <- true:
	default:
	}
}

func (b *wakingBroker) wakeFwd() {
	select {
	case b.wakeFwdCh <- true:
	default:
	}
}

func (b *wakingBroker) wakeFwdAt(t time.Time) {
	now := time.Now()
	if t.Before(now) || t.Equal(now) {
		b.wakeFwd()
	} else {
		b.deadlines.Add("", t, b.wakeFwd)
	}
}

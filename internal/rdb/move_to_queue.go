package rdb

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

var (
	ErrTaskNotCompleted = errors.New("no completed task found")
)

func (r *RDB) MoveToQueue(ctx context.Context, fromQueue string, msg *base.TaskMessage, processAt time.Time, active bool) (base.TaskState, error) {
	if active {
		return r.moveActiveToQueue(ctx, fromQueue, msg, processAt)
	}
	return r.moveCompletedToQueue(ctx, fromQueue, msg, processAt)
}

// KEYS[1] -> asynq:{<prev_qname>}:active
// KEYS[2] -> asynq:{<prev_qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:pending
// KEYS[4] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> task ID
// ARGV[2] -> task msg
// ARGV[3] -> task timeout
// ARGV[4] -> task deadline
// ARGV[5] -> pending_since
// Note: Use RPUSH to push to the head of the queue.
var requeueActiveToQueueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("TASK NOT ACTIVE")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end

redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4],
           "msg", ARGV[2],
           "state", "pending",
           "timeout", ARGV[3],
           "deadline", ARGV[4],
           "pending_since", ARGV[5])
if redis.call("GET", KEYS[5]) == ARGV[1] then
  redis.call("DEL", KEYS[5])
end
return redis.status_reply("OK")`)

// requeueActiveUniqueToQueueCmd is similar as requeueActiveToQueueCmd and moves the unique key TTL.
// KEYS[5] -> unique key
// ARGV[2] -> unique key TTL
var requeueActiveUniqueToQueueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("TASK NOT ACTIVE")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4],
           "msg", ARGV[2],
           "state", "pending",
           "timeout", ARGV[3],
           "deadline", ARGV[4],
           "pending_since", ARGV[5])
redis.call("SET", KEYS[5], ARGV[1], "EX", ARGV[6])
return redis.status_reply("OK")`)

var rescheduleActiveToQueueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("TASK NOT ACTIVE")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("HSET", KEYS[3],
           "msg", ARGV[2],
           "state", "scheduled",
           "timeout", ARGV[3],
           "deadline", ARGV[4])
redis.call("ZADD", KEYS[4], ARGV[5], ARGV[1])
if redis.call("GET", KEYS[5]) == ARGV[1] then
  redis.call("DEL", KEYS[5])
end
return redis.status_reply("OK")`)

var rescheduleActiveUniqueToQueueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("TASK NOT ACTIVE")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("HSET", KEYS[3],
           "msg", ARGV[2],
           "state", "scheduled",
           "timeout", ARGV[3],
           "deadline", ARGV[4],
           "unique_key", KEYS[4])
redis.call("ZADD", KEYS[4], ARGV[5], ARGV[1])
redis.call("SET", KEYS[5], ARGV[1], "EX", ARGV[6])
return redis.status_reply("OK")`)

func (r *RDB) moveActiveToQueue(ctx context.Context, fromQueue string, msg *base.TaskMessage, processAt time.Time) (base.TaskState, error) {
	var op errors.Op = "rdb.MoveActiveToQueue"
	now := r.clock.Now()

	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}

	retState := base.TaskStatePending
	if processAt.After(now) {
		retState = base.TaskStateScheduled
	}
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	if retState == base.TaskStatePending {
		// enqueue
		keys := []string{
			base.ActiveKey(fromQueue),
			base.DeadlinesKey(fromQueue),
			base.PendingKey(msg.Queue),
			base.TaskKey(msg.Queue, msg.ID),
			msg.UniqueKey,
		}
		argv := []interface{}{
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			r.clock.Now().UnixNano(),
			int(msg.UniqueKeyTTL),
		}
		if len(msg.UniqueKey) == 0 || msg.UniqueKeyTTL == 0 {
			err = r.runScript(ctx, op, requeueActiveToQueueCmd, keys, argv...)
		} else {
			err = r.runScript(ctx, op, requeueActiveUniqueToQueueCmd, keys, argv...)
		}
	} else {
		// schedule
		keys := []string{
			base.ActiveKey(fromQueue),
			base.DeadlinesKey(fromQueue),
			base.TaskKey(msg.Queue, msg.ID),
			base.ScheduledKey(msg.Queue),
			msg.UniqueKey,
		}
		argv := []interface{}{
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			processAt.Unix(),
			int(msg.UniqueKeyTTL),
		}
		if len(msg.UniqueKey) == 0 || msg.UniqueKeyTTL == 0 {
			err = r.runScript(ctx, op, rescheduleActiveToQueueCmd, keys, argv...)
		} else {
			err = r.runScript(ctx, op, rescheduleActiveUniqueToQueueCmd, keys, argv...)
		}
	}
	if err != nil {
		return 0, err
	}
	return retState, nil
}

// requeueCompletedCmd enqueues a given task message already completed.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// KEYS[3] -> asynq:{<qname>}:completed
// --
// ARGV[1] -> task ID
// ARGV[2] -> task message data
// ARGV[3] -> task timeout in seconds (0 if not timeout)
// ARGV[4] -> task deadline in unix time (0 if no deadline)
// ARGV[5] -> current unix time in nsec
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID not in completed state
var requeueCompletedCmd = redis.NewScript(`
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[2],
           "state", "pending",
           "timeout", ARGV[3],
           "deadline", ARGV[4],
           "pending_since", ARGV[5])
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
`)

// requeueCompletedUniqueToQueueCmd re-queues a completed task message if the task is unique.
//
// KEYS[1] -> asynq:{<qname>}:t:<taskid>
// KEYS[2] -> asynq:{<qname>}:pending
// KEYS[3] -> asynq:{<qname>}:completed
// KEYS[4] -> unique key
// --
// ARGV[1] -> task ID
// ARGV[2] -> task message data
// ARGV[3] -> task timeout in seconds (0 if not timeout)
// ARGV[4] -> task deadline in unix time (0 if no deadline)
// ARGV[5] -> current unix time in nsec
// ARGV[6] -> uniqueness lock TTL
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID not completed
// Returns -1 if task unique key already exists
// NOTE: A zero TTL makes the script fail with 'ERR invalid expire time in set'
var requeueCompletedUniqueToQueueCmd = redis.NewScript(`
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
	return 0
end
local ok = redis.call("SET", KEYS[4], ARGV[1], "EX", ARGV[6])
if not ok then
  return -1
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[2],
           "state", "pending",
           "timeout", ARGV[3],
           "deadline", ARGV[4],
           "pending_since", ARGV[5],
           "unique_key", KEYS[4])
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
`)

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:scheduled
// KEYS[3] -> asynq:{<qname>}:completed
//
// ARGV[1] -> task ID
// ARGV[2] -> process_at time in Unix time
// ARGV[3] -> task message data
// ARGV[4] -> task timeout in seconds (0 if not timeout)
// ARGV[5] -> task deadline in unix time (0 if no deadline)
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID not completed
var rescheduleCompletedCmd = redis.NewScript(`
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[3],
           "state", "scheduled",
           "timeout", ARGV[4],
           "deadline", ARGV[5])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
return 1
`)

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:scheduled
// KEYS[3] -> asynq:{<qname>}:completed
// KEYS[4] -> unique key
//
// ARGV[1] -> task ID
// ARGV[2] -> score (process_at timestamp)
// ARGV[3] -> task message
// ARGV[4] -> task timeout in seconds (0 if not timeout)
// ARGV[5] -> task deadline in unix time (0 if no deadline)
// ARGV[6] -> uniqueness lock TTL
//
// Output:
// Returns 1 if successfully scheduled
// Returns 0 if task ID not completed
// Returns -1 if task unique key already exists
var rescheduleCompletedUniqueCmd = redis.NewScript(`
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
	return 0
end
local ok = redis.call("SET", KEYS[4], ARGV[1], "EX", ARGV[6])
if not ok then
  return -1
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[3],
           "state", "scheduled",
           "timeout", ARGV[4],
           "deadline", ARGV[5],
           "unique_key", KEYS[3])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
return 1
`)

func (r *RDB) moveCompletedToQueue(ctx context.Context, fromQueue string, msg *base.TaskMessage, processAt time.Time) (base.TaskState, error) {
	var op errors.Op = "rdb.MoveCompletedToQueue"
	now := r.clock.Now()

	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}

	retState := base.TaskStatePending
	if processAt.After(now) {
		retState = base.TaskStateScheduled
	}
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	n := int64(0)
	if retState == base.TaskStatePending {
		// enqueue
		keys := []string{
			base.TaskKey(msg.Queue, msg.ID),
			base.PendingKey(msg.Queue),
			base.CompletedKey(fromQueue),
		}
		argv := []interface{}{
			msg.ID,
			encoded,
			msg.Timeout,
			msg.Deadline,
			r.clock.Now().UnixNano(),
		}
		if len(msg.UniqueKey) == 0 || msg.UniqueKeyTTL == 0 {
			// redis script fails when msg.UniqueKeyTTL is zero
			n, err = r.runScriptWithErrorCode(ctx, op, requeueCompletedCmd, keys, argv...)
		} else {
			keys = append(keys, msg.UniqueKey)
			argv = append(argv, int(msg.UniqueKeyTTL))
			n, err = r.runScriptWithErrorCode(ctx, op, requeueCompletedUniqueToQueueCmd, keys, argv...)
		}
	} else {
		// schedule
		keys := []string{
			base.TaskKey(msg.Queue, msg.ID),
			base.ScheduledKey(msg.Queue),
			base.CompletedKey(fromQueue),
		}
		argv := []interface{}{
			msg.ID,
			processAt.Unix(),
			encoded,
			msg.Timeout,
			msg.Deadline,
		}
		if len(msg.UniqueKey) == 0 || msg.UniqueKeyTTL == 0 {
			// redis script fails when msg.UniqueKeyTTL is zero
			n, err = r.runScriptWithErrorCode(ctx, op, rescheduleCompletedCmd, keys, argv...)
		} else {
			keys = append(keys, msg.UniqueKey)
			argv = append(argv, int(msg.UniqueKeyTTL))
			n, err = r.runScriptWithErrorCode(ctx, op, rescheduleCompletedUniqueCmd, keys, argv...)
		}
	}
	if err != nil {
		return 0, err
	}
	switch n {
	case -1:
		return 0, errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	case 0:
		return 0, errors.E(op, errors.FailedPrecondition, ErrTaskNotCompleted)
	}

	return retState, nil
}

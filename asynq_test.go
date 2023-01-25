// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"flag"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rqlite"
)

//============================================================================
// This file defines helper functions and variables used in other test files.
//============================================================================

// variables used for package testing.
var (
	brokerType string // redis | rqlite
	redisAddr  string
	redisDB    int

	useRedisCluster   bool
	redisClusterAddrs string // comma-separated list of host:port

	rqliteConfig RqliteConfig

	testLogLevel = FatalLevel
)

var testLogger *log.Logger

func init() {
	flag.StringVar(&brokerType, "broker_type", "redis", "broker type to use in testing: redis|rqlite")

	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&redisDB, "redis_db", 14, "redis db number to use in testing")
	flag.BoolVar(&useRedisCluster, "redis_cluster", false, "use redis cluster as a broker in testing")
	flag.StringVar(&redisClusterAddrs, "redis_cluster_addrs", "localhost:7000,localhost:7001,localhost:7002", "comma separated list of redis server addresses")

	rqliteConfig.InitDefaults()
	flag.StringVar(&rqliteConfig.RqliteUrl, "rqlite_addr", "http://localhost:4001", "rqlite address to use")
	flag.StringVar(&rqliteConfig.ConsistencyLevel, "rqlite_consistency_level", "strong", "rqlite consistency level")

	flag.Var(&testLogLevel, "loglevel", "log level to use in testing")
	testLogger = log.NewLogger(nil)
	testLogger.SetLevel(toInternalLogLevel(testLogLevel))
}

type TestContext interface {
	FlushDB()
	Close() error

	GetPendingMessages(qname string) []*base.TaskMessage
	GetCompletedEntries(qname string) []base.Z
	GetActiveMessages(qname string) []*base.TaskMessage
	GetRetryMessages(qname string) []*base.TaskMessage
	GetArchivedMessages(qname string) []*base.TaskMessage
	GetScheduledMessages(qname string) []*base.TaskMessage

	GetScheduledEntries(qname string) []base.Z
	GetDeadlinesEntries(qname string) []base.Z
	GetRetryEntries(qname string) []base.Z
	GetArchivedEntries(qname string) []base.Z

	GetUniqueKeyTTL(qname string, taskType string, taskPayload []byte) time.Duration

	InitQueue(qname string)
	QueueExist(qname string) bool

	SeedAllPendingQueues(pending map[string][]*base.TaskMessage)
	SeedAllCompletedQueues(completed map[string][]base.Z)
	SeedPendingQueue(pending []*base.TaskMessage, queue string)
	SeedAllActiveQueues(inProgress map[string][]*base.TaskMessage)
	SeedActiveQueue(msgs []*base.TaskMessage, queue string)
	SeedAllDeadlines(deadlines map[string][]base.Z)
	SeedAllRetryQueues(retry map[string][]base.Z)
	SeedAllArchivedQueues(archived map[string][]base.Z)
	SeedAllScheduledQueues(scheduled map[string][]base.Z)
	SeedAllProcessedQueues(processed map[string]int, doneAt time.Time)
	SeedProcessedQueue(processedCount int, qname string, ts time.Time)
	SeedAllFailedQueues(failed map[string]int, doneAt time.Time)
	SeedFailedQueue(failedCount int, qname string, ts time.Time)
}

func getClientConnOpt(tb testing.TB) ClientConnOpt {
	switch brokerType {
	case redisType:
		return getRedisConnOpt(tb)
	case rqliteType:
		return RqliteConnOpt{Config: rqliteConfig}
	}
	tb.Fatal("invalid broker type: " + brokerType)
	return nil
}

func setupTestContext(tb testing.TB) TestContext {
	var ret TestContext

	switch brokerType {
	case redisType:
		opt := getRedisConnOpt(tb)
		ret = &redisTestContext{
			tb: tb,
			r:  opt.MakeClient().(redis.UniversalClient),
		}
	case rqliteType:
		opt := RqliteConnOpt{Config: rqliteConfig}
		ret = &rqliteTestContext{
			tb: tb,
			r:  opt.MakeClient().(*rqlite.RQLite),
		}
	default:
		tb.Fatal("invalid broker type: " + brokerType)
	}
	ret.FlushDB()
	return ret
}

var sortTaskOpt = cmp.Transformer("SortMsg", func(in []*Task) []*Task {
	out := append([]*Task(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Type() < out[j].Type()
	})
	return out
})

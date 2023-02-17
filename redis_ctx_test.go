package asynq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/require"
)

func getRedisConnOpt(tb testing.TB) RedisConnOpt {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		return RedisClusterClientOpt{
			Addrs: addrs,
		}
	}
	return RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	}
}

type redisTestContext struct {
	tb testing.TB
	r  redis.UniversalClient
}

func (c *redisTestContext) Close() error {
	return c.r.Close()
}

func (c *redisTestContext) FlushDB() {
	h.FlushDB(c.tb, c.r)
}

func (c *redisTestContext) GetPendingMessages(qname string) []*base.TaskMessage {
	return h.GetPendingMessages(c.tb, c.r, qname)
}

func (c *redisTestContext) GetCompletedEntries(qname string) []base.Z {
	return h.GetCompletedEntries(c.tb, c.r, qname)
}

func (c *redisTestContext) GetActiveMessages(qname string) []*base.TaskMessage {
	return h.GetActiveMessages(c.tb, c.r, qname)
}

func (c *redisTestContext) GetRetryMessages(qname string) []*base.TaskMessage {
	return h.GetRetryMessages(c.tb, c.r, qname)
}

func (c *redisTestContext) GetArchivedMessages(qname string) []*base.TaskMessage {
	return h.GetArchivedMessages(c.tb, c.r, qname)
}

func (c *redisTestContext) GetScheduledMessages(qname string) []*base.TaskMessage {
	return h.GetScheduledMessages(c.tb, c.r, qname)
}

func (c *redisTestContext) GetScheduledEntries(qname string) []base.Z {
	return h.GetScheduledEntries(c.tb, c.r, qname)
}

func (c *redisTestContext) GetDeadlinesEntries(qname string) []base.Z {
	return h.GetDeadlinesEntries(c.tb, c.r, qname)
}

func (c *redisTestContext) GetRetryEntries(qname string) []base.Z {
	return h.GetRetryEntries(c.tb, c.r, qname)
}

func (c *redisTestContext) GetArchivedEntries(qname string) []base.Z {
	return h.GetArchivedEntries(c.tb, c.r, qname)
}

func (c *redisTestContext) GetUniqueKeyTTL(qname string, taskType string, taskPayload []byte) time.Duration {
	return h.GetUniqueKeyTTL(c.r, qname, taskType, taskPayload)
}

func (c *redisTestContext) InitQueue(qname string) {
	err := c.r.SAdd(context.Background(), base.AllQueues, qname).Err()
	require.NoError(c.tb, err, "could not initialize all queue set: %v", err)
}

func (c *redisTestContext) QueueExist(qname string) bool {
	return c.r.SIsMember(context.Background(), base.AllQueues, qname).Val()
}

func (c *redisTestContext) SeedAllPendingQueues(pending map[string][]*base.TaskMessage) {
	h.SeedAllPendingQueues(c.tb, c.r, pending)
}

func (c *redisTestContext) SeedAllCompletedQueues(completed map[string][]base.Z) {
	h.SeedAllCompletedQueues(c.tb, c.r, completed)
}

func (c *redisTestContext) SeedPendingQueue(pending []*base.TaskMessage, queue string) {
	h.SeedPendingQueue(c.tb, c.r, pending, queue)
}

func (c *redisTestContext) SeedAllActiveQueues(inProgress map[string][]*base.TaskMessage) {
	h.SeedAllActiveQueues(c.tb, c.r, inProgress)
}

func (c *redisTestContext) SeedActiveQueue(msgs []*base.TaskMessage, queue string) {
	h.SeedActiveQueue(c.tb, c.r, msgs, queue)
}

func (c *redisTestContext) SeedAllDeadlines(deadlines map[string][]base.Z) {
	h.SeedAllDeadlines(c.tb, c.r, deadlines)
}

func (c *redisTestContext) SeedAllRetryQueues(retry map[string][]base.Z) {
	h.SeedAllRetryQueues(c.tb, c.r, retry)
}

func (c *redisTestContext) SeedAllArchivedQueues(archived map[string][]base.Z) {
	h.SeedAllArchivedQueues(c.tb, c.r, archived)
}

func (c *redisTestContext) SeedAllScheduledQueues(scheduled map[string][]base.Z) {
	h.SeedAllScheduledQueues(c.tb, c.r, scheduled)
}

func (c *redisTestContext) SeedAllProcessedQueues(processed map[string]int, doneAt time.Time) {
	for qname, n := range processed {
		processedKey := base.ProcessedKey(qname, doneAt)
		c.r.Set(context.Background(), processedKey, n, 0)
	}
}

func (c *redisTestContext) SeedAllFailedQueues(failed map[string]int, doneAt time.Time) {
	for qname, n := range failed {
		failedKey := base.FailedKey(qname, doneAt)
		c.r.Set(context.Background(), failedKey, n, 0)
	}
}

func (c *redisTestContext) SeedProcessedQueue(processedCount int, qname string, ts time.Time) {
	processedKey := base.ProcessedKey(qname, ts)
	c.r.Set(context.Background(), processedKey, processedCount, 0)
}

func (c *redisTestContext) SeedFailedQueue(failedCount int, qname string, ts time.Time) {
	failedKey := base.FailedKey(qname, ts)
	c.r.Set(context.Background(), failedKey, failedCount, 0)
}

func (c *redisTestContext) SeedLastPendingSince(qname string, enqueueTime time.Time) {
	if enqueueTime.IsZero() {
		return
	}
	ctx := context.Background()
	oldestPendingMessageID := c.r.LRange(ctx, base.PendingKey(qname), -1, -1).Val()[0] // get the right most msg in the list
	c.r.HSet(ctx, base.TaskKey(qname, oldestPendingMessageID), "pending_since", enqueueTime.UnixNano())
}

func TestParseRedisURI(t *testing.T) {
	tests := []struct {
		uri  string
		want RedisConnOpt
	}{
		{
			"redis://localhost:6379",
			RedisClientOpt{Addr: "localhost:6379"},
		},
		{
			"redis://localhost:6379/3",
			RedisClientOpt{Addr: "localhost:6379", DB: 3},
		},
		{
			"redis://:mypassword@localhost:6379",
			RedisClientOpt{Addr: "localhost:6379", Password: "mypassword"},
		},
		{
			"redis://:mypassword@127.0.0.1:6379/11",
			RedisClientOpt{Addr: "127.0.0.1:6379", Password: "mypassword", DB: 11},
		},
		{
			"redis-socket:///var/run/redis/redis.sock",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock"},
		},
		{
			"redis-socket://:mypassword@/var/run/redis/redis.sock",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", Password: "mypassword"},
		},
		{
			"redis-socket:///var/run/redis/redis.sock?db=7",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", DB: 7},
		},
		{
			"redis-socket://:mypassword@/var/run/redis/redis.sock?db=12",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", Password: "mypassword", DB: 12},
		},
		{
			"redis-sentinel://localhost:5000,localhost:5001,localhost:5002?master=mymaster",
			RedisFailoverClientOpt{
				MasterName:    "mymaster",
				SentinelAddrs: []string{"localhost:5000", "localhost:5001", "localhost:5002"},
			},
		},
		{
			"redis-sentinel://:mypassword@localhost:5000,localhost:5001,localhost:5002?master=mymaster",
			RedisFailoverClientOpt{
				MasterName:    "mymaster",
				SentinelAddrs: []string{"localhost:5000", "localhost:5001", "localhost:5002"},
				Password:      "mypassword",
			},
		},
	}

	for _, tc := range tests {
		got, err := ParseRedisURI(tc.uri)
		if err != nil {
			t.Errorf("ParseRedisURI(%q) returned an error: %v", tc.uri, err)
			continue
		}

		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("ParseRedisURI(%q) = %+v, want %+v\n(-want,+got)\n%s", tc.uri, got, tc.want, diff)
		}
	}
}

func TestParseRedisURIErrors(t *testing.T) {
	tests := []struct {
		desc string
		uri  string
	}{
		{
			"unsupported scheme",
			"rdb://localhost:6379",
		},
		{
			"missing scheme",
			"localhost:6379",
		},
		{
			"multiple db numbers",
			"redis://localhost:6379/1,2,3",
		},
		{
			"missing path for socket connection",
			"redis-socket://?db=one",
		},
		{
			"non integer for db numbers for socket",
			"redis-socket:///some/path/to/redis?db=one",
		},
	}

	for _, tc := range tests {
		_, err := ParseRedisURI(tc.uri)
		if err == nil {
			t.Errorf("%s: ParseRedisURI(%q) succeeded for malformed input, want error",
				tc.desc, tc.uri)
		}
	}
}

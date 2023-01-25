package asynq

import (
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rqlite"
	"github.com/stretchr/testify/require"
)

type rqliteTestContext struct {
	tb testing.TB
	r  *rqlite.RQLite
}

func (c *rqliteTestContext) FlushDB() {
	client, err := c.r.Client()
	require.NoError(c.tb, err)
	rqlite.FlushDB(c.tb, client)
}

func (c *rqliteTestContext) Close() error {
	return c.r.Close()
}

func (c *rqliteTestContext) GetPendingMessages(qname string) []*base.TaskMessage {
	return rqlite.GetPendingMessages(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetCompletedEntries(qname string) []base.Z {
	return rqlite.GetCompletedEntries(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetActiveMessages(qname string) []*base.TaskMessage {
	return rqlite.GetActiveMessages(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetRetryMessages(qname string) []*base.TaskMessage {
	return rqlite.GetRetryMessages(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetArchivedMessages(qname string) []*base.TaskMessage {
	return rqlite.GetArchivedMessages(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetScheduledMessages(qname string) []*base.TaskMessage {
	return rqlite.GetScheduledMessages(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetScheduledEntries(qname string) []base.Z {
	return rqlite.GetScheduledEntries(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetDeadlinesEntries(qname string) []base.Z {
	return rqlite.GetDeadlinesEntries(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetRetryEntries(qname string) []base.Z {
	return rqlite.GetRetryEntries(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetArchivedEntries(qname string) []base.Z {
	return rqlite.GetArchivedEntries(c.tb, c.r, qname)
}

func (c *rqliteTestContext) GetUniqueKeyTTL(qname string, taskType string, taskPayload []byte) time.Duration {
	return rqlite.GetUniqueKeyTTL(c.tb, c.r, qname, taskType, taskPayload)
}

func (c *rqliteTestContext) InitQueue(qname string) {
	r, err := c.r.Client()
	require.NoError(c.tb, err)
	err = r.EnsureQueue(qname)
	require.NoError(c.tb, err)
}

func (c *rqliteTestContext) QueueExist(qname string) bool {
	b, err := c.r.QueueExist(qname)
	require.NoError(c.tb, err)
	return b
}

func (c *rqliteTestContext) SeedAllPendingQueues(pending map[string][]*base.TaskMessage) {
	rqlite.SeedAllPendingQueues(c.tb, c.r, pending)
}

func (c *rqliteTestContext) SeedAllCompletedQueues(completed map[string][]base.Z) {
	rqlite.SeedAllCompletedQueues(c.tb, c.r, completed)
}

func (c *rqliteTestContext) SeedPendingQueue(pending []*base.TaskMessage, queue string) {
	rqlite.SeedPendingQueue(c.tb, c.r, pending, queue)
}

func (c *rqliteTestContext) SeedAllActiveQueues(inProgress map[string][]*base.TaskMessage) {
	rqlite.SeedAllActiveQueues(c.tb, c.r, inProgress, true)
}

func (c *rqliteTestContext) SeedActiveQueue(msgs []*base.TaskMessage, queue string) {
	rqlite.SeedActiveQueue(c.tb, c.r, msgs, queue, true)
}

func (c *rqliteTestContext) SeedAllDeadlines(deadlines map[string][]base.Z) {
	rqlite.SeedAllDeadlines(c.tb, c.r, deadlines, 0)
}

func (c *rqliteTestContext) SeedAllRetryQueues(retry map[string][]base.Z) {
	rqlite.SeedAllRetryQueues(c.tb, c.r, retry)
}

func (c *rqliteTestContext) SeedAllArchivedQueues(archived map[string][]base.Z) {
	rqlite.SeedAllArchivedQueues(c.tb, c.r, archived)
}

func (c *rqliteTestContext) SeedAllScheduledQueues(scheduled map[string][]base.Z) {
	rqlite.SeedAllScheduledQueues(c.tb, c.r, scheduled)
}

func (c *rqliteTestContext) SeedAllProcessedQueues(processed map[string]int, doneAt time.Time) {
	rqlite.SeedAllProcessedQueues(c.tb, c.r, processed, doneAt.Unix())
}

func (c *rqliteTestContext) SeedAllFailedQueues(failed map[string]int, doneAt time.Time) {
	rqlite.SeedAllFailedQueues(c.tb, c.r, failed, doneAt.Unix())
}

func (c *rqliteTestContext) SeedProcessedQueue(processedCount int, qname string, ts time.Time) {
	rqlite.SeedProcessedQueue(c.tb, c.r, processedCount, qname, ts.Unix())
}

func (c *rqliteTestContext) SeedFailedQueue(failedCount int, qname string, ts time.Time) {
	rqlite.SeedFailedQueue(c.tb, c.r, failedCount, qname, ts.Unix())
}

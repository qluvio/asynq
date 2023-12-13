package asynq

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	QDist             = "qc_content"  // content distribution queue
	QPartDist         = "qd_part"     // part distribution queue
	QPartNodeDist     = "qe_node"     // part-to-node distribution queue
	QPartLiveDist     = "qf_livepart" // live part distribution queue
	QPartLiveNodeDist = "qg_livenode" // live-part-to-node distribution queue
)

var (
	allTestCases = []*testCase{
		{taskCount: 1000, enqueuePerSec: 10},
		{taskCount: 1000, enqueuePerSec: 25},
		{taskCount: 1000, enqueuePerSec: 50},
		{taskCount: 1000, enqueuePerSec: 100},
		{taskCount: 1000, enqueuePerSec: 200},
		{taskCount: 5000, enqueuePerSec: 250},
		{taskCount: 5000, enqueuePerSec: 275},
		{taskCount: 5000, enqueuePerSec: 300},
		{taskCount: 5000, enqueuePerSec: 325},
		{taskCount: 5000, enqueuePerSec: 350},
		{taskCount: 5000, enqueuePerSec: 375},
		{taskCount: 5000, enqueuePerSec: 400},
		{taskCount: 5000, enqueuePerSec: 425},
		{taskCount: 5000, enqueuePerSec: 450},
		{taskCount: 5000, enqueuePerSec: 475},
		{taskCount: 5000, enqueuePerSec: 500},
		{taskCount: 10000, enqueuePerSec: 1000},
		{taskCount: 10000, enqueuePerSec: 2000},
		{taskCount: 10000, enqueuePerSec: 3000},
	}
	smallTestCases0 = []*testCase{
		{taskCount: 1000, enqueuePerSec: 50},
		{taskCount: 5000, enqueuePerSec: 300},
		{taskCount: 5000, enqueuePerSec: 400},
		{taskCount: 5000, enqueuePerSec: 500},
		{taskCount: 10000, enqueuePerSec: 1000},
	}
	smallTestCases = []*testCase{
		{taskCount: 1000, enqueuePerSec: 50},
		{taskCount: 1000, enqueuePerSec: 100},
		{taskCount: 1000, enqueuePerSec: 200},
		{taskCount: 5000, enqueuePerSec: 400},
		{taskCount: 10000, enqueuePerSec: 1000},
	}

	verySmallTestCases = []*testCase{
		{taskCount: 300, enqueuePerSec: 50},
	}
)

// TestSingleQueueAloneAtRate is identical to TestSingleQueueAtRate but with a
// single queue alone in the queues config
func TestSingleQueueAloneAtRate(t *testing.T) {
	if testing.Short() {
		t.Skip("manual only - takes ~minutes")
	}
	testCases := allTestCases

	fn := func(t *testing.T, brokerType string, priority Priority) func(t *testing.T) {
		return func(t *testing.T) {
			doTestSingleQueueAtRate(
				t,
				brokerType,
				map[string]int{
					QPartLiveNodeDist: 5,
				},
				priority,
				QPartLiveNodeDist,
				nil,
				testCases)
		}
	}
	t.Run("redis/lenient", fn(t, RedisType, Lenient))
	t.Run("sqlite/lenient", fn(t, SqliteType, Lenient))
	t.Run("sqlite/iwrr", fn(t, SqliteType, Iwrr))
}

// TestSingleQueueAtRate looks for dequeue time on a single queue.
// The queues config contains 5 queues, only the one with the highest priority
// is used for testing, other queues DO NOT exist in the DB.
func TestSingleQueueAtRate(t *testing.T) {
	if testing.Short() {
		t.Skip("manual only - takes ~minutes")
	}
	testCases := smallTestCases

	fn := func(t *testing.T, brokerType string, priority Priority) func(t *testing.T) {
		return func(t *testing.T) {
			doTestSingleQueueAtRate(
				t,
				brokerType,
				map[string]int{
					QDist:             1,
					QPartDist:         2,
					QPartNodeDist:     3,
					QPartLiveDist:     4,
					QPartLiveNodeDist: 5,
				},
				priority,
				QPartLiveNodeDist,
				nil,
				testCases)
		}
	}
	t.Run("redis/lenient", fn(t, RedisType, Lenient))
	t.Run("sqlite/lenient", fn(t, SqliteType, Lenient))
	t.Run("sqlite/iwrr", fn(t, SqliteType, Iwrr))
}

// TestSingleQueueNotAloneAtRate is similar to TestSingleQueueAtRate but other
// queues exist in the DB.
// The queues config contains 5 queues, only the one with the highest priority
// is used for testing but other queues DO exist in the DB.
// Note: this test is now obsolete, since the removal of the SQL request that was
// testing existence of the queue before the actual dequeue-ing.
func TestSingleQueueNotAloneAtRate(t *testing.T) {
	t.Skip("obsolete - see comment - manual only - takes ~minutes")

	queueNames := []string{
		QDist,
		QPartDist,
		QPartNodeDist,
		QPartLiveDist,
		QPartLiveNodeDist,
	}
	testCases := smallTestCases

	fn := func(t *testing.T, brokerType string, priority Priority) func(t *testing.T) {
		return func(t *testing.T) {
			doTestSingleQueueAtRate(
				t,
				brokerType,
				map[string]int{
					QDist:             1,
					QPartDist:         2,
					QPartNodeDist:     3,
					QPartLiveDist:     4,
					QPartLiveNodeDist: 5,
				},
				priority,
				QPartLiveNodeDist,
				func(ctx *testCtx) {
					// just enqueue one task to make the queue exist in the DB
					for _, q := range queueNames {
						tid := fmt.Sprintf("before-test-%s", q)
						_, err := ctx.client.Enqueue(
							NewTask(q, []byte(tid)),
							Queue(q),
							TaskID(tid),
							Timeout(time.Hour),
							Retention(time.Hour))
						ctx.NoError(err)
					}
				},
				testCases)
		}
	}
	t.Run("redis/lenient", fn(t, RedisType, Lenient))
	t.Run("sqlite/lenient", fn(t, SqliteType, Lenient))
	t.Run("sqlite/iwrr", fn(t, SqliteType, Iwrr))
}

// TestAllQueuesAtRate looks for dequeue time on multiple queues.
// The queues config contains 5 queues, all of which are used.
func TestAllQueuesAtRate(t *testing.T) {
	if testing.Short() {
		t.Skip("manual only - takes ~8m")
	}

	testCases := allTestCases

	fn := func(t *testing.T, brokerType string, priority Priority) func(t *testing.T) {
		return func(t *testing.T) {
			doTestAllQueuesAtRate(
				t,
				brokerType,
				map[string]int{
					QDist:             1,
					QPartDist:         2,
					QPartNodeDist:     3,
					QPartLiveDist:     4,
					QPartLiveNodeDist: 5,
				},
				priority,
				nil,
				testCases)
		}
	}
	t.Run("redis/lenient", fn(t, RedisType, Lenient))
	t.Run("sqlite/lenient", fn(t, SqliteType, Lenient))
	t.Run("sqlite/iwrr", fn(t, SqliteType, Iwrr))
}

func doTestSingleQueueAtRate(
	t *testing.T,
	brokerType string,
	queues map[string]int,
	priorityType Priority,
	queue string,
	beforeTest func(ctx *testCtx),
	testCases []*testCase) {

	result := make([]map[string]*hStats, 0)
	{
		// output the queues config
		bb, err := json.MarshalIndent(queues, "", "  ")
		require.NoError(t, err)
		fmt.Println("queues", string(bb))
	}

	for i, tc := range testCases {
		tc := tc
		tc.usedQueues = []string{queue}

		t.Run(fmt.Sprintf("test-%d-%d-%d", i, tc.enqueuePerSec, tc.taskCount), func(t *testing.T) {
			ctx := newTestCtx(t, tc, brokerType, queues, priorityType)
			if beforeTest != nil {
				beforeTest(ctx)
			}
			r := ctx.distQueuesAtRate()
			result = append(result, r)
		})
	}

	fmt.Println()
	sb := strings.Builder{}
	for _, allstats := range result {
		for q, stat := range allstats {
			sb.WriteString(q + "," + stat.csvLine())
		}
		sb.WriteString("\n")
	}
	fmt.Println(sb.String())
}

func doTestAllQueuesAtRate(
	t *testing.T,
	brokerType string,
	queues map[string]int,
	priorityType Priority,
	beforeTest func(ctx *testCtx),
	testCases []*testCase) {
	result := make([]map[string]*hStats, 0)
	{
		// output the queues config
		bb, err := json.MarshalIndent(queues, "", "  ")
		require.NoError(t, err)
		fmt.Println("queues", string(bb))
	}
	queuesNames := []string(nil)
	for n := range queues {
		queuesNames = append(queuesNames, n)
	}
	sort.Strings(queuesNames)

	for i, tc := range testCases {
		tc := tc
		tc.usedQueues = queuesNames

		t.Run(fmt.Sprintf("test-%d-%d-%d", i, tc.enqueuePerSec, tc.taskCount), func(t *testing.T) {
			ctx := newTestCtx(t, tc, brokerType, queues, priorityType)
			if beforeTest != nil {
				beforeTest(ctx)
			}
			r := ctx.distQueuesAtRate()
			result = append(result, r)
		})
	}

	fmt.Println()
	sb := strings.Builder{}
	sb.WriteString("enqueue_sec, count, max, avg\n")
	for _, allstats := range result {
		qnames := []string(nil)
		for q := range allstats {
			qnames = append(qnames, q)
		}
		sort.Strings(qnames)

		for _, q := range qnames {
			stat := allstats[q]
			sb.WriteString(q + "," + stat.csvLine())
		}
		sb.WriteString("\n")
	}
	fmt.Println(sb.String())
}

func (t *testCtx) distQueuesAtRate() map[string]*hStats {
	t.NotNil(t.mux)

	d, err := newDistributor(t.mux, t.client)
	t.NoError(err)

	t.mayPreloadQQueue(d)

	d.addTasks(t.taskCount)
	var mu sync.Mutex
	var enqueuedTasks map[string]*tasksList
	var enqueueErr error

	go func(d *distributor) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedTasks, enqueueErr = t.enqueueAtRate(d)
	}(d)

	w := StartWatch()
	d.wait()
	fmt.Println("finished after", w.Duration())

	mu.Lock()
	defer mu.Unlock()
	t.NoError(enqueueErr)
	distTasks := d.dh.tasks
	for queue, queuedTasks := range enqueuedTasks {
		err = queuedTasks.expectEquals(distTasks[queue])
		t.NoError(err)
	}

	return d.dh.Stats(t.enqueuePerSec)
}

// TestAllQueues tests dequeue-ing on multiple queues.
func TestAllQueues(t *testing.T) {

	testCases := verySmallTestCases

	fn := func(t *testing.T, brokerType string, priority Priority) func(t *testing.T) {
		return func(t *testing.T) {
			doTestAllQueues(
				t,
				brokerType,
				map[string]int{
					QDist:             1,
					QPartDist:         2,
					QPartNodeDist:     3,
					QPartLiveDist:     4,
					QPartLiveNodeDist: 5,
				},
				priority,
				testCases)
		}
	}
	t.Run("redis/lenient", fn(t, RedisType, Lenient))
	t.Run("sqlite/lenient", fn(t, SqliteType, Lenient))
	t.Run("sqlite/iwrr", fn(t, SqliteType, Iwrr))
}

func doTestAllQueues(
	t *testing.T,
	brokerType string,
	queues map[string]int,
	priorityType Priority,
	testCases []*testCase) {
	result := make([]map[string]*hStats, 0)
	{
		// output the queues config
		bb, err := json.MarshalIndent(queues, "", "  ")
		require.NoError(t, err)
		fmt.Println("queues", string(bb))
	}
	queuesNames := []string(nil)
	for n := range queues {
		queuesNames = append(queuesNames, n)
	}
	sort.Strings(queuesNames)

	for i, tc := range testCases {
		tc := tc
		tc.usedQueues = queuesNames

		t.Run(fmt.Sprintf("test-%d-%d-%d", i, tc.enqueuePerSec, tc.taskCount), func(t *testing.T) {
			ctx := newTestCtx(t, tc, brokerType, queues, priorityType)
			r := ctx.distQueuesAtRate()
			result = append(result, r)
		})
	}

	fmt.Println()
	sb := strings.Builder{}
	sb.WriteString("enqueue_sec, count, max, avg\n")
	for _, allstats := range result {
		qnames := []string(nil)
		for q := range allstats {
			qnames = append(qnames, q)
		}
		sort.Strings(qnames)

		for _, q := range qnames {
			stat := allstats[q]
			sb.WriteString(q + "," + stat.csvLine())
		}
		sb.WriteString("\n")
	}
	fmt.Println(sb.String())
}

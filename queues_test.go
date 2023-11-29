package asynq

import (
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/stretchr/testify/require"
)

func TestQueuesPriorities(t *testing.T) {
	qcfg := NewQueuesConfig(
		map[string]int{
			"default":  10, // 5
			"critical": 40, // 20
			"flexible": 2,  // 1
		})
	q := newQueues(qcfg, 1).(*serverQueues)
	q.mayRebuild()

	require.Equal(t, &queue{name: "critical", priority: 20, concurrency: 1}, q.queues["critical"])
	require.Equal(t, &queue{name: "default", priority: 5, concurrency: 1}, q.queues["default"])
	require.Equal(t, &queue{name: "flexible", priority: 1, concurrency: 1}, q.queues["flexible"])
}

func TestAvailableForDequeue(t *testing.T) {
	type testCase struct {
		slots int
		sum   int
		curr  int
		max   int
		want  int
	}
	for i, tc := range []*testCase{
		// queues priorities: 5, 3, 1
		{slots: 100, sum: 9, curr: 1, max: 10, want: 10},
		{slots: 90, sum: 9, curr: 1, max: 10, want: 10},
		{slots: 8, sum: 9, curr: 1, max: 10, want: 1},
		{slots: 8, sum: 9, curr: 3, max: 10, want: 3},
		{slots: 8, sum: 9, curr: 5, max: 10, want: 4},
		{slots: 9, sum: 9, curr: 5, max: 10, want: 5},
	} {
		afd := availableForDequeue(tc.slots, tc.sum, tc.curr, tc.max)
		require.Equal(t, tc.want, afd, "failed at %d", i)
	}

	fmt.Println(fmt.Sprintf(""))
	max := 100
	prioSum := 9
	c := newConcurrency(max)
	afd := func(curr int) int {
		return c.availableForDequeue(prioSum, curr)
	}

	fmt.Println(fmt.Sprintf("w/%d\t\t%s\t\t%s\t\t%s", max, "p=1", "p=3", "p=5"))
	for i := 0; i < max; i++ {
		_ = c.acquire()
		require.Equal(t, int64(i+1), c.curr.Load())
		fmt.Println(fmt.Sprintf("%d\t\t\t%d\t\t%d\t\t%d", i+1, afd(1), afd(3), afd(5)))
	}

	//fmt.Println(availableForDequeue(9, 9, 5, 10))
}

type IWRRTestCtx struct {
	*testing.T
	*require.Assertions
	w   *iwrrNamer
	bdq *bdq

	cycle int
}

func newIWRRTestCtx(t *testing.T, queues []*queue, qs map[string][]string, concurrency, maxBuffers int) *IWRRTestCtx {
	rd := 0
	prioritiesSum := 0
	bufferedQueues := []*bufferedQueue(nil)
	for _, q := range queues {
		if q.priority > rd {
			rd = q.priority
		}
		prioritiesSum += q.priority
		bufferedQueues = append(bufferedQueues, newBufferedQueue(q, maxBuffers))
	}
	return &IWRRTestCtx{
		T:          t,
		Assertions: require.New(t),
		w: &iwrrNamer{
			srvConcurrency: newConcurrency(concurrency),
			sortedQueues:   bufferedQueues,
			roundCount:     rd,
			prioritiesSum:  prioritiesSum,
		},
		bdq: &bdq{qs: qs},
	}
}

func (c *IWRRTestCtx) assertTask(expq, expId string, pt procTask) {
	c.Equal(expq, pt.queue.name, "got: %s - %s", pt.queue.name, pt.Msg.ID)
	c.Equal(expId, pt.Msg.ID)
}

func (c *IWRRTestCtx) assertCycle(expq, expId string) {
	c.cycle++
	fmt.Println("cycle", c.cycle, expq, expId)
	tsk, err := c.w.cycle("", c.bdq)
	c.NoError(err)
	c.assertTask(expq, expId, tsk)
}

func (c *IWRRTestCtx) assertCycleNoTask() {
	c.cycle++
	fmt.Println("cycle", c.cycle)
	_, err := c.w.cycle("", c.bdq)
	c.Error(err)
	c.True(errors.Is(err, errors.ErrNoProcessableTask))
}

func TestIWRRCycle(t *testing.T) {
	concur := 100
	maxBuf := 100
	ctx := newIWRRTestCtx(
		t,
		[]*queue{ // actually not sorted in this test!
			{name: "q1", priority: 5, concurrency: concur},
			{name: "q2", priority: 2, concurrency: concur},
			{name: "q3", priority: 3, concurrency: concur},
		},
		map[string][]string{
			"q1": {"A", "B", "C", "D", "E", "F", "G"},
			"q2": {"U", "V", "W"},
			"q3": {"X", "Y"},
		},
		concur,
		maxBuf)

	ctx.assertCycle("q1", "A")
	ctx.assertCycle("q2", "U")
	ctx.assertCycle("q3", "X")

	ctx.assertCycle("q1", "B")
	ctx.assertCycle("q2", "V")
	ctx.assertCycle("q3", "Y")

	ctx.assertCycle("q1", "C")
	ctx.assertCycle("q1", "D")
	ctx.assertCycle("q1", "E")
	ctx.assertCycle("q1", "F")

	ctx.assertCycle("q2", "W")
	ctx.assertCycle("q1", "G")
}

func TestIWRREmptyq(t *testing.T) {
	concur := 100
	maxBuf := 100
	ctx := newIWRRTestCtx(
		t,
		[]*queue{ // actually not sorted in this test!
			{name: "q1", priority: 5, concurrency: concur},
			{name: "q2", priority: 2, concurrency: concur},
			{name: "q3", priority: 3, concurrency: concur},
		},
		map[string][]string{
			"q1": {},
			"q2": {},
			"q3": {"X"},
		},
		concur,
		maxBuf)

	ctx.assertCycle("q3", "X")
	ctx.assertCycleNoTask()
}

func TestIWRREmptyqDuringCycle(t *testing.T) {
	concur := 100
	maxBuf := 100
	t.Run("", func(t *testing.T) {
		ctx := newIWRRTestCtx(
			t,
			[]*queue{
				{name: "e", priority: 3, concurrency: concur},
				{name: "d", priority: 2, concurrency: concur},
				{name: "c", priority: 1, concurrency: concur},
			},
			map[string][]string{
				"e": {},
				"d": {},
				"c": {"X"},
			},
			concur,
			maxBuf)
		ctx.w.currRound = 1

		ctx.assertCycle("c", "X")
		ctx.assertCycleNoTask()
	})
	t.Run("", func(t *testing.T) {
		ctx := newIWRRTestCtx(
			t,
			[]*queue{
				{name: "e", priority: 3, concurrency: concur},
				{name: "d", priority: 2, concurrency: concur},
				{name: "c", priority: 1, concurrency: concur},
			},
			map[string][]string{
				"e": {"A"},
				"d": {},
				"c": {"X"},
			},
			concur,
			maxBuf)
		ctx.w.currQueue = 0
		ctx.w.currRound = 1

		ctx.assertCycle("e", "A")
		ctx.assertCycle("c", "X")
		ctx.assertCycleNoTask()
	})
}

func TestIWRREmptyqFilled(t *testing.T) {
	concur := 100
	maxBuf := 100
	ctx := newIWRRTestCtx(
		t,
		[]*queue{ // actually not sorted in this test!
			{name: "q1", priority: 5, concurrency: concur},
			{name: "q2", priority: 2, concurrency: concur},
			{name: "q3", priority: 3, concurrency: concur},
		},
		map[string][]string{
			"q1": {},
			"q2": {},
			"q3": {"X"},
		},
		concur,
		maxBuf)

	ctx.assertCycle("q3", "X")
	ctx.bdq.qs["q1"] = append(ctx.bdq.qs["q1"], "H")
	ctx.assertCycle("q1", "H")
	ctx.bdq.qs["q3"] = append(ctx.bdq.qs["q3"], "Y")
	ctx.assertCycle("q3", "Y")
	ctx.assertCycleNoTask()
}

type bdq struct {
	qs map[string][]string
}

var _ base.BufferedDequeuer = (*bdq)(nil)

func (b *bdq) Dequeue(serverID string, qnames ...string) (*base.TaskMessage, time.Time, error) {
	_ = serverID
	for _, name := range qnames {
		q := b.qs[name]
		if len(q) > 0 {
			ret := q[0]
			b.qs[name] = q[1:]
			return &base.TaskMessage{
				ID:    ret,
				Queue: name,
			}, time.Time{}, nil
		}
	}
	return nil, time.Time{}, errors.E("dequeue", errors.NotFound, errors.ErrNoProcessableTask)
}

func (b *bdq) DequeueN(serverID string, qname string, count int, ret []base.Task) ([]base.Task, error) {
	_ = serverID

	q := b.qs[qname]
	if len(q) > 0 {
		c := count
		if c > len(q) {
			c = len(q)
		}
		tasks := q[:c]
		if c == len(q) {
			b.qs[qname] = []string{}
		} else {
			b.qs[qname] = q[c+1:]
		}

		for _, t := range tasks {
			ret = append(ret, base.Task{
				Msg: &base.TaskMessage{Queue: qname, ID: t},
			})
		}
		return ret, nil
	}
	return nil, errors.E("dequeue", errors.NotFound, errors.ErrNoProcessableTask)
}

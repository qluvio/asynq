package asynq

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

// Priority is the type of priority used for dequeue-ing
type Priority int

const (
	Lenient       Priority = iota // lenient uses statistical ordering to give a chance to lower priority queues
	Strict                        // strict always tries to dequeue higher priority queues first
	Iwrr                          // IWRR (interleaved weighted round robin)
	defMaxBufSize = 10
)

var (
	defaultQueuesConfig = &QueuesConfig{
		Queues:   map[string]QueueConfig{base.DefaultQueueName: {Priority: 1}},
		Priority: Lenient,
	}
)

type QueueConfig struct {
	Priority    int `json:"priority"`
	Concurrency int `json:"concurrency"`
}

// NewQueuesConfig creates a new QueuesConfig with a list of queues to process with given priority value.
// Keys are the names of the queues and values are associated priority value.
func NewQueuesConfig(qs map[string]int, priorityType ...Priority) *QueuesConfig {
	queues := map[string]QueueConfig{}
	for n, q := range qs {
		queues[n] = QueueConfig{Priority: q}
	}
	prio := Lenient
	if len(priorityType) > 0 {
		prio = priorityType[0]
	}
	return &QueuesConfig{
		Queues:   queues,
		Priority: prio,
	}
}

// QueuesConfig is the configuration of server queues.
// After the server was started the configuration must be updated only via UpdateQueues or SetPriorityType
type QueuesConfig struct {
	// List of queues to process with given attributes or priority value (for backwards
	// compatibility).
	// Keys are the names of the queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// If a queue has a zero or negative attribute value, the queue will be ignored. As an
	// exception, if a queue has a zero concurrency value, the concurrency value will
	// default to the global concurrency value.
	//
	// Example:
	//
	//     Queues: map[string]QueueConfig{
	//         "critical": {
	//             Priority:    6,
	//             Concurrency: 10,
	//         },
	//         "default": {
	//             Priority:    3,
	//             Concurrency: 5,
	//         },
	//         "low": {
	//             Priority:    1,
	//             Concurrency: 1,
	//         },
	//     }
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Priority Example:
	//
	//     NewQueuesConfig(map[string]interface{}{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     })
	//
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	Queues map[string]QueueConfig `json:"queues"`

	// Priority is the type of priority indicating how the queue priority should be treated.
	//
	// If set to Strict, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	Priority Priority `json:"priority"`

	updCount int
	mu       sync.Mutex
}

func (p *QueuesConfig) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	bb, _ := json.Marshal(p)
	return string(bb)
}

func (p *QueuesConfig) SetPriorityType(priority Priority) *QueuesConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Priority = priority
	p.updCount++
	return p
}

func (p *QueuesConfig) UpdateQueues(queues map[string]QueueConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Queues = queues
	p.updCount++
	return nil
}

func (p *QueuesConfig) getQueues(fn func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn()
}

// uniq dedupes elements and returns a slice of unique names of length l.
// Order of the output slice is based on the input list.
func uniq(names []string, l int) []string {
	var res []string
	seen := make(map[string]struct{})
	for _, s := range names {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			res = append(res, s)
		}
		if len(res) == l {
			break
		}
	}
	return res
}

// sortByPriority returns a list of queue names sorted by
// their priority level in descending order.
func sortByPriority(qcfg map[string]*queue) []*queue {
	var queues []*queue
	for _, v := range qcfg {
		queues = append(queues, v)
	}
	sort.Sort(sort.Reverse(byPriority(queues)))
	return queues
}

type queue struct {
	name        string
	priority    int
	concurrency int

	mu      sync.Mutex
	running int
}

func (q *queue) canRun() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.running < q.concurrency
}

func (q *queue) concurrentSlots(maxSlots int) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	ret := q.concurrency - q.running
	if ret > maxSlots {
		ret = maxSlots
	}
	return ret
}

func (q *queue) acquire() *queue {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.running++
	return q
}

func (q *queue) release() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.running--
}

type byPriority []*queue

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].priority < x[j].priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// normalizeQueues divides priority numbers by their greatest common divisor.
func normalizeQueues(queues map[string]*queue) map[string]*queue {
	var xs []int
	for _, v := range queues {
		xs = append(xs, v.priority)
	}
	d := gcd(xs...)
	for _, q := range queues {
		q.priority = q.priority / d
	}
	return queues
}

func gcd(xs ...int) int {
	fn := func(x, y int) int {
		for y > 0 {
			x, y = y, x%y
		}
		return x
	}
	res := xs[0]
	for i := 0; i < len(xs); i++ {
		res = fn(xs[i], res)
		if res == 1 {
			return 1
		}
	}
	return res
}

type queues interface {
	Names() []string
	Priorities() (map[string]int, bool)
	namedQueues() (map[string]*queue, queueNamer)
	maxConcurrency() int
	concurrency() *concurrency
}

func newQueues(c *QueuesConfig, srvConcurrency int) queues {
	if c == nil {
		panic(fmt.Errorf("nil queues config"))
	}
	return &serverQueues{
		cfg:            c,
		srvConcurrency: newConcurrency(srvConcurrency),
	}
}

type serverQueues struct {
	cfg            *QueuesConfig
	srvConcurrency *concurrency
	updCount       int

	mu     sync.Mutex
	queues map[string]*queue
	namer  queueNamer
}

func (q *serverQueues) mayRebuild() {
	if q == nil {
		return
	}
	q.cfg.getQueues(func() {
		if q.queues != nil && q.updCount == q.cfg.updCount {
			return
		}

		if q.queues == nil {
			q.queues = make(map[string]*queue)
		}
		queues := q.queues

		// remove existing queue that were removed from the config
		for qname, _ := range queues {
			if _, ok := q.cfg.Queues[qname]; !ok {
				delete(queues, qname)
			}
		}
		// add or update queues
		for qname, qc := range q.cfg.Queues {
			if err := base.ValidateQueueName(qname); err != nil {
				continue // ignore invalid queue names
			}

			if qc.Concurrency == 0 {
				qc.Concurrency = q.srvConcurrency.maxConcurrency()
			}
			if qc.Priority <= 0 || qc.Concurrency < 0 {
				continue
			}

			aqueue := queues[qname]
			if aqueue == nil {
				aqueue = &queue{
					name:        qname,
					priority:    qc.Priority,
					concurrency: qc.Concurrency,
					running:     0,
				}
			} else {
				aqueue.priority = qc.Priority
				aqueue.concurrency = qc.Concurrency
			}
			queues[qname] = aqueue
		}
		if len(queues) == 0 {
			queues[base.DefaultQueueName] = &queue{
				name:        base.DefaultQueueName,
				priority:    1,
				concurrency: q.srvConcurrency.maxConcurrency(),
			}
		}
		q.queues = normalizeQueues(queues)
		sortedQueues := sortByPriority(q.queues)
		q.updCount = q.cfg.updCount

		// now build dequeuer
		switch q.cfg.Priority {
		case Strict:
			q.namer = newStrictNamer(sortedQueues)
		case Lenient:
			q.namer = newLenientNamer(q.queues)
		case Iwrr:
			q.namer = newIwrrNamer(q.srvConcurrency, q.queues, sortedQueues)
		default:
			panic(fmt.Errorf("unknown priority_type %v", q.cfg.Priority))
		}
	})
}

func (q *serverQueues) maxConcurrency() int {
	return q.srvConcurrency.maxConcurrency()
}

func (q *serverQueues) concurrency() *concurrency {
	return q.srvConcurrency
}

// Names returns the list of queues to query.
// Order of the queue names is based on the priority of each queue.
// Queue names is sorted by their priority level if strict-priority is true.
// If strict-priority is false, then the order of queue names are roughly based on
// the priority level but randomized in order to avoid starving low priority queues.
func (q *serverQueues) Names() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.mayRebuild()

	return q.namer.queueNames()
}

func (q *serverQueues) namedQueues() (map[string]*queue, queueNamer) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.mayRebuild()

	return q.queues, q.namer
}

func (q *serverQueues) Priorities() (map[string]int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.mayRebuild()

	res := map[string]int{}
	for qname, v := range q.queues {
		res[qname] = v.priority
	}
	return res, q.cfg.Priority == Strict
}

type queueNamer interface {
	queueNames() []string
	priorityType() Priority
	selectQueues(queues map[string]*queue) []string
}

type strictNamer struct {
	sortedQueues []*queue // queues sorted by priority in descending order
	names        []string // queue names sorted by priority in descending order
	buf          []string
}

func newStrictNamer(q []*queue) *strictNamer {
	names := make([]string, 0, len(q))
	for _, n := range q {
		names = append(names, n.name)
	}
	return &strictNamer{
		sortedQueues: q,
		names:        names,
		buf:          make([]string, len(q)),
	}
}

func (q *strictNamer) priorityType() Priority {
	return Strict
}

func (q *strictNamer) queueNames() []string {
	return q.names
}

func (q *strictNamer) selectQueues(_ map[string]*queue) []string {
	count := 0
	for _, qu := range q.sortedQueues {
		if qu.canRun() {
			q.buf[count] = qu.name
			count++
		}
	}
	return q.buf[:count]
}

type lenientNamer struct {
	r             *rand.Rand
	weightedNames []string // weighted queue names
	len           int
	buf           []string
}

func newLenientNamer(queues map[string]*queue) *lenientNamer {
	var names []string
	if len(queues) == 1 {
		for qname, _ := range queues {
			names = append(names, qname)
		}
	} else {
		for qname, v := range queues {
			for i := 0; i < v.priority; i++ {
				names = append(names, qname)
			}
		}
	}
	return &lenientNamer{
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		weightedNames: names,
		len:           len(queues),
		buf:           make([]string, len(queues)),
	}
}

func (q *lenientNamer) priorityType() Priority {
	return Lenient
}

func (q *lenientNamer) queueNames() []string {
	if len(q.weightedNames) == 1 {
		return q.weightedNames
	}
	q.r.Shuffle(
		len(q.weightedNames),
		func(i, j int) { q.weightedNames[i], q.weightedNames[j] = q.weightedNames[j], q.weightedNames[i] })
	return uniq(q.weightedNames, q.len)
}

func (q *lenientNamer) selectQueues(queues map[string]*queue) []string {
	names := q.queueNames()
	count := 0
	for _, n := range names {
		qu := queues[n]
		if qu == nil {
			continue //should never happen
		}
		if qu.canRun() {
			q.buf[count] = qu.name
			count++
		}
	}
	return q.buf[:count]
}

// iwrrNamer implements IWRR (interleaved weighted round robin)
// see: https://en.wikipedia.org/wiki/Weighted_round_robin
type iwrrNamer struct {
	*lenientNamer
	srvConcurrency *concurrency
	sortedQueues   []*bufferedQueue // queues sorted by priority in descending order
	roundCount     int

	currRound     int
	currQueue     int
	prioritiesSum int
}

type bufferedQueue struct {
	queue      *queue
	backBuffer []base.Task
	buf        []base.Task
}

func newBufferedQueue(q *queue, maxBuffersCap int) *bufferedQueue {
	buffersCap := q.concurrency
	if buffersCap > maxBuffersCap {
		buffersCap = maxBuffersCap
	}
	return &bufferedQueue{
		queue:      q,
		backBuffer: make([]base.Task, 0, buffersCap),
	}
}

func newIwrrNamer(srvConcurrency *concurrency, queues map[string]*queue, sortedQueues []*queue) *iwrrNamer {
	qs := make([]*bufferedQueue, 0, len(sortedQueues))
	max := 0
	prioritiesSum := 0
	for _, q := range sortedQueues {
		qs = append(qs, newBufferedQueue(q, srvConcurrency.maxDequeueBufferSize()))
		if q.priority > max {
			max = q.priority
		}
		prioritiesSum += q.priority
	}
	return &iwrrNamer{
		lenientNamer:   newLenientNamer(queues),
		srvConcurrency: srvConcurrency,
		sortedQueues:   qs,
		roundCount:     max,
		prioritiesSum:  prioritiesSum,
	}
}

func (q *iwrrNamer) priorityType() Priority {
	return Iwrr
}

// cycle is the implementation of the 'interleaved weighted round robin' described at:
// https://en.wikipedia.org/wiki/Weighted_round_robin
// * in each cycle, each queue qi has wi (priority) emissions opportunities.
// * let wmax = max { wi }, be the maximum weight: here it's the max of the queues priorities.
// * each cycle is split into wmax rounds.
// * A queue with weight (priority) wi can emit one packet at round r only if r ≤ wi (with r counted from 1)
func (q *iwrrNamer) cycle(srvId string, broker base.BufferedDequeuer) (procTask, error) {

	startQueue := q.currQueue
	startRound := q.currRound

	// next returns false at the end of a cycle
	next := func() bool {
		q.currQueue++
		if q.currQueue == len(q.sortedQueues) {
			q.currRound++
			q.currQueue = 0
		}
		if q.currRound == q.roundCount {
			q.currRound = 0
		}
		return q.currQueue != startQueue || q.currRound != startRound
	}

	// rounds
	for {
		bufq := q.sortedQueues[q.currQueue]
		if bufq.queue.priority > q.currRound { // prio >= r, if r starting at 1

			// count of active workers available
			canRun := bufq.queue.concurrentSlots(q.srvConcurrency.availableCount()+1) > 0
			bufCount := 0
			if canRun && len(bufq.buf) == 0 && q.currRound == 0 {
				// how many tasks can we dequeue for this queue
				bufCount = q.srvConcurrency.availableForDequeue(q.prioritiesSum, bufq.queue.priority)
				if bufCount == 0 {
					// PENDING(GIL): log a warning ?
					fmt.Println("can't dequeue", bufq.queue.name)
					canRun = false
				}
			}

			if canRun {
				// note:
				// - we do a Buffered Dequeue (BDQ) only on first round
				// - at the opposite we could do a BDQ as soon as the buffer is
				//   empty, but it would be a penalty for lower priority queues
				//   when high priority queues are all empty
				// - other empirical/practical approach could use condition like:
				//   q.currRound%3 == 0, but showed less performant
				if len(bufq.buf) == 0 && q.currRound == 0 {
					var err error
					if bufCount == 1 {
						// if count == 1, a regular Dequeue is more performant
						var msg *base.TaskMessage
						var deadline time.Time
						msg, deadline, err = broker.Dequeue(srvId, bufq.queue.name)
						if err == nil {
							bufq.buf = []base.Task{
								{
									Msg:      msg,
									Deadline: deadline,
								},
							}
						}
					} else {
						bufq.buf, err = broker.DequeueN(srvId, bufq.queue.name, bufCount, bufq.backBuffer)
					}
					if errors.Is(err, errors.ErrNoProcessableTask) {
						if !next() {
							return zeroTask, err
						}
						continue
					}
					if err != nil {
						next()
						return zeroTask, err
					}
				}
				if len(bufq.buf) > 0 {
					ret := procTask{
						Task:  bufq.buf[0],
						queue: bufq.queue.acquire(),
					}
					bufq.buf = bufq.buf[1:]
					next()
					return ret, nil
				}
			}
		}
		if !next() {
			return zeroTask, errors.E(errors.Op("dequeue"), errors.NotFound, errors.ErrNoProcessableTask)
		}
	}

}

// concurrency handles limits of active workers at the processor level.
type concurrency struct {
	max  int           // max number of active workers
	curr atomic.Int64  // count of currently running active workers
	sema chan struct{} // sema is a counting semaphore to ensure the number of active workers does not exceed the limit.
	quit chan struct{} // quit channel is closed by the "processor" when its shutdown goroutine starts.
}

func newConcurrency(maxConcurrency int) *concurrency {
	if maxConcurrency < 1 {
		maxConcurrency = runtime.NumCPU()
	}
	return &concurrency{
		max:  maxConcurrency,
		sema: make(chan struct{}, maxConcurrency),
	}
}

func (p *concurrency) setQuit(quit chan struct{}) {
	p.quit = quit
}

func (p *concurrency) acquire() error {
	select {
	case <-p.quit:
		return errProcessorStopped
	case p.sema <- struct{}{}: // acquire token
		p.curr.Add(1)
		return nil
	}
}

// acquireAll blocks until all workers have released the token
func (p *concurrency) acquireAll() {
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
}

func (p *concurrency) release() {
	<-p.sema
	p.curr.Add(-1)
}

func (p *concurrency) runningCount() int64 {
	return p.curr.Load()
}

func (p *concurrency) availableCount() int {
	return int(int64(p.max) - p.curr.Load())
}

// maxDequeueBufferSize returns the maximum count of tasks to dequeue in a buffered
// dequeue (could be a configuration setting).
func (p *concurrency) maxDequeueBufferSize() int {
	return defMaxBufSize
}

// availableForDequeue returns the count of available concurrency for buffered
// dequeue-ing. Since we are dequeue-ing, we already took one slot that we are
// filling, hence the server available slots are: (available count) + 1
//
// As long as there are enough active workers available, the  function returns
// the maximum allowed for a buffered dequeue, otherwise the returned value is
// proportional  to  'currPriority',  i.e. the priority of the current  queue.
func (p *concurrency) availableForDequeue(prioritiesSum, currPriority int) int {
	slots := int(int64(p.max)-p.curr.Load()) + 1
	return availableForDequeue(slots, prioritiesSum, currPriority, p.maxDequeueBufferSize())
}

func availableForDequeue(activeSlots int, sumPriority, currPriority int, max int) int {
	if sumPriority <= 0 {
		return 1
	}
	sum := float64(sumPriority)
	curr := float64(currPriority)
	slots := float64(activeSlots)

	unit := 1.0 / sum

	ret := int(math.Round(slots * unit))
	switch {
	case ret >= max:
		// handled below
	default:
		// start decreasing
		ret = int(math.Round(slots * unit * curr))
	}

	if ret > max {
		ret = max
	}
	return ret
}

func (p *concurrency) maxConcurrency() int {
	return p.max
}

package asynq

import (
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

type Priority int

const (
	Lenient Priority = iota
	Strict
)

var _ = Lenient

var (
	defaultQueues = map[string]int{
		base.DefaultQueueName: 1,
	}
	defaultQueuesConfig = &QueuesConfig{
		Queues:   defaultQueues,
		Priority: Lenient,
	}
)

type Queues interface {
	Names() []string
	Configure() error
	Priorities() map[string]int
	StrictPriority() bool
}

type QueuesConfig struct {
	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	//
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	//
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	Queues map[string]int `json:"queues"`

	// Priority indicates whether the queue priority should be treated strictly.
	//
	// If set to Strict, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	Priority Priority `json:"priority"`

	mu          sync.Mutex
	queueConfig map[string]int

	// orderedQueues is set only in strict-priority mode.
	orderedQueues []string
}

func (p *QueuesConfig) String() string {
	bb, _ := json.Marshal(p)
	return string(bb)
}

func (p *QueuesConfig) Configure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configure()
	return nil
}

func (p *QueuesConfig) configure() *QueuesConfig {
	if p == nil {
		return p
	}

	queues := map[string]int{}
	for qname, p := range p.Queues {
		if err := base.ValidateQueueName(qname); err != nil {
			continue // ignore invalid queue names
		}
		if p > 0 {
			queues[qname] = p
		}
	}
	if len(queues) == 0 {
		queues = defaultQueues
	}

	p.orderedQueues = nil
	p.queueConfig = normalizeQueues(queues)
	if p.Priority == Strict {
		p.orderedQueues = sortByPriority(p.queueConfig)
	}
	return p
}

func (p *QueuesConfig) StrictPriority() bool {
	return p.Priority == Strict
}

func (p *QueuesConfig) Priorities() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queueConfig
}

// Names returns the list of queues to query.
// Order of the queue names is based on the priority of each queue.
// Queue names is sorted by their priority level if strict-priority is true.
// If strict-priority is false, then the order of queue names are roughly based on
// the priority level but randomized in order to avoid starving low priority queues.
func (p *QueuesConfig) Names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.queueConfig == nil {
		p.configure()
	}
	// skip the overhead of generating a list of queue names
	// if we are processing one queue.
	if len(p.queueConfig) == 1 {
		for qname := range p.queueConfig {
			return []string{qname}
		}
	}
	if p.orderedQueues != nil {
		return p.orderedQueues
	}
	var names []string
	for qname, priority := range p.queueConfig {
		for i := 0; i < priority; i++ {
			names = append(names, qname)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return uniq(names, len(p.queueConfig))
}

func (p *QueuesConfig) UpdateQueues(queues map[string]int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Queues = queues
	p.configure()
	return nil
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
func sortByPriority(qcfg map[string]int) []string {
	var queues []*queue
	for qname, n := range qcfg {
		queues = append(queues, &queue{qname, n})
	}
	sort.Sort(sort.Reverse(byPriority(queues)))
	var res []string
	for _, q := range queues {
		res = append(res, q.name)
	}
	return res
}

type queue struct {
	name     string
	priority int
}

type byPriority []*queue

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].priority < x[j].priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// normalizeQueues divides priority numbers by their greatest common divisor.
func normalizeQueues(queues map[string]int) map[string]int {
	var xs []int
	for _, x := range queues {
		xs = append(xs, x)
	}
	d := gcd(xs...)
	res := make(map[string]int)
	for q, x := range queues {
		res[q] = x / d
	}
	return res
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

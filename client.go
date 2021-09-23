// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/rqlite"
	"go.uber.org/multierr"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
type Client struct {
	logger *log.Logger
	mu     sync.Mutex
	opts   map[string][]Option
	rdb    base.Broker
	loc    *time.Location
}

// BatchError is the error returned when doing batch processing
type BatchError interface {
	error
	// MapErrors returns a map of index of failed tasks in the input to error
	MapErrors() map[int]error
}

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

func NewClientWithBroker(broker base.Broker) *Client {
	return &Client{
		opts: make(map[string][]Option),
		rdb:  broker,
	}
}

// NewClient returns a new Client instance given a client connection option.
func NewClient(r ClientConnOpt, loc ...*time.Location) *Client {
	if r == nil {
		panic("no client connection option provided")
	}
	broker, err := makeBroker(r)
	if err != nil {
		panic(err)
	}
	var l *time.Location
	if len(loc) > 0 {
		l = loc[0]
	}
	return &Client{
		logger: log.NewLogger(r.Logger()),
		opts:   make(map[string][]Option),
		rdb:    broker,
		loc:    l,
	}
}

type OptionType int

const (
	MaxRetryOpt OptionType = iota
	QueueOpt
	TimeoutOpt
	DeadlineOpt
	UniqueOpt
	ProcessAtOpt
	ProcessInOpt
)

// Option specifies the task processing behavior.
type Option interface {
	// String returns a string representation of the option.
	String() string

	// Type describes the type of the option.
	Type() OptionType

	// Value returns a value used to create this option.
	Value() interface{}
}

// Internal option representations.
type (
	retryOption     int
	queueOption     string
	timeoutOption   time.Duration
	deadlineOption  time.Time
	uniqueOption    time.Duration
	processAtOption time.Time
	processInOption time.Duration
)

// MaxRetry returns an option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

func (n retryOption) String() string     { return fmt.Sprintf("MaxRetry(%d)", int(n)) }
func (n retryOption) Type() OptionType   { return MaxRetryOpt }
func (n retryOption) Value() interface{} { return int(n) }

// Queue returns an option to specify the queue to enqueue the task into.
func Queue(qname string) Option {
	return queueOption(qname)
}

func (qname queueOption) String() string     { return fmt.Sprintf("Queue(%q)", string(qname)) }
func (qname queueOption) Type() OptionType   { return QueueOpt }
func (qname queueOption) Value() interface{} { return string(qname) }

// Timeout returns an option to specify how long a task may run.
// If the timeout elapses before the Handler returns, then the task
// will be retried.
//
// Zero duration means no limit.
//
// If there's a conflicting Deadline option, whichever comes earliest
// will be used.
func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}

func (d timeoutOption) String() string     { return fmt.Sprintf("Timeout(%v)", time.Duration(d)) }
func (d timeoutOption) Type() OptionType   { return TimeoutOpt }
func (d timeoutOption) Value() interface{} { return time.Duration(d) }

// Deadline returns an option to specify the deadline for the given task.
// If it reaches the deadline before the Handler returns, then the task
// will be retried.
//
// If there's a conflicting Timeout option, whichever comes earliest
// will be used.
func Deadline(t time.Time) Option {
	return deadlineOption(t)
}

func (t deadlineOption) String() string {
	return fmt.Sprintf("Deadline(%v)", time.Time(t).Format(time.UnixDate))
}
func (t deadlineOption) Type() OptionType   { return DeadlineOpt }
func (t deadlineOption) Value() interface{} { return time.Time(t) }

// Unique returns an option to enqueue a task only if the given task is unique.
// Task enqueued with this option is guaranteed to be unique within the given ttl.
// Once the task gets processed successfully or once the TTL has expired, another task with the same uniqueness may be enqueued.
// ErrDuplicateTask error is returned when enqueueing a duplicate task.
//
// Uniqueness of a task is based on the following properties:
//     - Task Type
//     - Task Payload
//     - Queue Name
func Unique(ttl time.Duration) Option {
	return uniqueOption(ttl)
}

func (ttl uniqueOption) String() string     { return fmt.Sprintf("Unique(%v)", time.Duration(ttl)) }
func (ttl uniqueOption) Type() OptionType   { return UniqueOpt }
func (ttl uniqueOption) Value() interface{} { return time.Duration(ttl) }

// ProcessAt returns an option to specify when to process the given task.
//
// If there's a conflicting ProcessIn option, the last option passed to Enqueue overrides the others.
func ProcessAt(t time.Time) Option {
	return processAtOption(t)
}

func (t processAtOption) String() string {
	return fmt.Sprintf("ProcessAt(%v)", time.Time(t).Format(time.UnixDate))
}
func (t processAtOption) Type() OptionType   { return ProcessAtOpt }
func (t processAtOption) Value() interface{} { return time.Time(t) }

// ProcessIn returns an option to specify when to process the given task relative to the current time.
//
// If there's a conflicting ProcessAt option, the last option passed to Enqueue overrides the others.
func ProcessIn(d time.Duration) Option {
	return processInOption(d)
}

func (d processInOption) String() string     { return fmt.Sprintf("ProcessIn(%v)", time.Duration(d)) }
func (d processInOption) Type() OptionType   { return ProcessInOpt }
func (d processInOption) Value() interface{} { return time.Duration(d) }

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.ErrDuplicateTask

type option struct {
	retry     int
	queue     string
	timeout   time.Duration
	deadline  time.Time
	uniqueTTL time.Duration
	processAt time.Time
}

// composeOptions merges user provided options into the default options
// and returns the composed option.
// It also validates the user provided options and returns an error if any of
// the user provided options fail the validations.
func composeOptions(opts ...Option) (option, error) {
	res := option{
		retry:     defaultMaxRetry,
		queue:     base.DefaultQueueName,
		timeout:   0, // do not set to defaultTimeout here
		deadline:  time.Time{},
		processAt: time.Now(),
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			qname := string(opt)
			if err := base.ValidateQueueName(qname); err != nil {
				return option{}, err
			}
			res.queue = qname
		case timeoutOption:
			res.timeout = time.Duration(opt)
		case deadlineOption:
			res.deadline = time.Time(opt)
		case uniqueOption:
			res.uniqueTTL = time.Duration(opt)
		case processAtOption:
			res.processAt = time.Time(opt)
		case processInOption:
			res.processAt = time.Now().Add(time.Duration(opt))
		default:
			// ignore unexpected option
		}
	}
	return res, nil
}

const (
	// Default max retry count used if nothing is specified.
	defaultMaxRetry = 25

	// Default timeout used if both timeout and deadline are not specified.
	defaultTimeout = 30 * time.Minute
)

// Value zero indicates no timeout and no deadline.
var (
	noTimeout  time.Duration = 0
	noDeadline               = time.Unix(0, 0)
)

// SetDefaultOptions sets options to be used for a given task type.
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
//
// Default options can be overridden by options passed at enqueue time.
func (c *Client) SetDefaultOptions(taskType string, opts ...Option) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opts[taskType] = opts
}

// Close closes the connection with redis.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Enqueue enqueues the given task to be processed asynchronously.
//
// Enqueue returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
	if strings.TrimSpace(task.Type()) == "" {
		return nil, fmt.Errorf("task typename cannot be empty")
	}
	c.mu.Lock()
	if defaults, ok := c.opts[task.Type()]; ok {
		opts = append(defaults, opts...)
	}
	c.mu.Unlock()
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}
	deadline := noDeadline
	if !opt.deadline.IsZero() {
		deadline = opt.deadline
	}
	timeout := noTimeout
	if opt.timeout != 0 {
		timeout = opt.timeout
	}
	if deadline.Equal(noDeadline) && timeout == noTimeout {
		// If neither deadline nor timeout are set, use default timeout.
		timeout = defaultTimeout
	}
	var uniqueKey string
	if opt.uniqueTTL > 0 {
		uniqueKey = base.UniqueKey(opt.queue, task.Type(), task.Payload())
	}
	msg := &base.TaskMessage{
		ID:        uuid.New(),
		Type:      task.Type(),
		Payload:   task.Payload(),
		Queue:     opt.queue,
		Retry:     opt.retry,
		Deadline:  deadline.Unix(),
		Timeout:   int64(timeout.Seconds()),
		UniqueKey: uniqueKey,
	}

	now := time.Now()
	var state base.TaskState
	if opt.processAt.Before(now) || opt.processAt.Equal(now) {
		opt.processAt = now
		if c.loc != nil {
			opt.processAt = now.In(c.loc)
		}
		err = c.enqueue(msg, opt.uniqueTTL)
		state = base.TaskStatePending
	} else {
		err = c.schedule(msg, opt.processAt, opt.uniqueTTL)
		state = base.TaskStateScheduled
	}

	switch {
	case errors.Is(err, errors.ErrDuplicateTask):
		return nil, fmt.Errorf("%w", ErrDuplicateTask)
	case err != nil:
		c.logger.Debug("enqueue", err, "state", state, "type", msg.Type, "id", msg.ID)
		return nil, err
	}
	return newTaskInfo(msg, state, opt.processAt), nil
}

func (c *Client) enqueue(msg *base.TaskMessage, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		return c.rdb.EnqueueUnique(msg, uniqueTTL)
	}
	return c.rdb.Enqueue(msg)
}

func (c *Client) schedule(msg *base.TaskMessage, t time.Time, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		ttl := t.Add(uniqueTTL).Sub(time.Now())
		return c.rdb.ScheduleUnique(msg, t, ttl)
	}
	return c.rdb.Schedule(msg, t)
}

// EnqueueBatch enqueues the given tasks to be processed asynchronously.
//
// EnqueueBatch returns a slice of TaskInfo and an error.
// If all tasks are enqueued successfully the returned error is nil, otherwise returns a non-nil error.
// If enqueuing a task raised an error, the returned error is a BatchError indicating
// which the index of the failed tasks and the task info in the returned slice is nil.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
func (c *Client) EnqueueBatch(tasks []*Task, opts ...Option) ([]*TaskInfo, error) {
	for _, task := range tasks {
		if strings.TrimSpace(task.Type()) == "" {
			return nil, fmt.Errorf("task typename cannot be empty")
		}
	}
	tasksOpts := make([]option, 0, len(tasks))
	var err error
	{
		var opt option
		c.mu.Lock()
		for _, task := range tasks {
			taskOpts := append([]Option{}, opts...)
			if defaults, ok := c.opts[task.Type()]; ok {
				taskOpts = append(defaults, taskOpts...)
			}
			opt, err = composeOptions(taskOpts...)
			if err != nil {
				break
			}
			tasksOpts = append(tasksOpts, opt)
		}
		c.mu.Unlock()
	}
	if err != nil {
		return nil, err
	}

	allBm := make([]*base.MessageBatch, 0, len(tasks))
	enqueue := make([]*base.MessageBatch, 0, len(tasks))
	enqueueUnique := make([]*base.MessageBatch, 0, len(tasks))
	schedule := make([]*base.MessageBatch, 0, len(tasks))
	scheduleUnique := make([]*base.MessageBatch, 0, len(tasks))
	now := time.Now()

	for i, task := range tasks {
		opt := tasksOpts[i]

		deadline := noDeadline
		if !opt.deadline.IsZero() {
			deadline = opt.deadline
		}
		timeout := noTimeout
		if opt.timeout != 0 {
			timeout = opt.timeout
		}
		if deadline.Equal(noDeadline) && timeout == noTimeout {
			// If neither deadline nor timeout are set, use default timeout.
			timeout = defaultTimeout
		}
		var uniqueKey string
		if opt.uniqueTTL > 0 {
			uniqueKey = base.UniqueKey(opt.queue, task.Type(), task.Payload())
		}
		msg := &base.TaskMessage{
			ID:        uuid.New(),
			Type:      task.Type(),
			Payload:   task.Payload(),
			Queue:     opt.queue,
			Retry:     opt.retry,
			Deadline:  deadline.Unix(),
			Timeout:   int64(timeout.Seconds()),
			UniqueKey: uniqueKey,
		}

		var bm *base.MessageBatch
		if opt.processAt.Before(now) || opt.processAt.Equal(now) {
			opt.processAt = now
			if c.loc != nil {
				opt.processAt = now.In(c.loc)
			}
			if opt.uniqueTTL > 0 {
				bm = &base.MessageBatch{
					InputIndex: i,
					Msg:        msg,
					UniqueTTL:  opt.uniqueTTL,
					ProcessAt:  opt.processAt,
					State:      base.TaskStatePending,
				}
				enqueueUnique = append(enqueueUnique, bm)
			} else {
				bm = &base.MessageBatch{
					InputIndex: i,
					Msg:        msg,
					ProcessAt:  opt.processAt,
					State:      base.TaskStatePending,
				}
				enqueue = append(enqueue, bm)
			}
		} else {
			if opt.uniqueTTL > 0 {
				bm = &base.MessageBatch{
					InputIndex: i,
					Msg:        msg,
					UniqueTTL:  opt.processAt.Add(opt.uniqueTTL).Sub(now),
					ProcessAt:  opt.processAt,
					State:      base.TaskStateScheduled,
				}
				scheduleUnique = append(scheduleUnique, bm)
			} else {
				bm = &base.MessageBatch{
					InputIndex: i,
					Msg:        msg,
					ProcessAt:  opt.processAt,
					State:      base.TaskStateScheduled,
				}
				schedule = append(schedule, bm)
			}
		}
		allBm = append(allBm, bm)
	}

	if len(enqueue) > 0 {
		err = c.rdb.EnqueueBatch(enqueue...)
	}
	if len(enqueueUnique) > 0 {
		ex := c.rdb.EnqueueUniqueBatch(enqueueUnique...)
		err = multierr.Append(err, ex)
	}
	if len(schedule) > 0 {
		ex := c.rdb.ScheduleBatch(schedule...)
		err = multierr.Append(err, ex)
	}
	if len(scheduleUnique) > 0 {
		ex := c.rdb.ScheduleUniqueBatch(scheduleUnique...)
		err = multierr.Append(err, ex)
	}

	ret := make([]*TaskInfo, len(tasks))
	allErrs := make(map[int]error)
	for i, msg := range allBm {
		if msg.Err == nil {
			ret[i] = newTaskInfo(msg.Msg, msg.State, msg.ProcessAt)
		} else {
			allErrs[i] = msg.Err
		}
	}
	if err != nil {
		c.logger.Debug("enqueueBatch", "error", err,
			"enqueuing tasks_count", len(enqueue),
			"enqueuing unique tasks_count", len(enqueueUnique),
			"scheduling tasks_count", len(schedule),
			"scheduling unique tasks_count", len(scheduleUnique),
			"inner errors", allErrs)
	}
	if len(allErrs) > 0 {
		return ret, &errors.BatchError{Errors: allErrs}
	}
	return ret, nil
}

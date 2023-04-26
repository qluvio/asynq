// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"go.uber.org/multierr"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
type Client struct {
	logger *log.Logger    // logger
	rdb    base.Broker    // connection to the broker
	shared bool           // shared is true when rdb is shared with server
	loc    *time.Location // time location
}

// BatchError is the error returned when doing batch processing
type BatchError interface {
	error
	// MapErrors returns a map of index of failed tasks in the input to error
	MapErrors() map[int]error
}

func NewClientWithBroker(broker base.Broker) *Client {
	return &Client{
		rdb:    broker,
		shared: true,
	}
}

// NewClientFrom returns a Client that shares the broker of the given server
// The connexion will not be closed when calling *Client.Close.
func NewClientFrom(server *Server, loc ...*time.Location) *Client {
	if server == nil {
		panic("server is nil")
	}
	var l *time.Location
	if len(loc) > 0 {
		l = loc[0]
	}
	return &Client{
		logger: server.processor.logger,
		rdb:    server.broker,
		shared: true,
		loc:    l,
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
	ForceUniqueOpt
	RecurrentOpt
	ReprocessAfterOpt
	ServerAffinityOpt
	TaskIDOpt
	RetentionOpt
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
	retryOption          int
	queueOption          string
	taskIDOption         string
	timeoutOption        time.Duration
	deadlineOption       time.Time
	uniqueOption         time.Duration
	processAtOption      time.Time
	processInOption      time.Duration
	forceUniqueOption    bool
	recurrentOption      bool
	reprocessAfterOption time.Duration
	serverAffinityOption time.Duration
	retentionOption      time.Duration
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

// TaskID returns an option to specify the task ID.
func TaskID(id string) Option {
	return taskIDOption(id)
}

func (id taskIDOption) String() string     { return fmt.Sprintf("TaskID(%q)", string(id)) }
func (id taskIDOption) Type() OptionType   { return TaskIDOpt }
func (id taskIDOption) Value() interface{} { return string(id) }

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
// TTL duration must be greater than or equal to 1 second.
//
// Uniqueness of a task is based on the following properties:
//   - Task Type
//   - Task Payload
//   - Queue Name
func Unique(ttl time.Duration) Option {
	return uniqueOption(ttl)
}

func (ttl uniqueOption) String() string     { return fmt.Sprintf("Unique(%v)", time.Duration(ttl)) }
func (ttl uniqueOption) Type() OptionType   { return UniqueOpt }
func (ttl uniqueOption) Value() interface{} { return time.Duration(ttl) }

// ForceUnique returns an option to force uniqueness of a task previously enqueued
// with Unique option.
func ForceUnique(b bool) Option {
	return forceUniqueOption(b)
}

func (b forceUniqueOption) String() string     { return fmt.Sprintf("ForceUnique(%v)", bool(b)) }
func (b forceUniqueOption) Type() OptionType   { return ForceUniqueOpt }
func (b forceUniqueOption) Value() interface{} { return bool(b) }

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

// Recurrent returns an option to define a recurrent task
func Recurrent(b bool) Option {
	return recurrentOption(b)
}

func (r recurrentOption) String() string     { return fmt.Sprintf("Recurrent(%v)", bool(r)) }
func (r recurrentOption) Type() OptionType   { return RecurrentOpt }
func (r recurrentOption) Value() interface{} { return bool(r) }

// ReprocessAfter returns an option to define the delay to reprocess a recurrent task
func ReprocessAfter(d time.Duration) Option {
	return reprocessAfterOption(d)
}

func (r reprocessAfterOption) String() string {
	return fmt.Sprintf("ReprocessAfter(%v)", time.Duration(r))
}
func (r reprocessAfterOption) Type() OptionType   { return ReprocessAfterOpt }
func (r reprocessAfterOption) Value() interface{} { return time.Duration(r) }

// ServerAffinity returns an option to define a server affinity timeout. This
// can be used with recurrent tasks to request that execution of the task should
// remain on the server that first started it unless the affinity timeout expired.
// The timeout is used to compute a deadline after which other servers can take
// the task.
func ServerAffinity(d time.Duration) Option {
	return serverAffinityOption(d)
}

func (r serverAffinityOption) String() string {
	return fmt.Sprintf("ServerAffinity(%v)", time.Duration(r))
}
func (r serverAffinityOption) Type() OptionType   { return ServerAffinityOpt }
func (r serverAffinityOption) Value() interface{} { return time.Duration(r) }

// Retention returns an option to specify the duration of retention period for the task.
// If this option is provided, the task will be stored as a completed task after successful processing.
// A completed task will be deleted after the specified duration elapses.
func Retention(d time.Duration) Option {
	return retentionOption(d)
}

func (ttl retentionOption) String() string     { return fmt.Sprintf("Retention(%v)", time.Duration(ttl)) }
func (ttl retentionOption) Type() OptionType   { return RetentionOpt }
func (ttl retentionOption) Value() interface{} { return time.Duration(ttl) }

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.ErrDuplicateTask

// ErrTaskIDConflict indicates that the given task could not be enqueued since its task ID already exists.
//
// ErrTaskIDConflict error only applies to tasks enqueued with a TaskID option.
var ErrTaskIDConflict = errors.New("task ID conflicts with another task")

type option struct {
	retry          int
	queue          string
	taskID         string
	timeout        time.Duration
	deadline       time.Time
	uniqueTTL      time.Duration
	processAt      time.Time
	forceUnique    bool
	recurrent      bool
	reprocessAfter time.Duration
	serverAffinity time.Duration
	retention      time.Duration
}

// composeOptions merges user provided options into the default options
// and returns the composed option.
// It also validates the user provided options and returns an error if any of
// the user provided options fail the validations.
func composeOptions(opts ...Option) (option, error) {
	res := option{
		retry:     defaultMaxRetry,
		queue:     base.DefaultQueueName,
		taskID:    uuid.NewString(),
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
		case taskIDOption:
			id := string(opt)
			if err := validateTaskID(id); err != nil {
				return option{}, err
			}
			res.taskID = id
		case timeoutOption:
			res.timeout = time.Duration(opt).Truncate(time.Second)
		case deadlineOption:
			res.deadline = time.Time(opt)
		case uniqueOption:
			ttl := time.Duration(opt)
			if ttl < 1*time.Second {
				return option{}, errors.New("Unique TTL cannot be less than 1s")
			}
			res.uniqueTTL = ttl
		case processAtOption:
			res.processAt = time.Time(opt)
		case processInOption:
			res.processAt = time.Now().Add(time.Duration(opt))
		case forceUniqueOption:
			res.forceUnique = bool(opt)
		case recurrentOption:
			res.recurrent = bool(opt)
		case reprocessAfterOption:
			res.reprocessAfter = time.Duration(opt)
		case serverAffinityOption:
			res.serverAffinity = time.Duration(opt)
		case retentionOption:
			res.retention = time.Duration(opt)
		default:
			// ignore unexpected option
		}
	}
	return res, nil
}

// validates user provided task ID string.
func validateTaskID(id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("task ID cannot be empty")
	}
	return nil
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

// Close closes the connection with redis.
func (c *Client) Close() error {
	if c.shared {
		// when shared, rdb needs to be closed by the actual 'owner'
		return nil
	}
	return c.rdb.Close()
}

// Enqueue enqueues the given task to a queue.
//
// Enqueue returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// Enqueue uses context.Background internally; to specify the context, use EnqueueContext.
func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
	return c.EnqueueContext(context.Background(), task, opts...)
}

// EnqueueContext enqueues the given task to a queue.
//
// EnqueueContext returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// The first argument context applies to the enqueue operation. To specify task timeout and deadline, use Timeout and Deadline option instead.
func (c *Client) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	if strings.TrimSpace(task.Type()) == "" {
		return nil, fmt.Errorf("task typename cannot be empty")
	}
	// merge task options with the options provided at enqueue time.
	opts = append(task.opts, opts...)
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}
	if opt.serverAffinity > 0 {
		if _, ok := c.rdb.(*rdb.RDB); ok {
			return nil, fmt.Errorf("server affinity not supported with redis")
		}
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
		ID:             opt.taskID,
		Type:           task.Type(),
		Payload:        task.Payload(),
		Queue:          opt.queue,
		Retry:          opt.retry,
		Deadline:       deadline.Unix(),
		Timeout:        int64(timeout.Seconds()),
		UniqueKey:      uniqueKey,
		UniqueKeyTTL:   int64(opt.uniqueTTL.Seconds()),
		Recurrent:      opt.recurrent,
		ReprocessAfter: int64(opt.reprocessAfter.Seconds()),
		ServerAffinity: int64(opt.serverAffinity.Seconds()),
		Retention:      int64(opt.retention.Seconds()),
	}

	now := time.Now()
	var state base.TaskState
	if opt.processAt.Before(now) || opt.processAt.Equal(now) {
		opt.processAt = now
		if c.loc != nil {
			opt.processAt = now.In(c.loc)
		}
		err = c.enqueue(ctx, msg, opt.uniqueTTL, opt.forceUnique)
		state = base.TaskStatePending
	} else {
		err = c.schedule(ctx, msg, opt.processAt, opt.uniqueTTL, opt.forceUnique)
		state = base.TaskStateScheduled
	}

	switch {
	case errors.Is(err, errors.ErrDuplicateTask):
		return nil, fmt.Errorf("%w", ErrDuplicateTask)
	case errors.Is(err, errors.ErrTaskIdConflict):
		return nil, fmt.Errorf("%w", ErrTaskIDConflict)
	case err != nil:
		c.logger.Debug(fmt.Sprintf(
			"enqueue errors: %v, state: %v, type: %v, id: %v",
			err, state, msg.Type, msg.ID))
		return nil, err
	}
	return newTaskInfo(msg, state, opt.processAt, nil), nil
}

func (c *Client) enqueue(ctx context.Context, msg *base.TaskMessage, uniqueTTL time.Duration, forceUnique bool) error {
	if uniqueTTL > 0 {
		return c.rdb.EnqueueUnique(ctx, msg, uniqueTTL, forceUnique)
	}
	return c.rdb.Enqueue(ctx, msg)
}

func (c *Client) schedule(ctx context.Context, msg *base.TaskMessage, t time.Time, uniqueTTL time.Duration, forceUnique bool) error {
	if uniqueTTL > 0 {
		ttl := t.Add(uniqueTTL).Sub(time.Now())
		return c.rdb.ScheduleUnique(ctx, msg, t, ttl, forceUnique)
	}
	return c.rdb.Schedule(ctx, msg, t)
}

func (c *Client) EnqueueBatch(tasks []*Task, opts ...Option) ([]*TaskInfo, error) {
	return c.EnqueueBatchContext(context.Background(), tasks, opts...)
}

// EnqueueBatchContext enqueues the given tasks to be processed asynchronously.
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
func (c *Client) EnqueueBatchContext(ctx context.Context, tasks []*Task, opts ...Option) ([]*TaskInfo, error) {
	for _, task := range tasks {
		if strings.TrimSpace(task.Type()) == "" {
			return nil, fmt.Errorf("task typename cannot be empty")
		}
	}
	tasksOpts := make([]option, 0, len(tasks))
	var err error
	{
		var opt option
		for _, task := range tasks {
			// merge task options with the options provided at enqueue time.
			opts = append(task.opts, opts...)
			opt, err = composeOptions(opts...)
			if err != nil {
				break
			}

			if opt.serverAffinity > 0 {
				if _, ok := c.rdb.(*rdb.RDB); ok {
					return nil, fmt.Errorf("server affinity not supported with redis")
				}
			}
			tasksOpts = append(tasksOpts, opt)
		}
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
			ID:             uuid.New().String(),
			Type:           task.Type(),
			Payload:        task.Payload(),
			Queue:          opt.queue,
			Retry:          opt.retry,
			Deadline:       deadline.Unix(),
			Timeout:        int64(timeout.Seconds()),
			UniqueKey:      uniqueKey,
			UniqueKeyTTL:   int64(opt.uniqueTTL.Seconds()),
			Recurrent:      opt.recurrent,
			ReprocessAfter: int64(opt.reprocessAfter.Seconds()),
			ServerAffinity: int64(opt.serverAffinity.Seconds()),
		}

		var bm *base.MessageBatch
		if opt.processAt.Before(now) || opt.processAt.Equal(now) {
			opt.processAt = now
			if c.loc != nil {
				opt.processAt = now.In(c.loc)
			}
			if opt.uniqueTTL > 0 {
				bm = &base.MessageBatch{
					InputIndex:  i,
					Msg:         msg,
					UniqueTTL:   opt.uniqueTTL,
					ForceUnique: opt.forceUnique,
					ProcessAt:   opt.processAt,
					State:       base.TaskStatePending,
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
					InputIndex:  i,
					Msg:         msg,
					UniqueTTL:   opt.processAt.Add(opt.uniqueTTL).Sub(now),
					ForceUnique: opt.forceUnique,
					ProcessAt:   opt.processAt,
					State:       base.TaskStateScheduled,
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
		err = c.rdb.EnqueueBatch(ctx, enqueue...)
	}
	if len(enqueueUnique) > 0 {
		ex := c.rdb.EnqueueUniqueBatch(ctx, enqueueUnique...)
		err = multierr.Append(err, ex)
	}
	if len(schedule) > 0 {
		ex := c.rdb.ScheduleBatch(ctx, schedule...)
		err = multierr.Append(err, ex)
	}
	if len(scheduleUnique) > 0 {
		ex := c.rdb.ScheduleUniqueBatch(ctx, scheduleUnique...)
		err = multierr.Append(err, ex)
	}

	ret := make([]*TaskInfo, len(tasks))
	allErrs := make(map[int]error)
	for i, msg := range allBm {
		if msg.Err == nil {
			ret[i] = newTaskInfo(msg.Msg, msg.State, msg.ProcessAt, nil)
		} else {
			allErrs[i] = msg.Err
		}
	}
	if err != nil {
		c.logger.Debug(fmt.Sprintf("enqueueBatch error %v enqueuing "+
			"[tasks_count: %d unique tasks_count: %d], "+
			"scheduling [tasks_count: %d unique tasks_count: %d], "+
			"inner errors: %v", err,
			len(enqueue), len(enqueueUnique),
			len(schedule), len(scheduleUnique),
			allErrs))
	}
	if len(allErrs) > 0 {
		return ret, &errors.BatchError{Errors: allErrs}
	}
	return ret, nil
}

// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rqlite encapsulates the interactions with rqlite.
package rqlite

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/rqlite/gorqlite"
)

const (
	maxArchiveSize           = 10000               // maximum number of tasks in archive
	archivedExpirationInDays = 90                  // number of days before an archived task gets deleted permanently
	statsTTL                 = 90 * 24 * time.Hour // 90 days
	maxEvents                = 1000                // Maximum number of enqueue events to store per scheduler entry.
)

var _ base.Broker = (*RQLite)(nil)
var _ base.Scheduler = (*RQLite)(nil)
var _ base.Inspector = (*RQLite)(nil)
var slog log.Base

type Config struct {
	RqliteUrl        string `json:"rqlite_url"`        // Rqlite server url, e.g. http://localhost:4001.
	ConsistencyLevel string `json:"consistency_level"` // consistency level: none | weak| strong
}

func (c *Config) InitDefaults() *Config {
	c.ConsistencyLevel = "strong"
	return c
}

func (c *Config) Validate() error {
	if len(c.RqliteUrl) == 0 {
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "no rqlite url provided")
	}
	c.ConsistencyLevel = strings.ToLower(c.ConsistencyLevel)
	if c.ConsistencyLevel != "none" && c.ConsistencyLevel != "weak" && c.ConsistencyLevel != "strong" {
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition,
			fmt.Sprintf("invalid consistency level: %s", c.ConsistencyLevel))
	}
	return nil
}

// RQLite is a client interface to query and mutate task queues.
type RQLite struct {
	config     Config
	httpClient *http.Client
	mu         sync.Mutex
	conn       *gorqlite.Connection
	logger     log.Base
}

// NewRQLite returns a new instance of RQLite.
// * config must be a valid Config
// * client is an optional http.Client. If nil a new client will be used for
//   each request made to the rqlite cluster.
// * logger is an optional logger. If nil a default logger printing to stderr
//   will be created.
func NewRQLite(config Config, client *http.Client, logger log.Base) *RQLite {
	if logger == nil {
		logger = log.NewLogger(nil)
	}
	if slog == nil {
		slog = logger
	}
	return &RQLite{
		config:     config,
		httpClient: client,
		logger:     logger,
	}
}

// Close closes the connection with rqlite server.
func (r *RQLite) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		r.conn.Close()
	}
	return nil
}

func (r *RQLite) Open() error {
	_, err := r.Client()
	return err
}

func (r *RQLite) context() context.Context {
	// PENDING(GIL): for now just basic
	return context.Background()
}

// open validates the config and opens the connection with rqlite.
// It must be called under lock.
func (r *RQLite) open() error {
	if r.conn != nil {
		return nil
	}
	err := r.config.Validate()
	if err != nil {
		return err
	}
	conn, err := gorqlite.OpenContext(r.context(), r.config.RqliteUrl, r.httpClient)
	if err != nil {
		return errors.E("open", errors.Internal, err)
	}
	r.conn = &conn
	_ = r.conn.SetConsistencyLevel(r.config.ConsistencyLevel)
	return nil
}

// Client returns the reference to underlying rqlite client.
func (r *RQLite) Client() (*gorqlite.Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.open()
	if err != nil {
		return nil, err
	}
	return r.conn, nil
}

func (r *RQLite) client(op errors.Op) (*gorqlite.Connection, error) {
	conn, err := r.Client()
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return conn, nil
}

func (r *RQLite) Inspector() base.Inspector {
	return r
}

// Ping checks the connection with rqlite server.
func (r *RQLite) Ping() error {
	conn, err := r.client("rqlite.Ping")
	if err != nil {
		return err
	}
	_, err = conn.Leader(r.context())
	return err
}

// Enqueue adds the given task to the pending list of the queue.
func (r *RQLite) Enqueue(msg *base.TaskMessage) error {
	var op errors.Op = "rqlite.Enqueue"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return enqueueMessages(conn, &base.MessageBatch{Msg: msg})
}

// EnqueueBatch adds the given tasks to the pending list of the queue.
func (r *RQLite) EnqueueBatch(msgs ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.EnqueueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return enqueueMessages(conn, msgs...)
}

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) EnqueueUnique(msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "rqlite.EnqueueUnique"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return enqueueUniqueMessages(conn, &base.MessageBatch{
		Msg:       msg,
		UniqueTTL: ttl,
	})
}

// EnqueueUniqueBatch inserts the given tasks if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) EnqueueUniqueBatch(msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.EnqueueUniqueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return enqueueUniqueMessages(conn, msg...)
}

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and deadline.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RQLite) Dequeue(qnames ...string) (msg *base.TaskMessage, deadline time.Time, err error) {
	var op errors.Op = "rqlite.Dequeue"
	conn, err := r.client(op)
	if err != nil {
		return nil, time.Time{}, err
	}

	for _, qname := range qnames {

		q, err := r.getQueue(qname)
		if err != nil {
			return nil, time.Time{}, errors.E(op, fmt.Sprintf("get queue error: %v", err))
		}
		if q == nil || q.state == paused {
			continue
		}

		data, err := dequeueMessage(conn, qname)
		if err != nil {
			return nil, time.Time{}, errors.E(op, fmt.Sprintf("rqlite eval error: %v", err))
		}
		if data == nil {
			continue
		}
		if msg, err = decodeMessage([]byte(data.msg)); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		return msg, utc.Unix(data.deadline, 0).Time, nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// Done removes the task from active queue to mark the task as done and set its
// state to 'processed'.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RQLite) Done(msg *base.TaskMessage) error {
	conn, err := r.client("rqlite.Done")
	if err != nil {
		return err
	}
	return setTaskDone(conn, msg)
}

// Requeue moves the task from active to pending in the specified queue.
func (r *RQLite) Requeue(msg *base.TaskMessage) error {
	conn, err := r.client("rqlite.Requeue")
	if err != nil {
		return err
	}
	return requeueTask(conn, msg)
}

// Schedule adds the task to the scheduled set to be processed in the future.
func (r *RQLite) Schedule(msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rqlite.Schedule"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return scheduleTasks(conn, &base.MessageBatch{
		Msg:       msg,
		ProcessAt: processAt,
	})
}

// ScheduleBatch adds the tasks to the scheduled set to be processed in the future.
func (r *RQLite) ScheduleBatch(msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.ScheduleBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return scheduleTasks(conn, msg...)
}

// ScheduleUnique adds the task to the backlog queue to be processed in the future
// if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) ScheduleUnique(msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "rqlite.ScheduleUnique"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return scheduleUniqueTasks(conn, &base.MessageBatch{
		Msg:       msg,
		UniqueTTL: ttl,
		ProcessAt: processAt,
	})
}

// ScheduleUniqueBatch adds the tasks to the backlog queue to be processed in the future
// if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) ScheduleUniqueBatch(msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.ScheduleUniqueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return scheduleUniqueTasks(conn, msg...)
}

// Retry moves the task from active to retry queue, incrementing retry count
// and assigning error message to the task message.
func (r *RQLite) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	conn, err := r.client("rqlite.Retry")
	if err != nil {
		return err
	}
	return retryTask(conn, msg, processAt, errMsg, isFailure)
}

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
func (r *RQLite) Archive(msg *base.TaskMessage, errMsg string) error {
	conn, err := r.client("rqlite.Archive")
	if err != nil {
		return err
	}
	return archiveTask(conn, msg, errMsg)
}

// ForwardIfReady checks scheduled and retry sets of the given queues
// and move any tasks that are ready to be processed to the pending set.
func (r *RQLite) ForwardIfReady(qnames ...string) error {
	var op errors.Op = "rqlite.ForwardIfReady"
	for _, qname := range qnames {
		if err := r.forwardAll(qname); err != nil {
			return errors.E(op, errors.CanonicalCode(err), err)
		}
	}
	return nil
}

// forward moves tasks with an 'xx_at' value less than the current unix time
// from the src state to 'pending'. It returns the number of tasks moved.
func (r *RQLite) forward(qname, src string) (int, error) {
	conn, err := r.client("rqlite.forward")
	if err != nil {
		return 0, err
	}

	return forwardTasks(conn, qname, src)
}

// forwardAll checks for tasks in scheduled/retry state that are ready to be run,
// and updates their state to "pending".
func (r *RQLite) forwardAll(qname string) (err error) {
	sources := []string{
		scheduled,
		retry,
	}

	for _, src := range sources {
		n := 1
		for n != 0 {
			n, err = r.forward(qname, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ListDeadlineExceeded returns a list of task messages that have exceeded the
// deadline from the given queues.
func (r *RQLite) ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	conn, err := r.client("rqlite.ListDeadlineExceeded")
	if err != nil {
		return nil, err
	}

	var op errors.Op = "rqlite.ListDeadlineExceeded"
	var msgs []*base.TaskMessage
	for _, qname := range qnames {
		res, err := listDeadlineExceededTasks(conn, qname, deadline)
		if err != nil {
			return nil, errors.E(op, fmt.Sprintf("rqlite eval error: %v", err))
		}
		if res == nil {
			continue
		}
		msgs = append(msgs, res...)
	}
	return msgs, nil
}

// WriteServerState writes server state data to rqlite with expiration set to the value ttl.
func (r *RQLite) WriteServerState(serverInfo *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	conn, err := r.client("rqlite.WriteServerState")
	if err != nil {
		return err
	}
	return writeServerState(conn, serverInfo, workers, ttl)
}

// ClearServerState deletes server state data from rqlite.
func (r *RQLite) ClearServerState(host string, pid int, serverID string) error {
	conn, err := r.client("rqlite.ClearServerState")
	if err != nil {
		return err
	}
	return clearServerState(conn, host, pid, serverID)
}

// WriteSchedulerEntries writes scheduler entries data to rqlite with expiration set to the value ttl.
func (r *RQLite) WriteSchedulerEntries(schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	conn, err := r.client("rqlite.WriteSchedulerEntries")
	if err != nil {
		return err
	}
	return writeSchedulerEntries(conn, schedulerID, entries, ttl)
}

// ClearSchedulerEntries deletes scheduler entries data from rqlite.
func (r *RQLite) ClearSchedulerEntries(schedulerID string) error {
	conn, err := r.client("rqlite.ClearSchedulerEntries")
	if err != nil {
		return err
	}
	return clearSchedulerEntries(conn, schedulerID)
}

var _ base.PubSub = (*rqlitePubSub)(nil)

type rqlitePubSub struct {
	done chan interface{}
	ch   chan interface{}
}

func (ps *rqlitePubSub) Close() error {
	ps.done <- true
	return nil
}

func (ps *rqlitePubSub) Channel() <-chan interface{} {
	return ps.ch
}

// CancelationPubSub returns a pubsub for cancelation messages.
func (r *RQLite) CancelationPubSub() (base.PubSub, error) {
	var op errors.Op = "rqlite.CancelationPubSub"
	conn, err := r.client(op)
	if err != nil {
		return nil, err
	}

	ret := &rqlitePubSub{
		done: make(chan interface{}),
		ch:   make(chan interface{}, 1),
	}

	ticker := time.NewTicker(time.Millisecond * 200)
	go func() {
	out:
		for {
			select {
			case <-ret.done:
				// terminated
				close(ret.ch)
				break out
			case <-ticker.C:
				st := Statement("SELECT uuid FROM " + CancellationTable +
					" WHERE ndx = (SELECT COALESCE(MAX(ndx),0) FROM " + CancellationTable + ")")
				qrs, err := conn.QueryStmt(context.Background(), st)
				if err != nil {
					r.logger.Error(fmt.Sprintf("cancellation channel query failed: %v", err))
					if strings.Contains(err.Error(), "gorqlite: connection is closed") {
						return
					}
					// assume a temporary error
					continue
				}
				qr := qrs[0]
				if qr.NumRows() == 0 {
					continue
				}
				qr.Next()
				uuid := ""
				err = qr.Scan(&uuid)
				if err != nil {
					r.logger.Error(fmt.Sprintf("cancellation channel scan failed: %v", errors.E(op, errors.Internal, err)))
					continue
				}
				ret.ch <- uuid

				st = Statement("DELETE FROM "+CancellationTable+
					" WHERE uuid=?",
					uuid)
				wrs, err := conn.WriteStmt(context.Background(), st)
				if err != nil {
					r.logger.Error(fmt.Sprintf("cancellation channel delete failed: %v", NewRqliteWError(op, wrs[0], err, st)))
				}

			}
		}
	}()
	return ret, nil
}

// PublishCancelation publish cancelation message to all subscribers.
// The message is the ID for the task to be canceled.
func (r *RQLite) PublishCancelation(id string) error {
	var op errors.Op = "rqlite.PublishCancelation"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	st := Statement(
		"INSERT INTO "+CancellationTable+"(uuid, cancelled_at) VALUES(?, ?) ",
		id,
		utc.Now().Unix())
	wrs, err := conn.WriteStmt(context.Background(), st)
	if err != nil {
		return NewRqliteWError(op, wrs[0], err, st)
	}
	return nil
}

// RecordSchedulerEnqueueEvent records the time when the given task was enqueued.
func (r *RQLite) RecordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	conn, err := r.client("rqlite.RecordSchedulerEnqueueEvent")
	if err != nil {
		return err
	}
	return recordSchedulerEnqueueEvent(conn, entryID, event)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
func (r *RQLite) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rqlite.ClearSchedulerHistory"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return clearSchedulerHistory(conn, entryID)
}

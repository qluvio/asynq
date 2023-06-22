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
	"github.com/hibiken/asynq/internal/sqlite3"
	"github.com/hibiken/asynq/internal/timeutil"
)

const (
	maxArchiveSize            = 10000                  // maximum number of tasks in archive
	archivedExpirationInDays  = 90                     // number of days before an archived task gets deleted permanently
	statsTTL                  = 90 * 24 * time.Hour    // same as a duration 90 days
	schedulerHistoryMaxEvents = 1000                   // Maximum number of enqueue events to store per scheduler entry.
	PubsubPollingInterval     = time.Millisecond * 200 // polling period for pub-sub
	rqliteType                = "rqlite"
	sqliteType                = "sqlite"
	dayFormat                 = "2006-01-02"
)

var _ base.Broker = (*RQLite)(nil)
var _ base.Scheduler = (*RQLite)(nil)
var _ base.Inspector = (*RQLite)(nil)
var slog log.Base

type Config struct {
	Type                      string        `json:"type"`                                   // rqlite | sqlite
	SqliteDbPath              string        `json:"sqlite_db_path,omitempty"`               // sqlite: db path
	SqliteInMemory            bool          `json:"sqlite_in_memory,omitempty"`             // sqlite: im memory DB
	SqliteTracing             bool          `json:"sqlite_tracing,omitempty"`               // sqlite: true to trace sql requests execution
	RqliteUrl                 string        `json:"rqlite_url,omitempty"`                   // rqlite: server url, e.g. http://localhost:4001.
	ConsistencyLevel          string        `json:"consistency_level"`                      // rqlite: consistency level: none | weak| strong
	TablesPrefix              string        `json:"tables_prefix,omitempty"`                // tables prefix
	MaxArchiveSize            int           `json:"max_archive_size,omitempty"`             // maximum number of tasks in archive
	ArchivedExpirationInDays  int           `json:"archived_expiration_in_days,omitempty"`  // number of days before an archived task gets deleted permanently
	ArchiveTTL                time.Duration `json:"archive_ttl,omitempty"`                  // expiration of archived entries
	SchedulerHistoryMaxEvents int           `json:"scheduler_history_max_events,omitempty"` // Maximum number of enqueue events to store per scheduler entry.
	PubsubPollingInterval     time.Duration `json:"pubsub_polling_interval,omitempty"`      // interval for polling the pub-sub table. Zero to disable.
}

func (c *Config) InitDefaults() *Config {
	c.ConsistencyLevel = "strong"
	c.MaxArchiveSize = maxArchiveSize
	c.ArchivedExpirationInDays = archivedExpirationInDays
	c.ArchiveTTL = statsTTL
	c.SchedulerHistoryMaxEvents = schedulerHistoryMaxEvents
	c.PubsubPollingInterval = PubsubPollingInterval
	return c
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "nil config")
	}
	if c.RqliteUrl == "" && c.SqliteDbPath == "" {
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "no rqlite url and no sqlite db path provided")
	}
	switch c.Type {
	case rqliteType:
		if c.RqliteUrl == "" {
			return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "no rqlite url provided")
		}
		c.SqliteDbPath = ""
	case sqliteType:
		// require a db path even with in-memory as we need it to share the connection
		if c.SqliteDbPath == "" {
			return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "no sqlite db path provided")
		}
		c.RqliteUrl = ""
	default:
		if c.RqliteUrl != "" && c.SqliteDbPath != "" {
			return errors.E(errors.Op("config.validate"), errors.FailedPrecondition, "no type specified and both rqlite url and sqlite db path provided")
		}
		if c.RqliteUrl != "" {
			c.Type = rqliteType
		}
		if c.SqliteDbPath != "" {
			c.Type = sqliteType
		}
	}
	c.ConsistencyLevel = strings.ToLower(c.ConsistencyLevel)
	if c.ConsistencyLevel != "none" && c.ConsistencyLevel != "weak" && c.ConsistencyLevel != "strong" {
		return errors.E(errors.Op("config.validate"), errors.FailedPrecondition,
			fmt.Sprintf("invalid consistency level: %s", c.ConsistencyLevel))
	}
	if c.MaxArchiveSize <= 0 {
		c.MaxArchiveSize = maxArchiveSize
	}
	if c.ArchivedExpirationInDays <= 0 {
		c.ArchivedExpirationInDays = archivedExpirationInDays
	}
	if c.ArchiveTTL <= 0 {
		c.ArchiveTTL = statsTTL
	}
	if c.SchedulerHistoryMaxEvents <= 0 {
		c.SchedulerHistoryMaxEvents = schedulerHistoryMaxEvents
	}

	return nil
}

// RQLite is a client interface to query and mutate task queues.
type RQLite struct {
	config     *Config
	httpClient *http.Client
	mu         sync.Mutex
	conn       *Connection
	clock      timeutil.Clock
	logger     log.Base
}

// NewRQLite returns a new instance of RQLite.
//   - config must be a valid Config
//   - client is an optional http.Client. If nil a new client will be used for
//     each request made to the rqlite cluster.
//   - logger is an optional logger. If nil a default logger printing to stderr
//     will be created.
func NewRQLite(config *Config, client *http.Client, logger log.Base) *RQLite {
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
		clock:      timeutil.NewRealClock(),
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

func (r *RQLite) SetClock(c timeutil.Clock) {
	r.clock = c
}

func (r *RQLite) Now() time.Time {
	return r.clock.Now().UTC()
}

func (r *RQLite) context() context.Context {
	return r.conn.ctx()
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

	r.conn, err = newConnection(
		context.Background(), // PENDING(GIL): use a context with deadline ...
		r.config,
		r.httpClient,
		r.logger)
	if err != nil {
		return err
	}
	return nil
}

// Client returns the reference to underlying rqlite client.
func (r *RQLite) Client() (*Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.open()
	if err != nil {
		return nil, err
	}
	return r.conn, nil
}

func (r *RQLite) client(op errors.Op) (*Connection, error) {
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
	return conn.PingContext(r.context())
}

// Enqueue adds the given task to the pending list of the queue.
func (r *RQLite) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rqlite.Enqueue"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return conn.enqueueMessages(ctx, r.Now(), &base.MessageBatch{Msg: msg})
}

// EnqueueBatch adds the given tasks to the pending list of the queue.
func (r *RQLite) EnqueueBatch(ctx context.Context, msgs ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.EnqueueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return conn.enqueueMessages(ctx, r.Now(), msgs...)
}

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration, forceUnique ...bool) error {
	var op errors.Op = "rqlite.EnqueueUnique"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	funique := false
	if len(forceUnique) > 0 {
		funique = forceUnique[0]
	}
	return conn.enqueueUniqueMessages(ctx, r.Now(), &base.MessageBatch{
		Msg:         msg,
		UniqueTTL:   ttl,
		ForceUnique: funique,
	})
}

// EnqueueUniqueBatch inserts the given tasks if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) EnqueueUniqueBatch(ctx context.Context, msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.EnqueueUniqueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return conn.enqueueUniqueMessages(ctx, r.Now(), msg...)
}

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and deadline.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RQLite) Dequeue(serverID string, qnames ...string) (msg *base.TaskMessage, deadline time.Time, err error) {
	var op errors.Op = "rqlite.Dequeue"
	conn, err := r.client(op)
	if err != nil {
		return nil, time.Time{}, err
	}

	for _, qname := range qnames {
		t := time.Now()
		q, err := r.getQueue(qname)
		r.logger.Debug(fmt.Sprintf("getQueue [%s], qname=%s, err=%v", time.Since(t).String(), qname, err))
		if err != nil {
			return nil, time.Time{}, errors.E(op, fmt.Sprintf("get queue error: %v", err))
		}
		if q == nil || q.state == paused {
			continue
		}

		// here we would use dequeueMessage0 to perform dequeue in 2 steps
		t = time.Now()
		data, err := conn.dequeueMessage(r.Now(), serverID, qname)
		r.logger.Debug(fmt.Sprintf("dequeueMessage [%s], qname=%s, err=%v", time.Since(t).String(), qname, err))
		if err != nil {
			return nil, time.Time{}, errors.E(op, fmt.Sprintf("rqlite eval error: %v", err))
		}
		if data == nil {
			continue
		}
		return data.msg, time.Unix(data.deadline, 0), nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// Done removes the task from active queue to mark the task as done and set its
// state to 'processed'.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RQLite) Done(serverID string, msg *base.TaskMessage) error {
	conn, err := r.client("rqlite.Done")
	if err != nil {
		return err
	}
	return conn.setTaskDone(r.Now(), serverID, msg)
}

// Requeue moves the task from active to pending in the specified queue.
func (r *RQLite) Requeue(serverID string, msg *base.TaskMessage, aborted bool) error {
	conn, err := r.client("rqlite.Requeue")
	if err != nil {
		return err
	}
	return conn.requeueTask(r.Now(), serverID, msg, aborted)
}

// Schedule adds the task to the scheduled set to be processed in the future.
func (r *RQLite) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rqlite.Schedule"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return conn.scheduleTasks(ctx, &base.MessageBatch{
		Msg:       msg,
		ProcessAt: processAt,
	})
}

// ScheduleBatch adds the tasks to the scheduled set to be processed in the future.
func (r *RQLite) ScheduleBatch(ctx context.Context, msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.ScheduleBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return conn.scheduleTasks(ctx, msg...)
}

// ScheduleUnique adds the task to the backlog queue to be processed in the future
// if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration, forceUnique ...bool) error {
	var op errors.Op = "rqlite.ScheduleUnique"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	funique := false
	if len(forceUnique) > 0 {
		funique = forceUnique[0]
	}

	return conn.scheduleUniqueTasks(ctx, r.Now(), &base.MessageBatch{
		Msg:         msg,
		UniqueTTL:   ttl,
		ProcessAt:   processAt,
		ForceUnique: funique,
	})
}

// ScheduleUniqueBatch adds the tasks to the backlog queue to be processed in the future
// if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RQLite) ScheduleUniqueBatch(ctx context.Context, msg ...*base.MessageBatch) error {
	var op errors.Op = "rqlite.ScheduleUniqueBatch"
	conn, err := r.client(op)
	if err != nil {
		return err
	}

	return conn.scheduleUniqueTasks(ctx, r.Now(), msg...)
}

// Retry moves the task from active to retry queue, incrementing retry count
// and assigning error message to the task message.
func (r *RQLite) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	conn, err := r.client("rqlite.Retry")
	if err != nil {
		return err
	}
	return conn.retryTask(r.Now(), msg, processAt, errMsg, isFailure)
}

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
func (r *RQLite) Archive(msg *base.TaskMessage, errMsg string) error {
	conn, err := r.client("rqlite.Archive")
	if err != nil {
		return err
	}
	return conn.archiveTask(r.Now(), msg, errMsg)
}

func (r *RQLite) MarkAsComplete(serverID string, msg *base.TaskMessage) error {
	conn, err := r.client("rqlite.Complete")
	if err != nil {
		return err
	}
	return conn.setTaskCompleted(r.Now(), serverID, msg)
}

func (r *RQLite) DeleteExpiredCompletedTasks(qname string) error {
	conn, err := r.client("rqlite.DeleteExpiredCompletedTasks")
	if err != nil {
		return err
	}
	return conn.deleteExpiredCompletedTasks(r.Now(), qname)
}

func (r *RQLite) WriteResult(qname, id string, data []byte) (int, error) {
	conn, err := r.client("rqlite.WriteResult")
	if err != nil {
		return 0, err
	}
	return conn.writeTaskResult(qname, id, data, true)
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

	return conn.forwardTasks(r.Now(), qname, src)
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
		res, err := conn.listDeadlineExceededTasks(qname, deadline)
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
	return conn.writeServerState(r.Now(), serverInfo, workers, ttl)
}

// ClearServerState deletes server state data from rqlite.
func (r *RQLite) ClearServerState(host string, pid int, serverID string) error {
	conn, err := r.client("rqlite.ClearServerState")
	if err != nil {
		return err
	}
	return conn.clearServerState(host, pid, serverID)
}

// WriteSchedulerEntries writes scheduler entries data to rqlite with expiration set to the value ttl.
func (r *RQLite) WriteSchedulerEntries(schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	conn, err := r.client("rqlite.WriteSchedulerEntries")
	if err != nil {
		return err
	}
	return conn.writeSchedulerEntries(r.Now(), schedulerID, entries, ttl)
}

// ClearSchedulerEntries deletes scheduler entries data from rqlite.
func (r *RQLite) ClearSchedulerEntries(schedulerID string) error {
	conn, err := r.client("rqlite.ClearSchedulerEntries")
	if err != nil {
		return err
	}
	return conn.clearSchedulerEntries(schedulerID)
}

var _ base.PubSub = (*rqlitePubSub)(nil)

type rqlitePubSub struct {
	done chan interface{}
	ch   chan interface{}
}

func (ps *rqlitePubSub) Close() error {
	if ps.done == nil {
		return nil
	}
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
	if r.config.PubsubPollingInterval <= 0 {
		ret.done = nil
		return ret, nil
	}

	ticker := time.NewTicker(r.config.PubsubPollingInterval)
	go func() {
	out:
		for {
			select {
			case <-ret.done:
				// terminated
				close(ret.ch)
				break out
			case <-ticker.C:
				st := Statement("SELECT uuid FROM " + conn.table(CancellationTable) +
					" WHERE ndx = (SELECT COALESCE(MAX(ndx),0) FROM " + conn.table(CancellationTable) + ")")
				qrs, err := conn.QueryStmt(r.context(), st)
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

				st = Statement("DELETE FROM "+conn.table(CancellationTable)+
					" WHERE uuid=?",
					uuid)
				wrs, err := conn.WriteStmt(r.context(), st)
				if err != nil {
					r.logger.Error(fmt.Sprintf("cancellation channel delete failed: %v",
						NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})))
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
		"INSERT INTO "+conn.table(CancellationTable)+"(uuid, cancelled_at) VALUES(?, ?) ",
		id,
		r.Now().Unix())
	wrs, err := conn.WriteStmt(r.context(), st)
	if err != nil {
		return NewRqliteWsError(op, wrs, err, []*sqlite3.Statement{st})
	}
	return nil
}

// RecordSchedulerEnqueueEvent records the time when the given task was enqueued.
func (r *RQLite) RecordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	conn, err := r.client("rqlite.RecordSchedulerEnqueueEvent")
	if err != nil {
		return err
	}
	return conn.recordSchedulerEnqueueEvent(entryID, event)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
func (r *RQLite) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rqlite.ClearSchedulerHistory"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	return conn.clearSchedulerHistory(entryID)
}

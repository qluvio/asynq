// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

// Inspector is a client interface to inspect and mutate the state of
// queues and tasks.
type Inspector struct {
	rdb base.Inspector
}

// NewInspector returns a new instance of Inspector.
func NewInspector(r ClientConnOpt) *Inspector {
	c, err := makeBroker(r)
	if err != nil {
		panic(err)
	}
	return newInspector(c)
}

// NewInspectorFrom returns a new instance of Inspector built with the broker of
// the given server.
func NewInspectorFrom(s *Server) (*Inspector, error) {
	if s == nil {
		return nil, errors.E(errors.Op("NewInspectorFrom"), errors.Internal, "server is nil")
	}
	return newInspector(s.broker), nil
}

// NewInspectorClient returns a new instance of Inspector built with the broker of
// the given Client.
func NewInspectorClient(c *Client) (*Inspector, error) {
	if c == nil {
		return nil, errors.E(errors.Op("NewInspectorFrom"), errors.Internal, "client is nil")
	}
	return newInspector(c.rdb), nil
}

func newInspector(c base.Broker) *Inspector {
	return &Inspector{
		rdb: c.Inspector(),
	}
}

// Close closes the connection with redis.
func (i *Inspector) Close() error {
	return i.rdb.Close()
}

// Queues returns a list of all queue names.
func (i *Inspector) Queues() ([]string, error) {
	return i.rdb.AllQueues()
}

// QueueInfo represents a state of queues at a certain time.
type QueueInfo struct {
	// Name of the queue.
	Queue string

	// Total number of bytes that the queue and its tasks require to be stored in redis.
	// It is an approximate memory usage value in bytes since the value is computed by sampling.
	MemoryUsage int64

	// Latency of the queue, measured by the oldest pending task in the queue.
	Latency time.Duration

	// Size is the total number of tasks in the queue.
	// The value is the sum of Pending, Active, Scheduled, Retry, and Archived.
	Size int

	// Number of pending tasks.
	Pending int
	// Number of active tasks.
	Active int
	// Number of scheduled tasks.
	Scheduled int
	// Number of retry tasks.
	Retry int
	// Number of archived tasks.
	Archived int
	// Number of stored completed tasks.
	Completed int

	// Total number of tasks being processed during the given date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed to be processed during the given date.
	Failed int

	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue will not be processed.
	Paused bool

	// Time when this queue info snapshot was taken.
	Timestamp time.Time
}

// GetQueueInfo returns current information of the given queue.
func (i *Inspector) GetQueueInfo(qname string) (*QueueInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, err
	}
	stats, err := i.rdb.CurrentStats(qname)
	if err != nil {
		return nil, err
	}
	return &QueueInfo{
		Queue:       stats.Queue,
		MemoryUsage: stats.MemoryUsage,
		Latency:     stats.Latency,
		Size:        stats.Size,
		Pending:     stats.Pending,
		Active:      stats.Active,
		Scheduled:   stats.Scheduled,
		Retry:       stats.Retry,
		Archived:    stats.Archived,
		Completed:   stats.Completed,
		Processed:   stats.Processed,
		Failed:      stats.Failed,
		Paused:      stats.Paused,
		Timestamp:   stats.Timestamp,
	}, nil
}

// DailyStats holds aggregate data for a given day for a given queue.
type DailyStats struct {
	// Name of the queue.
	Queue string
	// Total number of tasks being processed during the given date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed to be processed during the given date.
	Failed int
	// Date this stats was taken.
	Date time.Time
}

// History returns a list of stats from the last n days.
func (i *Inspector) History(qname string, n int) ([]*DailyStats, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, err
	}
	stats, err := i.rdb.HistoricalStats(qname, n)
	if err != nil {
		return nil, err
	}
	var res []*DailyStats
	for _, s := range stats {
		res = append(res, &DailyStats{
			Queue:     s.Queue,
			Processed: s.Processed,
			Failed:    s.Failed,
			Date:      s.Time,
		})
	}
	return res, nil
}

var (
	// ErrQueueNotFound indicates that the specified queue does not exist.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrQueueNotEmpty indicates that the specified queue is not empty.
	ErrQueueNotEmpty = errors.New("queue is not empty")

	// ErrTaskNotFound indicates that the specified task cannot be found in the queue.
	ErrTaskNotFound = errors.New("task not found")
)

// DeleteQueue removes the specified queue.
//
// If force is set to true, DeleteQueue will remove the queue regardless of
// the queue size as long as no tasks are active in the queue.
// If force is set to false, DeleteQueue will remove the queue only if
// the queue is empty.
//
// If the specified queue does not exist, DeleteQueue returns ErrQueueNotFound.
// If force is set to false and the specified queue is not empty, DeleteQueue
// returns ErrQueueNotEmpty.
func (i *Inspector) DeleteQueue(qname string, force bool) error {
	err := i.rdb.RemoveQueue(qname, force)
	if errors.IsQueueNotFound(err) {
		return fmt.Errorf("%w: queue=%q", ErrQueueNotFound, qname)
	}
	if errors.IsQueueNotEmpty(err) {
		return fmt.Errorf("%w: queue=%q", ErrQueueNotEmpty, qname)
	}
	return err
}

// GetTaskInfo retrieves task information given a task id and queue name.
//
// Returns an error wrapping ErrQueueNotFound if a queue with the given name doesn't exist.
// Returns an error wrapping ErrTaskNotFound if a task with the given id doesn't exist in the queue.
func (i *Inspector) GetTaskInfo(qname, id string) (*TaskInfo, error) {
	info, err := i.rdb.GetTaskInfo(qname, id)
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case errors.IsTaskNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrTaskNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	return newTaskInfo(info.Message, info.State, info.NextProcessAt, info.Result), nil
}

// ListOption specifies behavior of list operation.
type ListOption interface{}

// Internal list option representations.
type (
	pageSizeOpt       int
	pageNumOpt        int
	startAfterUuidOpt string
)

type listOption struct {
	pageSize       int
	pageNum        int
	startAfterUuid string
}

func (opt listOption) pagination() base.Pagination {
	return base.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1, StartAfterUuid: opt.startAfterUuid}
}

const (
	// Page size used by default in list operation.
	defaultPageSize = 30

	// Page number used by default in list operation.
	defaultPageNum = 1
)

func composeListOptions(opts ...ListOption) listOption {
	res := listOption{
		pageSize: defaultPageSize,
		pageNum:  defaultPageNum,
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case pageSizeOpt:
			res.pageSize = int(opt)
		case pageNumOpt:
			res.pageNum = int(opt)
		case startAfterUuidOpt:
			res.startAfterUuid = string(opt)
		default:
			// ignore unexpected option
		}
	}
	return res
}

// PageSize returns an option to specify the page size for list operation.
//
// Negative page size is treated as zero.
func PageSize(n int) ListOption {
	if n < 0 {
		n = 0
	}
	return pageSizeOpt(n)
}

// Page returns an option to specify the page number for list operation.
// The value 1 fetches the first page.
//
// Negative page number is treated as one.
func Page(n int) ListOption {
	if n <= 0 {
		n = 1
	}
	return pageNumOpt(n)
}

// StartAfterUuid returns an option to specify the ID of the task after which to start listing.
func StartAfterUuid(id string) ListOption {
	return startAfterUuidOpt(id)
}

// ListPendingTasks retrieves pending tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListPendingTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListPending(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, err
}

// ListActiveTasks retrieves active tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListActiveTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListActive(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, err
}

// ListScheduledTasks retrieves scheduled tasks from the specified queue.
// Tasks are sorted by NextProcessAt in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListScheduledTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListScheduled(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, nil
}

// ListRetryTasks retrieves retry tasks from the specified queue.
// Tasks are sorted by NextProcessAt in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListRetryTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListRetry(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, nil
}

// ListArchivedTasks retrieves archived tasks from the specified queue.
// Tasks are sorted by LastFailedAt in descending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListArchivedTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListArchived(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, nil
}

// ListCompletedTasks retrieves completed tasks from the specified queue.
// Tasks are sorted by expiration time (i.e. CompletedAt + Retention) in descending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListCompletedTasks(qname string, opts ...ListOption) ([]*TaskInfo, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, fmt.Errorf("asynq: %v", err)
	}
	opt := composeListOptions(opts...)
	infos, err := i.rdb.ListCompleted(qname, opt.pagination())
	switch {
	case errors.IsQueueNotFound(err):
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case err != nil:
		return nil, fmt.Errorf("asynq: %v", err)
	}
	var tasks []*TaskInfo
	for _, i := range infos {
		tasks = append(tasks, newTaskInfo(
			i.Message,
			i.State,
			i.NextProcessAt,
			i.Result,
		))
	}
	return tasks, nil
}

// DeleteAllPendingTasks deletes all pending tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllPendingTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllPendingTasks(qname)
	return int(n), err
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllScheduledTasks(qname)
	return int(n), err
}

// DeleteAllRetryTasks deletes all retry tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllRetryTasks(qname)
	return int(n), err
}

// DeleteAllArchivedTasks deletes all archived tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllArchivedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllArchivedTasks(qname)
	return int(n), err
}

// DeleteAllCompletedTasks deletes all completed tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllCompletedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllCompletedTasks(qname)
	return int(n), err
}

// DeleteAllProcessedTasks deletes all processed tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllProcessedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllProcessedTasks(qname)
	return int(n), err
}

// DeleteTask deletes a task with the given id from the given queue.
// The task needs to be in pending, scheduled, retry, or archived state,
// otherwise DeleteTask will return an error.
//
// If a queue with the given name doesn't exist, it returns an error wrapping ErrQueueNotFound.
// If a task with the given id doesn't exist in the queue, it returns an error wrapping ErrTaskNotFound.
// If the task is in active state, it returns a non-nil error.
func (i *Inspector) DeleteTask(qname, id string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	err := i.rdb.DeleteTask(qname, id)
	switch {
	case errors.IsQueueNotFound(err):
		return fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case errors.IsTaskNotFound(err):
		return fmt.Errorf("asynq: %w", ErrTaskNotFound)
	case err != nil:
		return fmt.Errorf("asynq: %v", err)
	}
	return nil

}

// RunAllScheduledTasks transition all scheduled tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllScheduledTasks(qname)
	return int(n), err
}

// RunAllRetryTasks transition all retry tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllRetryTasks(qname)
	return int(n), err
}

// RunAllArchivedTasks transition all archived tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllArchivedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllArchivedTasks(qname)
	return int(n), err
}

// RunTask updates the task to pending state given a queue name and task id.
// The task needs to be in scheduled, retry, or archived state, otherwise RunTask
// will return an error.
//
// If a queue with the given name doesn't exist, it returns an error wrapping ErrQueueNotFound.
// If a task with the given id doesn't exist in the queue, it returns an error wrapping ErrTaskNotFound.
// If the task is in pending or active state, it returns a non-nil error.
func (i *Inspector) RunTask(qname, id string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	err := i.rdb.RunTask(qname, id)
	switch {
	case errors.IsQueueNotFound(err):
		return fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case errors.IsTaskNotFound(err):
		return fmt.Errorf("asynq: %w", ErrTaskNotFound)
	case err != nil:
		return fmt.Errorf("asynq: %v", err)
	}
	return nil
}

// ArchiveAllPendingTasks archives all pending tasks from the given queue,
// and reports the number of tasks archived.
func (i *Inspector) ArchiveAllPendingTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllPendingTasks(qname)
	return int(n), err
}

// ArchiveAllScheduledTasks archives all scheduled tasks from the given queue,
// and reports the number of tasks archiveed.
func (i *Inspector) ArchiveAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllScheduledTasks(qname)
	return int(n), err
}

// ArchiveAllRetryTasks archives all retry tasks from the given queue,
// and reports the number of tasks archiveed.
func (i *Inspector) ArchiveAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllRetryTasks(qname)
	return int(n), err
}

// ArchiveTask archives a task with the given id in the given queue.
// The task needs to be in pending, scheduled, or retry state, otherwise ArchiveTask
// will return an error.
//
// If a queue with the given name doesn't exist, it returns an error wrapping ErrQueueNotFound.
// If a task with the given id doesn't exist in the queue, it returns an error wrapping ErrTaskNotFound.
// If the task is in already archived, it returns a non-nil error.
func (i *Inspector) ArchiveTask(qname, id string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return fmt.Errorf("asynq: err")
	}
	err := i.rdb.ArchiveTask(qname, id)
	switch {
	case errors.IsQueueNotFound(err):
		return fmt.Errorf("asynq: %w", ErrQueueNotFound)
	case errors.IsTaskNotFound(err):
		return fmt.Errorf("asynq: %w", ErrTaskNotFound)
	case err != nil:
		return fmt.Errorf("asynq: %v", err)
	}
	return nil
}

// CancelProcessing sends a signal to cancel processing of the task
// given a task id. CancelProcessing is best-effort, which means that it does not
// guarantee that the task with the given id will be canceled. The return
// value only indicates whether the cancelation signal has been sent.
func (i *Inspector) CancelProcessing(id string) error {
	return i.rdb.PublishCancelation(id)
}

// PauseQueue pauses task processing on the specified queue.
// If the queue is already paused, it will return a non-nil error.
func (i *Inspector) PauseQueue(qname string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Pause(qname)
}

// UnpauseQueue resumes task processing on the specified queue.
// If the queue is not paused, it will return a non-nil error.
func (i *Inspector) UnpauseQueue(qname string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Unpause(qname)
}

// Servers return a list of running servers' information.
func (i *Inspector) Servers() ([]*ServerInfo, error) {
	servers, err := i.rdb.ListServers()
	if err != nil {
		return nil, err
	}
	workers, err := i.rdb.ListWorkers()
	if err != nil {
		return nil, err
	}
	m := make(map[string]*ServerInfo) // ServerInfo keyed by serverID
	for _, s := range servers {
		m[s.ServerID] = &ServerInfo{
			ID:             s.ServerID,
			Host:           s.Host,
			PID:            s.PID,
			Concurrency:    s.Concurrency,
			Queues:         s.Queues,
			StrictPriority: s.StrictPriority,
			Started:        s.Started,
			Status:         s.Status,
			ActiveWorkers:  make([]*WorkerInfo, 0),
		}
	}
	for _, w := range workers {
		srvInfo, ok := m[w.ServerID]
		if !ok {
			continue
		}
		wrkInfo := &WorkerInfo{
			TaskID:      w.ID,
			TaskType:    w.Type,
			TaskPayload: w.Payload,
			Queue:       w.Queue,
			Started:     w.Started,
			Deadline:    w.Deadline,
		}
		srvInfo.ActiveWorkers = append(srvInfo.ActiveWorkers, wrkInfo)
	}
	var out []*ServerInfo
	for _, srvInfo := range m {
		out = append(out, srvInfo)
	}
	return out, nil
}

// ServerInfo describes a running Server instance.
type ServerInfo struct {
	// Unique Identifier for the server.
	ID string
	// Host machine on which the server is running.
	Host string
	// PID of the process in which the server is running.
	PID int

	// Server configuration details.
	// See Config doc for field descriptions.
	Concurrency    int
	Queues         map[string]int
	StrictPriority bool

	// Time the server started.
	Started time.Time
	// Status indicates the status of the server.
	// TODO: Update comment with more details.
	Status string
	// A List of active workers currently processing tasks.
	ActiveWorkers []*WorkerInfo
}

// WorkerInfo describes a running worker processing a task.
type WorkerInfo struct {
	// ID of the task the worker is processing.
	TaskID string
	// Type of the task the worker is processing.
	TaskType string
	// Payload of the task the worker is processing.
	TaskPayload []byte
	// Queue from which the worker got its task.
	Queue string
	// Time the worker started processing the task.
	Started time.Time
	// Time the worker needs to finish processing the task by.
	Deadline time.Time
}

// ClusterKeySlot returns an integer identifying the hash slot the given queue hashes to.
func (i *Inspector) ClusterKeySlot(qname string) (int64, error) {
	return i.rdb.ClusterKeySlot(qname)
}

// ClusterNodes returns a list of nodes the given queue belongs to.
//
// Only relevant if task queues are stored in redis cluster.
func (i *Inspector) ClusterNodes(qname string) ([]*base.ClusterNode, error) {
	return i.rdb.ClusterNodes(qname)
}

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Periodic Task registered for this entry.
	Task *Task

	// Opts is the options for the periodic task.
	Opts []Option

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// SchedulerEntries returns a list of all entries registered with
// currently running schedulers.
func (i *Inspector) SchedulerEntries() ([]*SchedulerEntry, error) {
	var entries []*SchedulerEntry
	res, err := i.rdb.ListSchedulerEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range res {
		task := NewTask(e.Type, e.Payload)
		var opts []Option
		for _, s := range e.Opts {
			if o, err := parseOption(s); err == nil {
				// ignore bad data
				opts = append(opts, o)
			}
		}
		entries = append(entries, &SchedulerEntry{
			ID:   e.ID,
			Spec: e.Spec,
			Task: task,
			Opts: opts,
			Next: e.Next,
			Prev: e.Prev,
		})
	}
	return entries, nil
}

// parseOption interprets a string s as an Option and returns the Option if parsing is successful,
// otherwise returns non-nil error.
func parseOption(s string) (Option, error) {
	fn, arg := parseOptionFunc(s), parseOptionArg(s)
	switch fn {
	case "Queue":
		qname, err := strconv.Unquote(arg)
		if err != nil {
			return nil, err
		}
		return Queue(qname), nil
	case "MaxRetry":
		n, err := strconv.Atoi(arg)
		if err != nil {
			return nil, err
		}
		return MaxRetry(n), nil
	case "Timeout":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return Timeout(d), nil
	case "Deadline":
		t, err := time.Parse(time.UnixDate, arg)
		if err != nil {
			return nil, err
		}
		return Deadline(t), nil
	case "Unique":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return Unique(d), nil
	case "ProcessAt":
		t, err := time.Parse(time.UnixDate, arg)
		if err != nil {
			return nil, err
		}
		return ProcessAt(t), nil
	case "ProcessIn":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return ProcessIn(d), nil
	case "Retention":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return Retention(d), nil
	default:
		return nil, fmt.Errorf("cannot not parse option string %q", s)
	}
}

func parseOptionFunc(s string) string {
	i := strings.Index(s, "(")
	return s[:i]
}

func parseOptionArg(s string) string {
	i := strings.Index(s, "(")
	if i >= 0 {
		j := strings.Index(s, ")")
		if j > i {
			return s[i+1 : j]
		}
	}
	return ""
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// ListSchedulerEnqueueEvents retrieves a list of enqueue events from the specified scheduler entry.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListSchedulerEnqueueEvents(entryID string, opts ...ListOption) ([]*SchedulerEnqueueEvent, error) {
	opt := composeListOptions(opts...)
	data, err := i.rdb.ListSchedulerEnqueueEvents(entryID, opt.pagination())
	if err != nil {
		return nil, err
	}
	var events []*SchedulerEnqueueEvent
	for _, e := range data {
		events = append(events, &SchedulerEnqueueEvent{TaskID: e.TaskID, EnqueuedAt: e.EnqueuedAt})
	}
	return events, nil
}

func (i *Inspector) Purge(dropTables bool) error {
	return i.rdb.Purge(dropTables)
}

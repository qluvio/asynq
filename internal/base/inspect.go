package base

import (
	"time"
)

// Inspector groups function required for inspection
type Inspector interface {
	Broker
	// AllQueues returns the list of all queues
	AllQueues() ([]string, error)

	// RemoveQueue removes the specified queue.
	//
	// If force is set to true, it will remove the queue regardless
	// as long as no tasks are active for the queue.
	// If force is set to false, it will only remove the queue if
	// the queue is empty.
	RemoveQueue(qname string, force bool) error

	// CurrentStats returns the current state of the given queue.
	CurrentStats(qname string) (*Stats, error)

	// HistoricalStats returns a list of stats from the last n days.
	HistoricalStats(qname string, n int) ([]*DailyStats, error)

	// GetTaskInfo returns a TaskInfo describing the task from the given queue.
	GetTaskInfo(qname string, taskid string) (*TaskInfo, error)
	// ListPending returns pending tasks that are ready to be processed.
	ListPending(qname string, pgn Pagination) ([]*TaskInfo, error)
	// ListActive returns all tasks that are currently being processed for the given queue.
	ListActive(qname string, pgn Pagination) ([]*TaskInfo, error)
	// ListScheduled returns all tasks from the given queue that are scheduled
	// to be processed in the future.
	ListScheduled(qname string, pgn Pagination) ([]*TaskInfo, error)
	// ListRetry returns all tasks from the given queue that have failed before
	// and will be retried in the future.
	ListRetry(qname string, pgn Pagination) ([]*TaskInfo, error)
	// ListArchived returns all tasks from the given queue that have exhausted its retry limit.
	ListArchived(qname string, pgn Pagination) ([]*TaskInfo, error)
	// ListCompleted returns all tasks from the given queue that have completed successfully.
	ListCompleted(qname string, pgn Pagination) ([]*TaskInfo, error)
	// DeleteAllPendingTasks deletes all pending tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllPendingTasks(qname string) (int64, error)
	// DeleteAllScheduledTasks deletes all scheduled tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllScheduledTasks(qname string) (int64, error)
	// DeleteAllRetryTasks deletes all retry tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllRetryTasks(qname string) (int64, error)
	// DeleteAllArchivedTasks deletes all archived tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllArchivedTasks(qname string) (int64, error)
	// DeleteAllCompletedTasks deletes all completed tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllCompletedTasks(qname string) (int64, error)
	// DeleteAllProcessedTasks deletes all processed tasks from the given queue
	// and returns the number of tasks deleted.
	DeleteAllProcessedTasks(qname string) (int64, error)

	// DeleteTask finds a task that matches the id from the given queue and deletes it.
	// It returns nil if it successfully archived the task.
	//
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
	// If a task is in active state it returns non-nil error with Code FailedPrecondition.
	DeleteTask(qname string, taskid string) error

	// RunAllScheduledTasks enqueues all scheduled tasks from the given queue
	// and returns the number of tasks enqueued.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	RunAllScheduledTasks(qname string) (int64, error)
	// RunAllRetryTasks enqueues all retry tasks from the given queue
	// and returns the number of tasks enqueued.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	RunAllRetryTasks(qname string) (int64, error)
	// RunAllArchivedTasks enqueues all archived tasks from the given queue
	// and returns the number of tasks enqueued.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	RunAllArchivedTasks(qname string) (int64, error)
	// RunTask finds a task that matches the id from the given queue and updates it to pending state.
	// It returns nil if it successfully updated the task.
	//
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
	// If a task is in active or pending state it returns non-nil error with Code FailedPrecondition.
	RunTask(qname string, taskid string) error

	// ArchiveAllPendingTasks archives all pending tasks from the given queue and
	// returns the number of tasks moved.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	ArchiveAllPendingTasks(qname string) (int64, error)
	// ArchiveAllScheduledTasks archives all scheduled tasks from the given queue and
	// returns the number of tasks that were moved.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	ArchiveAllScheduledTasks(qname string) (int64, error)
	// ArchiveAllRetryTasks archives all retry tasks from the given queue and
	// returns the number of tasks that were moved.
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	ArchiveAllRetryTasks(qname string) (int64, error)
	// ArchiveTask finds a task that matches the id from the given queue and archives it.
	// It returns nil if it successfully archived the task.
	//
	// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
	// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
	// If a task is already archived, it returns TaskAlreadyArchivedError.
	// If a task is in active state it returns non-nil error with FailedPrecondition code.
	ArchiveTask(qname string, taskid string) error

	// Pause pauses processing of tasks from the given queue.
	Pause(qname string) error
	// Unpause resumes processing of tasks from the given queue.
	Unpause(qname string) error

	// ListWorkers returns the list of worker stats.
	ListWorkers() ([]*WorkerInfo, error)
	// ClusterKeySlot returns an integer identifying the hash slot the given queue hashes to.
	ClusterKeySlot(qname string) (int64, error)
	// ClusterNodes returns a list of nodes the given queue belongs to.
	ClusterNodes(qname string) ([]*ClusterNode, error)
	// ListSchedulerEntries returns the list of scheduler entries.
	ListSchedulerEntries() ([]*SchedulerEntry, error)
	// ListSchedulerEnqueueEvents returns the list of scheduler enqueue events.
	ListSchedulerEnqueueEvents(entryID string, pgn Pagination) ([]*SchedulerEnqueueEvent, error)

	// Purge removes all data regardless of any state. Mainly useful for tests.
	// If dropTables is true, it additionally drops tables (for brokers using SQL)
	Purge(dropTables bool) error
}

// Pagination specifies the page size and page number
// for the list operation.
type Pagination struct {
	// Number of items in the page.
	Size int

	// Page number starting from zero.
	Page int
}

func (p Pagination) Start() int64 {
	return int64(p.Size * p.Page)
}

func (p Pagination) Stop() int64 {
	return int64(p.Size*p.Page + p.Size - 1)
}

// Stats represents a state of queues at a certain time.
type Stats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// MemoryUsage is the total number of bytes the queue and its tasks require
	// to be stored in redis. It is an approximate memory usage value in bytes
	// since the value is computed by sampling.
	MemoryUsage int64
	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue should not be processed.
	Paused bool
	// Size is the total number of tasks in the queue.
	Size int
	// Number of tasks in each state.
	Pending   int
	Active    int
	Scheduled int
	Retry     int
	Archived  int
	Completed int
	// Total number of tasks processed during the current date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed during the current date.
	Failed int
	// Latency of the queue, measured by the oldest pending task in the queue.
	Latency time.Duration
	// Time this stats was taken.
	Timestamp time.Time
}

// DailyStats holds aggregate data for a given day.
type DailyStats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// Total number of tasks processed during the given day.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed during the given day.
	Failed int
	// Date this stats was taken.
	Time time.Time
}

// ClusterNode describes a node in redis cluster.
type ClusterNode struct {
	// Node ID in the cluster.
	ID string

	// Address of the node.
	Addr string
}

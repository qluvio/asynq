package rqlite

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

func (r *RQLite) getQueue(qname string) (*queueRow, error) {
	conn, err := r.Client()
	if err != nil {
		return nil, err
	}
	return GetQueue(conn, qname)
}

func (r *RQLite) QueueExist(qname string) (bool, error) {
	queue, err := r.getQueue(qname)
	if err != nil {
		return false, err
	}
	return queue != nil, nil
}

// Pause pauses processing of tasks from the given queue.
func (r *RQLite) Pause(queue string) error {
	return r.pauseQueue(queue, true)
}

// Unpause resumes processing of tasks from the given queue.
func (r *RQLite) Unpause(queue string) error {
	return r.pauseQueue(queue, false)
}

func (r *RQLite) pauseQueue(queue string, b bool) error {
	conn, err := r.client("rqlite.pauseQueue")
	if err != nil {
		return err
	}

	return pauseQueue(conn, queue, b)
}

// ListServers returns the list of server info.
func (r *RQLite) ListServers() ([]*base.ServerInfo, error) {
	conn, err := r.client("rqlite.ListServers")
	if err != nil {
		return nil, err
	}
	rows, err := listAllServers(conn)
	if err != nil {
		return nil, err
	}
	ret := make([]*base.ServerInfo, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.server)
	}
	return ret, nil
}

func (r *RQLite) AllQueues() ([]string, error) {
	conn, err := r.client("rqlite.AllQueues")
	if err != nil {
		return nil, err
	}
	queues, err := listQueues(conn)
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(queues))
	for _, q := range queues {
		ret = append(ret, q.queueName)
	}
	return ret, nil
}

func (r *RQLite) RemoveQueue(qname string, force bool) error {
	var op errors.Op = "rqlite.RemoveQueue"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	n, err := removeQueue(conn, qname, force)
	switch n {
	case 0:
		return errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	case 1:
		// if zero is returned, ignore it
		return nil
	case -1:
		return errors.E(op, errors.NotFound, &errors.QueueNotEmptyError{Queue: qname})
	case -2:
		return errors.E(op, errors.FailedPrecondition, "cannot remove queue with active tasks")
	default:
		return errors.E(op, errors.Unknown, fmt.Sprintf("unexpected return value: %d", n))
	}
}

func (r *RQLite) CurrentStats(qname string) (*base.Stats, error) {
	var op errors.Op = "CurrentStats"

	conn, err := r.client(op)
	if err != nil {
		return nil, err
	}
	ret, err := currentStats(conn, qname)
	if err != nil {
		return nil, err
	}

	if ret == nil {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	return ret, nil
}

func (r *RQLite) queueMustExist(op errors.Op, qname string) error {
	b, err := r.QueueExist(qname)
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	if !b {
		return errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	return nil
}

func (r *RQLite) HistoricalStats(qname string, n int) ([]*base.DailyStats, error) {
	var op errors.Op = "rqlite.HistoricalStats"
	if n < 1 {
		return nil, errors.E(op, errors.FailedPrecondition, "the number of days must be positive")
	}
	err := r.queueMustExist(op, qname)
	if err != nil {
		return nil, err
	}
	conn, _ := r.Client()

	return historicalStats(conn, qname, n)
}

func (r *RQLite) GetTaskInfo(qname string, taskid uuid.UUID) (*base.TaskInfo, error) {
	var op errors.Op = "rqlite.GetTaskInfo"

	err := r.queueMustExist(op, qname)
	if err != nil {
		return nil, err
	}
	conn, _ := r.Client()

	return getTaskInfo(conn, qname, taskid)
}

func (r *RQLite) listMessages(qname string, state string, pgn base.Pagination) ([]*base.TaskMessage, error) {
	var op errors.Op = "rqlite.listMessages"
	err := r.queueMustExist(op, qname)
	if err != nil {
		return nil, err
	}
	conn, _ := r.Client()
	tasks, err := listTasksPaged(conn, qname, state, &pgn, "")
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	ret := make([]*base.TaskMessage, 0, len(tasks))
	for _, task := range tasks {
		ret = append(ret, task.msg)
	}
	return ret, nil
}

func (r *RQLite) ListPending(qname string, pgn base.Pagination) ([]*base.TaskMessage, error) {
	return r.listMessages(qname, pending, pgn)
}
func (r *RQLite) ListActive(qname string, pgn base.Pagination) ([]*base.TaskMessage, error) {
	return r.listMessages(qname, active, pgn)
}

func (r *RQLite) listEntries(qname string, state string, pgn base.Pagination, orderBy string) ([]base.Z, error) {
	var op errors.Op = "rqlite.listEntries"
	err := r.queueMustExist(op, qname)
	if err != nil {
		return nil, err
	}
	conn, _ := r.Client()
	tasks, err := listTasksPaged(conn, qname, state, &pgn, orderBy)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	ret := make([]base.Z, 0, len(tasks))
	for _, task := range tasks {
		score := int64(0)
		switch state {
		case scheduled:
			score = task.scheduledAt
		case retry:
			score = task.retryAt
		case archived:
			score = task.archivedAt
		}
		ret = append(ret, base.Z{Message: task.msg, Score: score})
	}
	return ret, nil
}

func (r *RQLite) ListScheduled(qname string, pgn base.Pagination) ([]base.Z, error) {
	return r.listEntries(qname, scheduled, pgn, "scheduled_at")
}
func (r *RQLite) ListRetry(qname string, pgn base.Pagination) ([]base.Z, error) {
	return r.listEntries(qname, retry, pgn, "retry_at")
}
func (r *RQLite) ListArchived(qname string, pgn base.Pagination) ([]base.Z, error) {
	return r.listEntries(qname, archived, pgn, "archived_at")
}

func (r *RQLite) deleteTasks(qname, state string) (int64, error) {
	var op errors.Op = "rqlite.deleteTasks"
	err := r.queueMustExist(op, qname)
	if err != nil {
		return 0, err
	}
	conn, _ := r.Client()
	count, err := deleteTasks(conn, qname, state)
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return count, nil
}

func (r *RQLite) DeleteAllPendingTasks(qname string) (int64, error) {
	return r.deleteTasks(qname, pending)
}
func (r *RQLite) DeleteAllScheduledTasks(qname string) (int64, error) {
	return r.deleteTasks(qname, scheduled)
}
func (r *RQLite) DeleteAllRetryTasks(qname string) (int64, error) {
	return r.deleteTasks(qname, retry)
}
func (r *RQLite) DeleteAllArchivedTasks(qname string) (int64, error) {
	return r.deleteTasks(qname, archived)
}

func (r *RQLite) DeleteTask(qname string, taskid uuid.UUID) error {
	var op errors.Op = "rqlite.deleteTasks"
	err := r.queueMustExist(op, qname)
	if err != nil {
		return err
	}
	conn, _ := r.Client()
	count, err := deleteTask(conn, qname, taskid.String())
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}

	switch count {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: taskid.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, "cannot delete task in active state. use CancelTask instead.")
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from deleteTask: %d", count))
	}
}

func (r *RQLite) runAllTasks(qname string, state string) (int64, error) {
	var op errors.Op = "rqlite.runAllTasks"

	err := r.queueMustExist(op, qname)
	if err != nil {
		return 0, err
	}
	conn, _ := r.client(op)
	count, err := setPending(conn, qname, state)
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return count, nil
}

func (r *RQLite) RunAllScheduledTasks(qname string) (int64, error) {
	return r.runAllTasks(qname, scheduled)
}
func (r *RQLite) RunAllRetryTasks(qname string) (int64, error) {
	return r.runAllTasks(qname, retry)
}
func (r *RQLite) RunAllArchivedTasks(qname string) (int64, error) {
	return r.runAllTasks(qname, archived)
}

func (r *RQLite) RunTask(qname string, taskid uuid.UUID) error {
	var op errors.Op = "rqlite.runTask"

	err := r.queueMustExist(op, qname)
	if err != nil {
		return err
	}
	conn, _ := r.Client()

	count, err := setTaskPending(conn, qname, taskid.String())
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	switch count {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: taskid.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, "task is already running")
	case -2:
		return errors.E(op, errors.FailedPrecondition, "task is already in pending state")
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value %d", count))
	}
}

func (r *RQLite) archiveAllTasks(qname string, state string) (int64, error) {
	var op errors.Op = "rqlite.archiveAllTasks"

	err := r.queueMustExist(op, qname)
	if err != nil {
		return 0, err
	}
	conn, _ := r.Client()

	count, err := setArchived(conn, qname, state)
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return count, nil
}

func (r *RQLite) ArchiveAllPendingTasks(qname string) (int64, error) {
	return r.archiveAllTasks(qname, pending)
}
func (r *RQLite) ArchiveAllScheduledTasks(qname string) (int64, error) {
	return r.archiveAllTasks(qname, scheduled)
}
func (r *RQLite) ArchiveAllRetryTasks(qname string) (int64, error) {
	return r.archiveAllTasks(qname, retry)
}

func (r *RQLite) ArchiveTask(qname string, taskid uuid.UUID) error {
	var op errors.Op = "rqlite.archiveTasks"

	err := r.queueMustExist(op, qname)
	if err != nil {
		return err
	}
	conn, _ := r.Client()

	count, err := setTaskArchived(conn, qname, taskid.String())
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	switch count {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: taskid.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, &errors.TaskAlreadyArchivedError{Queue: qname, ID: taskid.String()})
	case -2:
		return errors.E(op, errors.FailedPrecondition, "cannot archive task in active state. use CancelTask instead.")
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from archiveTask: %d", count))
	}
}

func (r *RQLite) ListWorkers() ([]*base.WorkerInfo, error) {
	conn, err := r.client("rqlite.ListWorkers")
	if err != nil {
		return nil, err
	}

	rows, err := listAllWorkers(conn)
	if err != nil {
		return nil, err
	}
	ret := make([]*base.WorkerInfo, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.worker)
	}
	return ret, nil
}

func (r *RQLite) ClusterKeySlot(qname string) (int64, error) {
	// PENDING(GIL): TODO ?
	_ = qname
	return 0, nil
}

func (r *RQLite) ClusterNodes(qname string) ([]*base.ClusterNode, error) {
	// PENDING(GIL): TODO ?
	_ = qname
	return []*base.ClusterNode{}, nil
}

func (r *RQLite) ListSchedulerEntries() ([]*base.SchedulerEntry, error) {
	conn, err := r.client("rqlite.ListSchedulerEntries")
	if err != nil {
		return nil, err
	}

	rows, err := listSchedulerEntries(conn, "")
	if err != nil {
		return nil, err
	}
	ret := make([]*base.SchedulerEntry, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.entry)
	}

	return ret, nil
}

func (r *RQLite) ListSchedulerEnqueueEvents(entryID string, pgn base.Pagination) ([]*base.SchedulerEnqueueEvent, error) {
	conn, err := r.client("rqlite.ListSchedulerEnqueueEvents")
	if err != nil {
		return nil, err
	}

	rows, err := listSchedulerEnqueueEvents(conn, entryID, pgn)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	ret := make([]*base.SchedulerEnqueueEvent, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.event)
	}

	return ret, nil
}

func (r *RQLite) Purge(dropTables bool) error {
	var op errors.Op = "rqlite.Purge"
	conn, err := r.client(op)
	if err != nil {
		return err
	}
	if dropTables {
		return DropTables(conn)
	}
	return PurgeTables(conn)
}

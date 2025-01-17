// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/hibiken/asynq/internal/errors"
	pb "github.com/hibiken/asynq/internal/proto"
	"github.com/hibiken/asynq/internal/timeutil"
	"google.golang.org/protobuf/proto"
)

// Version of asynq library and CLI.
const Version = "0.19.1"

// DefaultQueueName is the queue name used if none are specified by user.
const DefaultQueueName = "default"

// DefaultQueue is the redis key for the default queue.
var DefaultQueue = PendingKey(DefaultQueueName)

// Global Redis keys.
const (
	AllServers    = "asynq:servers"    // ZSET
	AllWorkers    = "asynq:workers"    // ZSET
	AllSchedulers = "asynq:schedulers" // ZSET
	AllQueues     = "asynq:queues"     // SET
	CancelChannel = "asynq:cancel"     // PubSub channel
)

// TaskState denotes the state of a task.
type TaskState int

const (
	TaskStateActive TaskState = iota + 1
	TaskStatePending
	TaskStateScheduled
	TaskStateRetry
	TaskStateArchived
	TaskStateCompleted
	TaskStateProcessed
)

func (s TaskState) String() string {
	switch s {
	case TaskStateActive:
		return "active"
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRetry:
		return "retry"
	case TaskStateArchived:
		return "archived"
	case TaskStateCompleted:
		return "completed"
	}
	panic(fmt.Sprintf("internal error: unknown task state %d", s))
}

func TaskStateFromString(s string) (TaskState, error) {
	switch s {
	case "active":
		return TaskStateActive, nil
	case "pending":
		return TaskStatePending, nil
	case "scheduled":
		return TaskStateScheduled, nil
	case "retry":
		return TaskStateRetry, nil
	case "archived":
		return TaskStateArchived, nil
	case "completed":
		return TaskStateCompleted, nil
	}
	return 0, errors.E(errors.FailedPrecondition, fmt.Sprintf("%q is not supported task state", s))
}

// ValidateQueueName validates a given qname to be used as a queue name.
// Returns nil if valid, otherwise returns non-nil error.
func ValidateQueueName(qname string) error {
	if len(strings.TrimSpace(qname)) == 0 {
		return fmt.Errorf("queue name must contain one or more characters")
	}
	return nil
}

// QueueKeyPrefix returns a prefix for all keys in the given queue.
func QueueKeyPrefix(qname string) string {
	return fmt.Sprintf("asynq:{%s}:", qname)
}

// TaskKeyPrefix returns a prefix for task key.
func TaskKeyPrefix(qname string) string {
	return fmt.Sprintf("%st:", QueueKeyPrefix(qname))
}

// TaskKey returns a redis key for the given task message.
func TaskKey(qname, id string) string {
	return fmt.Sprintf("%s%s", TaskKeyPrefix(qname), id)
}

// PendingKey returns a redis key for the given queue name.
func PendingKey(qname string) string {
	return fmt.Sprintf("%spending", QueueKeyPrefix(qname))
}

// ActiveKey returns a redis key for the active tasks.
func ActiveKey(qname string) string {
	return fmt.Sprintf("%sactive", QueueKeyPrefix(qname))
}

// ScheduledKey returns a redis key for the scheduled tasks.
func ScheduledKey(qname string) string {
	return fmt.Sprintf("%sscheduled", QueueKeyPrefix(qname))
}

// RetryKey returns a redis key for the retry tasks.
func RetryKey(qname string) string {
	return fmt.Sprintf("%sretry", QueueKeyPrefix(qname))
}

// ArchivedKey returns a redis key for the archived tasks.
func ArchivedKey(qname string) string {
	return fmt.Sprintf("%sarchived", QueueKeyPrefix(qname))
}

// DeadlinesKey returns a redis key for the deadlines.
func DeadlinesKey(qname string) string {
	return fmt.Sprintf("%sdeadlines", QueueKeyPrefix(qname))
}

func CompletedKey(qname string) string {
	return fmt.Sprintf("%scompleted", QueueKeyPrefix(qname))
}

// PausedKey returns a redis key to indicate that the given queue is paused.
func PausedKey(qname string) string {
	return fmt.Sprintf("%spaused", QueueKeyPrefix(qname))
}

// ProcessedKey returns a redis key for processed count for the given day for the queue.
func ProcessedKey(qname string, t time.Time) string {
	return fmt.Sprintf("%sprocessed:%s", QueueKeyPrefix(qname), t.UTC().Format("2006-01-02"))
}

// FailedKey returns a redis key for failure count for the given day for the queue.
func FailedKey(qname string, t time.Time) string {
	return fmt.Sprintf("%sfailed:%s", QueueKeyPrefix(qname), t.UTC().Format("2006-01-02"))
}

// ServerInfoKey returns a redis key for process info.
func ServerInfoKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:servers:{%s:%d:%s}", hostname, pid, serverID)
}

// WorkersKey returns a redis key for the workers given hostname, pid, and server ID.
func WorkersKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:workers:{%s:%d:%s}", hostname, pid, serverID)
}

// SchedulerEntriesKey returns a redis key for the scheduler entries given scheduler ID.
func SchedulerEntriesKey(schedulerID string) string {
	return fmt.Sprintf("asynq:schedulers:{%s}", schedulerID)
}

// SchedulerHistoryKey returns a redis key for the scheduler's history for the given entry.
func SchedulerHistoryKey(entryID string) string {
	return fmt.Sprintf("asynq:scheduler_history:%s", entryID)
}

// UniqueKey returns a redis key with the given type, payload, and queue name.
func UniqueKey(qname, tasktype string, payload []byte) string {
	if payload == nil {
		return fmt.Sprintf("%sunique:%s:", QueueKeyPrefix(qname), tasktype)
	}
	checksum := md5.Sum(payload)
	return fmt.Sprintf("%sunique:%s:%s", QueueKeyPrefix(qname), tasktype, hex.EncodeToString(checksum[:]))
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type TaskMessage struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload []byte

	// ID is a unique identifier for each task.
	ID string

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// ErrorMsg holds the error message from the last failure.
	ErrorMsg string

	// Time of last failure in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no last failure
	LastFailedAt int64

	// Timeout specifies timeout in seconds.
	// If task processing doesn't complete within the timeout, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no timeout.
	Timeout int64

	// Deadline specifies the deadline for the task in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	// If task processing doesn't complete before the deadline, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no deadline.
	Deadline int64

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string

	// TTL of the unique key in seconds
	UniqueKeyTTL int64

	// Recurrent indicates a recurrent task when true
	Recurrent bool

	// Delay in seconds to re-process a recurrent task after execution.
	ReprocessAfter int64

	// ServerAffinity is used with a recurrent task to specify a timeout after
	// which the task can be handled by a server other than the one that handled
	// it the first time.
	ServerAffinity int64

	// Retention specifies the number of seconds the task should be retained after completion.
	Retention int64

	// CompletedAt is the time the task was processed successfully in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no value.
	CompletedAt int64
}

type MessageBatch struct {
	InputIndex  int
	Msg         *TaskMessage
	UniqueTTL   time.Duration
	ForceUnique bool
	ProcessAt   time.Time
	State       TaskState
	Err         error
}

// EncodeMessage marshals the given task message and returns an encoded bytes.
func EncodeMessage(msg *TaskMessage) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot encode nil message")
	}
	recurrent := int64(0)
	if msg.Recurrent {
		recurrent = 1
	}
	return proto.Marshal(&pb.TaskMessage{
		Type:             msg.Type,
		Payload:          msg.Payload,
		Id:               msg.ID,
		Queue:            msg.Queue,
		Retry:            int32(msg.Retry),
		Retried:          int32(msg.Retried),
		ErrorMsg:         msg.ErrorMsg,
		LastFailedAt:     msg.LastFailedAt,
		Timeout:          msg.Timeout,
		Deadline:         msg.Deadline,
		UniqueKey:        msg.UniqueKey,
		UniqueKeyTimeout: msg.UniqueKeyTTL,
		Recurrent:        recurrent,
		ReprocessAfter:   msg.ReprocessAfter,
		ServerAffinity:   msg.ServerAffinity,
		Retention:        msg.Retention,
		CompletedAt:      msg.CompletedAt,
	})
}

// DecodeMessage unmarshals the given bytes and returns a decoded task message.
func DecodeMessage(data []byte) (*TaskMessage, error) {
	var pbmsg pb.TaskMessage
	if err := proto.Unmarshal(data, &pbmsg); err != nil {
		return nil, err
	}
	return &TaskMessage{
		Type:           pbmsg.GetType(),
		Payload:        pbmsg.GetPayload(),
		ID:             pbmsg.GetId(),
		Queue:          pbmsg.GetQueue(),
		Retry:          int(pbmsg.GetRetry()),
		Retried:        int(pbmsg.GetRetried()),
		ErrorMsg:       pbmsg.GetErrorMsg(),
		LastFailedAt:   pbmsg.GetLastFailedAt(),
		Timeout:        pbmsg.GetTimeout(),
		Deadline:       pbmsg.GetDeadline(),
		UniqueKey:      pbmsg.GetUniqueKey(),
		UniqueKeyTTL:   pbmsg.GetUniqueKeyTimeout(),
		Recurrent:      pbmsg.GetRecurrent() > 0,
		ReprocessAfter: pbmsg.GetReprocessAfter(),
		ServerAffinity: pbmsg.GetServerAffinity(),
		Retention:      pbmsg.GetRetention(),
		CompletedAt:    pbmsg.GetCompletedAt(),
	}, nil
}

// TaskInfo describes a task message and its metadata.
type TaskInfo struct {
	Message       *TaskMessage
	State         TaskState
	NextProcessAt time.Time
	Result        []byte
}

// Z represents sorted set member.
type Z struct {
	Message *TaskMessage
	Score   int64
}

// ServerState represents state of a server.
// ServerState methods are concurrency safe.
type ServerState struct {
	mu  sync.Mutex
	val ServerStateValue
}

// NewServerState returns a new state instance.
// Initial state is set to StateNew.
func NewServerState() *ServerState {
	return &ServerState{val: StateNew}
}

type ServerStateValue int

const (
	// StateNew represents a new server. Server begins in
	// this state and then transition to StatusActive when
	// Start or Run is callled.
	StateNew ServerStateValue = iota

	// StateActive indicates the server is up and active.
	StateActive

	// StateStopped indicates the server is up but no longer processing new tasks.
	StateStopped

	// StateClosed indicates the server has been shutdown.
	StateClosed
)

var serverStates = []string{
	"new",
	"active",
	"stopped",
	"closed",
}

func (s *ServerState) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if StateNew <= s.val && s.val <= StateClosed {
		return serverStates[s.val]
	}
	return "unknown status"
}

// Get returns the status value.
func (s *ServerState) Get() ServerStateValue {
	s.mu.Lock()
	v := s.val
	s.mu.Unlock()
	return v
}

// Set sets the status value.
func (s *ServerState) Set(v ServerStateValue) {
	s.mu.Lock()
	s.val = v
	s.mu.Unlock()
}

// ServerInfo holds information about a running server.
type ServerInfo struct {
	Host              string
	PID               int
	ServerID          string
	Concurrency       int
	Queues            map[string]int
	StrictPriority    bool
	Status            string
	Started           time.Time
	ActiveWorkerCount int
}

// EncodeServerInfo marshals the given ServerInfo and returns the encoded bytes.
func EncodeServerInfo(info *ServerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil server info")
	}
	queues := make(map[string]int32)
	for q, p := range info.Queues {
		queues[q] = int32(p)
	}
	started, err := ptypes.TimestampProto(info.Started)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.ServerInfo{
		Host:              info.Host,
		Pid:               int32(info.PID),
		ServerId:          info.ServerID,
		Concurrency:       int32(info.Concurrency),
		Queues:            queues,
		StrictPriority:    info.StrictPriority,
		Status:            info.Status,
		StartTime:         started,
		ActiveWorkerCount: int32(info.ActiveWorkerCount),
	})
}

// DecodeServerInfo decodes the given bytes into ServerInfo.
func DecodeServerInfo(b []byte) (*ServerInfo, error) {
	var pbmsg pb.ServerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	queues := make(map[string]int)
	for q, p := range pbmsg.GetQueues() {
		queues[q] = int(p)
	}
	startTime, err := ptypes.Timestamp(pbmsg.GetStartTime())
	if err != nil {
		return nil, err
	}
	return &ServerInfo{
		Host:              pbmsg.GetHost(),
		PID:               int(pbmsg.GetPid()),
		ServerID:          pbmsg.GetServerId(),
		Concurrency:       int(pbmsg.GetConcurrency()),
		Queues:            queues,
		StrictPriority:    pbmsg.GetStrictPriority(),
		Status:            pbmsg.GetStatus(),
		Started:           startTime,
		ActiveWorkerCount: int(pbmsg.GetActiveWorkerCount()),
	}, nil
}

// WorkerInfo holds information about a running worker.
type WorkerInfo struct {
	Host     string
	PID      int
	ServerID string
	ID       string
	Type     string
	Payload  []byte
	Queue    string
	Started  time.Time
	Deadline time.Time
}

// EncodeWorkerInfo marshals the given WorkerInfo and returns the encoded bytes.
func EncodeWorkerInfo(info *WorkerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil worker info")
	}
	startTime, err := ptypes.TimestampProto(info.Started)
	if err != nil {
		return nil, err
	}
	deadline, err := ptypes.TimestampProto(info.Deadline)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.WorkerInfo{
		Host:        info.Host,
		Pid:         int32(info.PID),
		ServerId:    info.ServerID,
		TaskId:      info.ID,
		TaskType:    info.Type,
		TaskPayload: info.Payload,
		Queue:       info.Queue,
		StartTime:   startTime,
		Deadline:    deadline,
	})
}

// DecodeWorkerInfo decodes the given bytes into WorkerInfo.
func DecodeWorkerInfo(b []byte) (*WorkerInfo, error) {
	var pbmsg pb.WorkerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	startTime, err := ptypes.Timestamp(pbmsg.GetStartTime())
	if err != nil {
		return nil, err
	}
	deadline, err := ptypes.Timestamp(pbmsg.GetDeadline())
	if err != nil {
		return nil, err
	}
	return &WorkerInfo{
		Host:     pbmsg.GetHost(),
		PID:      int(pbmsg.GetPid()),
		ServerID: pbmsg.GetServerId(),
		ID:       pbmsg.GetTaskId(),
		Type:     pbmsg.GetTaskType(),
		Payload:  pbmsg.GetTaskPayload(),
		Queue:    pbmsg.GetQueue(),
		Started:  startTime,
		Deadline: deadline,
	}, nil
}

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Type is the task type of the periodic task.
	Type string

	// Payload is the payload of the periodic task.
	Payload []byte

	// Opts is the options for the periodic task.
	Opts []string

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// EncodeSchedulerEntry marshals the given entry and returns an encoded bytes.
func EncodeSchedulerEntry(entry *SchedulerEntry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("cannot encode nil scheduler entry")
	}
	next, err := ptypes.TimestampProto(entry.Next)
	if err != nil {
		return nil, err
	}
	prev, err := ptypes.TimestampProto(entry.Prev)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.SchedulerEntry{
		Id:              entry.ID,
		Spec:            entry.Spec,
		TaskType:        entry.Type,
		TaskPayload:     entry.Payload,
		EnqueueOptions:  entry.Opts,
		NextEnqueueTime: next,
		PrevEnqueueTime: prev,
	})
}

// DecodeSchedulerEntry unmarshals the given bytes and returns a decoded SchedulerEntry.
func DecodeSchedulerEntry(b []byte) (*SchedulerEntry, error) {
	var pbmsg pb.SchedulerEntry
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	next, err := ptypes.Timestamp(pbmsg.GetNextEnqueueTime())
	if err != nil {
		return nil, err
	}
	prev, err := ptypes.Timestamp(pbmsg.GetPrevEnqueueTime())
	if err != nil {
		return nil, err
	}
	return &SchedulerEntry{
		ID:      pbmsg.GetId(),
		Spec:    pbmsg.GetSpec(),
		Type:    pbmsg.GetTaskType(),
		Payload: pbmsg.GetTaskPayload(),
		Opts:    pbmsg.GetEnqueueOptions(),
		Next:    next,
		Prev:    prev,
	}, nil
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// EncodeSchedulerEnqueueEvent marshals the given event
// and returns an encoded bytes.
func EncodeSchedulerEnqueueEvent(event *SchedulerEnqueueEvent) ([]byte, error) {
	if event == nil {
		return nil, fmt.Errorf("cannot encode nil enqueue event")
	}
	enqueuedAt, err := ptypes.TimestampProto(event.EnqueuedAt)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.SchedulerEnqueueEvent{
		TaskId:      event.TaskID,
		EnqueueTime: enqueuedAt,
	})
}

// DecodeSchedulerEnqueueEvent unmarshals the given bytes
// and returns a decoded SchedulerEnqueueEvent.
func DecodeSchedulerEnqueueEvent(b []byte) (*SchedulerEnqueueEvent, error) {
	var pbmsg pb.SchedulerEnqueueEvent
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	enqueuedAt, err := ptypes.Timestamp(pbmsg.GetEnqueueTime())
	if err != nil {
		return nil, err
	}
	return &SchedulerEnqueueEvent{
		TaskID:     pbmsg.GetTaskId(),
		EnqueuedAt: enqueuedAt,
	}, nil
}

// Deadlines is a collection that holds deadlines for all active tasks.
// When a deadline is reached, the associated cancel function is executed.
// Assumes that deadlines have unique IDs and will not be added multiples times.
//
// Deadlines is safe for concurrent use by multiple goroutines.
type Deadlines struct {
	update chan deadline
}

// NewDeadlines returns a Deadlines instance.
func NewDeadlines(abort chan struct{}, size int) *Deadlines {
	d := &Deadlines{update: make(chan deadline, size)}
	go func() {
		dls := []deadline{}
		t := time.NewTimer(time.Hour) // dummy duration
		defer stopTimer(t)
		for {
			if func() bool {
				if len(dls) == 0 {
					stopTimer(t)
				} else {
					resetTimer(t, time.Until(dls[0].dl))
				}
				select {
				case <-abort:
					return true
				case dl := <-d.update:
					if dl.fn != nil {
						dls = insertDeadline(dls, dl)
					} else {
						dls = removeDeadline(dls, dl)
					}
				case <-t.C:
					dl := dls[0]
					dls = removeDeadline(dls, dl)
					dl.fn()
				}
				return false
			}() {
				return
			}
		}
	}()
	return d
}

// Add adds a new cancel func to the collection.
func (d *Deadlines) Add(id string, dl time.Time, fn context.CancelFunc) {
	d.update <- deadline{id: id, dl: dl, fn: fn}
}

// Delete deletes a cancel func from the collection given an id.
func (d *Deadlines) Delete(id string) {
	d.update <- deadline{id: id, fn: nil}
}

func stopTimer(t *time.Timer) {
	t.Stop()
	// Drain timer channel, if needed
	select {
	case <-t.C:
	default:
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

type deadline struct {
	id string
	dl time.Time
	fn context.CancelFunc
}

// insertDeadline inserts a deadline into a given sorted list of deadlines.
func insertDeadline(dls []deadline, dl deadline) []deadline {
	i := sort.Search(len(dls), func(i int) bool {
		return dls[i].dl.After(dl.dl)
	})
	res := append(dls, dl)
	copy(res[i+1:], res[i:])
	res[i] = dl
	return res
}

// removeDeadline removes a deadline (by ID) from a given sorted list of deadlines.
func removeDeadline(dls []deadline, dl deadline) []deadline {
	for i, dl2 := range dls {
		if dl2.id == dl.id {
			return append(dls[:i], dls[i+1:]...)
		}
	}
	return dls
}

// Cancelations is a collection that holds cancel functions for all active tasks.
//
// Cancelations is safe for concurrent use by multiple goroutines.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations returns a Cancelations instance.
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add adds a new cancel func to the collection.
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Get returns a cancel func given an id.
func (c *Cancelations) Get(id string) (fn context.CancelFunc, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.cancelFuncs[id]
	return fn, ok
}

type PubSub interface {
	Close() error
	Channel() <-chan interface{}
}

// Broker is a message broker that supports operations to manage task queues.
//
// See rdb.RDB as a reference implementation.
type Broker interface {
	// Ping checks the connection with store server.
	Ping() error

	// SetClock sets the clock used by RDB to the given clock.
	//
	// Use this function to set the clock to SimulatedClock in tests.
	SetClock(c timeutil.Clock)

	// Enqueue adds the given task to the pending list of the queue.
	Enqueue(ctx context.Context, msg *TaskMessage) error
	// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
	// It returns ErrDuplicateTask if the lock cannot be acquired.
	EnqueueUnique(ctx context.Context, msg *TaskMessage, ttl time.Duration, forceUnique ...bool) error
	// Dequeue queries given queues in order and pops a task message
	// off a queue if one exists and returns the message and deadline.
	// Dequeue skips a queue if the queue is paused.
	// If all queues are empty, ErrNoProcessableTask error is returned.
	// - qnames are the queues to process
	// - serverID is used the ID of the processor/server processing Dequeue and
	//   is used to select tasks with server affinity.
	Dequeue(serverID string, qnames ...string) (*TaskMessage, time.Time, error)
	// Done removes the task from active queue to mark the task as done.
	// It removes a uniqueness lock acquired by the task, if any.
	// ServerID is the ID of the server that processed the task (used for server affinity)
	Done(serverID string, msg *TaskMessage) error
	// MarkAsComplete marks the task as completed
	MarkAsComplete(serverID string, msg *TaskMessage) error
	// Requeue moves the task from active queue to the specified queue.
	// ServerID is the ID of the server that processed the task (used for server affinity)
	// aborted is true when re-queuing occurs because the server stops and false for recurrent tasks
	Requeue(serverID string, msg *TaskMessage, aborted bool) error
	// Schedule adds the task to the scheduled set to be processed in the future.
	Schedule(ctx context.Context, msg *TaskMessage, processAt time.Time) error
	// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
	// It returns ErrDuplicateTask if the lock cannot be acquired.
	ScheduleUnique(ctx context.Context, msg *TaskMessage, processAt time.Time, ttl time.Duration, forceUnique ...bool) error
	// Retry moves the task from active to retry queue.
	// It also annotates the message with the given error message and
	// if isFailure is true increments the retried counter.
	Retry(msg *TaskMessage, processAt time.Time, errMsg string, isFailure bool) error
	// Archive sends the given task to archive, attaching the error message to the task.
	// It also trims the archive by timestamp and set size.
	Archive(msg *TaskMessage, errMsg string) error
	// ForwardIfReady checks scheduled and retry sets of the given queues
	// and move any tasks that are ready to be processed to the pending set.
	ForwardIfReady(qnames ...string) error
	// DeleteExpiredCompletedTasks checks for any expired tasks in the given queue's completed set,
	// and delete all expired tasks.
	DeleteExpiredCompletedTasks(qname string) error
	// ListDeadlineExceeded returns a list of task messages that have exceeded the deadline from the given queues.
	ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*TaskMessage, error)
	// WriteServerState writes server state data to the store with expiration set to the value ttl.
	WriteServerState(info *ServerInfo, workers []*WorkerInfo, ttl time.Duration) error
	// ClearServerState deletes server state data from the store.
	ClearServerState(host string, pid int, serverID string) error
	// CancelationPubSub returns a pubsub for cancelation messages.
	CancelationPubSub() (PubSub, error)
	// PublishCancelation publish cancelation message to all subscribers.
	// The message is the ID for the task to be canceled.
	PublishCancelation(id string) error
	// WriteResult
	WriteResult(qname, taskID string, data []byte) (n int, err error)
	// Close closes the connection with the store server.
	Close() error
	// ListServers returns the list of server info.
	ListServers() ([]*ServerInfo, error)
	// Inspector returns an Inspector instance based on this Broker or nil.
	Inspector() Inspector

	// EnqueueBatch adds the given tasks to the pending list of the queue.
	EnqueueBatch(ctx context.Context, msgs ...*MessageBatch) error

	// EnqueueUniqueBatch inserts the given tasks if the task's uniqueness lock can be acquired.
	// It returns ErrDuplicateTask if the lock cannot be acquired.
	EnqueueUniqueBatch(ctx context.Context, msgs ...*MessageBatch) error

	// ScheduleBatch adds the tasks to the scheduled set to be processed in the future.
	ScheduleBatch(ctx context.Context, msgs ...*MessageBatch) error

	// ScheduleUniqueBatch adds the tasks to the backlog queue to be processed in the future
	// if the uniqueness lock can be acquired.
	// It returns ErrDuplicateTask if the lock cannot be acquired.
	ScheduleUniqueBatch(ctx context.Context, msgs ...*MessageBatch) error
}

type Scheduler interface {
	Broker
	ClearSchedulerEntries(schedulerID string) error
	RecordSchedulerEnqueueEvent(entryID string, event *SchedulerEnqueueEvent) error
	WriteSchedulerEntries(schedulerID string, entries []*SchedulerEntry, ttl time.Duration) error
	ClearSchedulerHistory(entryID string) error
}

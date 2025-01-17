// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/robfig/cron/v3"
)

// A Scheduler kicks off tasks at regular intervals based on the user defined schedule.
//
// Schedulers are safe for concurrent use by multiple goroutines.
type Scheduler struct {
	id         string
	state      *base.ServerState
	logger     *log.Logger
	client     *Client
	rdb        base.Scheduler
	cron       *cron.Cron
	location   *time.Location
	done       chan struct{}
	wg         sync.WaitGroup
	errHandler func(task *Task, opts []Option, err error)

	// guards idmap
	mu sync.Mutex
	// idmap maps Scheduler's entry ID to cron.EntryID
	// to avoid using cron.EntryID as the public API of
	// the Scheduler.
	idmap map[string]cron.EntryID
}

// NewScheduler returns a new Scheduler instance given the broker connection option.
// The parameter opts is optional, defaults will be used if opts is set to nil
func NewScheduler(r ClientConnOpt, opts *SchedulerOpts) *Scheduler {
	broker, err := makeBroker(r)
	if err != nil {
		panic(err)
	}
	scheduler, ok := broker.(base.Scheduler)
	if !ok {
		panic(errors.E(errors.Op("NewScheduler"), errors.Internal, fmt.Sprintf("expecting a Scheduler, got %T", broker)))
	}
	return newScheduler(scheduler, opts)
}

// NewSchedulerFrom returns a new Scheduler instance that will share the broker
// connection with the given server instance.
// The parameter opts is optional, defaults will be used if opts is set to nil
func NewSchedulerFrom(s *Server, opts *SchedulerOpts) (*Scheduler, error) {
	op := errors.Op("NewSchedulerFrom")
	if s == nil {
		return nil, errors.E(op, errors.Internal, "server is nil")
	}
	broker := s.broker
	scheduler, ok := broker.(base.Scheduler)
	if !ok {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("expecting a Scheduler, got %T", broker))
	}
	return newScheduler(scheduler, opts), nil
}

func newScheduler(scheduler base.Scheduler, opts *SchedulerOpts) *Scheduler {

	if opts == nil {
		opts = &SchedulerOpts{}
	}

	logger := log.NewLogger(opts.Logger)
	loglevel := opts.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))

	loc := opts.Location
	if loc == nil {
		loc = time.UTC
	}

	return &Scheduler{
		id:         generateSchedulerID(),
		state:      base.NewServerState(),
		logger:     logger,
		client:     NewClientWithBroker(scheduler),
		rdb:        scheduler,
		cron:       cron.New(cron.WithLocation(loc)),
		location:   loc,
		done:       make(chan struct{}),
		errHandler: opts.EnqueueErrorHandler,
		idmap:      make(map[string]cron.EntryID),
	}
}

func generateSchedulerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%v", host, os.Getpid(), uuid.New())
}

// SchedulerOpts specifies scheduler options.
type SchedulerOpts struct {
	// Logger specifies the logger used by the scheduler instance.
	//
	// If unset, the default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// Location specifies the time zone location.
	//
	// If unset, the UTC time zone (time.UTC) is used.
	Location *time.Location

	// EnqueueErrorHandler gets called when scheduler cannot enqueue a registered task
	// due to an error.
	EnqueueErrorHandler func(task *Task, opts []Option, err error)
}

// enqueueJob encapsulates the job of enqueing a task and recording the event.
type enqueueJob struct {
	id         uuid.UUID
	cronspec   string
	task       *Task
	opts       []Option
	location   *time.Location
	logger     *log.Logger
	client     *Client
	rdb        base.Scheduler
	errHandler func(task *Task, opts []Option, err error)
}

func (j *enqueueJob) Run() {
	info, err := j.client.Enqueue(j.task, j.opts...)
	if err != nil {
		j.logger.Errorf("scheduler could not enqueue a task %+v: %v", j.task, err)
		if j.errHandler != nil {
			j.errHandler(j.task, j.opts, err)
		}
		return
	}
	j.logger.Debugf("scheduler enqueued a task: %+v", info)
	event := &base.SchedulerEnqueueEvent{
		TaskID:     info.ID,
		EnqueuedAt: time.Now().In(j.location),
	}
	err = j.rdb.RecordSchedulerEnqueueEvent(j.id.String(), event)
	if err != nil {
		j.logger.Errorf("scheduler could not record enqueue event of enqueued task %+v: %v", j.task, err)
	}
}

// Register registers a task to be enqueued on the given schedule specified by the cronspec.
// It returns an ID of the newly registered entry.
func (s *Scheduler) Register(cronspec string, task *Task, opts ...Option) (entryID string, err error) {
	job := &enqueueJob{
		id:         uuid.New(),
		cronspec:   cronspec,
		task:       task,
		opts:       opts,
		location:   s.location,
		client:     s.client,
		rdb:        s.rdb,
		logger:     s.logger,
		errHandler: s.errHandler,
	}
	cronID, err := s.cron.AddJob(cronspec, job)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.idmap[job.id.String()] = cronID
	s.mu.Unlock()
	return job.id.String(), nil
}

// Unregister removes a registered entry by entry ID.
// Unregister returns a non-nil error if no entries were found for the given entryID.
func (s *Scheduler) Unregister(entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cronID, ok := s.idmap[entryID]
	if !ok {
		return fmt.Errorf("asynq: no scheduler entry found")
	}
	delete(s.idmap, entryID)
	s.cron.Remove(cronID)
	return nil
}

// Run starts the scheduler until an os signal to exit the program is received.
// It returns an error if scheduler is already running or has been shutdown.
func (s *Scheduler) Run() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.waitForSignals()
	s.Shutdown()
	return nil
}

// Start starts the scheduler.
// It returns an error if the scheduler is already running or has been shutdown.
func (s *Scheduler) Start() error {
	switch s.state.Get() {
	case base.StateActive:
		return fmt.Errorf("asynq: the scheduler is already running")
	case base.StateClosed:
		return fmt.Errorf("asynq: the scheduler has already been stopped")
	}
	s.logger.Info("Scheduler starting")
	s.logger.Infof("Scheduler timezone is set to %v", s.location)
	s.cron.Start()
	s.wg.Add(1)
	go s.runHeartbeater()
	s.state.Set(base.StateActive)
	return nil
}

// Shutdown stops and shuts down the scheduler.
func (s *Scheduler) Shutdown() {
	s.logger.Info("Scheduler shutting down")
	close(s.done) // signal heartbeater to stop
	ctx := s.cron.Stop()
	<-ctx.Done()
	s.wg.Wait()

	s.clearHistory()
	_ = s.client.Close()
	_ = s.rdb.Close()
	s.state.Set(base.StateClosed)
	s.logger.Info("Scheduler stopped")
}

func (s *Scheduler) runHeartbeater() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-s.done:
			s.logger.Debugf("Scheduler heatbeater shutting down")
			_ = s.rdb.ClearSchedulerEntries(s.id)
			return
		case <-ticker.C:
			s.beat()
		}
	}
}

// beat writes a snapshot of entries to redis.
func (s *Scheduler) beat() {
	var entries []*base.SchedulerEntry
	for _, entry := range s.cron.Entries() {
		job := entry.Job.(*enqueueJob)
		e := &base.SchedulerEntry{
			ID:      job.id.String(),
			Spec:    job.cronspec,
			Type:    job.task.Type(),
			Payload: job.task.Payload(),
			Opts:    stringifyOptions(job.opts),
			Next:    entry.Next,
			Prev:    entry.Prev,
		}
		entries = append(entries, e)
	}
	s.logger.Debugf("Writing entries %v", entries)
	if err := s.rdb.WriteSchedulerEntries(s.id, entries, 5*time.Second); err != nil {
		s.logger.Warnf("Scheduler could not write heartbeat data: %v", err)
	}
}

func stringifyOptions(opts []Option) []string {
	var res []string
	for _, opt := range opts {
		res = append(res, opt.String())
	}
	return res
}

func (s *Scheduler) clearHistory() {
	for _, entry := range s.cron.Entries() {
		job := entry.Job.(*enqueueJob)
		if err := s.rdb.ClearSchedulerHistory(job.id.String()); err != nil {
			s.logger.Warnf("Could not clear scheduler history for entry %q: %v", job.id.String(), err)
		}
	}
}

// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	aserrors "github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
)

// Server is responsible for task processing and task lifecycle management.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will schedule it for a retry.
//
// A task will be retried until either the task gets processed successfully
// or until it reaches its max retry count.
//
// If a task exhausts its retries, it will be moved to the archive and
// will be kept in the archive set.
// Note that the archive size is finite and once it reaches its max size,
// oldest tasks in the archive will be deleted.
type Server struct {
	logger *log.Logger

	broker base.Broker

	deadlines *base.Deadlines

	state *base.ServerState

	// wait group to wait for all goroutines to finish.
	wg            sync.WaitGroup
	forwarder     *forwarder
	processor     *processor
	syncer        *syncer
	heartbeater   *heartbeater
	subscriber    *subscriber
	recoverer     *recoverer
	healthchecker *healthchecker
	janitor       *janitor
}

// Config specifies the server's background-task processing behavior.
type Config struct {
	// server identifier. If not provided a random UUID is used.
	ServerID string

	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the currennt process.
	Concurrency int

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	RetryDelayFunc RetryDelayHandler

	// Predicate function to determine whether the error returned from Handler is a failure.
	// If the function returns false, Server will not increment the retried counter for the task,
	// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
	// rate of the queue.
	//
	// By default, if the given error is non-nil the function returns true.
	IsFailure func(error) bool

	// Queues is the list of queues to process with given priority value
	// If nil the server will process only the "default" queue
	Queues Queues

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	//
	//     func reportError(ctx context, task *asynq.Task, err error) {
	//         retried, _ := asynq.GetRetryCount(ctx)
	//         maxRetry, _ := asynq.GetMaxRetry(ctx)
	//     	   if retried >= maxRetry {
	//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	//     	   }
	//         errorReportingService.Notify(err)
	//     })
	//
	//     ErrorHandler: asynq.ErrorHandlerFunc(reportError)
	ErrorHandler ErrorHandler

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration

	// HealthCheckFunc is called periodically with any errors encountered during ping to the
	// connected redis server.
	HealthCheckFunc func(error)

	// HealthCheckInterval specifies the interval between healthchecks.
	//
	// If unset or zero, the interval is set to 15 seconds.
	HealthCheckInterval time.Duration

	// ProcessorEmptyQSleep specifies the wait period when empty queues
	// Default to 1 second
	ProcessorEmptyQSleep time.Duration

	// RecovererInterval specifies the recoverer polling period
	// Default to 1 minute
	RecovererInterval time.Duration

	// RecovererExpiration specifies the amount of time that must have elapsed
	// after a task deadline in order to retry or archive it.
	// Default to 30 seconds
	RecovererExpiration time.Duration

	// ForwarderInterval specifies the forwarder polling period
	// Default to 5 seconds
	ForwarderInterval time.Duration

	// HeartBeaterInterval specifies the heart beater polling period
	// Default to 5 seconds
	HeartBeaterInterval time.Duration

	// SyncerInterval specifies the syncer polling period
	// Default to 5 seconds
	SyncerInterval time.Duration

	// SubscriberRetryTimeout defines the timeout to retry connecting to the
	// cancellation pubsub channel.
	// Default to 5 second
	SubscriberRetryTimeout time.Duration

	// JanitorInterval specifies the janitor polling period
	// Default to 8 seconds
	JanitorInterval time.Duration
}

// An ErrorHandler handles an error that occurred during task processing.
type ErrorHandler interface {
	HandleError(ctx context.Context, task *Task, err error)
}

// The ErrorHandlerFunc type is an adapter to allow the use of  ordinary functions as a ErrorHandler.
// If f is a function with the appropriate signature, ErrorHandlerFunc(f) is a ErrorHandler that calls f.
type ErrorHandlerFunc func(ctx context.Context, task *Task, err error)

// HandleError calls fn(ctx, task, err)
func (fn ErrorHandlerFunc) HandleError(ctx context.Context, task *Task, err error) {
	fn(ctx, task, err)
}

type RetryDelayHandler interface {
	RetryDelay(n int, e error, t *Task) time.Duration
}

// RetryDelayFunc calculates the retry delay duration for a failed task given
// the retry count, error, and the task.
//
// n is the number of times the task has been retried.
// e is the error returned by the task handler.
// t is the task in question.
type RetryDelayFunc func(n int, e error, t *Task) time.Duration

func (fn RetryDelayFunc) RetryDelay(n int, e error, t *Task) time.Duration {
	return fn(n, e, t)
}

// Logger supports logging at various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})
}

// LogLevel represents logging level.
//
// It satisfies flag.Value interface.
type LogLevel int32

const (
	// Note: reserving value zero to differentiate unspecified case.
	level_unspecified LogLevel = iota

	// DebugLevel is the lowest level of logging.
	// Debug logs are intended for debugging and development purposes.
	DebugLevel

	// InfoLevel is used for general informational log messages.
	InfoLevel

	// WarnLevel is used for undesired but relatively expected events,
	// which may indicate a problem.
	WarnLevel

	// ErrorLevel is used for undesired and unexpected events that
	// the program can recover from.
	ErrorLevel

	// FatalLevel is used for undesired and unexpected events that
	// the program cannot recover from.
	FatalLevel
)

// String is part of the flag.Value interface.
func (l *LogLevel) String() string {
	switch *l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", *l))
}

// Set is part of the flag.Value interface.
func (l *LogLevel) Set(val string) error {
	switch strings.ToLower(val) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn", "warning":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("asynq: unsupported log level %q", val)
	}
	return nil
}

func toInternalLogLevel(l LogLevel) log.Level {
	switch l {
	case DebugLevel:
		return log.DebugLevel
	case InfoLevel:
		return log.InfoLevel
	case WarnLevel:
		return log.WarnLevel
	case ErrorLevel:
		return log.ErrorLevel
	case FatalLevel:
		return log.FatalLevel
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", l))
}

// DefaultRetryDelayFunc is the default RetryDelayFunc used if one is not specified in Config.
// It uses exponential back-off strategy to calculate the retry delay.
func DefaultRetryDelayFunc(n int, e error, t *Task) time.Duration {
	_ = e
	_ = t
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Formula taken from https://github.com/mperham/sidekiq.
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

var DefaultRetryDelay = RetryDelayFunc(DefaultRetryDelayFunc)

func defaultIsFailureFunc(err error) bool { return err != nil }

const (
	defaultShutdownTimeout     = 8 * time.Second
	defaultHealthCheckInterval = 15 * time.Second
)

// NewServer returns a new Server given a connection option and server configuration.
func NewServer(r ClientConnOpt, cfg Config) *Server {
	broker, err := makeBroker(r)
	if err != nil {
		panic(err)
	}
	return newServer(broker, cfg)
}

func newServer(broker base.Broker, cfg Config) *Server {
	n := cfg.Concurrency
	if n < 1 {
		n = runtime.NumCPU()
	}
	if len(cfg.ServerID) == 0 {
		cfg.ServerID = uuid.New().String()
	}
	delayFunc := cfg.RetryDelayFunc
	if IsNil(delayFunc) {
		delayFunc = DefaultRetryDelay
	}
	isFailureFunc := cfg.IsFailure
	if isFailureFunc == nil {
		isFailureFunc = defaultIsFailureFunc
	}

	if cfg.ProcessorEmptyQSleep <= 0 {
		cfg.ProcessorEmptyQSleep = time.Second
	}
	if cfg.RecovererInterval <= 0 {
		cfg.RecovererInterval = time.Minute
	}
	if cfg.RecovererExpiration <= 0 {
		cfg.RecovererExpiration = time.Second * 30
	}
	if cfg.ForwarderInterval <= 0 {
		cfg.ForwarderInterval = time.Second * 5
	}
	if cfg.HeartBeaterInterval <= 0 {
		cfg.HeartBeaterInterval = time.Second * 5
	}
	if cfg.SyncerInterval <= 0 {
		cfg.SyncerInterval = time.Second * 5
	}
	if cfg.SubscriberRetryTimeout <= 0 {
		cfg.SubscriberRetryTimeout = time.Second * 5
	}
	if cfg.JanitorInterval <= 0 {
		cfg.JanitorInterval = time.Second * 8
	}

	if cfg.Queues == nil {
		cfg.Queues = &QueuesConfig{}
	}
	err := cfg.Queues.Configure()
	if err != nil {
		panic(aserrors.E(aserrors.Op("newServer"), aserrors.Internal, err))
	}

	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	healthcheckInterval := cfg.HealthCheckInterval
	if healthcheckInterval == 0 {
		healthcheckInterval = defaultHealthCheckInterval
	}
	logger := log.NewLogger(cfg.Logger)
	loglevel := cfg.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))

	starting := make(chan *workerInfo, n)
	finished := make(chan *base.TaskMessage, n)
	syncCh := make(chan *syncRequest, n)
	state := base.NewServerState()
	cancels := base.NewCancelations()
	afterTasks := base.NewAfterTasks()
	deadlines := base.NewDeadlines(n)

	wakePrcCh := make(chan bool, 1)
	wakeFwdCh := make(chan bool, 1)
	broker = newWakingBroker(broker, deadlines, wakePrcCh, wakeFwdCh)

	syncer := newSyncer(syncerParams{
		logger:     logger,
		requestsCh: syncCh,
		interval:   cfg.SyncerInterval,
	})
	heartbeater := newHeartbeater(heartbeaterParams{
		logger:      logger,
		broker:      broker,
		serverID:    cfg.ServerID,
		interval:    cfg.HeartBeaterInterval,
		concurrency: n,
		queues:      cfg.Queues,
		state:       state,
		starting:    starting,
		finished:    finished,
	})
	subscriber := newSubscriber(subscriberParams{
		logger:       logger,
		broker:       broker,
		cancelations: cancels,
		retryTimeout: cfg.SubscriberRetryTimeout,
	})
	processor := newProcessor(processorParams{
		logger:          logger,
		serverID:        cfg.ServerID,
		broker:          broker,
		retryDelayFunc:  delayFunc,
		isFailureFunc:   isFailureFunc,
		syncCh:          syncCh,
		deadlines:       deadlines,
		cancelations:    cancels,
		afterTasks:      afterTasks,
		concurrency:     n,
		queues:          cfg.Queues,
		errHandler:      cfg.ErrorHandler,
		shutdownTimeout: shutdownTimeout,
		starting:        starting,
		finished:        finished,
		wakeCh:          wakePrcCh,
		emptyQSleep:     cfg.ProcessorEmptyQSleep,
	})
	healthchecker := newHealthChecker(healthcheckerParams{
		logger:          logger,
		broker:          broker,
		interval:        healthcheckInterval,
		healthcheckFunc: cfg.HealthCheckFunc,
	})
	forwarder := newForwarder(forwarderParams{
		logger:      logger,
		broker:      broker,
		queues:      cfg.Queues,
		interval:    cfg.ForwarderInterval,
		healthCheck: healthchecker,
		wakeCh:      wakeFwdCh,
		wakePrcCh:   wakePrcCh,
	})
	recoverer := newRecoverer(recovererParams{
		logger:         logger,
		broker:         broker,
		retryDelayFunc: delayFunc,
		isFailureFunc:  isFailureFunc,
		queues:         cfg.Queues,
		interval:       cfg.RecovererInterval,
		expiration:     cfg.RecovererExpiration,
		healthCheck:    healthchecker,
	})
	janitor := newJanitor(janitorParams{
		logger:      logger,
		broker:      broker,
		queues:      cfg.Queues,
		interval:    cfg.JanitorInterval,
		healthCheck: healthchecker,
	})
	return &Server{
		logger:        logger,
		broker:        broker,
		deadlines:     deadlines,
		state:         state,
		forwarder:     forwarder,
		processor:     processor,
		syncer:        syncer,
		heartbeater:   heartbeater,
		subscriber:    subscriber,
		recoverer:     recoverer,
		healthchecker: healthchecker,
		janitor:       janitor,
	}
}

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask returns an AsynchronousTask error, the task is indicating that
// additional processing will happen asynchronously separate from the Handler
// goroutine. In this case, the task will not be marked as completed or failed
// until after the task status is updated via AsyncProcessor.
//
// If ProcessTask returns any other non-nil error or panics, the task
// will be retried after delay if retry-count is remaining,
// otherwise the task will be archived.
//
// One exception to this rule is when ProcessTask returns a SkipRetry error.
// If the returned error is SkipRetry or an error wraps SkipRetry, retry is
// skipped and the task will be immediately archived instead.
type Handler interface {
	ProcessTask(context.Context, *Task) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Task) error

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return fn(ctx, task)
}

// ErrServerClosed indicates that the operation is now illegal because of the server has been shutdown.
var ErrServerClosed = errors.New("asynq: Server closed")

// Run starts the task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all active workers and other
// goroutines to process the tasks.
//
// Run returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
func (srv *Server) Run(handler Handler) error {
	if err := srv.Start(handler); err != nil {
		return err
	}
	srv.waitForSignals()
	srv.Shutdown()
	return nil
}

// Start starts the worker server. Once the server has started,
// it pulls tasks off queues and starts a worker goroutine for each task
// and then call Handler to process it.
// Tasks are processed concurrently by the workers up to the number of
// concurrency specified in Config.Concurrency.
//
// Start returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
func (srv *Server) Start(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("asynq: server cannot run with nil handler")
	}
	switch srv.state.Get() {
	case base.StateActive:
		return fmt.Errorf("asynq: the server is already running")
	case base.StateStopped:
		return fmt.Errorf("asynq: the server is in the stopped state. Waiting for shutdown")
	case base.StateClosed:
		return ErrServerClosed
	}
	srv.state.Set(base.StateActive)
	srv.processor.handler = handler

	srv.logger.Info("Starting processing")

	srv.heartbeater.start(&srv.wg)
	srv.healthchecker.start(&srv.wg)
	srv.subscriber.start(&srv.wg)
	srv.syncer.start(&srv.wg)
	srv.recoverer.start(&srv.wg)
	srv.forwarder.start(&srv.wg)
	srv.processor.start(&srv.wg)
	srv.janitor.start(&srv.wg)
	return nil
}

// Shutdown gracefully shuts down the server.
// It gracefully closes all active workers. The server will wait for
// active workers to finish processing tasks for duration specified in Config.ShutdownTimeout.
// If worker didn't finish processing a task during the timeout, the task will be pushed back to Redis.
func (srv *Server) Shutdown() {
	srv.shutdown(false)
}

func (srv *Server) shutdown(now bool) {
	switch srv.state.Get() {
	case base.StateNew, base.StateClosed:
		// server is not running, do nothing and return.
		return
	}

	srv.logger.Info("Starting graceful shutdown")
	// Note: The order of shutdown is important.
	// Sender goroutines should be terminated before the receiver goroutines.
	// processor -> syncer (via syncCh)
	// processor -> heartbeater (via starting, finished channels)
	srv.forwarder.shutdown()
	srv.processor.shutdownNow(now)
	srv.recoverer.shutdown()
	srv.syncer.shutdown()
	srv.subscriber.shutdown()
	srv.janitor.shutdown()
	srv.healthchecker.shutdown()
	srv.heartbeater.shutdown()

	srv.wg.Wait()

	srv.deadlines.Close()
	_ = srv.broker.Close()
	srv.state.Set(base.StateClosed)

	srv.logger.Info("Exiting")
}

// Stop signals the server to stop pulling new tasks off queues.
// Stop can be used before shutting down the server to ensure that all
// currently active tasks are processed before server shutdown.
//
// Stop does not shutdown the server, make sure to call Shutdown before exit.
func (srv *Server) Stop() {
	switch srv.state.Get() {
	case base.StateNew, base.StateClosed, base.StateStopped:
		// server is not running, do nothing and return.
		return
	}

	srv.logger.Info("Stopping processor")
	srv.processor.stop()
	srv.state.Set(base.StateStopped)
	srv.logger.Info("Processor stopped")
}

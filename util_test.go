package asynq

import (
	"fmt"
	stdlog "log"
	"os"
	"path/filepath"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

type StopWatch struct {
	startTime time.Time
	stopTime  time.Time
}

// StartWatch starts and returns a stopwatch.
//
// The StopWatch is super simple:
//
//	sw := timeutil.StartWatch()
//	...
//	sw.Stop()
//	sw.Duration() // get the elapsed time between start and stop time
func StartWatch() *StopWatch {
	return &StopWatch{startTime: time.Now()}
}

// Reset resets the stopwatch by re-recording the start time.
func (w *StopWatch) Reset() {
	w.startTime = time.Now()
}

// Stop stops the stopwatch by recording the stop time. The stopwatch may be
// stopped multiple times, but only the last stop time is retained.
func (w *StopWatch) Stop() {
	w.stopTime = time.Now()
}

// StartTime returns the time when the stopwatch was started.
func (w *StopWatch) StartTime() time.Time {
	return w.startTime
}

// StopTime returns the time when the stopwatch was stopped or the zero value of
// utc.UTC if the stopwatch hasn't been stopped yet.
func (w *StopWatch) StopTime() time.Time {
	return w.stopTime
}

// Duration returns the elapsed duration between start and stop time. If the
// stopwatch has not been stopped yet, returns the duration between now and the
// start time.
func (w *StopWatch) Duration() time.Duration {
	if w.stopTime.IsZero() {
		return time.Now().Sub(w.startTime)
	}
	return w.stopTime.Sub(w.startTime)
}

// String returns the duration as a string.
func (w *StopWatch) String() string {
	return w.Duration().String()
}

func MkdirTemp(prefix string) (string, func(), error) {
	asynqDir := filepath.Join(os.TempDir(), "asynq")
	err := os.MkdirAll(asynqDir, os.ModePerm)
	if err != nil {
		return "", nil, err
	}
	ret, err := os.MkdirTemp(asynqDir, prefix)
	if err != nil {
		return "", nil, err
	}
	return ret, func() { _ = os.RemoveAll(ret) }, nil
}

func newFileLogger(path string) (*fileLogger, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("asynq: pid=%d ", os.Getpid())
	return &fileLogger{
		Logger: stdlog.New(f, prefix, stdlog.Ldate|stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC),
	}, nil
}

var _ log.Base = (*fileLogger)(nil)

type fileLogger struct {
	*stdlog.Logger
}

func (l *fileLogger) Debug(args ...interface{}) {
	l.prefixPrint("DEBUG: ", args...)
}

func (l *fileLogger) Info(args ...interface{}) {
	l.prefixPrint("INFO: ", args...)
}

func (l *fileLogger) Warn(args ...interface{}) {
	l.prefixPrint("WARN: ", args...)
}

func (l *fileLogger) Error(args ...interface{}) {
	l.prefixPrint("ERROR: ", args...)
}

func (l *fileLogger) Fatal(args ...interface{}) {
	l.prefixPrint("FATAL: ", args...)
	os.Exit(1)
}

func (l *fileLogger) prefixPrint(prefix string, args ...interface{}) {
	args = append([]interface{}{prefix}, args...)
	l.Print(args...)
}

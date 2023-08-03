package asynq

import (
	"time"
)

type RetryMux struct {
	taskMatcher *TaskMatcher[RetryDelayHandler]
	defHandler  func() RetryDelayHandler
}

func NewRetryMux(defaultRetry RetryDelayHandler) *RetryMux {
	def := noRetryHandler
	if !IsNil(defaultRetry) {
		def = func() RetryDelayHandler { return defaultRetry }
	}
	return &RetryMux{
		taskMatcher: NewTaskMatcher[RetryDelayHandler](),
		defHandler:  def,
	}
}

func (mux *RetryMux) RetryDelay(n int, e error, task *Task) time.Duration {
	h, _ := mux.Handler(task)
	return h.RetryDelay(n, e, task)
}

func (mux *RetryMux) Handler(t *Task) (h RetryDelayHandler, pattern string) {
	return mux.taskMatcher.Handler(t, mux.defHandler)
}

func (mux *RetryMux) Handle(pattern string, handler RetryDelayHandler) {
	mux.taskMatcher.Handle(pattern, handler)
}

func (mux *RetryMux) HandleFunc(pattern string, handler func(n int, e error, task *Task) time.Duration) {
	if handler == nil {
		panic("asynq: nil retry handler")
	}
	mux.Handle(pattern, RetryDelayFunc(handler))
}

// noRetryHandler returns the default retry delay func.
func noRetryHandler() RetryDelayHandler {
	return DefaultRetryDelay
}

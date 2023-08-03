package asynq

import "context"

type ErrorMux struct {
	taskMatcher *TaskMatcher[ErrorHandler]
	defHandler  func() ErrorHandler
}

func NewErrorMux(defaultHandler ErrorHandler) *ErrorMux {
	def := noErrorHandler
	if !IsNil(defaultHandler) {
		def = func() ErrorHandler { return defaultHandler }
	}
	return &ErrorMux{
		taskMatcher: NewTaskMatcher[ErrorHandler](),
		defHandler:  def,
	}
}

func (mux *ErrorMux) HandleError(ctx context.Context, task *Task, err error) {
	h, _ := mux.Handler(task)
	h.HandleError(ctx, task, err)
}

func (mux *ErrorMux) Handler(t *Task) (h ErrorHandler, pattern string) {
	return mux.taskMatcher.Handler(t, mux.defHandler)
}

func (mux *ErrorMux) Handle(pattern string, handler ErrorHandler) {
	mux.taskMatcher.Handle(pattern, handler)
}

func (mux *ErrorMux) HandleFunc(pattern string, handler func(context.Context, *Task, error)) {
	if handler == nil {
		panic("asynq: nil error handler")
	}
	mux.Handle(pattern, ErrorHandlerFunc(handler))
}

// noErrorHandler returns a noop error handler.
func noErrorHandler() ErrorHandler {
	return ErrorHandlerFunc(func(ctx context.Context, task *Task, err error) {})
}

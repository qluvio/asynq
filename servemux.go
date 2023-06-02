// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
)

// ServeMux is a multiplexer for asynchronous tasks.
// It matches the type of each task against a list of registered patterns
// and calls the handler for the pattern that most closely matches the
// task's type name.
//
// Longer patterns take precedence over shorter ones, so that if there are
// handlers registered for both "images" and "images:thumbnails",
// the latter handler will be called for tasks with a type name beginning with
// "images:thumbnails" and the former will receive tasks with type name beginning
// with "images".
type ServeMux struct {
	taskMatcher *TaskMatcher[Handler]
}

// MiddlewareFunc is a function which receives an asynq.Handler and returns another asynq.Handler.
// Typically, the returned handler is a closure which does something with the context and task passed
// to it, and then calls the handler passed as parameter to the MiddlewareFunc.
type MiddlewareFunc func(Handler) Handler

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	return &ServeMux{taskMatcher: NewTaskMatcher[Handler]()}
}

// ProcessTask dispatches the task to the handler whose
// pattern most closely matches the task type.
func (mux *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	h, _ := mux.Handler(task)
	return h.ProcessTask(ctx, task)
}

// Handler returns the handler to use for the given task.
// It always return a non-nil handler.
//
// Handler also returns the registered pattern that matches the task.
//
// If there is no registered handler that applies to the task,
// handler returns a 'not found' handler which returns an error.
func (mux *ServeMux) Handler(t *Task) (h Handler, pattern string) {
	return mux.taskMatcher.Handler(t, NotFoundHandler)
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	mux.taskMatcher.Handle(pattern, handler)
}

// HandleFunc registers the handler function for the given pattern.
func (mux *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	if handler == nil {
		panic("asynq: nil handler")
	}
	mux.Handle(pattern, HandlerFunc(handler))
}

// Use appends a MiddlewareFunc to the chain.
// Middlewares are executed in the order that they are applied to the ServeMux.
func (mux *ServeMux) Use(mws ...MiddlewareFunc) {
	mux.taskMatcher.UseI(mws)
}

// NotFound returns an error indicating that the handler was not found for the given task.
func NotFound(ctx context.Context, task *Task) error {
	_ = ctx
	return fmt.Errorf("handler not found for task %q", task.Type())
}

// NotFoundHandler returns a simple task handler that returns a “not found“ error.
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }

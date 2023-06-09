package asynq

import (
	"reflect"
	"sort"
	"strings"
	"sync"
)

type TaskMatcher[T any] struct {
	mu  sync.RWMutex               // protect following fields
	m   map[string]matcherEntry[T] // exact match
	es  []matcherEntry[T]          // slice of entries sorted from longest to shortest.
	mws []func(T) T                // middlewares
}

type matcherEntry[T any] struct {
	h       T
	pattern string
}

// NewTaskMatcher allocates and returns a new TaskMatcher.
func NewTaskMatcher[T any]() *TaskMatcher[T] {
	return new(TaskMatcher[T])
}

// Handler returns the handler to use for the given task.
// It always returns a non-nil handler.
//
// Handler also returns the registered pattern that matches the task.
//
// If there is no registered handler that applies to the task,
// handler returns the result of calling the notFound parameter.
func (mux *TaskMatcher[T]) Handler(t *Task, notFound func() T) (h T, pattern string) {
	if notFound == nil {
		panic("'notFound' must not be nil")
	}

	mux.mu.RLock()
	defer mux.mu.RUnlock()

	h, pattern = mux.match(t.Type())
	if IsNil(h) {
		h, pattern = notFound(), ""
	}
	h = mux.middleware(h)
	return h, pattern
}

func (mux *TaskMatcher[T]) middleware(h T) T {
	for i := len(mux.mws) - 1; i >= 0; i-- {
		h = mux.mws[i](h)
	}
	return h
}

// Find a handler on a handler map given a typename string.
// Most-specific (longest) pattern wins.
func (mux *TaskMatcher[T]) match(typename string) (h T, pattern string) {
	// Check for exact match first.
	v, ok := mux.m[typename]
	if ok {
		return v.h, v.pattern
	}

	// Check for longest valid match.
	// mux.es contains all patterns from longest to shortest.
	for _, e := range mux.es {
		if strings.HasPrefix(typename, e.pattern) {
			return e.h, e.pattern
		}
	}
	var zero T
	return zero, ""
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (mux *TaskMatcher[T]) Handle(pattern string, handler T) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if strings.TrimSpace(pattern) == "" {
		panic("asynq: invalid pattern")
	}
	if IsNil(handler) {
		panic("asynq: nil handler")
	}
	if _, exist := mux.m[pattern]; exist {
		panic("asynq: multiple registrations for " + pattern)
	}

	if mux.m == nil {
		mux.m = make(map[string]matcherEntry[T])
	}
	e := matcherEntry[T]{h: handler, pattern: pattern}
	mux.m[pattern] = e
	mux.es = mux.appendSorted(mux.es, e)
}

func (mux *TaskMatcher[T]) appendSorted(es []matcherEntry[T], e matcherEntry[T]) []matcherEntry[T] {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}
	// we now know that i points at where we want to insert.
	es = append(es, matcherEntry[T]{}) // try to grow the slice in place, any entry works.
	copy(es[i+1:], es[i:])             // shift shorter entries down.
	es[i] = e
	return es
}

func (mux *TaskMatcher[T]) Use(mws ...func(T) T) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	for _, fn := range mws {
		mux.mws = append(mux.mws, fn)
	}
}

func (mux *TaskMatcher[T]) UseI(mws interface{}) {
	k := reflect.TypeOf(mws).Kind()
	if k != reflect.Slice {
		// ignore
		return
	}
	mux.mu.Lock()
	defer mux.mu.Unlock()

	tx := reflect.TypeOf((func(T) T)(nil))

	s := reflect.ValueOf(mws)
	for i := 0; i < s.Len(); i++ {
		vfn := s.Index(i)
		mux.mws = append(mux.mws, vfn.Convert(tx).Interface().(func(T) T))
	}
}

var nillableKinds = []reflect.Kind{
	reflect.Chan, reflect.Func,
	reflect.Interface, reflect.Map,
	reflect.Ptr, reflect.Slice}

// IsNil returns true if the given object is nil (== nil) or is a nillable type
// (channel, function, interface, map, pointer or slice) with a nil value.
func IsNil(obj interface{}) bool {
	if obj == nil {
		return true
	}

	value := reflect.ValueOf(obj)
	kind := value.Kind()
	for _, k := range nillableKinds {
		if k == kind {
			return value.IsNil()
		}
	}

	return false
}

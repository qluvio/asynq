package base

import "sync"

// AfterTasks is a collection that holds functions called after an active task finishes.
//
// AfterTasks are safe for concurrent use by multiple goroutines.
type AfterTasks struct {
	mu    sync.Mutex
	funcs map[string]func(id string, err error, isFailure bool)
}

// NewAfterTasks returns a AfterTasks instance.
func NewAfterTasks() *AfterTasks {
	return &AfterTasks{
		funcs: make(map[string]func(id string, err error, isFailure bool)),
	}
}

// Add adds a new cancel func to the collection.
func (c *AfterTasks) Add(id string, fn func(id string, err error, isFailure bool)) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funcs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
func (c *AfterTasks) Delete(id string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.funcs, id)
}

// Get returns a cancel func given an id.
func (c *AfterTasks) Get(id string) (fn func(id string, err error, isFailure bool), ok bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.funcs[id]
	return fn, ok
}

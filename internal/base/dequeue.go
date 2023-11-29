package base

import "time"

type Dequeuer interface {
	Dequeue(serverID string, qnames ...string) (msg *TaskMessage, deadline time.Time, err error)
}

type BufferedDequeuer interface {
	Dequeuer
	DequeueN(serverID string, qname string, count int, ret []Task) ([]Task, error)
}

type Task struct {
	Msg      *TaskMessage
	Deadline time.Time
}

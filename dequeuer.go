package asynq

import (
	"time"

	"github.com/hibiken/asynq/internal/base"
)

type procTask struct {
	base.Task
	queue *queue
}

var zeroTask = procTask{}

type Dequeuer interface {
	Dequeue() (procTask, error)
}

func newDq(serverID string, broker base.Broker, queues queues) *dq {
	return &dq{
		serverID: serverID,
		broker:   broker,
		queues:   queues,
	}
}

type dq struct {
	serverID string
	broker   base.Broker
	queues   queues
}

func (dq *dq) Dequeue() (procTask, error) {
	var ret procTask
	var err error

	broker := dq.broker
	if wb, ok := broker.(*wakingBroker); ok {
		broker = wb.Scheduler.(base.Broker)
	}
	bufdq, ok := broker.(base.BufferedDequeuer)

	queues, namer := dq.queues.namedQueues()
	var iwrr *iwrrNamer
	if ok {
		iwrr, ok = namer.(*iwrrNamer)
	}

	if ok {
		ret, err = iwrr.cycle(dq.serverID, bufdq)
	} else {
		var msg *base.TaskMessage
		var deadline time.Time

		qnames := namer.selectQueues(queues)
		msg, deadline, err = dq.broker.Dequeue(dq.serverID, qnames...)
		if err == nil {
			ret = procTask{
				Task: base.Task{
					Msg:      msg,
					Deadline: deadline,
				},
				queue: queues[msg.Queue].acquire(),
			}
		}
	}

	return ret, err
}

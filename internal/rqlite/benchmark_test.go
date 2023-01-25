package rqlite

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/stretchr/testify/require"
)

func BenchmarkEnqueue(b *testing.B) {
	r := setup(b)
	FlushDB(b, r.conn)
	msg := asynqtest.NewTaskMessage("task1", nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.ID = uuid.New().String()
		if err := r.Enqueue(msg); err != nil {
			b.Fatalf("Enqueue failed: %v", err)
		}
	}
}

func TestOneSecEnqueue(t *testing.T) {
	r := setup(t)
	FlushDB(t, r.conn)
	msg := asynqtest.NewTaskMessage("task1", nil)
	now := time.Now()

	count := 0
	d := time.Since(now)
	for d < time.Second {
		msg.ID = uuid.New().String()
		if err := r.Enqueue(msg); err != nil {
			require.NoError(t, err)
		}
		count++
		d = time.Since(now)
	}
	fmt.Println("count", count, "d", d)
}

func TestEnqueueBatch(t *testing.T) {
	doTestEnqueueBatch(t, 1)
	doTestEnqueueBatch(t, 10)
	doTestEnqueueBatch(t, 100)
	doTestEnqueueBatch(t, 1000)
}

func doTestEnqueueBatch(t *testing.T, batchSize int) {
	r := setup(t)
	FlushDB(t, r.conn)

	msgs := make([]*base.MessageBatch, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs = append(msgs, &base.MessageBatch{
			Msg: asynqtest.NewTaskMessage(fmt.Sprintf("task%d", i), nil),
		})
	}
	updateIds := func() {
		for i := 0; i < batchSize; i++ {
			msgs[i].Msg.ID = uuid.New().String()
		}
	}

	now := time.Now()
	count := 0
	d := time.Duration(0)

	for d < time.Second*2 {

		err := r.EnqueueBatch(msgs...)
		require.NoError(t, err)

		count += batchSize
		d = time.Since(now)
		updateIds()
	}
	fmt.Println(fmt.Sprintf("batch_size %4d count: %4d d: %v", batchSize, count, d))
}

// PENDING(GIL): add other benchmarks

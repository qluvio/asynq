package asynq

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestLocalLongRun(t *testing.T) {
	// To execute this manually uncomment this line
	//brokerType = SqliteType

	if testing.Short() || brokerType != SqliteType {
		// test takes 1 or 2 minutes
		t.Skip("long run (1m) with sqlite")
	}

	dir, err := os.MkdirTemp("", "testLocalLongRun")
	require.NoError(t, err)
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	defer cleanup()

	dbPath := fmt.Sprintf("%s/db.sqlite", dir)
	fmt.Println("db_path", dbPath)

	rqliteConfig.TablesPrefix = "local_"
	rqliteConfig.SqliteDbPath = dbPath
	rqliteConfig.Type = "sqlite"

	client := NewClient(getClientConnOpt(t))
	srv := newServer(client.rdb, Config{
		ServerID:    "inod1111",
		Concurrency: 10,
		RetryDelayFunc: RetryDelayFunc(func(n int, err error, t *Task) time.Duration {
			return time.Millisecond * 200
		}),
		LogLevel:             InfoLevel,
		ProcessorEmptyQSleep: time.Millisecond * 200,
		ShutdownTimeout:      time.Millisecond * 10,
		HealthCheckInterval:  time.Millisecond * 200,
		ForwarderInterval:    time.Millisecond * 200,
		Queues: &QueuesConfig{
			Queues: map[string]int{
				"low":    1,
				"normal": 3,
				"high":   5,
			},
		},
	})

	mux := NewServeMux()
	_ = srv.Start(mux)
	defer srv.Stop()

	inspector := newInspector(srv.broker)

	log := srv.logger
	handled := atomic.NewInt64(0)
	startCh := make(chan struct{})
	done := atomic.NewBool(false)
	doneCh := make(chan struct{})

	handleError := func(err error) {
		if err == nil {
			return
		}
		serr := err.Error()
		log.Warnf("error: %v, type: %v", serr, reflect.TypeOf(err))
		if !strings.Contains(serr, "database is locked") {
			return
		}
		log.Warnf("full stack dump - stacktraces %s", "\n"+string(debug.Stack()))
		if !done.Load() {
			done.Store(true)
			close(doneCh)
		}
	}

	readTask := func(task *Task) error {
		id := string(task.Payload())
		queue := task.Type()
		_, err := inspector.GetTaskInfo(queue, id)
		return err
	}

	mux.HandleFunc(
		"normal",
		func(ctx context.Context, task *Task) error {
			c := handled.Inc()

			errr := readTask(task)
			handleError(errr)

			time.Sleep(time.Millisecond * 250)
			wa := task.ResultWriter()

			bb := binary.LittleEndian.AppendUint64(nil, uint64(c))
			_, errw := wa.Write(bb)
			handleError(errw)

			if c%10 == 0 {
				log.Infof("handled: %d (task: %s)", c, string(task.Payload()))
			}

			return nil

		})
	require.NoError(t, err)
	mux.HandleFunc(
		"high",
		func(ctx context.Context, task *Task) error {
			errr := readTask(task)
			handleError(errr)

			wa := task.ResultWriter()
			_, errw := wa.Write([]byte("b"))
			handleError(errw)

			c := handled.Inc()
			if c%10 == 0 {
				log.Infof("handled count: %d (task: %s)", c, string(task.Payload()))
			}

			return nil
		})
	require.NoError(t, err)

	w := time.Now()
	enqueued := atomic.NewInt64(0)
	execDuration := time.Minute * 2
	go func() {
		iter := 0

		for time.Now().Sub(w) < execDuration && enqueued.Load() <= 20000 {
			if iter == 5 {
				close(startCh)
			}

			c := handled.Load()
			switch c % 3 {
			case 0:
			case 1:
				time.Sleep(time.Millisecond * 300)
			case 2:
				time.Sleep(time.Millisecond * 600)
			}
			id := fmt.Sprintf("a-%d", iter)
			_, err := client.EnqueueContext(
				context.Background(),
				NewTask("normal", []byte(id)),
				Queue("normal"),
				MaxRetry(1),
				ReprocessAfter(time.Millisecond*250),
				Timeout(time.Hour),
				TaskID(id),
				Retention(time.Hour))
			handleError(err)
			if err != nil {
				break
			}
			enqueued.Inc()
			id = fmt.Sprintf("b-%d", iter)
			_, err = client.EnqueueContext(
				context.Background(),
				NewTask("high", []byte(id)),
				Queue("high"),
				Timeout(time.Millisecond*150),
				TaskID(id),
				Retention(time.Hour))
			handleError(err)
			if err != nil {
				break
			}
			enqueued.Inc()
			iter++
			if iter%100 == 0 {
				log.Infof("enqueue - iter count: %d", iter)
			}
			select {
			case <-doneCh:
				break
			default:
			}
		}
		log.Infof("<< finished enqueuing - took %v, iter: %d, enqueued: %d, handled: %d", time.Now().Sub(w),
			iter,
			enqueued.Load(),
			handled.Load())
		if !done.Load() {
			done.Store(true)
			close(doneCh)
		}
	}()

	<-startCh
	ticker := time.NewTicker(time.Second * 10)
out:
	for {
		select {
		case <-doneCh:
			break out
		case <-ticker.C:
			enq := enqueued.Load()
			han := handled.Load()
			log.Infof("queue - d: %v, enqueued: %d, handled: %d",
				time.Now().Sub(w),
				enq,
				han)
			if enq == han && enq >= 1000 {
				break out
			}
		}
	}

	log.Infof("<< queue closing - took: %v, enqueued: %d, handled: %d",
		time.Now().Sub(w),
		enqueued.Load(),
		handled.Load())
}

// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/testbroker"
	"go.uber.org/goleak"
)

func ignoreTopFunctions() []goleak.Option {
	// https://github.com/go-redis/redis/issues/1029
	return []goleak.Option{
		goleak.IgnoreTopFunction("github.com/go-redis/redis/v8/internal/pool.(*ConnPool).reaper"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionCleaner"),
	}
}

// WaitForSignals expose waitForSignals for tests
func (srv *Server) WaitForSignals() {
	srv.waitForSignals()
}

func TestServer(t *testing.T) {
	defer goleak.VerifyNone(t, ignoreTopFunctions()...)

	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	connOpt := getClientConnOpt(t)
	client := NewClient(connOpt)
	defer func() { _ = client.Close() }()
	srv := NewServer(connOpt, Config{
		Concurrency: 10,
		LogLevel:    testLogLevel,
	})

	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Enqueue(NewTask("send_email", asynqtest.JSON(map[string]interface{}{"recipient_id": 123})))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	_, err = client.Enqueue(NewTask("send_email", asynqtest.JSON(map[string]interface{}{"recipient_id": 456})), ProcessIn(1*time.Hour))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	srv.Shutdown()
}

func TestServerRun(t *testing.T) {
	defer goleak.VerifyNone(t, ignoreTopFunctions()...)

	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})

	done := make(chan struct{})
	// Make sure server exits when receiving TERM signal.
	go func() {
		time.Sleep(2 * time.Second)
		_ = kill()
		done <- struct{}{}
	}()

	go func() {
		select {
		case <-time.After(10 * time.Second):
			panic("server did not stop after receiving TERM signal")
		case <-done:
		}
	}()

	mux := NewServeMux()
	if err := srv.Run(mux); err != nil {
		t.Fatal(err)
	}
}

func TestServerErrServerClosed(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()
	err := srv.Start(handler)
	if err != ErrServerClosed {
		t.Errorf("Restarting server: (*Server).Start(handler) = %v, want ErrServerClosed error", err)
	}
}

func TestServerErrNilHandler(t *testing.T) {
	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})
	err := srv.Start(nil)
	if err == nil {
		t.Error("Starting server with nil handler: (*Server).Start(nil) did not return error")
		srv.Shutdown()
	}
}

func TestServerErrServerRunning(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	err := srv.Start(handler)
	if err == nil {
		t.Error("Calling (*Server).Start(handler) on already running server did not return error")
	}
	srv.Shutdown()
}

func TestServerWithRedisDown(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()

	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})

	testBroker := testbroker.NewTestBroker(srv.broker)
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker
	testBroker.Sleep()

	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestServerWithFlakyBroker(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	connOpt := getClientConnOpt(t)
	srv := NewServer(connOpt, Config{
		LogLevel: testLogLevel,
	})

	testBroker := testbroker.NewTestBroker(srv.broker)
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker

	client := NewClient(connOpt)
	defer func() { _ = client.Close() }()

	h := func(ctx context.Context, task *Task) error {
		// force task retry.
		if task.Type() == "bad_task" {
			return fmt.Errorf("could not process %q", task.Type())
		}
		time.Sleep(2 * time.Second)
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := client.Enqueue(NewTask("enqueued", nil), MaxRetry(i))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Enqueue(NewTask("bad_task", nil))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Enqueue(NewTask("scheduled", nil), ProcessIn(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	// simulate redis going down.
	testBroker.Sleep()

	time.Sleep(3 * time.Second)

	// simulate redis comes back online.
	testBroker.Wakeup()

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestLogLevel(t *testing.T) {
	tests := []struct {
		flagVal string
		want    LogLevel
		wantStr string
	}{
		{"debug", DebugLevel, "debug"},
		{"Info", InfoLevel, "info"},
		{"WARN", WarnLevel, "warn"},
		{"warning", WarnLevel, "warn"},
		{"Error", ErrorLevel, "error"},
		{"fatal", FatalLevel, "fatal"},
	}

	for _, tc := range tests {
		level := new(LogLevel)
		if err := level.Set(tc.flagVal); err != nil {
			t.Fatal(err)
		}
		if *level != tc.want {
			t.Errorf("Set(%q): got %v, want %v", tc.flagVal, level, &tc.want)
			continue
		}
		if got := level.String(); got != tc.wantStr {
			t.Errorf("String() returned %q, want %q", got, tc.wantStr)
		}
	}
}

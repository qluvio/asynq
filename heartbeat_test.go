// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/testbroker"
)

func TestHeartbeater(t *testing.T) {
	ctx := setupTestContext(t)
	defer func() { _ = ctx.Close() }()

	connOpt := getClientConnOpt(t)
	client := NewClient(connOpt)
	defer func() { _ = client.Close() }()

	tests := []struct {
		interval    time.Duration
		host        string
		pid         int
		queues      Queues
		concurrency int
	}{
		{
			interval:    2 * time.Second,
			host:        "localhost",
			pid:         45678,
			queues:      (&QueuesConfig{Queues: map[string]int{"default": 1}}).configure(),
			concurrency: 10,
		},
	}

	timeCmpOpt := cmpopts.EquateApproxTime(10 * time.Millisecond)
	ignoreOpt := cmpopts.IgnoreUnexported(base.ServerInfo{})
	ignoreFieldOpt := cmpopts.IgnoreFields(base.ServerInfo{}, "ServerID")
	for _, tc := range tests {
		ctx.FlushDB()

		state := base.NewServerState()
		hb := newHeartbeater(heartbeaterParams{
			logger:      testLogger,
			broker:      client.rdb,
			interval:    tc.interval,
			concurrency: tc.concurrency,
			queues:      tc.queues,
			state:       state,
			starting:    make(chan *workerInfo),
			finished:    make(chan *base.TaskMessage),
		})

		// Change host and pid fields for testing purpose.
		hb.host = tc.host
		hb.pid = tc.pid

		state.Set(base.StateActive)
		var wg sync.WaitGroup
		hb.start(&wg)

		want := &base.ServerInfo{
			Host:        tc.host,
			PID:         tc.pid,
			Queues:      tc.queues.Priorities(),
			Concurrency: tc.concurrency,
			Started:     time.Now(),
			Status:      "active",
		}

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval)

		ss, err := client.rdb.ListServers()
		if err != nil {
			t.Errorf("could not read server info from redis: %v", err)
			hb.shutdown()
			continue
		}

		if len(ss) != 1 {
			t.Errorf("(*RDB).ListServers returned %d process info, want 1", len(ss))
			hb.shutdown()
			continue
		}

		if diff := cmp.Diff(want, ss[0], timeCmpOpt, ignoreOpt, ignoreFieldOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", ss[0], want, diff)
			hb.shutdown()
			continue
		}

		// status change
		state.Set(base.StateClosed)

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		want.Status = "closed"
		ss, err = client.rdb.ListServers()
		if err != nil {
			t.Errorf("could not read process status from redis: %v", err)
			hb.shutdown()
			continue
		}

		if len(ss) != 1 {
			t.Errorf("(*RDB).ListProcesses returned %d process info, want 1", len(ss))
			hb.shutdown()
			continue
		}

		if diff := cmp.Diff(want, ss[0], timeCmpOpt, ignoreOpt, ignoreFieldOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", ss[0], want, diff)
			hb.shutdown()
			continue
		}

		hb.shutdown()
	}
}

func TestHeartbeaterWithRedisDown(t *testing.T) {
	// Make sure that heartbeater goroutine doesn't panic
	// if it cannot connect to redis.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()

	connOpt := getClientConnOpt(t)
	client := NewClient(connOpt)
	defer func() { _ = client.Close() }()

	testBroker := testbroker.NewTestBroker(client.rdb)

	state := base.NewServerState()
	state.Set(base.StateActive)
	hb := newHeartbeater(heartbeaterParams{
		logger:      testLogger,
		broker:      testBroker,
		interval:    time.Second,
		concurrency: 10,
		queues:      (&QueuesConfig{Queues: map[string]int{"default": 1}}).configure(),
		state:       state,
		starting:    make(chan *workerInfo),
		finished:    make(chan *base.TaskMessage),
	})

	testBroker.Sleep()
	var wg sync.WaitGroup
	hb.start(&wg)

	// wait for heartbeater to try writing data to redis
	time.Sleep(2 * time.Second)

	hb.shutdown()
}

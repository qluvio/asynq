package rqlite

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/stretchr/testify/require"
)

// variables used for package testing.
var (
	brokerType string // redis | rqlite
	config     Config
)

func init() {
	flag.StringVar(&brokerType, "broker_type", "", "broker type to use in testing: rqlite")
	config.InitDefaults()
	flag.StringVar(&config.RqliteUrl, "rqlite_url", "http://localhost:4001", "rqlite url to use in testing")
	flag.StringVar(&config.ConsistencyLevel, "consistency_level", "strong", "rqlite consistency level")
}

func skipUnknownBroker(tb testing.TB) {
	run := false
	switch brokerType {
	case "rqlite":
		run = true
	}
	if !run {
		tb.Skip(fmt.Sprintf("skipping test with broker type: [%s]", brokerType))
	}
}

func setup(tb testing.TB) *RQLite {
	skipUnknownBroker(tb)
	tb.Helper()
	ret := NewRQLite(&config, nil, nil)
	err := ret.Open()
	if err != nil {
		tb.Fatal("Unable to connect rqlite", err)
	}
	FlushDB(tb, ret.conn)
	return ret
}

func TestCreateTables(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	ok, err := r.conn.CreateTablesIfNotExist()
	require.NoError(t, err)
	require.False(t, ok)

	err = r.conn.DropTables()
	require.NoError(t, err)

	ok, err = r.conn.CreateTablesIfNotExist()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestBasicEnqueueDequeue(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()
	t1 := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"}))
	t2 := h.NewTaskMessageWithQueue("generate_csv", h.JSON(map[string]interface{}{}), "csv")
	t3 := h.NewTaskMessageWithQueue("sync", nil, "low")

	for _, tm := range []*base.TaskMessage{t1, t2, t3} {
		err := r.Enqueue(tm)
		require.NoError(t, err)
	}

	for _, q := range []string{"csv", "low", base.DefaultQueueName} {
		msg, deadline, err := r.Dequeue("", q)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.NotZero(t, deadline)

		require.Equal(t, q, msg.Queue)
		require.Equal(t, 25, msg.Retry)
		require.Equal(t, int64(1800), msg.Timeout)

		switch q {
		case base.DefaultQueueName:
			require.Equal(t, "send_email", msg.Type)
		case "csv":
			require.Equal(t, "generate_csv", msg.Type)
		case "low":
			require.Equal(t, "sync", msg.Type)
		}
	}

	for _, q := range []string{"csv", "low", base.DefaultQueueName} {
		_, _, err := r.Dequeue("", q)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask))
	}
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	t1 := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"}))
	t2 := h.NewTaskMessageWithQueue("generate_csv", h.JSON(map[string]interface{}{}), "csv")
	t3 := h.NewTaskMessageWithQueue("sync", nil, "low")
	tests := []struct {
		msg *base.TaskMessage
	}{
		{t1},
		{t2},
		{t3},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		err := r.Enqueue(tc.msg)
		require.NoError(t, err)

		// Check Pending list has task ID.
		pending, err := r.conn.getPending("", tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, tc.msg.ID, pending.msg.ID)
		// Check the value under the task key.
		diff := cmp.Diff(tc.msg, pending.msg)
		require.Equal(t, "", diff, "persisted message was %v, want %v; (-want, +got)\n%s", pending.msg, tc.msg, diff)

		// Check queue is in the AllQueues table.
		ok, err := r.QueueExist(tc.msg.Queue)
		require.NoError(t, err)
		require.True(t, ok, "%s queue not found in table %s", tc.msg.Queue, r.conn.table(QueuesTable))
	}
}

func TestEnqueueUnique(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()
	m1 := base.TaskMessage{
		ID:        uuid.New().String(),
		Type:      "email",
		Payload:   h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
	}
	m2 := base.TaskMessage{
		ID:        uuid.New().String(),
		Type:      "email",
		Payload:   h.JSON(map[string]interface{}{"user_id": json.Number("456")}),
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 456})),
	}

	tests := []struct {
		msg                     *base.TaskMessage
		ttl                     time.Duration // uniqueness ttl
		waitBeforeSecondEnqueue bool
		failUnique              bool
	}{
		{msg: &m1, ttl: time.Minute, failUnique: true},
		{msg: &m2, ttl: time.Second, failUnique: false, waitBeforeSecondEnqueue: true},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		// Enqueue the first message, should succeed.
		err := r.EnqueueUnique(tc.msg, tc.ttl)
		require.NoError(t, err)

		// Check Pending list has task ID.
		pending, err := r.conn.getPending("", tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, tc.msg.ID, pending.msg.ID)
		// Check the value under the task key.
		diff := cmp.Diff(tc.msg, pending.msg)
		require.Equal(t, "", diff, "persisted message was %v, want %v; (-want, +got)\n%s", pending.msg, tc.msg, diff)

		// Check queue is in the AllQueues table.
		ok, err := r.QueueExist(tc.msg.Queue)
		require.NoError(t, err)
		require.True(t, ok, "%s queue not found in table %s", tc.msg.Queue, r.conn.table(QueuesTable))

		if tc.waitBeforeSecondEnqueue {
			time.Sleep(tc.ttl)
		}

		err = r.EnqueueUnique(tc.msg, tc.ttl)
		if tc.failUnique {
			// Enqueue the second message, should fail.
			require.True(t, errors.Is(err, errors.ErrDuplicateTask),
				"Second message: (*Rqlite).EnqueueUnique(msg, ttl) = %v, want %v", err, errors.ErrDuplicateTask)
		} else {
			// the unique_key constraint is ignored when the unique_key_deadline is expired
			require.NoError(t, err)
		}
	}
}

func TestEnqueueWithServerAffinity(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	m1 := base.TaskMessage{
		ID:             uuid.New().String(),
		Type:           "email",
		Payload:        h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:          base.DefaultQueueName,
		UniqueKey:      base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
		Recurrent:      true,
		Timeout:        1,
		ServerAffinity: 1,
	}
	m2 := base.TaskMessage{
		ID:             uuid.New().String(),
		Type:           "email",
		Payload:        h.JSON(map[string]interface{}{"user_id": json.Number("456")}),
		Queue:          base.DefaultQueueName,
		UniqueKey:      base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 456})),
		Recurrent:      true,
		Timeout:        1,
		ServerAffinity: 1,
	}
	serverID := "inod11"
	ttl := time.Minute // uniqueness ttl
	tests := []struct {
		msg *base.TaskMessage
	}{
		{msg: &m1},
		{msg: &m2},
	}

	for _, tc := range tests {
		//fmt.Println("TestEnqueueWithServerAffinity - test", i, "now", now.Unix())
		FlushDB(t, r.conn)

		// initial dequeue
		err := r.EnqueueUnique(tc.msg, ttl)
		require.NoError(t, err)
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.NoError(t, err)

		// re-queuing with no server id: can be dequeued
		err = r.Requeue("", tc.msg, false)
		require.NoError(t, err)
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.NoError(t, err)

		err = r.Requeue("", tc.msg, false)
		require.NoError(t, err)
		_, _, err = r.Dequeue("bla", tc.msg.Queue)
		require.NoError(t, err)

		// re-queueing with a server id: can be dequeued only by this server
		err = r.Requeue(serverID, tc.msg, false)
		require.NoError(t, err)
		// not available for other servers
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask), err)
		_, _, err = r.Dequeue("inod222", tc.msg.Queue)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask), err)

		m, _, err := r.Dequeue(serverID, tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, tc.msg, m)

		// still cannot enqueue
		err = r.EnqueueUnique(tc.msg, ttl)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrDuplicateTask))

		err = r.Requeue(serverID, tc.msg, false)
		require.NoError(t, err)

		// after the server affinity elapsed, another server can dequeue it
		now = now.Add(time.Second * time.Duration(tc.msg.ServerAffinity))
		utc.MockNow(now)
		m, _, err = r.Dequeue("", tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, tc.msg, m)

		// still cannot enqueue
		err = r.EnqueueUnique(tc.msg, ttl)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrDuplicateTask))
		now = now.Add(ttl)
		utc.MockNow(now)
		err = r.EnqueueUnique(tc.msg, ttl)
		require.NoError(t, err)

	}
}

func TestEnqueueWithServerAffinityAfterError(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	m1 := base.TaskMessage{
		ID:             uuid.New().String(),
		Type:           "email",
		Payload:        h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:          base.DefaultQueueName,
		UniqueKey:      base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
		Recurrent:      true,
		Timeout:        1,
		ServerAffinity: 1,
	}
	serverID := "inod11"
	serverID2 := "inod222"
	ttl := time.Minute // uniqueness ttl
	tests := []struct {
		msg *base.TaskMessage
	}{
		{msg: &m1},
	}

	for _, tc := range tests {
		//fmt.Println("TestEnqueueWithServerAffinity - test", i, "now", now.Unix())
		FlushDB(t, r.conn)

		// initial dequeue
		err := r.EnqueueUnique(tc.msg, ttl)
		require.NoError(t, err)
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.NoError(t, err)

		// re-queueing with a server id: can be dequeued only by this server
		err = r.Requeue(serverID, tc.msg, false)
		require.NoError(t, err)
		// not available for other servers
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask), err)
		_, _, err = r.Dequeue(serverID2, tc.msg.Queue)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask), err)

		m, _, err := r.Dequeue(serverID, tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, tc.msg, m)

		err = r.Archive(tc.msg, "there was an error")
		require.NoError(t, err)
		// after an error we can re-enqueue
		err = r.EnqueueUnique(tc.msg, ttl)
		require.NoError(t, err)

		// the server cannot take it right away
		m, _, err = r.Dequeue(serverID, tc.msg.Queue)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNoProcessableTask), err)

		// but another one can
		m, _, err = r.Dequeue(serverID2, tc.msg.Queue)
		require.NoError(t, err)
		err = r.Archive(tc.msg, "there was an error")
		require.NoError(t, err)
		// after an error we can re-enqueue
		err = r.EnqueueUnique(tc.msg, ttl)
		require.NoError(t, err)

		// after server affinity elapsed, the same server can take it again
		now = now.Add(time.Second * time.Duration(tc.msg.ServerAffinity))
		utc.MockNow(now)
		m, _, err = r.Dequeue(serverID2, tc.msg.Queue)
		require.NoError(t, err)

	}
}

func TestRequeueScheduled(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	m1 := base.TaskMessage{
		ID:             uuid.New().String(),
		Type:           "email",
		Payload:        h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:          base.DefaultQueueName,
		UniqueKey:      base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
		Recurrent:      true,
		ReprocessAfter: 3,
		Timeout:        1,
		ServerAffinity: 1,
	}
	m2 := base.TaskMessage{
		ID:             uuid.New().String(),
		Type:           "email",
		Payload:        h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:          base.DefaultQueueName,
		Recurrent:      true,
		ReprocessAfter: 3,
		Timeout:        1,
		ServerAffinity: 1,
	}
	serverID := "inod11"
	ttl := time.Minute // uniqueness ttl
	tests := []struct {
		msg      *base.TaskMessage
		serverID string
	}{
		{msg: &m1, serverID: serverID},
		{msg: &m1, serverID: ""},
		{msg: &m2, serverID: serverID},
		{msg: &m2, serverID: ""},
	}

	for _, tc := range tests {
		//fmt.Println("TestRequeueScheduled - test", i, "now", now.Unix())
		FlushDB(t, r.conn)

		// initial enqueue/dequeue
		var err error
		if len(tc.msg.UniqueKey) > 0 {
			err = r.EnqueueUnique(tc.msg, ttl)
		} else {
			err = r.Enqueue(tc.msg)
		}
		require.NoError(t, err)
		_, _, err = r.Dequeue("", tc.msg.Queue)
		require.NoError(t, err)

		// re-queue
		err = r.Requeue(tc.serverID, tc.msg, false)
		require.NoError(t, err)

		task, err := r.conn.getTask(tc.msg.Queue, tc.msg.ID)
		require.NoError(t, err)

		require.Equal(t, scheduled, task.state)
		require.Equal(t,
			now.Add(time.Second*time.Duration(tc.msg.ReprocessAfter)).Unix(),
			task.scheduledAt)
		require.Equal(t, tc.serverID, task.sid)
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	// use utc.MockNow in order to avoid the one-second approximation of the
	// deadline that results from the delay between the start of the test
	// function and the invocation of 'Dequeue'
	now := utc.Now()
	defer utc.MockNow(now)()

	t1 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "send_email",
		Payload:  h.JSON(map[string]interface{}{"subject": "hello!"}),
		Queue:    "default",
		Timeout:  1800,
		Deadline: 0,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "export_csv",
		Payload:  nil,
		Queue:    "critical",
		Timeout:  0,
		Deadline: 1593021600, //2020-06-24T18:00:00.000Z
	}
	t2Deadline := t2.Deadline
	t3 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "reindex",
		Payload:  nil,
		Queue:    "low",
		Timeout:  int64((5 * time.Minute).Seconds()),
		Deadline: now.Add(10 * time.Minute).Unix(),
	}

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		args          []string // list of queues to query
		wantMsg       *base.TaskMessage
		wantDeadline  time.Time
		wantPending   map[string][]*base.TaskMessage
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:         []string{"default"},
			wantMsg:      t1,
			wantDeadline: utc.Unix(t1Deadline, 0).Time,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
				"low":      {t3},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t2,
			wantDeadline: utc.Unix(t2Deadline, 0).Time,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {{Message: t2, Score: t2Deadline}},
				"low":      {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t1,
			wantDeadline: utc.Unix(t1Deadline, 0).Time,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t1, Score: t1Deadline}},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {

		FlushDB(t, r.conn)
		SeedAllPendingQueues(t, r, tc.pending)

		gotMsg, gotDeadline, err := r.Dequeue("", tc.args...)
		require.NoError(t, err, "(*RQLite.Dequeue(%v) returned error %v", tc.args, err)

		if !cmp.Equal(gotMsg, tc.wantMsg) {
			t.Errorf("(*RQLite).Dequeue(%v) returned message %v; want %v",
				tc.args, gotMsg, tc.wantMsg)
			continue
		}
		//if !cmp.Equal(gotDeadline, tc.wantDeadline, cmpopts.EquateApproxTime(1*time.Second)) {
		if !cmp.Equal(gotDeadline, tc.wantDeadline) {
			t.Errorf("(*RQLite).Dequeue(%v) returned deadline %v; want %v",
				tc.args, gotDeadline, tc.wantDeadline)
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := GetPendingMessages(t, r, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			//if diff := cmp.Diff(want, gotDeadlines, cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
			if diff := cmp.Diff(want, gotDeadlines); diff != "" {
				t.Errorf("mismatch deadline found in %q: (-want,+got):\n%s", queue, diff)
			}
		}
	}
}

func TestDequeueError(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		args          []string // list of queues to query
		wantErr       error
		wantPending   map[string][]*base.TaskMessage
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			args:    []string{"default"},
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			args:    []string{"critical", "default", "low"},
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedAllPendingQueues(t, r, tc.pending)

		gotMsg, gotDeadline, gotErr := r.Dequeue("", tc.args...)
		if !errors.Is(gotErr, tc.wantErr) {
			t.Errorf("(*RQLite).Dequeue(%v) returned error %v; want %v",
				tc.args, gotErr, tc.wantErr)
			continue
		}
		if gotMsg != nil {
			t.Errorf("(*RQLite).Dequeue(%v) returned message %v; want nil", tc.args, gotMsg)
			continue
		}
		if !gotDeadline.IsZero() {
			t.Errorf("(*RQLite).Dequeue(%v) returned deadline %v; want %v", tc.args, gotDeadline, time.Time{})
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := GetPendingMessages(t, r, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			if diff := cmp.Diff(want, gotDeadlines); diff != "" {
				t.Errorf("mismatch deadline found in %q: (-want,+got):\n%s", queue, diff)
			}
		}
	}
}

func TestDequeueIgnoresPausedQueues(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	t1 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "send_email",
		Payload:  h.JSON(map[string]interface{}{"subject": "hello!"}),
		Queue:    "default",
		Timeout:  1800,
		Deadline: 0,
	}
	t2 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "export_csv",
		Payload:  nil,
		Queue:    "critical",
		Timeout:  1800,
		Deadline: 0,
	}

	tests := []struct {
		paused      []string // list of paused queues
		pending     map[string][]*base.TaskMessage
		args        []string // list of queues to query
		wantMsg     *base.TaskMessage
		wantErr     error
		wantPending map[string][]*base.TaskMessage
		wantActive  map[string][]*base.TaskMessage
	}{
		{
			paused: []string{"default"},
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: t2,
			wantErr: nil,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
			},
		},
		{
			paused: []string{"default"},
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:    []string{"default"},
			wantMsg: nil,
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			paused: []string{"critical", "default"},
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: nil,
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		//TODO: is this a problem ??
		// queues are created lazily: hence need to populate tasks first
		SeedAllPendingQueues(t, r, tc.pending)

		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatal(err)
			}
		}

		got, _, err := r.Dequeue("", tc.args...)
		if !cmp.Equal(got, tc.wantMsg) || !errors.Is(err, tc.wantErr) {
			t.Errorf("Dequeue(%v) = %v, %v; want %v, %v",
				tc.args, got, err, tc.wantMsg, tc.wantErr)
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := GetPendingMessages(t, r, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	t1 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "send_email",
		Payload:  nil,
		Timeout:  1800,
		Deadline: 0,
		Queue:    "default",
	}
	t2 := &base.TaskMessage{
		ID:       uuid.NewString(),
		Type:     "export_csv",
		Payload:  nil,
		Timeout:  0,
		Deadline: 1592485787, //2020-06-18T13:09:47.000Z
		Queue:    "custom",
	}
	t3 := &base.TaskMessage{
		ID:        uuid.NewString(),
		Type:      "reindex",
		Payload:   nil,
		Timeout:   1800,
		Deadline:  0,
		UniqueKey: "asynq:{default}:unique:reindex:nil",
		Queue:     "default",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := t2.Deadline
	t3Deadline := now.Unix() + t3.Deadline

	tests := []struct {
		desc          string
		active        map[string][]*base.TaskMessage // initial state of the active list
		deadlines     map[string][]base.Z            // initial state of deadlines set
		target        *base.TaskMessage              // task to remove
		wantActive    map[string][]*base.TaskMessage // final state of the active list
		wantDeadlines map[string][]base.Z            // final state of the deadline set
	}{
		{
			desc: "removes message from the correct queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
		{
			desc: "with one queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			desc: "with multiple messages in a queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1, t3},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t3, Score: t3Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t3,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		// fill active queues and deadline with 1 minute TTL for unique keys
		SeedAllDeadlines(t, r, tc.deadlines, time.Minute)
		SeedAllActiveQueues(t, r, tc.active)

		err := r.Done("", tc.target)
		if err != nil {
			t.Errorf("%s; (*RQLite).Done(task) = %v, want nil", tc.desc, err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got):\n%s", tc.desc, base.ActiveKey(queue), diff)
				continue
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			if diff := cmp.Diff(want, gotDeadlines); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got):\n%s", tc.desc, base.DeadlinesKey(queue), diff)
				continue
			}
		}

		gotProcessed, err := r.conn.listTasks(tc.target.Queue, processed)
		require.NoError(t, err)
		if len(gotProcessed) != 1 {
			t.Errorf("%s; GET %q, want 1", tc.desc, len(gotProcessed))
			continue
		}

		cleanupAt := gotProcessed[0].cleanupAt
		doneAt := gotProcessed[0].doneAt
		gotTTL := time.Duration(cleanupAt - doneAt)

		if gotTTL > statsTTL {
			t.Errorf("%s; TTL %q = %v, want less than or equal to %v", tc.desc, gotProcessed[0].taskUuid, gotTTL, statsTTL)
		}

		if len(tc.target.UniqueKey) > 0 && gotProcessed[0].uniqueKeyDeadline > 0 {
			t.Errorf("%s; Uniqueness lock %q still exists", tc.desc, tc.target.UniqueKey)
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	t1 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Timeout: 1800,
	}
	t2 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "export_csv",
		Payload: nil,
		Queue:   "default",
		Timeout: 3000,
	}
	t3 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "critical",
		Timeout: 80,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t3Deadline := now.Unix() + t3.Timeout

	tests := []struct {
		pending       map[string][]*base.TaskMessage // initial state of queues
		active        map[string][]*base.TaskMessage // initial state of the active list
		deadlines     map[string][]base.Z            // initial state of the deadlines set
		target        *base.TaskMessage              // task to requeue
		wantPending   map[string][]*base.TaskMessage // final state of queues
		wantActive    map[string][]*base.TaskMessage // final state of the active list
		wantDeadlines map[string][]base.Z            // final state of the deadlines set
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
				},
			},
			target: t1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
				},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			active: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
				},
			},
			target: t2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			active: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {t3},
			},
			deadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: t2Deadline}},
				"critical": {{Message: t3, Score: t3Deadline}},
			},
			target: t3,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: t2Deadline}},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		SeedAllPendingQueues(t, r, tc.pending)
		SeedAllDeadlines(t, r, tc.deadlines, 0)
		SeedAllActiveQueues(t, r, tc.active)

		err := r.Requeue("", tc.target, true)
		if err != nil {
			t.Errorf("(*RQLite).Requeue(task) = %v, want nil", err)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, qname)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.ActiveKey(qname), diff)
			}
		}
		for qname, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDeadlines); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DeadlinesKey(qname), diff)
			}
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	msg := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"subject": "hello"}))
	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
	}{
		{msg, time.Now().Add(15 * time.Minute).UTC()},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("(*RQLite).Schedule(%v, %v) = %v, want nil",
				tc.msg, tc.processAt, err)
			continue
		}

		msgs, err := r.conn.listTasks(tc.msg.Queue, scheduled)
		require.NoError(t, err)
		require.Equal(t, 1, len(msgs), "expects 1 element, got %d", len(msgs))
		require.Equal(t, msgs[0].taskUuid, tc.msg.ID)
		require.Equal(t, tc.processAt.Unix(), msgs[0].scheduledAt)

		// Check the values under the task key.
		decoded := msgs[0].msg
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s",
				decoded, tc.msg, diff)
		}
		timeout := msgs[0].taskTimeout // "timeout" field
		if want := tc.msg.Timeout; timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := msgs[0].deadline // "deadline" field
		if want := tc.msg.Deadline; deadline != want {
			t.Errorf("deadline field under task-ke is set to %v, want %v", deadline, want)
		}

		// Check queue is in the AllQueues set.
		queues, err := r.conn.listQueues(tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, 1, len(queues))
	}
}

func TestScheduleUnique(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	m1 := base.TaskMessage{
		ID:        uuid.NewString(),
		Type:      "email",
		Payload:   h.JSON(map[string]interface{}{"user_id": 123}),
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
	}

	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
		ttl       time.Duration // uniqueness lock ttl
	}{
		{&m1, time.Now().UTC().Add(15 * time.Minute), time.Minute},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)

		desc := "(*RQLite).ScheduleUnique(msg, processAt, ttl)"
		expectUniqueKeyDeadline := time.Now().UTC().Add(tc.ttl).Unix()
		err := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if err != nil {
			t.Errorf("Frist task: %s = %v, want nil", desc, err)
			continue
		}

		// Check Scheduled zset has task ID.
		msgs, err := r.conn.listTasks(tc.msg.Queue, scheduled)
		require.NoError(t, err)
		require.Equal(t, 1, len(msgs), "expects 1 element, got %d", len(msgs))
		require.Equal(t, msgs[0].taskUuid, tc.msg.ID)
		require.Equal(t, tc.processAt.Unix(), msgs[0].scheduledAt)

		// Check the values under the task key.
		decoded := msgs[0].msg
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s",
				decoded, tc.msg, diff)
		}
		timeout := msgs[0].taskTimeout // "timeout" field
		if want := tc.msg.Timeout; timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := msgs[0].deadline // "deadline" field
		if want := tc.msg.Deadline; deadline != want {
			t.Errorf("deadline field under task-ke is set to %v, want %v", deadline, want)
		}

		uniqueKey := msgs[0].uniqueKey // "unique_key" field
		if uniqueKey != tc.msg.UniqueKey {
			t.Errorf("uniqueue_key field under task key is set to %q, want %q", uniqueKey, tc.msg.UniqueKey)
		}

		// Check queue is in the AllQueues set.
		queues, err := r.conn.listQueues(tc.msg.Queue)
		require.NoError(t, err)
		require.Equal(t, 1, len(queues))

		// Enqueue the second message, should fail.
		got := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if !errors.Is(got, errors.ErrDuplicateTask) {
			t.Errorf("Second task: %s = %v, want %v", desc, got, errors.ErrDuplicateTask)
			continue
		}

		gotTTL := msgs[0].uniqueKeyDeadline
		if !cmp.Equal(expectUniqueKeyDeadline, gotTTL, cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL %q = %v, want %v", tc.msg.UniqueKey, gotTTL, tc.ttl)
			continue
		}
	}
}

func TestScheduleUniqueTaskIdConflictError(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	m1 := base.TaskMessage{
		ID:        "custom_id",
		Type:      "foo",
		Payload:   nil,
		UniqueKey: "unique_key_one",
	}
	m2 := base.TaskMessage{
		ID:        "custom_id",
		Type:      "bar",
		Payload:   nil,
		UniqueKey: "unique_key_two",
	}
	const ttl = 30 * time.Second
	processAt := time.Now().Add(30 * time.Second)

	tests := []struct {
		firstMsg  *base.TaskMessage
		secondMsg *base.TaskMessage
	}{
		{firstMsg: &m1, secondMsg: &m2},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn) // clean up db before each test case.

		if err := r.ScheduleUnique(tc.firstMsg, processAt, ttl); err != nil {
			t.Errorf("First message: ScheduleUnique failed: %v", err)
			continue
		}
		if err := r.ScheduleUnique(tc.secondMsg, processAt, ttl); !errors.Is(err, errors.ErrTaskIdConflict) {
			t.Errorf("Second message: ScheduleUnique returned %v, want %v", err, errors.ErrTaskIdConflict)
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	t1 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: h.JSON(map[string]interface{}{"subject": "Hola!"}),
		Retried: 10,
		Timeout: 1800,
		Queue:   "default",
	}
	t2 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "gen_thumbnail",
		Payload: h.JSON(map[string]interface{}{"path": "some/path/to/image.jpg"}),
		Timeout: 3000,
		Queue:   "default",
	}
	t3 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "reindex",
		Payload: nil,
		Timeout: 60,
		Queue:   "default",
	}
	t4 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_notification",
		Payload: nil,
		Timeout: 1800,
		Queue:   "custom",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server is not responding"

	tests := []struct {
		active        map[string][]*base.TaskMessage
		deadlines     map[string][]base.Z
		retry         map[string][]base.Z
		msg           *base.TaskMessage
		processAt     time.Time
		errMsg        string
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
		getWantRetry  func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: now.Add(time.Minute).Unix()}},
			},
			msg:       t1,
			processAt: now.Add(5 * time.Minute).Time,
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageAfterRetry(*t1, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
						{Message: t3, Score: now.Add(time.Minute).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {{Message: t4, Score: t4Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			msg:       t4,
			processAt: now.Add(5 * time.Minute).Time,
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						{Message: h.TaskMessageAfterRetry(*t4, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {

		FlushDB(t, r.conn)
		SeedAllDeadlines(t, r, tc.deadlines, 0)
		SeedAllActiveQueues(t, r, tc.active)
		SeedAllRetryQueues(t, r, tc.retry)

		callTime := now // time when method is called
		err := r.Retry(tc.msg, tc.processAt, tc.errMsg, true)
		if err != nil {
			t.Errorf("(*RQLite).Retry = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			if diff := cmp.Diff(want, gotDeadlines, SortDeadlineEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		cmpOpts := []cmp.Option{
			SortDeadlineEntryOpt,
			//cmpopts.EquateApproxTime(5 * time.Second), // for LastFailedAt field
		}
		wantRetry := tc.getWantRetry(callTime.Time)
		for queue, want := range wantRetry {
			gotRetry := GetRetryEntries(t, r, queue)
			if diff := cmp.Diff(want, gotRetry, cmpOpts...); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(queue), diff)
			}
		}

		/* PENDING(GIL): to revisit. In redis world there's a key associated with
		    the queue put in 'processed' state with an expiration after 90 days
		    that tracks count of processed tasks => add column 'processed_tasks'
		msgs, err := listTasks(r.conn, tc.msg.Queue, processed)
		require.NoError(t, err)
		require.Equal(t, 1, len(msgs))
		gotProcessed := msgs[0]

		cleanupAt := gotProcessed.cleanupAt
		doneAt := gotProcessed.doneAt
		gotTTL := time.Duration(cleanupAt - doneAt)

		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", gotProcessed.taskUuid, gotTTL, statsTTL)
		}
		require.True(t, gotProcessed.failed)
		*/
	}
}

func TestRetryWithNonFailureError(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: h.JSON(map[string]interface{}{"subject": "Hola!"}),
		Retried: 10,
		Timeout: 1800,
		Queue:   "default",
	}
	t2 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "gen_thumbnail",
		Payload: h.JSON(map[string]interface{}{"path": "some/path/to/image.jpg"}),
		Timeout: 3000,
		Queue:   "default",
	}
	t3 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "reindex",
		Payload: nil,
		Timeout: 60,
		Queue:   "default",
	}
	t4 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_notification",
		Payload: nil,
		Timeout: 1800,
		Queue:   "custom",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server is not responding"

	tests := []struct {
		active        map[string][]*base.TaskMessage
		deadlines     map[string][]base.Z
		retry         map[string][]base.Z
		msg           *base.TaskMessage
		processAt     time.Time
		errMsg        string
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
		getWantRetry  func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: now.Add(time.Minute).Unix()}},
			},
			msg:       t1,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						// Task message should include the error message but without incrementing the retry count.
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
						{Message: t3, Score: now.Add(time.Minute).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {{Message: t4, Score: t4Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			msg:       t4,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						// Task message should include the error message but without incrementing the retry count.
						{Message: h.TaskMessageWithError(*t4, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedAllActiveQueues(t, r, tc.active, true)
		SeedAllDeadlines(t, r, tc.deadlines, 0)
		SeedAllRetryQueues(t, r, tc.retry)

		callTime := time.Now() // time when method was called
		err := r.Retry(tc.msg, tc.processAt, tc.errMsg, false /*isFailure*/)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		cmpOpts := []cmp.Option{
			h.SortZSetEntryOpt,
			cmpopts.EquateApproxTime(5 * time.Second), // for LastFailedAt field
		}
		wantRetry := tc.getWantRetry(callTime)
		for queue, want := range wantRetry {
			gotRetry := GetRetryEntries(t, r, queue)
			if diff := cmp.Diff(want, gotRetry, cmpOpts...); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(queue), diff)
			}
		}

		// If isFailure is set to false, no stats should be recorded to avoid skewing the error rate.
		gotProcessed := GetProcessedMessages(t, r, tc.msg.Queue)
		if len(gotProcessed) != 0 {
			t.Errorf("GET 'processed' in queue %s = %d, want empty",
				tc.msg.Queue, len(gotProcessed))
		}
	}
}

func TestArchive(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	t1 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "reindex",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 3000,
	}
	t2Deadline := now.Unix() + t2.Timeout
	t3 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "generate_csv",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 60,
	}
	t3Deadline := now.Unix() + t3.Timeout
	t4 := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "custom",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server not responding"

	tests := []struct {
		active          map[string][]*base.TaskMessage
		deadlines       map[string][]base.Z
		archived        map[string][]base.Z
		target          *base.TaskMessage // task to archive
		wantActive      map[string][]*base.TaskMessage
		wantDeadlines   map[string][]base.Z
		getWantArchived func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {
					{Message: t3, Score: now.Add(-time.Hour).Unix()},
				},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: failedAt.Unix()},
						{Message: t3, Score: now.Add(-time.Hour).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: failedAt.Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
				},
				"custom": {
					{Message: t4, Score: t4Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			target: t4,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						{Message: h.TaskMessageWithError(*t4, errMsg, failedAt), Score: failedAt.Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {

		FlushDB(t, r.conn)
		SeedAllDeadlines(t, r, tc.deadlines, 0)
		SeedAllActiveQueues(t, r, tc.active)
		SeedAllArchivedQueues(t, r, tc.archived)

		callTime := now // record time `Archive` was called
		err := r.Archive(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RQLite).Archive(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := GetActiveMessages(t, r, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := GetDeadlinesEntries(t, r, queue)
			if diff := cmp.Diff(want, gotDeadlines, SortDeadlineEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q after calling (*RQLite).Archive: (-want, +got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		for queue, want := range tc.getWantArchived(callTime.Time) {
			gotArchived := GetArchivedEntries(t, r, queue)
			//if diff := cmp.Diff(want, gotArchived, SortDeadlineEntryOpt, zScoreCmpOpt, timeCmpOpt); diff != "" {
			if diff := cmp.Diff(want, gotArchived, SortDeadlineEntryOpt); diff != "" {

				t.Errorf("mismatch found in %q after calling (*RQLite).Archive: (-want, +got):\n%s", base.ArchivedKey(queue), diff)
			}
		}

		/* PENDING(GIL): to revisit. see same comment in TestRetry ==
		msgs, err := listTasks(r.conn, tc.target.Queue, processed)
		require.NoError(t, err)
		require.Equal(t, 1, len(msgs))
		gotProcessed := msgs[0]

		cleanupAt := gotProcessed.cleanupAt
		doneAt := gotProcessed.doneAt
		gotTTL := time.Duration(cleanupAt - doneAt)

		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", gotProcessed.taskUuid, gotTTL, statsTTL)
		}
		require.True(t, gotProcessed.failed)
		*/
	}
}

func TestForwardIfReady(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("generate_csv", nil)
	t3 := h.NewTaskMessage("gen_thumbnail", nil)
	t4 := h.NewTaskMessageWithQueue("important_task", nil, "critical")
	t5 := h.NewTaskMessageWithQueue("minor_task", nil, "low")

	now := utc.Now()
	defer utc.MockNow(now)()

	secondAgo := now.Add(-time.Second)
	hourFromNow := now.Add(time.Hour)

	tests := []struct {
		scheduled     map[string][]base.Z
		retry         map[string][]base.Z
		qnames        []string
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]*base.TaskMessage
		wantRetry     map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: secondAgo.Unix()},
					{Message: t2, Score: secondAgo.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: hourFromNow.Unix()},
					{Message: t2, Score: secondAgo.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: hourFromNow.Unix()},
					{Message: t2, Score: hourFromNow.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: hourFromNow.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t3},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default":  {{Message: t1, Score: secondAgo.Unix()}},
				"critical": {{Message: t4, Score: secondAgo.Unix()}},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {{Message: t5, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default", "critical", "low"},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t4},
				"low":      {t5},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedAllScheduledQueues(t, r, tc.scheduled)
		SeedAllRetryQueues(t, r, tc.retry)

		err := r.ForwardIfReady(tc.qnames...)
		if err != nil {
			t.Errorf("(*RQLite).CheckScheduled(%v) = %v, want nil", tc.qnames, err)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := GetScheduledMessages(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}
		for qname, want := range tc.wantRetry {
			gotRetry := GetRetryMessages(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}
	}
}

func newCompletedTask(qname, typename string, payload []byte, completedAt utc.UTC) *base.TaskMessage {
	msg := h.NewTaskMessageWithQueue(typename, payload, qname)
	msg.CompletedAt = completedAt.Unix()
	return msg
}

func TestDeleteExpiredCompletedTasks(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Unix(1674591000, 0)
	defer utc.MockNow(now)()

	secondAgo := now.Add(-time.Second)
	hourFromNow := now.Add(time.Hour)
	hourAgo := now.Add(-time.Hour)
	minuteAgo := now.Add(-time.Minute)

	t1 := newCompletedTask("default", "task1", nil, hourAgo)
	t2 := newCompletedTask("default", "task2", nil, minuteAgo)
	t3 := newCompletedTask("default", "task3", nil, secondAgo)
	t4 := newCompletedTask("critical", "critical_task", nil, hourAgo)
	t5 := newCompletedTask("low", "low_priority_task", nil, hourAgo)

	tests := []struct {
		desc          string
		completed     map[string][]base.Z
		qname         string
		wantCompleted map[string][]base.Z
	}{
		{
			desc: "deletes expired task from default queue",
			completed: map[string][]base.Z{
				"default": {
					{Message: t1, Score: secondAgo.Unix()},
					{Message: t2, Score: hourFromNow.Unix()},
					{Message: t3, Score: now.Unix()},
				},
			},
			qname: "default",
			wantCompleted: map[string][]base.Z{
				"default": {
					{Message: t2, Score: hourFromNow.Unix()},
					// the rdb test does not have t3 because we use utc.Mock
					// and time stopped flowing
					{Message: t3, Score: now.Unix()},
				},
			},
		},
		{
			desc: "deletes expired task from specified queue",
			completed: map[string][]base.Z{
				"default": {
					{Message: t2, Score: secondAgo.Unix()},
				},
				"critical": {
					{Message: t4, Score: secondAgo.Unix()},
				},
				"low": {
					{Message: t5, Score: now.Unix()},
				},
			},
			qname: "critical",
			wantCompleted: map[string][]base.Z{
				"default": {
					{Message: t2, Score: secondAgo.Unix()},
				},
				"critical": {},
				"low": {
					{Message: t5, Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedAllCompletedQueues(t, r, tc.completed)

		if err := r.DeleteExpiredCompletedTasks(tc.qname); err != nil {
			t.Errorf("DeleteExpiredCompletedTasks(%q) failed: %v", tc.qname, err)
			continue
		}

		for qname, want := range tc.wantCompleted {
			got := GetCompletedEntries(t, r, qname)
			if diff := cmp.Diff(want, got, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s: diff found in %q completed set: want=%v, got=%v\n%s", tc.desc, qname, want, got, diff)
			}
		}
	}
}

func TestListDeadlineExceeded(t *testing.T) {
	t1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	t2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	t3 := h.NewTaskMessageWithQueue("task3", nil, "critical")

	now := utc.Now()
	defer utc.MockNow(now)()

	oneHourFromNow := now.Add(1 * time.Hour)
	fiveMinutesFromNow := now.Add(5 * time.Minute)
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		desc      string
		deadlines map[string][]base.Z
		qnames    []string
		t         time.Time
		want      []*base.TaskMessage
	}{
		{
			desc: "with a single active task",
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: fiveMinutesAgo.Unix()}},
			},
			qnames: []string{"default"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple active tasks, and one expired",
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: fiveMinutesFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: oneHourFromNow.Unix()},
				},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple expired active tasks",
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: oneHourFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: fiveMinutesAgo.Unix()},
				},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1, t3},
		},
		{
			desc: "with empty active queue",
			deadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{},
		},
	}

	r := setup(t)
	defer func() { _ = r.Close() }()

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedAllDeadlines(t, r, tc.deadlines, 0)

		got, err := r.ListDeadlineExceeded(tc.t, tc.qnames...)
		if err != nil {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned error: %v", tc.desc, tc.t, err)
			continue
		}

		if diff := cmp.Diff(tc.want, got, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned %v, want %v;(-want,+got)\n%s",
				tc.desc, tc.t, got, tc.want, diff)
		}
	}
}

func assertServer(t *testing.T, info *base.ServerInfo, srv *serverRow, ttl time.Duration) {
	if diff := cmp.Diff(info, srv.server); diff != "" {
		t.Errorf("persisted ServerInfo was %v, want %v; (-want,+got)\n%s",
			srv.server, info, diff)
	}
	// Check ServerInfo TTL was set correctly.
	expectExpireAt := utc.Now().Add(ttl).Unix()
	if !cmp.Equal(expectExpireAt, srv.expireAt, cmpopts.EquateApprox(0, 1)) {
		t.Errorf("Expiration of %q was %v, want %v", srv.sid, expectExpireAt, srv.expireAt)
	}

	// Check WorkersInfo was written correctly.
	require.Equal(t, info.ServerID, srv.sid)
	require.Equal(t, info.Host, srv.host)
	require.Equal(t, info.PID, srv.pid)
}

func TestWriteServerState(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	var (
		ttl = 5 * time.Second
	)

	info := base.ServerInfo{
		Host:              "localhost",
		PID:               4242,
		ServerID:          "server123",
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           now.Time,
		Status:            "active",
		ActiveWorkerCount: 0,
	}

	err := r.WriteServerState(&info, nil /* workers */, ttl)
	if err != nil {
		t.Errorf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	srvs, err := r.conn.getServer(&info)
	require.NoError(t, err)
	require.Equal(t, 1, len(srvs))
	assertServer(t, &info, srvs[0], ttl)
}

func TestWriteServerStateWithWorkers(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	var (
		host = "127.0.0.1"
		pid  = 4242

		msg1 = h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"user_id": "123"}))
		msg2 = h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"path": "some/path/to/imgfile"}))

		ttl = 5 * time.Second
	)

	workers := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID,
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: now.Add(-10 * time.Second).Time,
		},
		{
			Host:    host,
			PID:     pid,
			ID:      msg2.ID,
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: now.Add(-2 * time.Minute).Time,
		},
	}

	serverInfo := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          "server123",
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           now.Add(-10 * time.Minute).Time,
		Status:            "active",
		ActiveWorkerCount: len(workers),
	}

	err := r.WriteServerState(&serverInfo, workers, ttl)
	if err != nil {
		t.Fatalf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	srvs, err := r.conn.getServer(&serverInfo)
	require.NoError(t, err)
	require.Equal(t, 1, len(srvs))
	assertServer(t, &serverInfo, srvs[0], ttl)

	// Check WorkersInfo was written correctly.
	workerRows, err := r.conn.getWorkers(serverInfo.ServerID)
	require.NoError(t, err)
	require.Equal(t, 2, len(workerRows))

	var gotWorkers []*base.WorkerInfo
	for _, w := range workerRows {
		gotWorkers = append(gotWorkers, w.worker)
	}
	if diff := cmp.Diff(workers, gotWorkers, h.SortWorkerInfoOpt); diff != "" {
		t.Errorf("persisted workers info was %v, want %v; (-want,+got)\n%s",
			gotWorkers, workers, diff)
	}

	// Check WorkersInfo TTL was set correctly.
	expectExpireAt := now.Add(ttl).Unix()
	for _, w := range workerRows {
		if !cmp.Equal(expectExpireAt, w.expireAt, cmpopts.EquateApprox(0, 1)) {
			t.Errorf("Expiration of %q was %v, want %v", w.taskUuid, expectExpireAt, w.expireAt)
		}
	}
}

func TestClearServerState(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	now := utc.Now()
	defer utc.MockNow(now)()

	var (
		host     = "127.0.0.1"
		pid      = 1234
		serverID = "server123"

		otherHost     = "127.0.0.2"
		otherPID      = 9876
		otherServerID = "server987"

		msg1 = h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"user_id": "123"}))
		msg2 = h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"path": "some/path/to/imgfile"}))

		ttl = 5 * time.Second
	)

	workers1 := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID,
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: now.Add(-10 * time.Second).Time,
		},
	}
	serverInfo1 := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           now.Add(-10 * time.Minute).Time,
		Status:            "active",
		ActiveWorkerCount: len(workers1),
	}

	workers2 := []*base.WorkerInfo{
		{
			Host:    otherHost,
			PID:     otherPID,
			ID:      msg2.ID,
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: now.Add(-30 * time.Second).Time,
		},
	}
	serverInfo2 := base.ServerInfo{
		Host:              otherHost,
		PID:               otherPID,
		ServerID:          otherServerID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           now.Add(-15 * time.Minute).Time,
		Status:            "active",
		ActiveWorkerCount: len(workers2),
	}

	// Write server and workers data.
	if err := r.WriteServerState(&serverInfo1, workers1, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}
	srvs1, err := r.conn.getServer(&serverInfo1)
	require.NoError(t, err)
	require.Equal(t, 1, len(srvs1))

	workerRows1, err := r.conn.getWorkers(serverInfo1.ServerID)
	require.NoError(t, err)
	require.Equal(t, 1, len(workerRows1))

	if err := r.WriteServerState(&serverInfo2, workers2, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}
	srvs2, err := r.conn.getServer(&serverInfo1)
	require.NoError(t, err)
	require.Equal(t, 1, len(srvs2))

	workerRows2, err := r.conn.getWorkers(serverInfo2.ServerID)
	require.NoError(t, err)
	require.Equal(t, 1, len(workerRows2))

	err = r.ClearServerState(host, pid, serverID)
	require.NoError(t, err, "(*RQLite).ClearServerState failed")

	srvs, err := r.conn.listAllServers()
	require.NoError(t, err)
	require.Equal(t, 1, len(srvs))
	require.Equal(t, otherServerID, srvs[0].sid)
	require.Equal(t, otherPID, srvs[0].pid)

	workerRows, err := r.conn.listAllWorkers()
	require.NoError(t, err)
	require.Equal(t, 1, len(workerRows))
	require.Equal(t, workers2[0].ID, workerRows[0].taskUuid)

}

func TestCancelationPubSub(t *testing.T) {

	r := setup(t)
	defer func() { _ = r.Close() }()

	pubsub, err := r.CancelationPubSub()
	if err != nil {
		t.Fatalf("(*RQLite).CancelationPubSub() returned an error: %v", err)
	}

	cancelCh := pubsub.Channel()

	var (
		mu       sync.Mutex
		received []string
	)

	go func() {
		for msg := range cancelCh {
			mu.Lock()
			received = append(received, msg.(string))
			mu.Unlock()
		}
	}()

	publish := []string{"one", "two", "three"}

	for _, msg := range publish {
		err = r.PublishCancelation(msg)
		require.NoError(t, err)
	}

	// allow for message to reach subscribers.
	time.Sleep(time.Second)

	err = pubsub.Close()
	require.NoError(t, err)

	mu.Lock()
	if diff := cmp.Diff(publish, received, h.SortStringSliceOpt); diff != "" {
		t.Errorf("subscriber received %v, want %v; (-want,+got)\n%s", received, publish, diff)
	}
	mu.Unlock()
}

func TestWriteResult(t *testing.T) {
	r := setup(t)
	defer func() { _ = r.Close() }()

	tests := []struct {
		qname  string
		taskID string
		data   []byte
	}{
		{
			qname:  "default",
			taskID: uuid.NewString(),
			data:   []byte("hello"),
		},
	}

	for _, tc := range tests {
		FlushDB(t, r.conn)
		SeedActiveQueue(t, r, []*base.TaskMessage{
			{
				Type:  "x",
				ID:    tc.taskID,
				Queue: tc.qname,
			},
		}, tc.qname, true)

		n, err := r.WriteResult(tc.qname, tc.taskID, tc.data)
		if err != nil {
			t.Errorf("WriteResult failed: %v", err)
			continue
		}
		if n != len(tc.data) {
			t.Errorf("WriteResult returned %d, want %d", n, len(tc.data))
		}

		ti, err := r.GetTaskInfo(tc.qname, tc.taskID)
		if err != nil {
			t.Errorf("GetTaskInfo failed: %v", err)
			continue
		}

		if string(ti.Result) != string(tc.data) {
			t.Errorf("`result` field under queue %s, taks %s is set to %q, want %q",
				tc.qname, tc.taskID, string(ti.Result), string(tc.data))
		}
	}
}

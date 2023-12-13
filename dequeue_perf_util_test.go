package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type testCase struct {
	testDir       string
	preserveDir   bool
	preloadCount  int
	taskCount     int
	enqueuePerSec int
	enqueuePrefix string
	usedQueues    []string
}

type testCtx struct {
	*testing.T
	*require.Assertions
	testDir string

	srv           *Server
	mux           *ServeMux
	client        *Client
	usedQueues    []string
	preloadCount  int
	enqueuePrefix string
	taskCount     int
	enqueuePerSec int
}

func makeConnOpt(t *testing.T, brokerType string, testDir string, fl log.Base) ClientConnOpt {
	switch brokerType {
	case SqliteType:
		return &RqliteConnOpt{
			Config: &RqliteConfig{
				Type:                  SqliteType,
				SqliteDbPath:          filepath.Join(testDir, "db.sqlite"),
				SqliteInMemory:        false,
				SqliteTracing:         false,
				TablesPrefix:          "ltaskq",
				PubsubPollingInterval: time.Second,
			},
			Log: fl,
		}
	case RedisType:
		ret := RedisClientOpt{
			Addr: redisAddr,
			DB:   redisDB,
			Log:  fl,
		}
		tb := &redisTestContext{
			tb: t,
			r:  ret.MakeClient().(redis.UniversalClient),
		}
		tb.FlushDB()
		return ret
	}
	t.Fatalf("unsupported broker type %s for test", brokerType)
	return nil
}

func newTestCtx(t *testing.T, tc *testCase, brokerType string, srvQueues map[string]int, priorityType Priority) *testCtx {
	require.True(t, tc.taskCount > 0)

	testDir := tc.testDir
	dirCleanup := func() {}
	var err error
	if testDir == "" {
		testDir, dirCleanup, err = MkdirTemp("test-localq-service")
		require.NoError(t, err)
	}

	fmt.Println("test dir", testDir)
	fl, err := newFileLogger(filepath.Join(testDir, "asynq.log"))
	require.NoError(t, err)
	logger := log.NewLogger(fl)

	usedQueues := tc.usedQueues
	require.True(t, len(usedQueues) > 0)

	ctx := &testCtx{
		T:             t,
		Assertions:    require.New(t),
		preloadCount:  tc.preloadCount,
		testDir:       testDir,
		enqueuePrefix: tc.enqueuePrefix,
		taskCount:     tc.taskCount,
		enqueuePerSec: tc.enqueuePerSec,
		usedQueues:    usedQueues,
	}

	// start and configure cleanup
	ctx.srv = NewServer(
		makeConnOpt(t, brokerType, testDir, fl),
		Config{
			ServerID:               "",
			Concurrency:            5000,
			RetryDelayFunc:         nil,
			IsFailure:              nil,
			Queues:                 NewQueuesConfig(srvQueues, priorityType),
			ErrorHandler:           nil,
			Logger:                 logger,
			LogLevel:               DebugLevel,
			ShutdownTimeout:        time.Second * 8,
			HealthCheckFunc:        nil,
			HealthCheckInterval:    time.Second * 15,
			ProcessorEmptyQSleep:   time.Second,
			RecovererInterval:      time.Minute,
			RecovererExpiration:    time.Second * 30,
			ForwarderInterval:      time.Second * 5,
			HeartBeaterInterval:    time.Second * 5,
			SyncerInterval:         time.Second * 5,
			SubscriberRetryTimeout: time.Second * 5,
			JanitorInterval:        0,
		})

	ctx.mux = NewServeMux()
	err = ctx.srv.Start(ctx.mux)
	require.NoError(t, err)
	ctx.client = NewClientWithBroker(ctx.srv.broker)

	t.Cleanup(func() {
		ctx.srv.Stop()
		ctx.srv.Shutdown()
		if !t.Failed() {
			if !tc.preserveDir {
				dirCleanup()
			}
		} else {
			fmt.Println("test failed - retaining test dir " + testDir)
		}
	})

	return ctx
}

// mayPreloadQQueue preloads with Q such as to have rows in completed
func (t *testCtx) mayPreloadQQueue(d *distributor) bool {
	if t.preloadCount <= 0 {
		return false
	}

	//d, err := newDistributor(t.mux, t.client, true)
	//t.NoError(err)
	wg := d.waitGroup()
	wg.Add(t.preloadCount)

	w := StartWatch()
	count := 0
	for count < t.preloadCount {
		for _, queue := range []string{QDist} {
			err := d.enqueue(queue, fmt.Sprintf("preload-%d", count))
			t.NoError(err)
			count++
		}
	}
	fmt.Println("enqueued (preload)", count, "duration", w.Duration())

	wg.Wait()
	fmt.Println("finished (preload) after", w.Duration(), "stats", d.dh.statsString())
	return true
}

func (t *testCtx) enqueueAtRate(d *distributor) (map[string]*tasksList, error) {
	taskCount := t.taskCount
	enqueuePerSec := t.enqueuePerSec

	prefix := t.enqueuePrefix
	if prefix == "" {
		prefix = "t"
	}

	w := StartWatch()
	count := 0
	perSec := float64(time.Second) / float64(enqueuePerSec)
	ticker := time.NewTicker(time.Duration(perSec))
	defer ticker.Stop()

	ret := make(map[string]*tasksList)
	for _, queue := range t.usedQueues {
		ret[queue] = &tasksList{}
	}
	var err error

out:
	for {
		select {
		case <-ticker.C:
			for _, queue := range t.usedQueues {
				taskId := fmt.Sprintf("%s-%s-%05d", prefix, queue, count)
				err = d.enqueue(queue, taskId)
				if err != nil {
					return nil, err
				}
				count++
				tasks := ret[queue]
				tasks.addTask(taskId)
				if count == taskCount {
					break out
				}
			}
		}
	}

	fmt.Println("enqueued", count, "duration", w.Duration())
	return ret, nil
}

type distributor struct {
	config *DistServiceConfig
	mux    *ServeMux
	client *Client
	dh     *dhandler
	wg     *sync.WaitGroup
}

func newDistributor(mux *ServeMux, client *Client, register ...bool) (ret *distributor, err error) {
	// asynq panics rather than returning an error
	defer func() {
		if ex := recover(); ex != nil {
			ok := false
			if err, ok = ex.(error); ok {
			} else {
				err = fmt.Errorf("%v", ex)
			}
		}
	}()

	wg := &sync.WaitGroup{}
	dh := newDhandler(wg)
	ret = &distributor{
		config: (&DistServiceConfig{}).InitDefaults(),
		mux:    mux,
		client: client,
		dh:     dh,
		wg:     wg,
	}
	if len(register) > 0 && !register[0] {
		return ret, nil
	}
	mux.HandleFunc(QPartLiveNodeDist, dh.distributeQPartNodeLive)
	mux.HandleFunc(QPartNodeDist, dh.distributeQPartNode)
	mux.HandleFunc(QPartLiveDist, dh.distributeQPartLive)
	mux.HandleFunc(QPartDist, dh.distributeQPart)
	mux.HandleFunc(QDist, dh.distributeQ)
	return ret, nil
}

func (d *distributor) waitGroup() *sync.WaitGroup {
	return d.wg
}

func (d *distributor) addTasks(count int) {
	d.wg.Add(count)
}

func (d *distributor) wait() {
	d.wg.Wait()
}

func (d *distributor) enqueue(queue string, task string) error {
	var conf QDistConfig

	switch queue {
	case QPartLiveNodeDist, QPartNodeDist:
		conf = d.config.QPartNode
	case QPartLiveDist, QPartDist:
		conf = d.config.QPart
	case QDist:
		conf = d.config.Q
	default:
		return fmt.Errorf("unknown queue: %s", queue)
	}
	nfo, err := d.client.Enqueue(
		NewTask(queue, []byte(task)),
		Queue(queue),
		TaskID(task),
		Timeout(conf.Timeout),
		MaxRetry(conf.MaxRetry),
		Retention(conf.Retention))
	if err != nil {
		return errors.E("enqueue", err)
	}
	_ = nfo
	d.dh.incExpected()
	d.dh.queued.Store(task, StartWatch())
	return nil
}

type handlerStats struct {
	mu    sync.Mutex
	min   time.Duration
	max   time.Duration
	avg   float64
	count int64
}

func (h *handlerStats) add(queueElapsed time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	avg := h.avg * float64(h.count)
	h.count++
	h.avg = (avg + float64(queueElapsed)) / float64(h.count)
	if queueElapsed < h.min {
		h.min = queueElapsed
	}
	if queueElapsed > h.max {
		h.max = queueElapsed
	}
}

type hStats struct {
	EnqueuePerSec int   `json:"enqueue_per_sec,omitempty"`
	Min           int64 `json:"min"`
	Max           int64 `json:"max"`
	Avg           int64 `json:"avg"`
	Count         int64 `json:"count"`
}

func (s *hStats) csvLine() string {
	return fmt.Sprintf("%d,%d,%d,%d,", s.EnqueuePerSec, s.Count, s.Max, s.Avg)
}

func (h *handlerStats) stats() *hStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &hStats{
		Min:   h.min.Milliseconds(),
		Max:   h.max.Milliseconds(),
		Avg:   time.Duration(math.Round(h.avg)).Milliseconds(),
		Count: h.count,
	}
}

type tasksList struct {
	tasks []string
	mu    sync.Mutex
}

func (l *tasksList) addTask(taskId string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tasks = append(l.tasks, taskId)
}

func (l *tasksList) expectEquals(o *tasksList) error {
	if l == nil && o == nil {
		return nil
	}
	if l == nil {
		return fmt.Errorf("list is nil")
	}
	if o == nil {
		return fmt.Errorf("other is nil")
	}
	if len(l.tasks) != len(o.tasks) {
		return fmt.Errorf("len tasks of tasks differ - list %d, other: %d", len(l.tasks), len(o.tasks))
	}

	// given tasks are dispatched on separate goroutines, we cannot be sure they
	// are received in the same order as dispatched.
	sort.Strings(o.tasks)

	var errs []string
	for i, t := range l.tasks {
		if o.tasks[i] != t {
			errs = append(errs, fmt.Sprintf("index %d - list %s - other: %s", i, t, o.tasks[i]))
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("tasks differ \n%s", strings.Join(errs, "\n"))
	}
	return nil
}

type dhandler struct {
	queued        sync.Map // [q string] -> [queued stopwatch]
	wg            *sync.WaitGroup
	expectedCount *atomic.Int64
	count         *atomic.Int64
	stats         map[string]*handlerStats
	tasks         map[string]*tasksList
}

func newDhandler(wg *sync.WaitGroup) *dhandler {
	stats := make(map[string]*handlerStats)
	tasks := make(map[string]*tasksList)
	for _, typ := range []string{QPartNodeDist, QPartLiveNodeDist, QPartDist, QPartLiveDist, QDist} {
		stats[typ] = &handlerStats{
			min: time.Hour,
		}
		tasks[typ] = &tasksList{}
	}
	return &dhandler{
		wg:            wg,
		expectedCount: atomic.NewInt64(0),
		count:         atomic.NewInt64(0),
		stats:         stats,
		tasks:         tasks,
	}
}

func (d *dhandler) Stats(perSec int) map[string]*hStats {
	stats := make(map[string]*hStats)
	for k, v := range d.stats {
		hs := v.stats()
		if hs.Count > 0 {
			hs.EnqueuePerSec = perSec
			stats[k] = hs
		}
	}
	return stats
}

func (d *dhandler) statsString() string {
	stats := d.Stats(0)
	ret, _ := json.MarshalIndent(stats, "", "  ")
	return string(ret)
}

func (d *dhandler) incExpected() {
	d.expectedCount.Add(1)
}

func (d *dhandler) distribute(typ string, t *Task) error {
	tasksId := string(t.Payload())
	val, loaded := d.queued.LoadAndDelete(tasksId)
	if !loaded {
		return errors.E("distribute", "reason", "task not found")
	}
	queued, ok := val.(*StopWatch)
	if !ok || queued == nil {
		return errors.E("distribute", "reason", "stopwatch not found")
	}
	queued.Stop()
	queueElapsed := queued.Duration()
	stats := d.stats[typ]
	if stats == nil {
		return errors.E("distribute", "reason", "stats not found")
	}
	stats.add(queueElapsed)

	tasks := d.tasks[typ]
	if tasks == nil {
		return errors.E("distribute", "reason", "tasks not found")
	}
	tasks.addTask(tasksId)

	count := d.count.Add(1)
	_ = count

	d.wg.Done()
	return nil
}

func (d *dhandler) distributeQPartNode(_ context.Context, t *Task) error {
	return d.distribute(QPartNodeDist, t)
}

func (d *dhandler) distributeQPartNodeLive(_ context.Context, t *Task) error {
	return d.distribute(QPartLiveNodeDist, t)
}

func (d *dhandler) distributeQPart(_ context.Context, t *Task) error {
	return d.distribute(QPartDist, t)
}

func (d *dhandler) distributeQPartLive(_ context.Context, t *Task) error {
	return d.distribute(QPartLiveDist, t)
}

func (d *dhandler) distributeQ(_ context.Context, t *Task) error {
	return d.distribute(QDist, t)
}

type DistServiceConfig struct {
	Disabled  bool        `json:"disabled,omitempty"` // for tests
	Q         QDistConfig `json:"content"`            // content distribution
	QPart     QDistConfig `json:"part"`               // part distribution
	QPartNode QDistConfig `json:"node"`               // part-to-node distribution
}

type QDistConfig struct {
	Timeout   time.Duration `json:"timeout"`   // max execution time of the distribution
	MaxRetry  int           `json:"max_retry"` // max number of times the distribution will be retried after failure
	Retention time.Duration `json:"retention"` // duration for which the distribution will be retained after success
}

func (c *DistServiceConfig) InitDefaults() *DistServiceConfig {
	c.Q.Timeout = time.Hour * 24
	c.Q.MaxRetry = 25
	c.Q.Retention = time.Hour * 4320 // 180 days
	c.QPart.Timeout = time.Hour
	c.QPart.MaxRetry = 25
	c.QPart.Retention = time.Hour * 720 // 30 days
	c.QPartNode.Timeout = time.Minute * 10
	c.QPartNode.MaxRetry = 25
	c.QPartNode.Retention = time.Hour * 720 // 30 days
	return c
}

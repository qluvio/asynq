package asynq

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/rqlite"
	"github.com/stretchr/testify/require"
)

func TestSqliteRestartWithWalChanged(t *testing.T) {
	if testing.Short() && brokerType != SqliteType {
		t.Skip("sqlite only")
	}
	cfg := NewRqliteConfig()
	cfg.Type = SqliteType
	cfg.Sqlite.InMemory = false
	dir, err := os.MkdirTemp("", "TestSqliteRestartWithWalChanged")
	require.NoError(t, err)
	fmt.Println("test dir: " + dir)

	defer func() {
		if t.Failed() {
			fmt.Println("preserving test dir: " + dir)
			return
		}
		_ = os.RemoveAll(dir)
	}()
	dbFiles := func() []string {
		d, err := os.Open(dir)
		require.NoError(t, err)
		files, err := d.Readdirnames(-1)
		require.NoError(t, err)
		return files
	}
	assertSingleDbFile := func(single bool) {
		files := dbFiles()
		if single {
			require.Equal(t, 1, len(files))
			require.Equal(t, "db.sqlite", files[0])
		} else {
			require.Equal(t, 3, len(files))
		}
	}

	queue := "queue"
	taskId := "mail_1234"

	type testCase struct {
		startWal bool
	}
	for _, tc := range []*testCase{
		{startWal: true},
		{startWal: false},
	} {
		dbPath := filepath.Join(dir, "db.sqlite")
		err = os.RemoveAll(dir)
		require.NoError(t, err)
		err = os.Mkdir(dir, os.ModePerm)
		require.NoError(t, err)

		cfg.Sqlite.DbPath = dbPath
		cfg.Sqlite.DisableWal = !tc.startWal
		opt := RqliteConnOpt{Config: cfg}
		ctx := &rqliteTestContext{
			tb: t,
			r:  opt.MakeClient().(*rqlite.RQLite),
		}
		ctx.FlushDB()
		_ = ctx.r.Close()

		client := NewClient(opt)
		task := NewTask(
			"send_email",
			h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))
		info, err := client.Enqueue(task, TaskID(taskId), Queue(queue))
		require.NoError(t, err)

		assertSingleDbFile(cfg.Sqlite.DisableWal)
		err = client.Close()
		require.NoError(t, err)
		//fmt.Println("after close", "db files:", len(dbFiles()), "wal enabled", !cfg.SqliteDisableWall)

		cfg.Sqlite.DisableWal = !cfg.Sqlite.DisableWal
		client = NewClient(opt)
		inspect, err := NewInspectorClient(client)
		require.NoError(t, err)
		info2, err := inspect.GetTaskInfo(queue, taskId)
		require.NoError(t, err)

		assertSingleDbFile(cfg.Sqlite.DisableWal)
		err = client.Close()
		require.NoError(t, err)
		//fmt.Println("after close", "db files:", len(dbFiles()), "wal enabled", !cfg.SqliteDisableWall)

		// next_process_at is always now since the task is pending
		info.NextProcessAt = time.Time{}
		info2.NextProcessAt = time.Time{}
		require.Equal(t, info, info2)
	}
}

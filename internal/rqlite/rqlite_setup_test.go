package rqlite

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// variables used for package testing.
var (
	initBrokerOnce sync.Once
	sqliteDbTemp   bool
	config         Config
)

func init() {
	config.InitDefaults()
	flag.StringVar(&config.Type, "broker_type", "", "broker type to use in testing: rqlite | sqlite")
	flag.StringVar(&config.SqliteDbPath, "db_path", "", "sqlite DB path to use in testing")
	flag.StringVar(&config.RqliteUrl, "rqlite_url", "http://localhost:4001", "rqlite url to use in testing")
	flag.StringVar(&config.ConsistencyLevel, "consistency_level", "strong", "consistency level (rqlite)")
	flag.BoolVar(&config.SqliteInMemory, "sqlite_in_memory", false, "use in memory DB (sqlite)")
}

// defaultBrokerIfNotShort defines whether to use rqlite or sqlite when:
// - no type ws set on the config
// - testing.Short returns false (as the test script passes -short by default)
// This is useful when running tests from a developer integrated environment like
// Goland and avoid redefining test functions for each type.
func defaultBrokerIfNotShort(tb testing.TB) {
	if !testing.Short() && config.Type == "" {
		config.Type = sqliteType
	}
}

func skipUnknownBroker(tb testing.TB) {
	run := false
	switch config.Type {
	case rqliteType, sqliteType:
		run = true
	}
	if !run {
		tb.Skip(fmt.Sprintf("skipping test with broker type: [%s]", config.Type))
	}
}

func setup(tb testing.TB) *RQLite {
	defaultBrokerIfNotShort(tb)
	skipUnknownBroker(tb)

	initBrokerOnce.Do(func() {
		if config.Type == sqliteType {
			if config.SqliteDbPath == "" {
				if config.SqliteInMemory {
					config.SqliteDbPath = RandomInMemoryDbPath()
				} else {
					sqliteDbTemp = true
					db, err := os.CreateTemp("", "sqlite")
					require.NoError(tb, err)
					config.SqliteDbPath = db.Name()
				}
			}
			fmt.Println("sqlite db path:", config.SqliteDbPath)
			config.RqliteUrl = ""
		}
	})
	if sqliteDbTemp {
		tb.Cleanup(func() {
			_ = os.Remove(config.SqliteDbPath)
		})
	}

	tb.Helper()
	ret := NewRQLite(&config, nil, nil)
	err := ret.Open()
	if err != nil {
		tb.Fatal("Unable to connect rqlite", err)
	}
	FlushDB(tb, ret.conn)
	return ret
}

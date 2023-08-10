package rqlite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	types := []string{rqliteType, sqliteType}

	minConf := func(typ string, c *Config) {
		c.Type = typ
		switch c.Type {
		case rqliteType:
			c.Rqlite.Url = "https://1.2.3.4"
		case sqliteType:
			c.Sqlite.DbPath = "xyz"
		default:
			t.Fatalf("invalid type: %s", c.Type)
		}
	}

	for _, ty := range types {
		def := (&Config{}).InitDefaults()
		minConf(ty, def)
		err := def.Validate()
		require.NoError(t, err, "type: %s", ty)

		c := &Config{}
		minConf(ty, c)
		err = c.Validate()
		require.NoError(t, err, "type: %s", ty)

		// pub-sub is only assigned when using 'initDefaults'
		c.PubsubPollingInterval = PubsubPollingInterval
		require.Equal(t, def, c, "type: %s", ty)
	}
}

func TestValidConfig(t *testing.T) {
	type testCase struct {
		typ        string
		rqliteUrl  string
		sqlitePath string
		wantType   string
		wantFail   bool
	}

	for i, tc := range []*testCase{
		{typ: "", wantFail: true},
		{typ: "", rqliteUrl: "xyz", sqlitePath: "xyz", wantFail: true},
		{typ: "", rqliteUrl: "xyz", wantType: rqliteType},
		{typ: "", sqlitePath: "xyz", wantType: sqliteType},

		{typ: rqliteType, rqliteUrl: "", sqlitePath: "", wantFail: true},
		{typ: rqliteType, rqliteUrl: "", sqlitePath: "xyz", wantFail: true},
		{typ: rqliteType, rqliteUrl: "xyz", sqlitePath: "xyz", wantType: rqliteType},
		{typ: rqliteType, rqliteUrl: "xyz", sqlitePath: "", wantType: rqliteType},

		{typ: sqliteType, rqliteUrl: "", sqlitePath: "", wantFail: true},
		{typ: sqliteType, rqliteUrl: "xyz", sqlitePath: "", wantFail: true},
		{typ: sqliteType, rqliteUrl: "xyz", sqlitePath: "xyz", wantType: sqliteType},
		{typ: sqliteType, rqliteUrl: "", sqlitePath: "xyz", wantType: sqliteType},
	} {
		c := &Config{} // also works with (&Config{}).InitDefaults()
		c.Type = tc.typ
		c.Rqlite.Url = tc.rqliteUrl
		c.Sqlite.DbPath = tc.sqlitePath

		err := c.Validate()
		if tc.wantFail {
			require.Error(t, err, "case: %d", i)
			continue
		}
		require.NoError(t, err, "case: %d", i)
		require.Equal(t, tc.wantType, c.Type)
	}

}

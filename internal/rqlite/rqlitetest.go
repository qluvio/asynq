package rqlite

import (
	"testing"
)

// FlushDB cleans up db before each test case
func FlushDB(tb testing.TB, conn *Connection) {
	err := conn.DropTables()
	if err != nil {
		tb.Fatal("Unable to drop rqlite tables", err)
	}

	err = conn.CreateTables()
	if err != nil {
		tb.Fatal("Unable to create rqlite tables", err)
	}
	err = conn.CreateIndexes()
	if err != nil {
		tb.Fatal("Unable to create rqlite indexes", err)
	}

}

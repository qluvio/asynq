package rqlite

import (
	"testing"

	"github.com/rqlite/gorqlite"
)

// FlushDB cleans up db before each test case
func FlushDB(tb testing.TB, conn *gorqlite.Connection) {
	err := DropTables(conn)
	if err != nil {
		tb.Fatal("Unable to drop rqlite tables", err)
	}

	err = CreateTables(conn)
	if err != nil {
		tb.Fatal("Unable to create rqlite tables", err)
	}
}

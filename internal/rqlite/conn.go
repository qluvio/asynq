package rqlite

import (
	"context"

	"github.com/rqlite/gorqlite"
)

type Connection struct {
	*gorqlite.Connection
	TablesPrefix string
	tableNames   map[string]string
	tables       map[string]string
}

func NewConnection(conn *gorqlite.Connection, tablesPrefix string) *Connection {
	ret := &Connection{
		Connection:   conn,
		TablesPrefix: tablesPrefix,
	}
	ret.buildTables()
	return ret
}

func (conn *Connection) ctx() context.Context {
	return context.Background()
}

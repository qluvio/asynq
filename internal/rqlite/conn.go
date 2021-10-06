package rqlite

import (
	"context"

	"github.com/rqlite/gorqlite"
)

type Connection struct {
	*gorqlite.Connection
	config     *Config
	tableNames map[string]string
	tables     map[string]string
}

func NewConnection(conn *gorqlite.Connection, config *Config) *Connection {
	ret := &Connection{
		Connection: conn,
		config:     config,
	}
	ret.buildTables()
	return ret
}

func (conn *Connection) ctx() context.Context {
	return context.Background()
}

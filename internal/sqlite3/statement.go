package sqlite3

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hibiken/asynq/internal/errors"
)

// Statement enables use of parameterized sql statement.
// The constructor issues a warning if the number of parameters does not match
// the count of ? in the query.
// example:
//
//	x := NewStatement(
//	   "INSERT INTO Foo (id, name) VALUES ( ?, ? )",
//	   1,
//	   "bob")
//
// Statement is structurally identical to *gorqlite.Statement in order to be
// usable with the gorqlite library as well as the sqlite3 package.
type Statement struct {
	Sql        string
	Parameters []interface{}
	Warning    string // unused but left for compatibility with *gorqlite.Statement
}

func NewStatement(sql string, params ...interface{}) *Statement {
	return &Statement{
		Sql:        sql,
		Parameters: params,
	}
}

// Check returns an error is the count of parameters does not match the count of
// '?' in the SQL string.
func (s *Statement) Check() error {
	paramsCount := strings.Count(s.Sql, "?")
	if paramsCount != len(s.Parameters) {
		return errors.New(
			fmt.Sprintf("Unexpected parameters count: %d, expected: %d",
				len(s.Parameters),
				paramsCount))
	}
	return nil
}

// Append appends the given sql string and parameters to the current and returns
// the modified statement.
func (s *Statement) Append(sql string, params ...interface{}) *Statement {
	s.Sql += sql
	s.Parameters = append(s.Parameters, params...)
	return s
}

// String reconstructs the sql request without parsing (as best effort).
// Use it for debug.
func (s *Statement) String() string {
	sql := strings.ReplaceAll(s.Sql, "?", "%v")
	return fmt.Sprintf(sql, s.Parameters...)
}

func (s *Statement) MarshalJSON() ([]byte, error) {
	all := make([]interface{}, 0, len(s.Parameters)+1)
	all = append(append(all, s.Sql), s.Parameters...)
	return json.Marshal(all)
}

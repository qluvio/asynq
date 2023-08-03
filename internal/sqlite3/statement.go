package sqlite3

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hibiken/asynq/internal/errors"
)

// Statement enables use of parameterized sql statement.
// The Check func can be used to verify if the number of parameters matches the
// count of ? in the query.
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
	Query     string        // SQL statement
	Arguments []interface{} // arguments of the SQL statement
	Returning bool          // must be true if the SQL statement includes a RETURNING clause
}

func NewStatement(sql string, params ...interface{}) *Statement {
	return &Statement{
		Query:     sql,
		Arguments: params,
	}
}

func (s *Statement) WithReturning(b bool) *Statement {
	s.Returning = b
	return s
}

// Check returns an error if the count of parameters does not match the count of
// '?' in the SQL string.
func (s *Statement) Check() error {
	paramsCount := strings.Count(s.Query, "?")
	if paramsCount != len(s.Arguments) {
		return errors.New(
			fmt.Sprintf("Unexpected parameters count: %d, expected: %d",
				len(s.Arguments),
				paramsCount))
	}
	return nil
}

// Append appends the given sql string and parameters to the current and returns
// the modified statement.
func (s *Statement) Append(sql string, params ...interface{}) *Statement {
	s.Query += sql
	s.Arguments = append(s.Arguments, params...)
	return s
}

// String reconstructs the sql request without parsing (as best effort).
// Use it for debug.
func (s *Statement) String() string {
	sql := strings.ReplaceAll(s.Query, "?", "%v")
	params := make([]interface{}, 0, len(s.Arguments))
	for _, p := range s.Arguments {
		s, ok := p.(string)
		if ok {
			params = append(params, fmt.Sprintf("'%s'", s))
		} else {
			params = append(params, p)
		}
	}
	return fmt.Sprintf(sql, params...)
}

func (s *Statement) MarshalJSON() ([]byte, error) {
	length := len(s.Arguments) + 1
	if s.Returning {
		length++
	}
	all := make([]interface{}, 0, length)
	if s.Returning {
		all = append(all, true)
	}
	all = append(append(all, s.Query), s.Arguments...)
	return json.Marshal(all)
}

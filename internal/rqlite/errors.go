package rqlite

import (
	"fmt"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/rqlite/gorqlite"
)

type StatementError struct {
	Error     error       // specific error
	Statement interface{} // SQL statement if available
}

func (s StatementError) String() string {
	ret := fmt.Sprintf("%v", s.Error)
	if s.Statement != nil {
		ret += fmt.Sprintf(" %q", s.Statement)
	}
	return ret
}

// RqliteError indicates a command sent to rqlite returned error.
type RqliteError struct {
	Op         errors.Op
	Err        error            // outer most error
	Statements []StatementError // specific error
}

func (e *RqliteError) Error() string {
	ret := fmt.Sprintf("%s - rqlite error: %v ", e.Op, e.Err)
	if len(e.Statements) == 0 {
		return ret
	}
	if len(e.Statements) == 1 {
		return fmt.Sprintf("%s -> %s", ret, e.Statements[0])
	}
	for _, st := range e.Statements {
		ret = fmt.Sprintf("%s\n  %v", ret, st)
	}
	return ret
}

func (e *RqliteError) Unwrap() error { return e.Err }

func NewRqliteWError(op errors.Op, wr gorqlite.WriteResult, err error, stmt interface{}) error {
	return &RqliteError{
		Op:         op,
		Err:        err,
		Statements: []StatementError{{Error: wr.Err, Statement: stmt}},
	}
}

func NewRqliteWsError(op errors.Op, wrs []gorqlite.WriteResult, err error, stmts []*gorqlite.Statement) error {
	statements := make([]StatementError, 0)
	for ndx, wr := range wrs {
		if wr.Err != nil {
			statements = append(statements, StatementError{Error: wr.Err, Statement: stmts[ndx]})
		}
	}
	return &RqliteError{
		Op:         op,
		Err:        err,
		Statements: statements,
	}
}

func NewRqliteRError(op errors.Op, qr gorqlite.QueryResult, err error, stmt interface{}) error {
	return &RqliteError{
		Op:         op,
		Err:        err,
		Statements: []StatementError{{Error: qr.Err, Statement: stmt}},
	}
}

func NewRqliteRsError(op errors.Op, qrs []gorqlite.QueryResult, err error, stmts []*gorqlite.Statement) error {
	statements := make([]StatementError, 0)
	for ndx, qr := range qrs {
		if qr.Err != nil {
			statements = append(statements, StatementError{Error: qr.Err, Statement: stmts[ndx]})
		}
	}
	return &RqliteError{
		Op:         op,
		Err:        err,
		Statements: statements,
	}
}

// expectQueryResultCount returns an error if the expected count does not match
// with the returned result
func expectQueryResultCount(op errors.Op, expectedCount int, qrs []gorqlite.QueryResult) error {
	if len(qrs) != expectedCount {
		return errors.E(op, errors.Internal, fmt.Sprintf(
			"query result length (%d) does not match expected count (%d)",
			len(qrs),
			expectedCount))
	}
	return nil
}

// expectOneRowUpdated returns an error if the write-result indicates zero or
// more than one row was updated
func expectOneRowUpdated(op errors.Op, wr gorqlite.WriteResult, st interface{}) error {
	switch wr.RowsAffected {
	case 0:
		errStr := fmt.Sprintf("row not found (%v)", st)
		if wr.Err != nil {
			errStr = fmt.Sprintf("error (%v): %s", st, wr.Err.Error())
		}
		return errors.E(op, errors.NotFound, errStr)
	case 1:
	default:
		errStr := fmt.Sprintf("expected one row updated but have %d (%s)", wr.RowsAffected, st)
		if wr.Err != nil {
			errStr = fmt.Sprintf("expected one row updated but have %d (%s) - error: %s", wr.RowsAffected, st, wr.Err.Error())
		}
		return errors.E(op, errStr)
	}
	return nil
}

package sqlite3

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/sqlite3/encoding"
)

// RequestResult represents the result of a request.
// When Err is nil, only one of Write or Query will be non nil
type RequestResult struct {
	Err   error // don't trust the rest if this isn't nil
	Write WriteResult
	Query QueryResult
}

// QueryResult type holds the results of a call to Query() in a rowset manner.
// So if you were to query:
//
//	SELECT id, name FROM some_table;
//
// then a QueryResult would hold any errors from that query, a list of columns and types, and the actual row values.
type QueryResult interface {
	IsZero() bool
	Err() error
	Columns() []string
	Types() []string
	Next() bool
	NumRows() int64
	RowNumber() int64
	Scan(dest ...interface{}) error
	Map() (map[string]interface{}, error)
}

// WriteResult holds the result of a single statement sent to Write().
type WriteResult struct {
	Err          error   // don't trust the rest if this isn't nil
	Timing       float64 // request execution duration in seconds
	RowsAffected int64   // affected by the change
	LastInsertID int64   // if relevant, otherwise zero value
}

func (w *WriteResult) IsZero() bool {
	return w.Timing == 0 && w.RowsAffected == 0 && w.LastInsertID == 0 && w.Err == nil
}

var _ QueryResult = (*queryResult)(nil)

type queryResult struct {
	*encoding.Rows
	rowNumber int64
	err       error
}

func newQueryResult(rows *encoding.Rows) QueryResult {
	var err error
	if rows.Error != "" {
		err = errors.New(rows.Error)
	}
	return &queryResult{
		Rows:      rows,
		rowNumber: -1,
		err:       err,
	}
}

func (qr *queryResult) IsZero() bool {
	return qr.err == nil && (qr.Rows == nil ||
		(len(qr.Columns()) == 0 && len(qr.Types()) == 0 && qr.NumRows() == 0))
}

func (qr *queryResult) Err() error {
	return qr.err
}

func (qr *queryResult) Columns() []string {
	return qr.Rows.Columns
}

func (qr *queryResult) Types() []string {
	return qr.Rows.Types
}

func (qr *queryResult) NumRows() int64 {
	return int64(len(qr.Rows.Values))
}

func (qr *queryResult) Next() bool {
	if qr.rowNumber >= int64(len(qr.Rows.Values)-1) {
		return false
	}

	qr.rowNumber += 1
	return true
}

func (qr *queryResult) RowNumber() int64 {
	return qr.rowNumber
}

func trace(format string, args ...interface{}) {
	//fmt.Println(fmt.Sprintf(format, args...))
}

func (qr *queryResult) Scan(dest ...interface{}) error {
	trace("scan() called for %d vars", len(dest))

	if qr.rowNumber == -1 {
		return errors.New("Next must be called before Scan")
	}

	if len(dest) != len(qr.Rows.Columns) {
		return errors.New(fmt.Sprintf("expected %d columns but got %d vars\n", len(qr.Rows.Columns), len(dest)))
	}

	thisRowValues := qr.Rows.Values[qr.rowNumber] //.([]interface{})
	for n, d := range dest {
		src := thisRowValues[n]
		if src == nil {
			trace("skipping nil scan data for variable #%d (%s)", n, qr.Rows.Columns[n])
			continue
		}
		switch d.(type) {
		case *time.Time:
			if src == nil {
				continue
			}
			t, err := toTime(src)
			if err != nil {
				return fmt.Errorf("%v: bad time col:(%d/%s) val:%v", err, n, qr.Columns()[n], src)
			}
			*d.(*time.Time) = t
		case *int:
			switch src := src.(type) {
			case float64:
				*d.(*int) = int(src)
			case int64:
				*d.(*int) = int(src)
			case string:
				i, err := strconv.Atoi(src)
				if err != nil {
					return err
				}
				*d.(*int) = i
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *bool:
			switch src := src.(type) {
			case bool:
				*d.(*bool) = src
			case string:
				b, err := strconv.ParseBool(src)
				if err != nil {
					return err
				}
				*d.(*bool) = b
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *int64:
			switch src := src.(type) {
			case float64:
				*d.(*int64) = int64(src)
			case int64:
				*d.(*int64) = src
			case string:
				i, err := strconv.ParseInt(src, 10, 64)
				if err != nil {
					return err
				}
				*d.(*int64) = i
			default:
				return fmt.Errorf("invalid int64 col:%d type:%T val:%v", n, src, src)
			}
		case *float64:
			*d.(*float64) = float64(src.(float64))
		case *string:
			switch src := src.(type) {
			case string:
				*d.(*string) = src
			default:
				return fmt.Errorf("invalid string col:%d type:%T val:%v", n, src, src)
			}
		default:
			return fmt.Errorf("unknown destination type (%T) to scan into in variable #%d", d, n)
		}
	}

	return nil
}

// Map returns the current row (as advanced by Next()) as a map[string]interface{}
// The key is a string corresponding to a column name.
// The value is the corresponding column.
func (qr *queryResult) Map() (map[string]interface{}, error) {
	trace("Map() called for row %d", qr.rowNumber)
	ans := make(map[string]interface{})

	if qr.rowNumber == -1 {
		return ans, errors.New("Next must be called before Map")
	}

	thisRowValues := qr.Rows.Values[qr.rowNumber] //.([]interface{})
	for i := 0; i < len(qr.Rows.Columns); i++ {
		// - creating a table with column 'ts DATETIME DEFAULT CURRENT_TIMESTAMP'
		//   makes it be always nil (affinity is NUMERIC - see: https://www.sqlite.org/datatype3.html)
		// - using 'ts INT_DATETIME DEFAULT CURRENT_TIMESTAMP' makes it work...
		// This used to work though - see comment in TestQueries
		if strings.Contains(qr.Rows.Types[i], "date") || strings.Contains(qr.Rows.Types[i], "time") {
			//case "date", "datetime":
			if thisRowValues[i] != nil {
				t, err := toTime(thisRowValues[i])
				if err != nil {
					return ans, err
				}
				ans[qr.Rows.Columns[i]] = t
			} else {
				ans[qr.Rows.Columns[i]] = nil
			}
		} else {
			ans[qr.Rows.Columns[i]] = thisRowValues[i]
		}
	}

	return ans, nil
}

func toTime(src interface{}) (time.Time, error) {
	switch src := src.(type) {
	case string:
		const layout = "2006-01-02 15:04:05"
		if t, err := time.Parse(layout, src); err == nil {
			return t, nil
		}
		return time.Parse(time.RFC3339, src)
	case float64:
		return time.Unix(int64(src), 0), nil
	case int64:
		return time.Unix(src, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time type:%T val:%v", src, src)
}

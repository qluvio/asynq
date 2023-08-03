package command

import (
	"encoding/json"
)

type Parameter struct {
	Value interface{}
	Name  string
}

func (x *Parameter) String() string {
	return stringOf(x)
}

func (x *Parameter) GetValue() interface{} {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Parameter) GetInt64() int64 {
	if x, ok := x.GetValue().(int64); ok {
		return x
	}
	return 0
}

func (x *Parameter) GetDouble() float64 {
	if x, ok := x.GetValue().(float64); ok {
		return x
	}
	return 0
}

func (x *Parameter) GetBool() bool {
	if x, ok := x.GetValue().(bool); ok {
		return x
	}
	return false
}

func (x *Parameter) GetBytes() []byte {
	if x, ok := x.GetValue().([]byte); ok {
		return x
	}
	return nil
}

func (x *Parameter) GetString() string {
	if x, ok := x.GetValue().(string); ok {
		return x
	}
	return ""
}

func (x *Parameter) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

type Statement struct {
	Sql        string       `json:"sql"`
	Parameters []*Parameter `json:"parameters"`
	Returning  bool         `json:"returning,omitempty"`
}

func (x *Statement) String() string {
	return stringOf(x)
}

func (x *Statement) GetSql() string {
	if x != nil {
		return x.Sql
	}
	return ""
}

func (x *Statement) GetParameters() []*Parameter {
	if x != nil {
		return x.Parameters
	}
	return nil
}

type Request struct {
	Transaction bool         `json:"transaction"`
	Statements  []*Statement `json:"statements"`
}

func (x *Request) String() string {
	return stringOf(x)
}

func (x *Request) GetTransaction() bool {
	if x != nil {
		return x.Transaction
	}
	return false
}

func (x *Request) GetStatements() []*Statement {
	if x != nil {
		return x.Statements
	}
	return nil
}

type QueryRequest struct {
	Request   *Request `json:"request"`
	Timings   bool     `json:"timings"`
	Freshness int64    `json:"freshness"`
}

func (x *QueryRequest) String() string {
	return stringOf(x)
}

func (x *QueryRequest) GetRequest() *Request {
	if x != nil {
		return x.Request
	}
	return nil
}

func (x *QueryRequest) GetTimings() bool {
	if x != nil {
		return x.Timings
	}
	return false
}

func (x *QueryRequest) GetFreshness() int64 {
	if x != nil {
		return x.Freshness
	}
	return 0
}

type Values struct {
	Parameters []*Parameter `json:"parameters"`
}

func (x *Values) String() string {
	return stringOf(x)
}

func (x *Values) GetParameters() []*Parameter {
	if x != nil {
		return x.Parameters
	}
	return nil
}

type QueryRows struct {
	Columns []string  `json:"columns,omitempty"`
	Types   []string  `json:"types,omitempty"`
	Values  []*Values `json:"values,omitempty"`
	Error   string    `json:"error,omitempty"`
	Time    float64   `json:"time,omitempty"`
}

func (x *QueryRows) String() string {
	return stringOf(x)
}

func (x *QueryRows) GetColumns() []string {
	if x != nil {
		return x.Columns
	}
	return nil
}

func (x *QueryRows) GetTypes() []string {
	if x != nil {
		return x.Types
	}
	return nil
}

func (x *QueryRows) GetValues() []*Values {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *QueryRows) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *QueryRows) GetTime() float64 {
	if x != nil {
		return x.Time
	}
	return 0
}

type ExecuteRequest struct {
	Request *Request `json:"request,omitempty"`
	Timings bool     `json:"timings,omitempty"`
}

func (x *ExecuteRequest) String() string {
	return stringOf(x)
}

func (x *ExecuteRequest) GetRequest() *Request {
	if x != nil {
		return x.Request
	}
	return nil
}

func (x *ExecuteRequest) GetTimings() bool {
	if x != nil {
		return x.Timings
	}
	return false
}

type ExecuteResult struct {
	LastInsertId int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

func (x *ExecuteResult) String() string {
	return stringOf(x)
}

func (x *ExecuteResult) GetLastInsertId() int64 {
	if x != nil {
		return x.LastInsertId
	}
	return 0
}

func (x *ExecuteResult) GetRowsAffected() int64 {
	if x != nil {
		return x.RowsAffected
	}
	return 0
}

func (x *ExecuteResult) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *ExecuteResult) GetTime() float64 {
	if x != nil {
		return x.Time
	}
	return 0
}

type ExecuteQueryRequest struct {
	Request   *Request `json:"request,omitempty"`
	Timings   bool     `json:"timings,omitempty"`
	Freshness int64    `json:"freshness,omitempty"`
}

func (x *ExecuteQueryRequest) String() string {
	return stringOf(x)
}

func (x *ExecuteQueryRequest) GetRequest() *Request {
	if x != nil {
		return x.Request
	}
	return nil
}

func (x *ExecuteQueryRequest) GetTimings() bool {
	if x != nil {
		return x.Timings
	}
	return false
}

func (x *ExecuteQueryRequest) GetFreshness() int64 {
	if x != nil {
		return x.Freshness
	}
	return 0
}

type ExecuteQueryResponse struct {
	// Types that are assignable to Result:
	//
	//	*ExecuteQueryResponse_Q
	//	*ExecuteQueryResponse_E
	//	*ExecuteQueryResponse_Error
	Result isExecuteQueryResponse_Result `json:"result"`
}

func (x *ExecuteQueryResponse) String() string {
	return stringOf(x)
}

func (x *ExecuteQueryResponse) GetResult() isExecuteQueryResponse_Result {
	if x != nil {
		return x.Result
	}
	return nil
}

func (x *ExecuteQueryResponse) GetQ() *QueryRows {
	if x, ok := x.GetResult().(*ExecuteQueryResponse_Q); ok {
		return x.Q
	}
	return nil
}

func (x *ExecuteQueryResponse) GetE() *ExecuteResult {
	if x, ok := x.GetResult().(*ExecuteQueryResponse_E); ok {
		return x.E
	}
	return nil
}

func (x *ExecuteQueryResponse) GetError() string {
	if x, ok := x.GetResult().(*ExecuteQueryResponse_Error); ok {
		return x.Error
	}
	return ""
}

type isExecuteQueryResponse_Result interface {
	isExecuteQueryResponse_Result()
}

type ExecuteQueryResponse_Q struct {
	Q *QueryRows `json:"q"`
}

type ExecuteQueryResponse_E struct {
	E *ExecuteResult `json:"e"`
}

type ExecuteQueryResponse_Error struct {
	Error string `json:"error"`
}

func (*ExecuteQueryResponse_Q) isExecuteQueryResponse_Result() {}

func (*ExecuteQueryResponse_E) isExecuteQueryResponse_Result() {}

func (*ExecuteQueryResponse_Error) isExecuteQueryResponse_Result() {}

type Noop struct {
	Id string `json:"id,omitempty"`
}

func (x *Noop) String() string {
	return stringOf(x)
}

func (x *Noop) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type Command struct {
	Type       int    `json:"type,omitempty"` //Command_Type
	SubCommand []byte `json:"sub_command,omitempty"`
	Compressed bool   `json:"compressed,omitempty"`
}

func (x *Command) String() string {
	return stringOf(x)
}

func (x *Command) GetSubCommand() []byte {
	if x != nil {
		return x.SubCommand
	}
	return nil
}

func (x *Command) GetCompressed() bool {
	if x != nil {
		return x.Compressed
	}
	return false
}

func stringOf(x interface{}) string {
	bb, err := json.Marshal(x)
	if err != nil {
		return err.Error()
	}
	return string(bb)
}

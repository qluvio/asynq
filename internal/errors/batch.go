package errors

import (
	"bytes"
	"strconv"
)

// BatchError is returned when an error is found in processing a list of tasks
type BatchError struct {
	Errors map[int]error // map index of failed tasks in the input to error
}

func (e *BatchError) MapErrors() map[int]error {
	return e.Errors
}

func (e *BatchError) Error() string {
	if len(e.Errors) == 0 {
		return ""
	}
	// sb := strings.Builder{}
	sb := bytes.Buffer{}
	sb.WriteString("error-list ")
	sb.WriteString("count [")
	sb.WriteString(strconv.Itoa(len(e.Errors)))
	sb.WriteString("]\n")
	for idx, err := range e.Errors {
		s := err.Error()
		sb.WriteString("\t")
		sb.WriteString(strconv.Itoa(idx))
		sb.WriteString(": ")
		sb.WriteString(s)
		sb.WriteString("\n")
	}
	return sb.String()
}

func (e *BatchError) Unwrap() error {
	if e == nil || len(e.Errors) == 0 {
		return nil
	}
	for _, ex := range e.Errors {
		return ex
	}
	return nil
}

package asynq

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorMuxDefault(t *testing.T) {
	{
		mux := NewErrorMux(nil)
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		mux.HandleError(context.Background(), task, io.EOF)
	}
	{
		var fn func(ctx context.Context, task *Task, err error)
		mux := NewErrorMux(ErrorHandlerFunc(fn))
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		mux.HandleError(context.Background(), task, io.EOF)
	}
	{
		var rcv error
		fn := func(ctx context.Context, task *Task, err error) { rcv = err }
		mux := NewErrorMux(ErrorHandlerFunc(fn))
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		mux.HandleError(context.Background(), task, io.EOF)
		require.Equal(t, io.EOF, rcv)
	}
}

func TestErrorMux(t *testing.T) {

	called := map[string]string{}
	makeHandler := func(s string) func(ctx context.Context, task *Task, err error) {
		return func(ctx context.Context, task *Task, err error) {
			called[task.typename] = s
		}
	}

	errMuxes := []struct {
		pattern string
		h       ErrorHandlerFunc
	}{
		{"email:", makeHandler("default email handler")},
		{"email:signup", makeHandler("signup email handler")},
		{"csv:export", makeHandler("csv export handler")},
	}

	mux := NewErrorMux(nil)
	for _, e := range errMuxes {
		mux.HandleFunc(e.pattern, e.h)
	}

	for _, tc := range []string{
		"email:signup",
		"csv:export",
		"email:daily",
	} {
		task := NewTask(tc, nil)
		mux.HandleError(context.Background(), task, io.EOF)
	}

	expected := map[string]string{
		"csv:export":   "csv export handler",
		"email:daily":  "default email handler",
		"email:signup": "signup email handler",
	}
	require.Equal(t, expected, called)
}

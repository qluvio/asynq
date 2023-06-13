package asynq

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryMuxDefault(t *testing.T) {
	{
		mux := NewRetryMux(nil)
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		d := mux.RetryDelay(1, io.EOF, task)
		require.True(t, d > 0)
	}
	{
		var fn func(n int, e error, t *Task) time.Duration
		mux := NewRetryMux(RetryDelayFunc(fn))
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		d := mux.RetryDelay(1, io.EOF, task)
		require.True(t, d > 0)
	}
	{
		var rcv error
		fn := func(n int, e error, t *Task) time.Duration {
			rcv = e
			return time.Millisecond
		}
		mux := NewRetryMux(RetryDelayFunc(fn))
		task := NewTask("test", nil)
		h, _ := mux.Handler(task)
		require.NotNil(t, h)
		d := mux.RetryDelay(1, io.EOF, task)
		require.Equal(t, io.EOF, rcv)
		require.Equal(t, time.Millisecond, d)
	}
}

func TestRetryMux(t *testing.T) {

	called := map[string]time.Duration{}
	makeHandler := func(s string, d time.Duration) func(n int, e error, t *Task) time.Duration {
		return func(n int, e error, task *Task) time.Duration {
			called[task.typename] = d
			return d
		}
	}

	retryMuxes := []struct {
		pattern string
		h       RetryDelayFunc
	}{
		{"email:", makeHandler("default email handler", time.Millisecond)},
		{"email:signup", makeHandler("signup email handler", time.Millisecond*2)},
		{"csv:export", makeHandler("csv export handler", time.Millisecond*3)},
	}

	mux := NewRetryMux(nil)
	for _, e := range retryMuxes {
		mux.HandleFunc(e.pattern, e.h)
	}

	for _, tc := range []string{
		"email:signup",
		"csv:export",
		"email:daily",
	} {
		task := NewTask(tc, nil)
		d := mux.RetryDelay(1, io.EOF, task)
		require.True(t, d > 0)
	}

	expected := map[string]time.Duration{
		"csv:export":   time.Millisecond * 3,
		"email:daily":  time.Millisecond,
		"email:signup": time.Millisecond * 2,
	}
	require.Equal(t, expected, called)
}

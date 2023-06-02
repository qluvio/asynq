package asynq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	nilPrinter := &Printer{path: "nil"}
	type testCase struct {
		path     string
		expected string
	}

	tm := NewTaskMatcher[*Printer]()
	handle := func(s string) {
		tm.Handle(s, &Printer{path: s})
	}
	handle("/")
	handle("/foo/bar")
	handle("/foo")

	for _, tc := range []*testCase{
		{path: "/", expected: "/"},
		{path: "/foo", expected: "/foo"},
		{path: "/foo/bar", expected: "/foo/bar"},
		{path: "/foobar", expected: "/foo"},
		{path: "/xyz", expected: "/"},
		{path: "xyz", expected: "nil"},
	} {
		p, _ := tm.Handler(&Task{typename: tc.path}, func() *Printer { return nilPrinter })
		require.Equal(t, tc.expected, p.path)
	}

}

type Printer struct {
	path string
}

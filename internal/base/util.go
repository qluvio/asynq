package base

import "reflect"

// credit: https://github.com/eapache/channels/

// BufferCap represents the capacity of the buffer backing a channel. Valid values consist of all
// positive integers, as well as the special values below.
type BufferCap int

const (
	// None is the capacity for channels that have no buffer at all.
	None BufferCap = 0
	// Infinity is the capacity for channels with no limit on their buffer size.
	Infinity BufferCap = -1
)

// SimpleOutChannel is an interface representing a readable channel that does not necessarily
// implement the Buffer interface.
type SimpleOutChannel interface {
	Channel() <-chan interface{} // The readable end of the channel.
}

// NativeOutChannel implements the OutChannel interface by wrapping a native go read-only channel.
type NativeOutChannel <-chan interface{}

func (ch NativeOutChannel) Channel() <-chan interface{} {
	return ch
}

func (ch NativeOutChannel) Len() int {
	return len(ch)
}

func (ch NativeOutChannel) Cap() BufferCap {
	return BufferCap(cap(ch))
}

// Wrap takes any readable channel type (chan or <-chan but not chan<-) and
// exposes it as a SimpleOutChannel for easy integration with existing channel sources.
// It panics if the input is not a readable channel.
func Wrap(ch interface{}, transform func(x interface{}) interface{}) SimpleOutChannel {
	t := reflect.TypeOf(ch)
	if t.Kind() != reflect.Chan || t.ChanDir()&reflect.RecvDir == 0 {
		panic("channels: input to Wrap must be readable channel")
	}
	realChan := make(chan interface{})

	go func() {
		v := reflect.ValueOf(ch)
		for {
			x, ok := v.Recv()
			if !ok {
				close(realChan)
				return
			}
			rcv := x.Interface()
			if transform != nil {
				rcv = transform(rcv)
			}
			realChan <- rcv
		}
	}()

	return NativeOutChannel(realChan)
}

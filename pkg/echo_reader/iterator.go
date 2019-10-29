package echo_reader

import (
	"fmt"
	"io"

	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	iterator   types.EventIterator
	event      *types.Event
	err        error
	writer io.Writer
	prefix string
	suffix string
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(eventIterator types.EventIterator, w io.Writer) *EventIterator {
	return &EventIterator{
		iterator:   eventIterator,
		writer: w,
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.iterator.Next() {
		ei.err = ei.iterator.Err()
		return false
	}

	ei.event = ei.iterator.At()
	fmt.Fprintf(ei.writer, "%s%s%s", ei.prefix, ei.event.Message, ei.suffix)

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}

func (er *EventIterator) SetPrefix(prefix string) *EventIterator {
	er.prefix = prefix
	return er
}

func (er *EventIterator) SetSuffix(suffix string) *EventIterator {
	er.suffix = suffix
	return er
}

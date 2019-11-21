package scanner

import (
	"bufio"
	"io"

	"github.com/anchorfree/data-go/pkg.v2/types"
)

type EventIterator struct {
	topic                 string
	event                 *types.Event
	err                   error
	scanner               *bufio.Scanner
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(inp io.Reader, topic string) *EventIterator {
	return &EventIterator{
		topic: topic,
		scanner: bufio.NewScanner(inp),
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.scanner.Scan() {
		ei.err = ei.scanner.Err()
		return false
	}

	ei.event = types.NewEvent(ei.topic, ei.scanner.Bytes())

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}
package metricbuilder

import (
	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	iterator types.EventIterator
	event    *types.Event
	err      error
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(eventIterator types.EventIterator) *EventIterator {
	return &EventIterator{
		iterator: eventIterator,
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.iterator.Next() {
		ei.err = ei.iterator.Err()
		return false
	}

	ei.event = ei.iterator.At()

	updateMetric(
		appendTopicToMessage(ei.event.Message, ei.event.Topic),
		ei.event.Topic,
	)

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}

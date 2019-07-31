package metricbuilder

import (
	"github.com/anchorfree/data-go/pkg/event"
)

type EventReader struct {
	eventReader event.Reader
	lineReader  *Reader
}

var _ event.Reader = (*EventReader)(nil)

func NewEventReader(eventReader event.Reader) *EventReader {
	return &EventReader{
		eventReader: eventReader,
		lineReader:  NewReader(nil, ""),
	}
}

func (er *EventReader) ReadEvent() *event.Event {
	event := er.eventReader.ReadEvent()
	const maxReplacements = 1
	updateMetric(
		appendTopicToMessage(event.Message, event.Topic),
		event.Topic,
	)
	return event
}

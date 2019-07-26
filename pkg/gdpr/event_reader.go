package gdpr

import (
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/event"
)

type EventReader struct {
	lineReader  *Reader
	eventReader event.Reader
}

var _ event.Reader = (*EventReader)(nil)

func NewEventReader(er event.Reader, geoSet *geo.Geo) *EventReader {
	return &EventReader{
		eventReader: er,
		lineReader:  &Reader{geoSet: geoSet},
	}
}

func (er *EventReader) ReadEvent() *event.Event {
	event := er.eventReader.ReadEvent()
	event.Message = er.lineReader.ApplyGDPR(event.Message)
	return event
}

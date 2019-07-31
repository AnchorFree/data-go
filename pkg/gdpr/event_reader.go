package gdpr

import (
	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/geo"
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
	eventEntry := er.eventReader.ReadEvent()
	eventEntry.Message = er.lineReader.ApplyGDPR(eventEntry.Message)
	return eventEntry
}

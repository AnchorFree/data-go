package echo_reader

import (
	"io"
	"fmt"

	"github.com/anchorfree/data-go/pkg/event"
)

type EventReader struct {
	eventReader event.Reader
	lineReader  *EchoReader
}

var _ event.Reader = (*EventReader)(nil)

func NewEventReader(eventReader event.Reader, w io.Writer) *EventReader {
	return &EventReader{
		eventReader: eventReader,
		lineReader: NewEchoReader(nil, w),
	}
}

func (er *EventReader) SetPrefix(prefix string) *EventReader {
	er.lineReader.Prefix = prefix
	return er
}

func (er *EventReader) SetSuffix(suffix string) *EventReader {
	er.lineReader.Suffix = suffix
	return er
}

func (er *EventReader) ReadEvent() *event.Event {
	eventEntry := er.eventReader.ReadEvent()
	fmt.Fprintf(er.lineReader.writer, "%s%s%s", er.lineReader.Prefix, eventEntry.Message, er.lineReader.Suffix)
	return eventEntry
}

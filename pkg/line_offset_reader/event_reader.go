package line_offset_reader

import (
	"io"

	"github.com/anchorfree/data-go/pkg/event"
)

type EventReader struct {
	lineReader *LineOffsetReader
	topic      string
}

var _ event.Reader = (*EventReader)(nil)

func NewEventReader(inp io.Reader, topic string) *EventReader {
	return &EventReader{
		lineReader: NewReader(inp),
		topic:      topic,
	}
}

func (er *EventReader) SetLookForJsonDelimiters(flag bool) *EventReader {
	er.lineReader.LookForJsonDelimiters = flag
	return er
}

func (er *EventReader) BytesRead() int64 {
	return er.lineReader.bytesRead
}

func (er *EventReader) LinesRead() int64 {
	return er.lineReader.linesRead
}

func (er *EventReader) ReadEvent() *event.Event {
	message, offset, err := er.lineReader.ReadLine()

	return &event.Event{
		Topic:   er.topic,
		Message: message,
		Offset:  offset,
		Error:   err,
		Type:    event.TypeUnknown,
	}
}
